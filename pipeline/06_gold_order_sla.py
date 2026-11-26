# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Order delivery SLA
# MAGIC Turns the silver event stream (many rows per order) into one row per order with stage
# MAGIC durations and an SLA verdict (placed → delivered ≤ 45 min), then enforces order-level
# MAGIC data quality.
# MAGIC
# MAGIC * `gold.fact_order_sla` — one row per order, **incrementally upserted with Delta MERGE**
# MAGIC   (an order's row is updated as its later events arrive).
# MAGIC * `gold.order_sla_dq_failures` — audit log of orders that failed an order-level check.
# MAGIC * `gold.agg_sla_by_hour_city` — hourly SLA rollup for dashboards / Genie.

# COMMAND ----------

import os, sys

dbutils.widgets.text("catalog", "uber_eats_lakehouse")
catalog = dbutils.widgets.get("catalog")
spark.sql(f"USE CATALOG {catalog}")

_nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
sys.path.append("/Workspace" + os.path.dirname(os.path.dirname(_nb)))

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.dq_utils import apply_dq_checks
from src.event_utils import build_order_sla, sla_dq_checks, SLA_MINUTES

SILVER = f"{catalog}.silver.order_events"
FACT = f"{catalog}.gold.fact_order_sla"
FAILURES = f"{catalog}.gold.order_sla_dq_failures"
AGG = f"{catalog}.gold.agg_sla_by_hour_city"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Find orders touched since the last run (incremental)
# MAGIC Only orders that received new events are recomputed; each is rebuilt from **all** of its events.

# COMMAND ----------

events = spark.table(SILVER)

if spark.catalog.tableExists(FACT):
    last = spark.table(FACT).agg(F.max("last_processed_at")).collect()[0][0]
    if last is not None:
        touched = events.filter(F.col("_silver_processed_at") > F.lit(last)).select("order_id").distinct()
        events = events.join(touched, "order_id", "inner")

print(f"Events to (re)compute: {events.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Build SLA rows + enrich with the latest merchant version
# MAGIC Streaming orders are newer than the batch dimension snapshots, so they join to each
# MAGIC merchant's **latest** version (one row per merchant, guaranteed by `row_number`).

# COMMAND ----------

sla = build_order_sla(events, sla_minutes=SLA_MINUTES)

w = Window.partitionBy("merchant_id").orderBy(F.col("effective_start_date").desc())
latest_merchant = (
    spark.table(f"{catalog}.silver.dim_merchant")
    .withColumn("_rn", F.row_number().over(w))
    .filter("_rn = 1")
    .select(
        "merchant_id",
        F.col("status").alias("merchant_status"),
        F.col("cuisine_category_id").alias("merchant_cuisine_category_id"),
    )
)
sla = sla.join(latest_merchant, "merchant_id", "left")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data quality split → MERGE clean rows, log failures

# COMMAND ----------

clean, failed = apply_dq_checks(sla, sla_dq_checks())

if not spark.catalog.tableExists(FACT):
    clean.write.format("delta").mode("overwrite").clusterBy("placed_date", "city").saveAsTable(FACT)
    print("Created fact_order_sla")
else:
    (DeltaTable.forName(spark, FACT).alias("t")
        .merge(clean.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

(failed.withColumn("_dq_run_at", F.current_timestamp())
    .write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(FAILURES))

print(f"clean orders merged: {clean.count()}, failed DQ: {failed.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Hourly SLA rollup

# COMMAND ----------

agg = (
    spark.table(FACT)
    .filter("order_status = 'Delivered'")
    .groupBy(F.date_trunc("hour", "placed_ts").alias("placed_hour"), "city")
    .agg(
        F.count("*").alias("delivered_orders"),
        F.sum(F.col("sla_met").cast("int")).alias("sla_met_orders"),
        F.round(1 - F.avg(F.col("sla_met").cast("double")), 4).alias("sla_breach_rate"),
        F.round(F.avg("minutes_to_deliver"), 2).alias("avg_minutes_to_deliver"),
        F.round(F.percentile_approx("minutes_to_deliver", 0.95), 2).alias("p95_minutes_to_deliver"),
    )
)
agg.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(AGG)

# COMMAND ----------

display(spark.sql(f"""
    SELECT order_status, COUNT(*) AS orders,
           ROUND(AVG(minutes_to_deliver), 1) AS avg_minutes,
           ROUND(100 * AVG(CASE WHEN sla_met THEN 1 ELSE 0 END), 1) AS pct_sla_met
    FROM {FACT} GROUP BY order_status
"""))
display(spark.sql(f"SELECT explode(_dq_failures) AS failed_check, COUNT(*) AS n FROM {FAILURES} GROUP BY 1"))
