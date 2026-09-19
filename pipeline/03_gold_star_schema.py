# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Star Schema + Optimization
# MAGIC Builds curated fact tables joined against SCD2 dimensions using
# MAGIC **as-of joins** — every fact row is enriched with the dimension
# MAGIC attributes that were *true at the time of the event*, not today's
# MAGIC current values. This is the whole point of doing SCD2 in the first
# MAGIC place: a suspended merchant's historical orders still show the
# MAGIC merchant as "Active" if that's what was true when the order happened.

# COMMAND ----------

dbutils.widgets.text("catalog", "uber_eats_lakehouse")
catalog = dbutils.widgets.get("catalog")
spark.sql(f"USE CATALOG {catalog}")

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## As-of join helper
# MAGIC Joins a fact table to an SCD2 dimension on business key + the fact's
# MAGIC event date falling inside the dimension row's [effective_start_date,
# MAGIC effective_end_date) window.

# COMMAND ----------

def as_of_join(fact_df, dim_table, business_key, fact_date_col, select_cols):
    dim_df = spark.table(f"{catalog}.silver.{dim_table}")

    joined = fact_df.join(
        dim_df,
        (fact_df[business_key] == dim_df[business_key])
        & (fact_df[fact_date_col] >= dim_df["effective_start_date"])
        & (
            dim_df["effective_end_date"].isNull()
            | (fact_df[fact_date_col] < dim_df["effective_end_date"])
        ),
        "left",
    )
    return joined.select(*select_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_orders — the central fact table

# COMMAND ----------

orders = spark.table(f"{catalog}.silver.orders_clean").withColumn(
    "order_date", F.to_date("order_datetime")
)

orders_with_merchant = as_of_join(
    orders,
    "dim_merchant",
    business_key="merchant_id",
    fact_date_col="order_date",
    select_cols=[
        orders["*"],
        F.col("dim_merchant.status").alias("merchant_status_at_order_time"),
        F.col("dim_merchant.rating").alias("merchant_rating_at_order_time"),
        F.col("dim_merchant.cuisine_category_id"),
        F.col("dim_merchant.location_id").alias("merchant_location_id"),
    ],
)

fact_orders = orders_with_merchant.withColumnRenamed(
    "merchant_id", "merchant_key"
).withColumnRenamed("customer_id", "customer_key").withColumnRenamed(
    "courier_id", "courier_key"
)

(
    fact_orders.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .clusterBy("order_date", "merchant_key")  # liquid clustering
    .saveAsTable(f"{catalog}.gold.fact_orders")
)
print(f"fact_orders: {fact_orders.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_payments

# COMMAND ----------

payments = spark.table(f"{catalog}.silver.payments_clean")
(
    payments.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .clusterBy("order_id")
    .saveAsTable(f"{catalog}.gold.fact_payments")
)
print(f"fact_payments: {payments.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_refunds

# COMMAND ----------

refunds = spark.table(f"{catalog}.silver.refunds_clean")
(
    refunds.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.gold.fact_refunds")
)
print(f"fact_refunds: {refunds.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_delivery_sla — enriched with courier status as-of delivery date

# COMMAND ----------

delivery = spark.table(f"{catalog}.silver.delivery_events_clean").withColumn(
    "event_date", F.to_date("snapshot_date")
)

delivery_with_courier = as_of_join(
    delivery,
    "dim_courier",
    business_key="courier_id",
    fact_date_col="event_date",
    select_cols=[
        delivery["*"],
        F.col("dim_courier.status").alias("courier_status_at_delivery_time"),
        F.col("dim_courier.vehicle_type"),
    ],
)

(
    delivery_with_courier.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.gold.fact_delivery_sla")
)
print(f"fact_delivery_sla: {delivery_with_courier.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_support_tickets

# COMMAND ----------

tickets = spark.table(f"{catalog}.silver.support_tickets_clean")
(
    tickets.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.gold.fact_support_tickets")
)
print(f"fact_support_tickets: {tickets.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension tables (current-view convenience versions for gold)

# COMMAND ----------

for dim in ["dim_merchant", "dim_courier", "dim_customer"]:
    spark.table(f"{catalog}.silver.{dim}").write.format("delta").mode(
        "overwrite"
    ).option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.{dim}")

spark.table(f"{catalog}.silver.dim_location").write.format("delta").mode(
    "overwrite"
).option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.dim_location")

spark.table(f"{catalog}.silver.dim_cuisine_category").write.format("delta").mode(
    "overwrite"
).option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.dim_cuisine_category")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enable Predictive Optimization on the gold schema
# MAGIC Lets Databricks auto-manage OPTIMIZE/VACUUM based on query patterns.

# COMMAND ----------

spark.sql(f"ALTER SCHEMA {catalog}.gold ENABLE PREDICTIVE OPTIMIZATION")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick validation query — the payoff of SCD2 done right
# MAGIC Orders from suspended merchants should still show correct historical
# MAGIC context, not just "whatever the merchant's status is today".

# COMMAND ----------

display(
    spark.sql(f"""
    SELECT merchant_status_at_order_time, COUNT(*) AS order_count, SUM(order_amount) AS total_revenue
    FROM {catalog}.gold.fact_orders
    GROUP BY merchant_status_at_order_time
    ORDER BY order_count DESC
    """)
)
