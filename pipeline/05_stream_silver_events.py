# Databricks notebook source
# MAGIC %md
# MAGIC # Silver (streaming) — dedup, watermark, data quality
# MAGIC Reads `bronze.raw_order_events` as a stream and produces
# MAGIC * `silver.order_events` — unique, valid events
# MAGIC * `silver.order_events_quarantine` — invalid events with the list of failed checks
# MAGIC
# MAGIC **Why a watermark?** De-duplicating a stream means remembering every `event_id`
# MAGIC seen so far. Without a bound that state grows forever. The 15-minute watermark
# MAGIC lets Spark forget ids older than (newest event time − 15 min). The simulator's
# MAGIC worst case is 10 min late + 3 min duplicate delay = 13 min, so nothing legitimate is lost.

# COMMAND ----------

import os, sys

dbutils.widgets.text("catalog", "uber_eats_lakehouse")
catalog = dbutils.widgets.get("catalog")
spark.sql(f"USE CATALOG {catalog}")

# Make the repo root importable (src/ lives next to pipeline/ in a Git folder / bundle deploy)
_nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
sys.path.append("/Workspace" + os.path.dirname(os.path.dirname(_nb)))

from pyspark.sql import functions as F
from src.dq_utils import apply_dq_checks
from src.event_utils import add_event_ts, dedupe_events, event_dq_checks

BRONZE = f"{catalog}.bronze.raw_order_events"
SILVER = f"{catalog}.silver.order_events"
QUARANTINE = f"{catalog}.silver.order_events_quarantine"
CHECKPOINT = f"/Volumes/{catalog}/bronze/landing/_checkpoints/silver_order_events"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream: parse → dedupe (watermarked) → DQ split per micro-batch
# MAGIC `foreachBatch` lets one stream feed two tables (clean / quarantine). The
# MAGIC `txnAppId` + `txnVersion` options make each write idempotent: if a micro-batch is
# MAGIC retried after a failure, Delta skips the already-committed write instead of duplicating rows.

# COMMAND ----------

def process_batch(batch_df, batch_id):
    clean, bad = apply_dq_checks(batch_df, event_dq_checks())

    (clean.withColumn("_silver_processed_at", F.current_timestamp())
        .write.format("delta").mode("append")
        .option("txnAppId", "silver_order_events").option("txnVersion", batch_id)
        .saveAsTable(SILVER))

    (bad.withColumn("_silver_processed_at", F.current_timestamp())
        .write.format("delta").mode("append")
        .option("txnAppId", "silver_order_events_quarantine").option("txnVersion", batch_id)
        .saveAsTable(QUARANTINE))


stream = dedupe_events(add_event_ts(spark.readStream.table(BRONZE)))

query = (
    stream.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reconciliation — every distinct bronze event must be accounted for
# MAGIC `distinct bronze event_ids = silver + quarantine + dropped-as-too-late`.
# MAGIC `unaccounted` should be 0 (or tiny); a large number means the watermark is too tight.

# COMMAND ----------

display(spark.sql(f"""
    WITH b AS (SELECT COUNT(DISTINCT event_id) AS bronze_distinct, COUNT(*) AS bronze_rows FROM {BRONZE}),
         s AS (SELECT COUNT(*) AS silver_rows FROM {SILVER}),
         q AS (SELECT COUNT(*) AS quarantine_rows FROM {QUARANTINE})
    SELECT bronze_rows, bronze_distinct,
           bronze_rows - bronze_distinct                       AS duplicates_removed,
           silver_rows, quarantine_rows,
           bronze_distinct - silver_rows - quarantine_rows     AS unaccounted
    FROM b, s, q
"""))

display(spark.sql(f"""
    SELECT explode(_dq_failures) AS failed_check, COUNT(*) AS n
    FROM {QUARANTINE} GROUP BY 1 ORDER BY n DESC
"""))
