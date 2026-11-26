# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze (streaming) — Order events via Auto Loader
# MAGIC Order lifecycle events flow **Kafka → consumer → S3 (JSON, partitioned by date)**.
# MAGIC This notebook ingests those files incrementally with Auto Loader into
# MAGIC `bronze.raw_order_events`. Bronze keeps events exactly as received —
# MAGIC duplicates, late events and bad records included. Cleaning happens in silver.
# MAGIC
# MAGIC **`source` widget**
# MAGIC * `s3` – read straight from the S3 bucket (needs the Unity Catalog external location)
# MAGIC * `volume` – read from the Volume fallback (`/Volumes/<catalog>/bronze/landing/order_events`),
# MAGIC   used when Databricks cannot reach S3 (see `docs/streaming_runbook.md`)

# COMMAND ----------

dbutils.widgets.text("catalog", "uber_eats_lakehouse")
dbutils.widgets.dropdown("source", "volume", ["volume", "s3"])
dbutils.widgets.text("s3_path", "s3://YOUR-BUCKET-NAME/order_events")

catalog = dbutils.widgets.get("catalog")
source = dbutils.widgets.get("source")
spark.sql(f"USE CATALOG {catalog}")

SOURCE_PATH = (
    dbutils.widgets.get("s3_path")
    if source == "s3"
    else f"/Volumes/{catalog}/bronze/landing/order_events"
)
CHECKPOINT = f"/Volumes/{catalog}/bronze/landing/_checkpoints/order_events"
TARGET = f"{catalog}.bronze.raw_order_events"
print(f"Reading from: {SOURCE_PATH}\nWriting to:   {TARGET}")

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader stream
# MAGIC * `trigger(availableNow=True)` — process everything that has landed, then stop.
# MAGIC   Runs as a scheduled job and works on Serverless / Free Edition; the exact same
# MAGIC   code becomes a continuous stream by swapping the trigger.
# MAGIC * `event_time` stays a **string** in bronze (raw). Parsing is silver's job.
# MAGIC * Malformed JSON lands in `_rescued_data` instead of failing the stream.

# COMMAND ----------

df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", CHECKPOINT)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaHints", "order_amount DOUBLE, courier_id STRING, event_time STRING")
    .load(SOURCE_PATH)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

query = (
    df.writeStream.format("delta")
    .option("checkpointLocation", CHECKPOINT)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(TARGET)
)
query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity check

# COMMAND ----------

display(spark.sql(f"""
    SELECT COUNT(*)                    AS total_rows,
           COUNT(DISTINCT event_id)    AS distinct_event_ids,
           COUNT(*) - COUNT(DISTINCT event_id) AS duplicate_rows,
           COUNT(DISTINCT _source_file) AS files_ingested
    FROM {TARGET}
"""))
display(spark.sql(f"SELECT event_type, COUNT(*) AS n FROM {TARGET} GROUP BY event_type ORDER BY n DESC"))
