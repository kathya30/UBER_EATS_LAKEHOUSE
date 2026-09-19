# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Auto Loader Ingestion
# MAGIC Incrementally ingests raw CSV files landed in the Volume into Bronze Delta
# MAGIC tables. Uses Auto Loader (`cloudFiles`) for schema inference + evolution and
# MAGIC exactly-once, checkpoint-tracked incremental processing — the same pattern
# MAGIC production pipelines use for files dropped by upstream source systems.

# COMMAND ----------

dbutils.widgets.text("catalog", "uber_eats_lakehouse")
catalog = dbutils.widgets.get("catalog")

spark.sql(f"USE CATALOG {catalog}")

LANDING_BASE = f"/Volumes/{catalog}/bronze/landing"
CHECKPOINT_BASE = f"/Volumes/{catalog}/bronze/landing/_checkpoints"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generic Auto Loader ingestion function
# MAGIC One reusable function drives ingestion for every entity — avoids
# MAGIC duplicating the same boilerplate 10 times.

# COMMAND ----------

def ingest_entity_autoloader(entity_name, table_name, is_streaming=True):
    """
    Reads CSV files landed under /Volumes/<catalog>/bronze/landing/<entity_name>/
    using Auto Loader and writes/merges into bronze.<table_name> as a Delta table.
    Adds ingestion metadata columns for lineage/debugging.
    """
    source_path = f"{LANDING_BASE}/{entity_name}"
    checkpoint_path = f"{CHECKPOINT_BASE}/{entity_name}"
    target_table = f"{catalog}.bronze.{table_name}"

    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", checkpoint_path)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(source_path)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )

    query = (
        df.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .trigger(availableNow=True)  # process everything currently landed, then stop
        .toTable(target_table)
    )
    query.awaitTermination()
    print(f"Ingested {entity_name} -> {target_table}")


def ingest_entity_batch(entity_name, table_name):
    """
    For small, fully-overwritten reference data (locations, cuisine categories)
    a plain batch read + overwrite is simpler and more correct than streaming.
    """
    source_path = f"{LANDING_BASE}/{entity_name}"
    target_table = f"{catalog}.bronze.{table_name}"

    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(source_path)
        .withColumn("_ingested_at", F.current_timestamp())
    )
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
    print(f"Loaded {entity_name} -> {target_table} ({df.count()} rows)")

# COMMAND ----------

from pyspark.sql import functions as F

# Static / SCD1 reference data — simple batch overwrite, no history needed at bronze
ingest_entity_batch("locations", "raw_locations")
ingest_entity_batch("cuisine_categories", "raw_cuisine_categories")

# COMMAND ----------

# Daily snapshot entities (master data with SCD2 relevance downstream) —
# ingested as streaming append so every day's snapshot is preserved as-is.
# The SCD2 logic in Silver decides what changed; Bronze just accumulates history.
ingest_entity_autoloader("merchants", "raw_merchants")
ingest_entity_autoloader("couriers", "raw_couriers")
ingest_entity_autoloader("customers", "raw_customers")

# COMMAND ----------

# Daily incremental transactional entities — pure append, no snapshotting needed
ingest_entity_autoloader("orders", "raw_orders")
ingest_entity_autoloader("payments", "raw_payments")
ingest_entity_autoloader("refunds", "raw_refunds")
ingest_entity_autoloader("delivery_events", "raw_delivery_events")
ingest_entity_autoloader("support_tickets", "raw_support_tickets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity check — row counts per bronze table

# COMMAND ----------

bronze_tables = [
    "raw_locations", "raw_cuisine_categories", "raw_merchants", "raw_couriers",
    "raw_customers", "raw_orders", "raw_payments", "raw_refunds",
    "raw_delivery_events", "raw_support_tickets",
]

for t in bronze_tables:
    count = spark.table(f"{catalog}.bronze.{t}").count()
    print(f"{t}: {count} rows")
