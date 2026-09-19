# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — SCD Type 2 Dimensions + Data Quality
# MAGIC Builds conformed SCD2 dimensions (merchant, courier, customer) tracking
# MAGIC full history of changes, SCD1 reference dimensions, and applies native
# MAGIC data quality checks on transactional data — bad records get routed to
# MAGIC quarantine tables instead of silently breaking downstream gold tables.

# COMMAND ----------

dbutils.widgets.text("catalog", "uber_eats_lakehouse")
catalog = dbutils.widgets.get("catalog")
spark.sql(f"USE CATALOG {catalog}")

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generic SCD Type 2 merge function
# MAGIC One reusable function drives SCD2 logic for merchant, courier, and
# MAGIC customer dimensions — the same MERGE pattern each time, parameterized
# MAGIC by business key and the columns that define "has this record changed".

# COMMAND ----------

def scd2_merge(
    source_df,
    target_table,
    business_key,
    tracked_columns,
    effective_date_col="snapshot_date",
):
    """
    Applies SCD Type 2 logic: when a tracked column changes for a business
    key, the old row is closed out (is_current=False, end_date set) and a
    new row is inserted (is_current=True, end_date=NULL). Unchanged rows
    are left alone. First-ever run creates the table with full history from
    day 1 collapsed into change-only versions.
    """
    # Collapse the raw daily snapshots down to only the rows where something
    # actually changed vs. the previous day for that business key - this is
    # what SCD2 should store: one row per *version*, not one row per day.
    from pyspark.sql.window import Window

    w = Window.partitionBy(business_key).orderBy(effective_date_col)
    change_expr = None
    for col in tracked_columns:
        prev = F.lag(col).over(w)
        col_changed = (prev.isNotNull()) & (F.col(col) != prev)
        change_expr = col_changed if change_expr is None else (change_expr | col_changed)

    versioned = (
        source_df.withColumn("_row_num", F.row_number().over(w))
        .withColumn("_changed", F.when(F.col("_row_num") == 1, F.lit(True)).otherwise(change_expr))
        .filter(F.col("_changed") == True)  # noqa: E712 - keep only first row + actual change rows
        .drop("_row_num", "_changed")
    )

    win_next = Window.partitionBy(business_key).orderBy(effective_date_col)
    versioned = versioned.withColumn(
        "effective_end_date",
        F.lead(effective_date_col).over(win_next),
    )
    versioned = versioned.withColumn(
        "is_current", F.col("effective_end_date").isNull()
    ).withColumnRenamed(effective_date_col, "effective_start_date")

    full_table_name = f"{catalog}.silver.{target_table}"

    if not spark.catalog.tableExists(full_table_name):
        versioned.write.format("delta").mode("overwrite").saveAsTable(full_table_name)
        print(f"Created {full_table_name} with {versioned.count()} versioned rows")
        return

    # Table exists: merge in only rows newer than what's already there
    existing_max_date = spark.table(full_table_name).agg(
        F.max("effective_start_date")
    ).collect()[0][0]

    new_versions = versioned.filter(F.col("effective_start_date") > existing_max_date)
    if new_versions.count() == 0:
        print(f"{full_table_name}: no new versions to merge")
        return

    new_versions.write.format("delta").mode("append").saveAsTable(full_table_name)
    print(f"Appended {new_versions.count()} new version rows to {full_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build SCD2 dimensions

# COMMAND ----------

merchants_src = spark.table(f"{catalog}.bronze.raw_merchants")
scd2_merge(
    merchants_src,
    target_table="dim_merchant",
    business_key="merchant_id",
    tracked_columns=["status", "rating", "location_id"],
)

# COMMAND ----------

couriers_src = spark.table(f"{catalog}.bronze.raw_couriers")
scd2_merge(
    couriers_src,
    target_table="dim_courier",
    business_key="courier_id",
    tracked_columns=["status", "vehicle_type"],
)

# COMMAND ----------

customers_src = spark.table(f"{catalog}.bronze.raw_customers")
scd2_merge(
    customers_src,
    target_table="dim_customer",
    business_key="customer_id",
    tracked_columns=["tier", "city", "state"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 1 reference dimensions (simple overwrite, no history)

# COMMAND ----------

spark.table(f"{catalog}.bronze.raw_locations").write.format("delta").mode(
    "overwrite"
).option("overwriteSchema", "true").saveAsTable(f"{catalog}.silver.dim_location")

spark.table(f"{catalog}.bronze.raw_cuisine_categories").write.format("delta").mode(
    "overwrite"
).option("overwriteSchema", "true").saveAsTable(f"{catalog}.silver.dim_cuisine_category")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality checks + quarantine
# MAGIC Native DQ checks on transactional data before it reaches gold. Records
# MAGIC failing checks go to a quarantine table instead of the clean silver
# MAGIC table — this is what supports "failed-record investigation" analytics.

# COMMAND ----------

def apply_dq_checks(df, checks: dict):
    """
    checks: dict of {check_name: boolean_column_expression}
    Adds a `_dq_failures` array column listing which checks failed per row.
    """
    fail_exprs = [
        F.when(~expr, F.lit(name)) for name, expr in checks.items()
    ]
    df_with_flags = df.withColumn(
        "_dq_failures",
        F.array_except(F.array(*fail_exprs), F.array(F.lit(None).cast("string"))),
    )
    clean_df = df_with_flags.filter(F.size("_dq_failures") == 0).drop("_dq_failures")
    quarantine_df = df_with_flags.filter(F.size("_dq_failures") > 0)
    return clean_df, quarantine_df

# COMMAND ----------

orders_src = spark.table(f"{catalog}.bronze.raw_orders")

order_checks = {
    "valid_order_amount": F.col("order_amount") > 0,
    "non_null_customer_id": F.col("customer_id").isNotNull(),
    "non_null_merchant_id": F.col("merchant_id").isNotNull(),
    "valid_status": F.col("status").isin(
        ["Delivered", "Cancelled", "In Progress", "Failed"]
    ),
}

clean_orders, quarantined_orders = apply_dq_checks(orders_src, order_checks)

clean_orders.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{catalog}.silver.orders_clean")

quarantined_orders.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{catalog}.silver.orders_quarantine")

print(f"Clean orders: {clean_orders.count()}, Quarantined orders: {quarantined_orders.count()}")

# COMMAND ----------

payments_src = spark.table(f"{catalog}.bronze.raw_payments")

payment_checks = {
    "valid_amount": F.col("amount") > 0,
    "non_null_order_id": F.col("order_id").isNotNull(),
    "valid_payment_status": F.col("payment_status").isin(["Success", "Failed"]),
}

clean_payments, quarantined_payments = apply_dq_checks(payments_src, payment_checks)

clean_payments.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{catalog}.silver.payments_clean")

quarantined_payments.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{catalog}.silver.payments_quarantine")

print(f"Clean payments: {clean_payments.count()}, Quarantined payments: {quarantined_payments.count()}")

# COMMAND ----------

# Refunds, delivery events, support tickets pass through as-is (already
# referentially generated) but get a light non-null check for safety.
for entity, key_col, target in [
    ("raw_refunds", "order_id", "refunds_clean"),
    ("raw_delivery_events", "order_id", "delivery_events_clean"),
    ("raw_support_tickets", "order_id", "support_tickets_clean"),
]:
    df = spark.table(f"{catalog}.bronze.{entity}").filter(F.col(key_col).isNotNull())
    df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(f"{catalog}.silver.{target}")
    print(f"{target}: {df.count()} rows")
