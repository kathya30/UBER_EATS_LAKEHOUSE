"""
Shared data quality check logic used by the Silver layer notebook.
Pulled out into a plain module (not a notebook cell) specifically so it can
be unit tested with pytest - testing logic buried inside notebook cells is
painful, testing a plain function is not.
"""
from pyspark.sql import functions as F


def apply_dq_checks(df, checks: dict):
    """
    checks: dict of {check_name: boolean_column_expression}
    Returns (clean_df, quarantine_df). Rows failing any check go to
    quarantine_df with a `_dq_failures` column listing which checks failed;
    rows passing all checks go to clean_df with no extra columns.
    """
    fail_exprs = [F.when(~expr, F.lit(name)) for name, expr in checks.items()]
    df_with_flags = df.withColumn(
        "_dq_failures",
        F.array_except(F.array(*fail_exprs), F.array(F.lit(None).cast("string"))),
    )
    clean_df = df_with_flags.filter(F.size("_dq_failures") == 0).drop("_dq_failures")
    quarantine_df = df_with_flags.filter(F.size("_dq_failures") > 0)
    return clean_df, quarantine_df


def order_dq_checks():
    """Standard set of data quality rules applied to raw orders."""
    return {
        "valid_order_amount": F.col("order_amount") > 0,
        "non_null_customer_id": F.col("customer_id").isNotNull(),
        "non_null_merchant_id": F.col("merchant_id").isNotNull(),
        "valid_status": F.col("status").isin(
            ["Delivered", "Cancelled", "In Progress", "Failed"]
        ),
    }


def payment_dq_checks():
    """Standard set of data quality rules applied to raw payments."""
    return {
        "valid_amount": F.col("amount") > 0,
        "non_null_order_id": F.col("order_id").isNotNull(),
        "valid_payment_status": F.col("payment_status").isin(["Success", "Failed"]),
    }
