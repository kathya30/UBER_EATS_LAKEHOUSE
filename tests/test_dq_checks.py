"""
Unit tests for the Silver layer's data quality logic. Runs against a local
PySpark session (no Databricks connection needed) - this is what lets
GitHub Actions run these tests automatically on every push, before anything
gets deployed to the actual workspace.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from src.dq_utils import apply_dq_checks, order_dq_checks, payment_dq_checks

ORDER_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("order_amount", DoubleType(), True),
        StructField("status", StringType(), True),
    ]
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("pytest-dq-checks")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_valid_orders_pass_all_checks(spark):
    df = spark.createDataFrame(
        [
            ("ORD1", "CUST1", "MERCH1", 25.50, "Delivered"),
            ("ORD2", "CUST2", "MERCH2", 40.00, "Cancelled"),
        ],
        ["order_id", "customer_id", "merchant_id", "order_amount", "status"],
    )
    clean, quarantine = apply_dq_checks(df, order_dq_checks())

    assert clean.count() == 2
    assert quarantine.count() == 0


def test_negative_amount_is_quarantined(spark):
    df = spark.createDataFrame(
        [
            ("ORD1", "CUST1", "MERCH1", -5.00, "Delivered"),
        ],
        ["order_id", "customer_id", "merchant_id", "order_amount", "status"],
    )
    clean, quarantine = apply_dq_checks(df, order_dq_checks())

    assert clean.count() == 0
    assert quarantine.count() == 1
    failures = quarantine.select("_dq_failures").collect()[0][0]
    assert "valid_order_amount" in failures


def test_null_merchant_id_is_quarantined(spark):
    df = spark.createDataFrame(
        [
            ("ORD1", "CUST1", None, 20.00, "Delivered"),
        ],
        schema=ORDER_SCHEMA,
    )
    clean, quarantine = apply_dq_checks(df, order_dq_checks())

    assert clean.count() == 0
    assert quarantine.count() == 1
    failures = quarantine.select("_dq_failures").collect()[0][0]
    assert "non_null_merchant_id" in failures


def test_invalid_status_is_quarantined(spark):
    df = spark.createDataFrame(
        [
            ("ORD1", "CUST1", "MERCH1", 20.00, "NotARealStatus"),
        ],
        ["order_id", "customer_id", "merchant_id", "order_amount", "status"],
    )
    clean, quarantine = apply_dq_checks(df, order_dq_checks())

    assert clean.count() == 0
    assert quarantine.count() == 1


def test_row_can_fail_multiple_checks_at_once(spark):
    df = spark.createDataFrame(
        [
            ("ORD1", None, None, -10.00, "BadStatus"),
        ],
        schema=ORDER_SCHEMA,
    )
    clean, quarantine = apply_dq_checks(df, order_dq_checks())

    assert clean.count() == 0
    failures = quarantine.select("_dq_failures").collect()[0][0]
    assert len(failures) == 4  # all four checks should fail


def test_valid_payments_pass_all_checks(spark):
    df = spark.createDataFrame(
        [
            ("PAY1", "ORD1", 25.50, "Success"),
        ],
        ["payment_id", "order_id", "amount", "payment_status"],
    )
    clean, quarantine = apply_dq_checks(df, payment_dq_checks())

    assert clean.count() == 1
    assert quarantine.count() == 0


def test_zero_amount_payment_is_quarantined(spark):
    df = spark.createDataFrame(
        [
            ("PAY1", "ORD1", 0.00, "Success"),
        ],
        ["payment_id", "order_id", "amount", "payment_status"],
    )
    clean, quarantine = apply_dq_checks(df, payment_dq_checks())

    assert clean.count() == 0
    assert quarantine.count() == 1
