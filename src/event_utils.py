"""
Pure PySpark logic for the streaming order-event pipeline (silver + gold).
Kept out of the notebooks so it can be unit tested with pytest, and shared
by the notebooks and the CI test suite.
"""
from pyspark.sql import functions as F

VALID_EVENT_TYPES = [
    "order_placed", "merchant_accepted", "courier_assigned",
    "order_picked_up", "order_delivered", "order_cancelled",
]
SLA_MINUTES = 45
WATERMARK_DELAY = "15 minutes"  # must exceed max lateness (10) + max dup delay (3) in streaming/events.py


# --------------------------------------------------------------------- silver
def add_event_ts(df):
    """Parse the raw ISO-8601 string into a timestamp used for watermarking."""
    return df.withColumn("event_ts", F.to_timestamp("event_time"))


def dedupe_events(df, delay=WATERMARK_DELAY):
    """
    Streaming de-duplication on event_id.

    The watermark bounds the dedup state: Spark remembers an event_id only
    until the watermark passes that event's time, so state does not grow
    forever. Trade-off: an event arriving more than `delay` behind the newest
    event time seen is dropped.

    `dedup_ts` falls back to ingestion time when event_ts is null so that
    malformed rows are not silently discarded by the watermark - the DQ step
    quarantines them instead.
    """
    return (
        df.withColumn("dedup_ts", F.coalesce(F.col("event_ts"), F.col("_ingested_at")))
        .withWatermark("dedup_ts", delay)
        .dropDuplicates(["event_id", "dedup_ts"])
        .drop("dedup_ts")
    )


def _null_safe(cond):
    """A NULL check result means 'could not verify' -> treat as a failure."""
    return F.coalesce(cond, F.lit(False))


def event_dq_checks():
    """Row-level rules for a single event (silver)."""
    return {
        "non_null_event_id": F.col("event_id").isNotNull(),
        "non_null_order_id": F.col("order_id").isNotNull(),
        "valid_event_type": _null_safe(F.col("event_type").isin(VALID_EVENT_TYPES)),
        "valid_event_time": F.col("event_ts").isNotNull(),
        "valid_order_amount": _null_safe(
            F.when(F.col("event_type") == "order_placed", F.col("order_amount") > 0)
            .otherwise(F.lit(True))
        ),
    }


# ----------------------------------------------------------------------- gold
def _first(col, cond):
    return F.first(F.when(cond, F.col(col)), ignorenulls=True)


def build_order_sla(events_df, sla_minutes=SLA_MINUTES):
    """
    Collapse one-row-per-event into one-row-per-order and compute SLA metrics.
    Uses event *timestamps*, not arrival order, so out-of-order / late events
    still produce correct stage durations.
    """
    t = F.col("event_type")
    ts = F.col("event_ts")

    g = events_df.groupBy("order_id").agg(
        F.min(F.when(t == "order_placed", ts)).alias("placed_ts"),
        F.min(F.when(t == "merchant_accepted", ts)).alias("accepted_ts"),
        F.min(F.when(t == "courier_assigned", ts)).alias("assigned_ts"),
        F.min(F.when(t == "order_picked_up", ts)).alias("picked_up_ts"),
        F.min(F.when(t == "order_delivered", ts)).alias("delivered_ts"),
        F.min(F.when(t == "order_cancelled", ts)).alias("cancelled_ts"),
        F.first("merchant_id", ignorenulls=True).alias("merchant_id"),
        F.first("customer_id", ignorenulls=True).alias("customer_id"),
        F.first("courier_id", ignorenulls=True).alias("courier_id"),
        F.first("city", ignorenulls=True).alias("city"),
        _first("order_amount", t == "order_placed").alias("order_amount"),
        F.max("event_ts").alias("last_event_ts"),
        F.max("_silver_processed_at").alias("last_processed_at"),
    )

    def mins(end, start):
        return (F.col(end).cast("double") - F.col(start).cast("double")) / 60.0

    out = (
        g.withColumn(
            "order_status",
            F.when(F.col("delivered_ts").isNotNull(), "Delivered")
            .when(F.col("cancelled_ts").isNotNull(), "Cancelled")
            .otherwise("In Progress"),
        )
        .withColumn("minutes_to_accept", mins("accepted_ts", "placed_ts"))
        .withColumn("minutes_to_assign", mins("assigned_ts", "accepted_ts"))
        .withColumn("minutes_to_pickup", mins("picked_up_ts", "assigned_ts"))
        .withColumn("minutes_delivery_leg", mins("delivered_ts", "picked_up_ts"))
        .withColumn("minutes_to_deliver", mins("delivered_ts", "placed_ts"))
        .withColumn(
            "sla_met",
            F.when(F.col("minutes_to_deliver").isNotNull(), F.col("minutes_to_deliver") <= sla_minutes),
        )
        .withColumn(
            "sla_breach_minutes",
            F.when(
                F.col("minutes_to_deliver").isNotNull(),
                F.greatest(F.col("minutes_to_deliver") - F.lit(float(sla_minutes)), F.lit(0.0)),
            ),
        )
        .withColumn("placed_date", F.to_date("placed_ts"))
    )
    return out


def sla_dq_checks():
    """Order-level rules: catch impossible or incomplete lifecycles before they reach gold."""
    return {
        "has_placed_event": F.col("placed_ts").isNotNull(),
        "non_negative_delivery_time": _null_safe(
            F.when(F.col("minutes_to_deliver").isNotNull(), F.col("minutes_to_deliver") >= 0)
            .otherwise(F.lit(True))
        ),
        "pickup_before_delivery": _null_safe(
            F.when(
                F.col("picked_up_ts").isNotNull() & F.col("delivered_ts").isNotNull(),
                F.col("picked_up_ts") <= F.col("delivered_ts"),
            ).otherwise(F.lit(True))
        ),
        "delivered_has_courier": _null_safe(
            F.when(F.col("order_status") == "Delivered", F.col("courier_id").isNotNull())
            .otherwise(F.lit(True))
        ),
        "not_delivered_and_cancelled": _null_safe(
            ~(F.col("delivered_ts").isNotNull() & F.col("cancelled_ts").isNotNull())
        ),
    }
