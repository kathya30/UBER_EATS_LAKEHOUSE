"""Spark tests for the streaming silver/gold logic (local PySpark, no Databricks)."""
import json
from datetime import datetime

import pytest
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType, TimestampType,
)

from src.dq_utils import apply_dq_checks
from src.event_utils import (
    add_event_ts, build_order_sla, dedupe_events, event_dq_checks, sla_dq_checks,
)

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()), StructField("order_id", StringType()),
    StructField("event_type", StringType()), StructField("event_time", StringType()),
    StructField("merchant_id", StringType()), StructField("customer_id", StringType()),
    StructField("courier_id", StringType()), StructField("city", StringType()),
    StructField("order_amount", DoubleType()),
])


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("pytest-events")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    yield s


def ev(eid, oid, etype, t, amount=None, courier=None):
    return (eid, oid, etype, f"2026-11-29T{t}Z", "MERCH00001", "CUST00001", courier, "Austin", amount)


def events_df(spark, rows):
    df = add_event_ts(spark.createDataFrame(rows, EVENT_SCHEMA))
    return df.withColumn("_silver_processed_at", F.current_timestamp())


# ------------------------------------------------------------------ silver DQ
def test_event_dq_flags_each_bad_record_type(spark):
    rows = [
        ev("e1", "O1", "order_placed", "10:00:00.000", 25.0),                 # good
        ev("e2", None, "order_placed", "10:00:00.000", 25.0),                 # null order
        ev("e3", "O3", "order_placed", "10:00:00.000", -5.0),                 # negative amount
        ev("e4", "O4", "order_placed", "10:00:00.000", None),                 # missing amount
        ev("e5", "O5", "order_teleported", "10:00:00.000"),                   # bad type
        ev("e6", "O6", "merchant_accepted", "10:01:00.000"),                  # no amount needed
        ("e7", "O7", "order_placed", "not-a-date", "M", "C", None, "Austin", 9.0),  # bad time
    ]
    clean, bad = apply_dq_checks(add_event_ts(spark.createDataFrame(rows, EVENT_SCHEMA)), event_dq_checks())
    assert {r.event_id for r in clean.collect()} == {"e1", "e6"}
    failures = {r.event_id: set(r._dq_failures) for r in bad.collect()}
    assert failures["e2"] == {"non_null_order_id"}
    assert failures["e3"] == {"valid_order_amount"}
    assert failures["e4"] == {"valid_order_amount"}
    assert failures["e5"] == {"valid_event_type"}
    assert failures["e7"] == {"valid_event_time"}


# ---------------------------------------------------------------- silver dedup
def test_streaming_dedupe_removes_duplicate_event_ids(spark, tmp_path):
    src = tmp_path / "in"
    src.mkdir()
    batch1 = [
        ev("e1", "O1", "order_placed", "10:00:00.000", 20.0),
        ev("e2", "O1", "merchant_accepted", "10:02:00.000"),
        ev("e1", "O1", "order_placed", "10:00:00.000", 20.0),   # duplicate in same file
    ]
    batch2 = [
        ev("e2", "O1", "merchant_accepted", "10:02:00.000"),    # duplicate across files
        ev("e3", "O1", "courier_assigned", "10:05:00.000", courier="COUR00001"),
    ]
    for i, rows in enumerate([batch1, batch2]):
        with open(src / f"f{i}.json", "w") as f:
            for r in rows:
                f.write(json.dumps(dict(zip(EVENT_SCHEMA.fieldNames(), r))) + "\n")

    stream = (spark.readStream.schema(EVENT_SCHEMA).json(str(src))
              .withColumn("_ingested_at", F.current_timestamp()))
    q = (dedupe_events(add_event_ts(stream)).writeStream.format("memory")
         .queryName("dedup_out").outputMode("append").trigger(availableNow=True).start())
    q.awaitTermination()

    got = [r.event_id for r in spark.table("dedup_out").collect()]
    assert sorted(got) == ["e1", "e2", "e3"]


# ------------------------------------------------------------------- gold SLA
def lifecycle(oid, start_min, durations, courier="COUR00001"):
    """durations = (accept, assign, pickup, deliver) minute gaps -> 5 events."""
    t, rows = start_min, []
    names = ["order_placed", "merchant_accepted", "courier_assigned", "order_picked_up", "order_delivered"]
    gaps = [0] + list(durations)
    for i, (name, gap) in enumerate(zip(names, gaps)):
        t += gap
        rows.append(ev(f"{oid}-{i}", oid, name, f"10:{int(t):02d}:00.000",
                       amount=30.0 if i == 0 else None,
                       courier=courier if i >= 2 else None))
    return rows


def test_sla_met_vs_breached_and_stage_durations(spark):
    rows = lifecycle("OK", 0, (2, 3, 10, 20)) + lifecycle("SLOW", 0, (2, 3, 10, 40))
    sla = {r.order_id: r for r in build_order_sla(events_df(spark, rows)).collect()}
    assert sla["OK"].minutes_to_deliver == pytest.approx(35.0)
    assert sla["OK"].sla_met is True and sla["OK"].sla_breach_minutes == 0.0
    assert sla["SLOW"].minutes_to_deliver == pytest.approx(55.0)
    assert sla["SLOW"].sla_met is False and sla["SLOW"].sla_breach_minutes == pytest.approx(10.0)
    assert sla["OK"].minutes_to_accept == pytest.approx(2.0)
    assert sla["OK"].minutes_delivery_leg == pytest.approx(20.0)
    assert sla["OK"].order_amount == 30.0 and sla["OK"].order_status == "Delivered"


def test_out_of_order_arrival_gives_same_result(spark):
    ordered = lifecycle("O1", 0, (2, 3, 10, 20))
    shuffled = list(reversed(ordered))
    a = build_order_sla(events_df(spark, ordered)).collect()[0]
    b = build_order_sla(events_df(spark, shuffled)).collect()[0]
    assert a.minutes_to_deliver == b.minutes_to_deliver == pytest.approx(35.0)


def test_in_progress_and_cancelled_status(spark):
    rows = lifecycle("D", 0, (2, 3, 10, 20))[:3]  # stops after courier_assigned
    rows += [ev("c0", "C", "order_placed", "10:00:00.000", 15.0), ev("c1", "C", "order_cancelled", "10:02:00.000")]
    sla = {r.order_id: r for r in build_order_sla(events_df(spark, rows)).collect()}
    assert sla["D"].order_status == "In Progress" and sla["D"].sla_met is None
    assert sla["C"].order_status == "Cancelled" and sla["C"].minutes_to_deliver is None


def test_sla_dq_catches_missing_placed_event_and_negative_time(spark):
    missing_placed = lifecycle("NOPLACE", 0, (2, 3, 10, 20))[1:]
    negative = [ev("n0", "NEG", "order_placed", "10:30:00.000", 20.0),
                ev("n1", "NEG", "courier_assigned", "10:31:00.000", courier="COUR00001"),
                ev("n2", "NEG", "order_delivered", "10:10:00.000", courier="COUR00001")]
    good = lifecycle("GOOD", 0, (2, 3, 10, 20))
    sla = build_order_sla(events_df(spark, missing_placed + negative + good))
    clean, bad = apply_dq_checks(sla, sla_dq_checks())
    assert [r.order_id for r in clean.collect()] == ["GOOD"]
    failures = {r.order_id: set(r._dq_failures) for r in bad.collect()}
    assert failures["NOPLACE"] == {"has_placed_event"}
    assert "non_negative_delivery_time" in failures["NEG"]
