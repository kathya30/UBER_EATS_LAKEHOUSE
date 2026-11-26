"""Tests for the Kafka-side code that needs no Kafka: simulator, partitioning, batching."""
from collections import Counter
from datetime import datetime, timedelta, timezone

from streaming.config import LIFECYCLE, VALID_EVENT_TYPES
from streaming.consumer import BatchWriter
from streaming.events import MAX_DUP_DELAY_MIN, MAX_LATE_MIN, NoiseConfig, OrderEventSimulator
from streaming.sinks import LocalSink, partition_key

T0 = datetime(2026, 11, 29, 10, 0, tzinfo=timezone.utc)


def drain(sim, until_minutes=240):
    return sim.pop_due(T0 + timedelta(minutes=until_minutes))


def test_clean_order_follows_full_lifecycle_in_time_order():
    sim = OrderEventSimulator(seed=1, noise=NoiseConfig(0, 0, 0, 0, 0))
    sim.new_order(T0)
    events = sorted(drain(sim), key=lambda e: e["event_time"])
    assert [e["event_type"] for e in events] == LIFECYCLE
    assert len({e["order_id"] for e in events}) == 1
    assert events[0]["order_amount"] > 0
    assert events[2]["courier_id"] is not None and events[0]["courier_id"] is None


def test_cancelled_order_never_reaches_delivery():
    sim = OrderEventSimulator(seed=1, noise=NoiseConfig(0, 0, 0, 1.0, 0))
    sim.new_order(T0)
    types = [e["event_type"] for e in drain(sim)]
    assert "order_cancelled" in types and "order_delivered" not in types


def test_same_seed_gives_identical_stream():
    def run(seed):
        sim = OrderEventSimulator(seed=seed)
        for i in range(20):
            sim.new_order(T0 + timedelta(minutes=i))
        return drain(sim)

    assert run(5) == run(5)
    assert run(5) != run(6)


def test_noise_creates_duplicates_late_and_bad_events():
    sim = OrderEventSimulator(seed=3, noise=NoiseConfig(0.5, 0.5, 0.5, 0, 0))
    for i in range(30):
        sim.new_order(T0 + timedelta(minutes=i))
    events = drain(sim)
    ids = Counter(e["event_id"] for e in events)
    assert any(c > 1 for c in ids.values()), "expected duplicate event_ids"
    assert any(e["order_id"] is None for e in events)
    assert any(e["event_type"] not in VALID_EVENT_TYPES for e in events)
    assert any((e["order_amount"] or 0) < 0 for e in events)


def test_late_events_are_not_emitted_before_their_delay():
    sim = OrderEventSimulator(seed=2, noise=NoiseConfig(0, 1.0, 0, 0, 0))
    sim.new_order(T0)
    # every event is late by >= 1 minute, so nothing is due at T0
    assert sim.pop_due(T0) == []
    assert sim.pending() == 5


def test_lateness_budget_stays_inside_silver_watermark():
    from src.event_utils import WATERMARK_DELAY

    assert MAX_LATE_MIN + MAX_DUP_DELAY_MIN < int(WATERMARK_DELAY.split()[0])


def test_partition_key_is_date_and_hour_partitioned():
    now = datetime(2026, 11, 29, 14, 15, 2, tzinfo=timezone.utc)
    key = partition_key("order_events/", now, batch_id="ab12cd34")
    assert key == "order_events/dt=2026-11-29/hour=14/events_20261129T141502_ab12cd34.json"


def test_batch_writer_flushes_on_size_and_writes_json_lines(tmp_path):
    writer = BatchWriter(LocalSink(str(tmp_path)), "order_events", batch_size=3, flush_seconds=999)
    for i in range(2):
        writer.add(f'{{"n": {i}}}')
    assert not writer.should_flush()
    writer.add('{"n": 2}')
    assert writer.should_flush()
    assert writer.flush() == 3

    files = list(tmp_path.rglob("*.json"))
    assert len(files) == 1
    assert "dt=" in str(files[0]) and "hour=" in str(files[0])
    assert files[0].read_text().strip().splitlines() == ['{"n": 0}', '{"n": 1}', '{"n": 2}']


def test_batch_writer_flushes_on_age_and_skips_empty(tmp_path):
    now = [0.0]
    writer = BatchWriter(LocalSink(str(tmp_path)), "p", batch_size=100, flush_seconds=30, clock=lambda: now[0])
    assert writer.flush() == 0 and not writer.should_flush()
    writer.add("{}")
    assert not writer.should_flush()
    now[0] = 31.0
    assert writer.should_flush()
