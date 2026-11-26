"""
Order lifecycle event simulator (pure Python - no Kafka needed, so it is unit
testable and also usable with --dry-run).

Each order produces a realistic lifecycle:
    order_placed -> merchant_accepted -> courier_assigned
                 -> order_picked_up -> order_delivered      (or order_cancelled)

On top of the clean lifecycle the simulator injects the mess real event
streams have, so the downstream pipeline has something real to handle:
    * duplicates     - same event_id sent twice (at-least-once delivery)
    * late events    - emitted minutes after their event_time (out of order)
    * bad records    - null order_id / negative amount / unknown event_type
"""
import heapq
import itertools
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from streaming.config import (
    CITIES, NUM_COURIERS, NUM_CUSTOMERS, NUM_MERCHANTS,
)

# Late events arrive at most MAX_LATE_MIN after their event_time and a
# duplicate trails its original by at most MAX_DUP_DELAY_MIN. The silver
# watermark (15 min) must be larger than the sum of the two - keep these in
# sync with pipeline/05_stream_silver_events.py.
MAX_LATE_MIN = 10
MAX_DUP_DELAY_MIN = 3


@dataclass
class NoiseConfig:
    duplicate_rate: float = 0.03
    late_rate: float = 0.05
    bad_rate: float = 0.01
    cancel_rate: float = 0.06
    slow_delivery_rate: float = 0.18  # drives SLA breaches


def iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class OrderEventSimulator:
    def __init__(self, seed=None, noise: NoiseConfig = None):
        self.rng = random.Random(seed)
        self.noise = noise or NoiseConfig()
        self._heap = []                 # (emit_time, seq, event)
        self._seq = itertools.count()
        self._order_no = 0

    # ---------------------------------------------------------------- helpers
    def _uuid(self):
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

    def _push(self, emit_at, event):
        heapq.heappush(self._heap, (emit_at, next(self._seq), event))

    def _minutes(self, lo, hi):
        return timedelta(minutes=self.rng.uniform(lo, hi))

    # ------------------------------------------------------------- public API
    def new_order(self, placed_at: datetime):
        """Schedule every lifecycle event for one new order."""
        rng = self.rng
        self._order_no += 1
        order_id = f"LORD{self._order_no:07d}"
        base = {
            "order_id": order_id,
            "merchant_id": f"MERCH{rng.randint(1, NUM_MERCHANTS):05d}",
            "customer_id": f"CUST{rng.randint(1, NUM_CUSTOMERS):05d}",
            "courier_id": None,
            "city": rng.choice(CITIES),
            "order_amount": None,
        }
        amount = round(rng.uniform(8, 85), 2)

        stages = [("order_placed", placed_at)]
        t = placed_at + self._minutes(1, 4)
        if rng.random() < self.noise.cancel_rate:
            stages.append(("order_cancelled", t))
        else:
            stages.append(("merchant_accepted", t))
            t += self._minutes(1, 5)
            stages.append(("courier_assigned", t))
            t += self._minutes(6, 15)
            stages.append(("order_picked_up", t))
            slow = rng.random() < self.noise.slow_delivery_rate
            t += self._minutes(25, 45) if slow else self._minutes(8, 25)
            stages.append(("order_delivered", t))

        courier_id = f"COUR{rng.randint(1, NUM_COURIERS):05d}"
        for event_type, event_time in stages:
            event = dict(base)
            event["event_id"] = self._uuid()
            event["event_type"] = event_type
            event["event_time"] = iso(event_time)
            if event_type == "order_placed":
                event["order_amount"] = amount
            if event_type in ("courier_assigned", "order_picked_up", "order_delivered"):
                event["courier_id"] = courier_id
            self._emit_with_noise(event, event_time)

    def _emit_with_noise(self, event, event_time):
        rng, n = self.rng, self.noise
        emit_at = event_time
        if rng.random() < n.late_rate:
            emit_at = event_time + self._minutes(1, MAX_LATE_MIN)
        self._push(emit_at, event)

        if rng.random() < n.duplicate_rate:
            self._push(emit_at + self._minutes(0.5, MAX_DUP_DELAY_MIN), dict(event))

        if rng.random() < n.bad_rate:
            bad = dict(event)
            bad["event_id"] = self._uuid()
            kind = rng.choice(["null_order", "neg_amount", "bad_type"])
            if kind == "null_order":
                bad["order_id"] = None
            elif kind == "neg_amount":
                bad["event_type"] = "order_placed"
                bad["order_amount"] = -abs(round(rng.uniform(5, 50), 2))
            else:
                bad["event_type"] = "order_teleported"
            self._push(emit_at, bad)

    def pop_due(self, sim_now: datetime):
        """Return every event whose emission time has been reached."""
        due = []
        while self._heap and self._heap[0][0] <= sim_now:
            due.append(heapq.heappop(self._heap)[2])
        return due

    def pending(self) -> int:
        return len(self._heap)


def utcnow():
    return datetime.now(timezone.utc)
