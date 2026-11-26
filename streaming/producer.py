"""
Order event producer: simulates a live marketplace and publishes order
lifecycle events to a Kafka topic (keyed by order_id).

    python -m streaming.producer --orders-per-min 30 --duration-min 5 --speed 30
    python -m streaming.producer --dry-run --duration-min 1 --speed 60   # no Kafka

--speed compresses time: with --speed 30 a 45-minute delivery plays out in
90 real seconds. --orders-per-min is in *simulated* minutes.
"""
import argparse
import json
import time
from datetime import timedelta

from streaming.config import DEFAULT_BOOTSTRAP, TOPIC
from streaming.events import OrderEventSimulator, utcnow


def run(args, send):
    sim = OrderEventSimulator(seed=args.seed)
    sim_start = utcnow()
    real_start = time.monotonic()
    rate_per_sim_sec = args.orders_per_min / 60.0
    next_arrival = sim_start
    sent = 0

    while True:
        real_elapsed = time.monotonic() - real_start
        sim_now = sim_start + timedelta(seconds=real_elapsed * args.speed)
        generating = real_elapsed < args.duration_min * 60

        while generating and next_arrival <= sim_now:
            sim.new_order(next_arrival)
            next_arrival += timedelta(seconds=sim.rng.expovariate(rate_per_sim_sec))

        for event in sim.pop_due(sim_now):
            send(event)
            sent += 1

        if not generating and sim.pending() == 0:
            break
        time.sleep(0.05)
    return sent


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    p.add_argument("--topic", default=TOPIC)
    p.add_argument("--orders-per-min", type=float, default=30)
    p.add_argument("--duration-min", type=float, default=5, help="real minutes to keep creating orders")
    p.add_argument("--speed", type=float, default=30, help="time compression factor")
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--dry-run", action="store_true", help="print events instead of sending to Kafka")
    args = p.parse_args()

    if args.dry_run:
        sent = run(args, lambda e: print(json.dumps(e)))
    else:
        from confluent_kafka import Producer  # imported lazily: not needed for tests/dry-run

        errors = []

        def on_delivery(err, _msg):
            if err is not None:
                errors.append(err)

        producer = Producer({"bootstrap.servers": args.bootstrap, "linger.ms": 50})

        def send(event):
            key = event["order_id"] or "unknown"
            producer.produce(args.topic, key=key, value=json.dumps(event), on_delivery=on_delivery)
            producer.poll(0)

        sent = run(args, send)
        producer.flush(30)
        if errors:
            print(f"WARNING: {len(errors)} messages failed to deliver, first error: {errors[0]}")

    print(f"Done. Sent {sent} events.")


if __name__ == "__main__":
    main()
