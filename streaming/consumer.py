"""
Kafka -> S3 consumer. Reads order events and writes them as newline-delimited
JSON files, partitioned by arrival date/hour.

    python -m streaming.consumer --sink s3 --bucket <your-bucket>
    python -m streaming.consumer --sink local --local-dir ./local_s3     # no AWS needed

Delivery semantics: offsets are committed only AFTER a file is written
successfully (at-least-once). A crash between write and commit re-delivers
that batch, which creates duplicate events - the silver layer removes them
with event_id de-duplication.
"""
import argparse
import time

from streaming.config import DEFAULT_BOOTSTRAP, TOPIC
from streaming.events import utcnow
from streaming.sinks import LocalSink, S3Sink, partition_key


class BatchWriter:
    """Buffers raw JSON lines and flushes them to a sink by size or age."""

    def __init__(self, sink, prefix, batch_size, flush_seconds, clock=time.monotonic):
        self.sink, self.prefix = sink, prefix
        self.batch_size, self.flush_seconds = batch_size, flush_seconds
        self.clock = clock
        self.buffer = []
        self.last_flush = clock()
        self.files_written = 0

    def add(self, line: str):
        self.buffer.append(line)

    def should_flush(self) -> bool:
        if not self.buffer:
            return False
        return len(self.buffer) >= self.batch_size or (self.clock() - self.last_flush) >= self.flush_seconds

    def flush(self):
        """Write the buffer as one file. Returns the number of events written."""
        if not self.buffer:
            return 0
        key = partition_key(self.prefix, utcnow())
        self.sink.write(key, "\n".join(self.buffer) + "\n")
        n = len(self.buffer)
        self.buffer = []
        self.last_flush = self.clock()
        self.files_written += 1
        return n


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    p.add_argument("--topic", default=TOPIC)
    p.add_argument("--group", default="s3-writer")
    p.add_argument("--sink", choices=["s3", "local"], default="s3")
    p.add_argument("--bucket", help="S3 bucket name (no dots in the name)")
    p.add_argument("--region", default=None)
    p.add_argument("--prefix", default="order_events")
    p.add_argument("--local-dir", default="./local_s3")
    p.add_argument("--batch-size", type=int, default=500)
    p.add_argument("--flush-seconds", type=float, default=30)
    p.add_argument("--idle-exit-seconds", type=float, default=0,
                   help="exit after this many seconds with no messages (0 = run until Ctrl+C)")
    args = p.parse_args()

    if args.sink == "s3":
        if not args.bucket:
            p.error("--bucket is required with --sink s3")
        sink = S3Sink(args.bucket, args.region)
    else:
        sink = LocalSink(args.local_dir)

    from confluent_kafka import Consumer  # lazy import

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": args.group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([args.topic])
    writer = BatchWriter(sink, args.prefix, args.batch_size, args.flush_seconds)

    total, last_msg = 0, time.monotonic()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None and not msg.error():
                writer.add(msg.value().decode("utf-8"))
                last_msg = time.monotonic()
            elif msg is not None and msg.error():
                print(f"Kafka error: {msg.error()}")

            if writer.should_flush():
                total += writer.flush()
                consumer.commit(asynchronous=False)   # commit only after a successful write
                print(f"Flushed batch. Total events written: {total}")

            if args.idle_exit_seconds and time.monotonic() - last_msg > args.idle_exit_seconds:
                break
    except KeyboardInterrupt:
        pass
    finally:
        total += writer.flush()
        consumer.commit(asynchronous=False)
        consumer.close()
        print(f"Stopped. Total events written: {total} in {writer.files_written} files.")


if __name__ == "__main__":
    main()
