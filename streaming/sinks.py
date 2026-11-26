"""Sinks for the consumer: S3 (real) or a local folder (dev / no AWS yet)."""
import os
import uuid
from datetime import datetime, timezone


def partition_key(prefix: str, now: datetime, batch_id: str = None) -> str:
    """
    Object key for one micro-batch file, partitioned by arrival date and hour:
        order_events/dt=2026-11-29/hour=14/events_20261129T141502_ab12cd34.json
    Partitioning by *arrival* time (not event_time) keeps a late event from
    reopening an old, already-processed partition.
    """
    now = now.astimezone(timezone.utc)
    batch_id = batch_id or uuid.uuid4().hex[:8]
    return (
        f"{prefix.strip('/')}/dt={now:%Y-%m-%d}/hour={now:%H}/"
        f"events_{now:%Y%m%dT%H%M%S}_{batch_id}.json"
    )


class LocalSink:
    def __init__(self, root):
        self.root = root

    def write(self, key: str, body: str):
        path = os.path.join(self.root, key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(body)
        return path


class S3Sink:
    def __init__(self, bucket, region=None):
        import boto3  # lazy: tests / local sink do not need it

        self.bucket = bucket
        self.client = boto3.client("s3", region_name=region)

    def write(self, key: str, body: str):
        self.client.put_object(Bucket=self.bucket, Key=key, Body=body.encode("utf-8"))
        return f"s3://{self.bucket}/{key}"
