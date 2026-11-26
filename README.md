# Agentic Uber Eats Marketplace Lakehouse Data Platform

A production-style lakehouse pipeline built on Databricks, processing a
synthetic Uber Eats-style marketplace dataset (orders, payments, merchants,
couriers, refunds, delivery SLA events, and support tickets) through a
governed medallion architecture with SCD Type 2 history tracking, native
data quality checks, and CI/CD-based deployment. A **real-time streaming
layer** (Kafka → S3 → Auto Loader → watermarked dedup → SLA gold table) sits
alongside the batch pipeline.

## Architecture

```
raw_data/ (synthetic source files)
        │
        ▼
  Unity Catalog Volume (bronze.landing)
        │
        ▼  Auto Loader (incremental, schema-evolving ingestion)
  Bronze layer  →  raw_* Delta tables (append-only)
        │
        ▼  SCD Type 2 merge + native data quality checks
  Silver layer  →  dim_merchant, dim_courier, dim_customer (versioned)
                    orders_clean / orders_quarantine, etc.
        │
        ▼  As-of joins, liquid clustering, predictive optimization
  Gold layer    →  fact_orders, fact_payments, fact_refunds,
                    fact_delivery_sla, fact_support_tickets
```

### Streaming layer (order lifecycle events)

```
Python producer ─► Kafka (Docker, KRaft) ─► consumer ─► S3 (JSON, dt=/hour= partitions)
                                                              │
                         Auto Loader (Structured Streaming)  ◄┘
                                   ▼
                    bronze.raw_order_events        (raw: duplicates, late + bad events kept)
                                   ▼  watermark + dedup on event_id, row-level DQ
        silver.order_events  /  silver.order_events_quarantine
                                   ▼  per-order SLA, Delta MERGE, order-level DQ
   gold.fact_order_sla · gold.agg_sla_by_hour_city · gold.order_sla_dq_failures
```

## Tech stack

Databricks, Databricks Serverless Compute, Unity Catalog, Unity Catalog
Volumes, Delta Lake, Auto Loader, Medallion Architecture, PySpark, Spark SQL,
Structured Streaming (watermarks, stateful dedup, foreachBatch), Apache Kafka,
Docker, AWS S3, SCD Type 2 (window-function versioning), Delta MERGE
(incremental SLA fact), Star Schema Data Modeling, Liquid Clustering,
Predictive Optimization, Databricks Workflows, Databricks Asset Bundles,
GitHub Actions, pytest.

## What makes this a real SCD2 pipeline, not just a schema exercise

`dim_merchant` tracks full history of merchant status, rating, and location
changes over a 14-day simulated period. Every fact row in `fact_orders` is
joined against merchant history **as of the order's date** — not today's
current merchant status — via an as-of join on
`[effective_start_date, effective_end_date)`.

This means a merchant that got suspended on day 10 still correctly shows as
"Active" for orders placed on day 3. Validated in this build: **314 orders
(~$16.9K in revenue) were placed with merchants that were `Suspended` at the
time of the order** — a real signal that would be lost with a naive
"join to current dimension state" approach.

## Repository layout

```
data_generator/     synthetic batch data generation scripts
streaming/           Kafka docker-compose, order-event producer, consumer -> S3
pipeline/            batch notebooks 01-03 + streaming notebooks 04-06
resources/           Databricks Asset Bundle job definitions (batch + streaming)
src/                 shared, testable PySpark modules (dq_utils, event_utils)
tests/               pytest unit tests (DQ, dedup, SLA, producer, consumer)
docs/                streaming runbook (AWS setup, run steps, troubleshooting)
.github/workflows/   CI/CD pipeline (test on every push, deploy on merge to main)
databricks.yml       Asset Bundle root config (dev/prod targets)
```

## Data quality

Native DQ checks are applied to orders and payments in the Silver layer.
Records failing any check (negative amounts, missing foreign keys, invalid
status values) are routed to `*_quarantine` tables instead of silently
breaking downstream Gold tables. The check logic itself lives in
`src/dq_utils.py` as plain, testable functions — covered by 7 pytest unit
tests that run on every push via GitHub Actions.

## Streaming design decisions

* **At-least-once delivery, exactly-once results.** The consumer commits Kafka offsets only after a
  file is written to S3, so a crash can re-deliver a batch (duplicates). Silver removes them by
  `event_id`, and its Delta writes carry `txnAppId`/`txnVersion` so retried micro-batches are not
  applied twice.
* **Bounded state with a watermark.** De-duplication keeps `event_id`s only for 15 minutes of event
  time. The producer's worst case (10 min late + 3 min duplicate delay) fits inside that budget, and a
  unit test fails if someone changes one without the other. Events later than the watermark are dropped
  by design; a reconciliation query (`bronze distinct ids = silver + quarantine + dropped`) makes any
  loss visible.
* **Bronze keeps everything.** Duplicates, late events and malformed records are preserved as received;
  cleaning is a silver concern, so bad data is always traceable.
* **Event-time, not arrival-order.** Gold computes stage durations from event timestamps, so
  out-of-order events still give the right SLA (covered by a test).
* **Incremental gold.** Only orders that received new events are recomputed and upserted with
  Delta `MERGE` on `order_id`.
* **Injected data problems.** The producer deliberately emits duplicates, late events and bad records
  (null order id, negative amount, unknown event type) so the quality layer has real work to do.

Run instructions: [`docs/streaming_runbook.md`](docs/streaming_runbook.md).

## CI/CD

Every push runs the full pytest suite against a local PySpark session (no
Databricks connection required for tests). On push to `main`, the pipeline
additionally deploys the Databricks Asset Bundle to the `dev` target,
updating the `uber_eats_lakehouse_pipeline` job definition in the workspace.

## Orchestration

The three notebooks run as a single Databricks Workflow
(`uber_eats_lakehouse_pipeline`) with task-level dependencies
(bronze → silver → gold), automatic retries, and a configurable `catalog`
parameter so the same job definition deploys cleanly to dev or prod.
A second job, `uber_eats_streaming_pipeline`, runs the streaming notebooks
(04 → 05 → 06) with `source` (`s3` / `volume`) and `s3_path` parameters.
