# Agentic Uber Eats Marketplace Lakehouse Data Platform

A production-style lakehouse pipeline built on Databricks, processing a
synthetic Uber Eats-style marketplace dataset (orders, payments, merchants,
couriers, refunds, delivery SLA events, and support tickets) through a
governed medallion architecture with SCD Type 2 history tracking, native
data quality checks, and CI/CD-based deployment.

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

## Tech stack

Databricks, Databricks Serverless Compute, Unity Catalog, Unity Catalog
Volumes, Delta Lake, Auto Loader, Medallion Architecture, PySpark, Spark SQL,
Delta MERGE / SCD Type 2, Star Schema Data Modeling, Liquid Clustering,
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
data_generator/     synthetic data generation scripts
pipeline/            the 3 core notebooks (bronze, silver, gold)
resources/           Databricks Asset Bundle job definition
src/                 shared, testable Python modules (e.g. dq_utils.py)
tests/               pytest unit tests for the DQ logic
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
