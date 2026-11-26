# Streaming upgrade runbook

Two parts, matching the plan: **weekday setup (~1 hr)** then the **Sunday build (6–7 hrs)**.
Everything here is for the `uber_eats_lakehouse` repo. Commands are for Windows PowerShell.

```
Kafka (Docker) ──► producer.py ──► topic: order_events ──► consumer.py ──► S3 (JSON, dt=/hour=)
                                                                              │
                                              Auto Loader (04)  ◄─────────────┘   (or Volume fallback)
                                                    ▼
                                        bronze.raw_order_events
                                                    ▼  05: parse → dedup (watermark) → DQ
                                  silver.order_events / silver.order_events_quarantine
                                                    ▼  06: per-order SLA, MERGE, DQ
                                  gold.fact_order_sla / agg_sla_by_hour_city / order_sla_dq_failures
```

---

## Part 1 — Weekday setup (about 1 hour)

### 1. AWS account + budget alert (15 min)
1. Create an account at aws.amazon.com and choose the **Free plan**.
2. **Billing → Budgets → Create budget → Monthly cost budget, $5**, with an email alert at 80%.
   Do this *before* creating anything else.
3. Do **not** use root access keys. **IAM → Users → Create user** `uber-eats-streaming`, attach the
   policy below (replace the bucket name), then **Security credentials → Create access key (CLI)**.
4. Install the AWS CLI and run `aws configure` (paste the access key, secret, your region, output `json`).
   Check with `aws sts get-caller-identity`.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow", "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": "arn:aws:s3:::YOUR-BUCKET-NAME" },
    { "Effect": "Allow", "Action": ["s3:PutObject", "s3:GetObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::YOUR-BUCKET-NAME/*" }
  ]
}
```

### 2. S3 bucket (5 min)
* Name: lowercase letters, digits and hyphens only, **no dots** (e.g. `kathya-uber-eats-streaming`).
* Region: same region as your Databricks workspace if you can tell; otherwise whichever you set in `aws configure`.
* Keep **Block all public access = ON**.
* Smoke test: `aws s3 cp README.md s3://YOUR-BUCKET-NAME/_smoke/README.md`, then `aws s3 ls s3://YOUR-BUCKET-NAME/_smoke/`.

### 3. Docker Desktop (15 min)
Install Docker Desktop (WSL 2 backend), reboot if asked, then:
```powershell
docker --version
docker run hello-world
```

### 4. Test Databricks → S3 (15–25 min) — decides the path
Databricks reads S3 through Unity Catalog: **Catalog → External Data → Credentials → Create credential**
(AWS IAM role), then **External locations → Create** with URL `s3://YOUR-BUCKET-NAME` (the whole bucket).
Databricks shows the exact trust policy and external ID to put on the IAM role — follow its on-screen
steps. The role needs the same S3 permissions as the policy above. Then press **Test connection**, and
in a notebook run:
```python
display(dbutils.fs.ls("s3://YOUR-BUCKET-NAME/_smoke/"))
```
* **Works** → use `source = s3` in notebook 04.
* **Fails, or the option is missing in Free Edition** → use the **fallback** (below) and `source = volume`.
  The rest of the pipeline is identical, so nothing else changes.

> I have not been able to verify what Free Edition allows for external locations, which is why the
> fallback exists. Don't spend more than ~30 minutes on this step.

**Fallback — copy S3 into the Volume** (run on your computer after the consumer has written files):
```powershell
aws s3 sync s3://YOUR-BUCKET-NAME/order_events ./s3_copy/order_events
databricks fs cp ./s3_copy/order_events dbfs:/Volumes/uber_eats_lakehouse/bronze/landing/order_events --recursive --overwrite
```
(`databricks` is the CLI you already installed for CI; if it isn't authenticated locally, run `databricks configure`.)
You can also upload the folder through the Volume UI in Catalog → bronze → landing, like you did for `raw_data`.

---

## Part 2 — Sunday build

### Step 0 — put the repo in Databricks as a Git folder (5 min)
Notebooks 05 and 06 import `src/`, so they must run from a folder that contains the whole repo.
**Workspace → Create → Git folder** → `https://github.com/kathya30/UBER_EATS_LAKEHOUSE`
(if the repo is private, add your GitHub PAT under Settings → Linked accounts first).
Run notebooks 04–06 from that folder, not from your hand-uploaded copies.

### Step 1 — Kafka in Docker
```powershell
docker compose -f streaming/docker-compose.yml up -d
docker ps          # uber-eats-kafka should be "Up"
```

### Step 2 — Python environment
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r streaming/requirements.txt
```

### Step 3 — smoke test with **no AWS** (local sink)
Terminal A (consumer):
```powershell
python -m streaming.consumer --sink local --local-dir ./local_s3 --flush-seconds 10
```
Terminal B (producer, ~1 real minute):
```powershell
python -m streaming.producer --orders-per-min 30 --duration-min 1 --speed 30 --seed 7
```
You should see files appear under `local_s3/order_events/dt=…/hour=…/`. Stop the consumer with Ctrl+C
(it flushes what it has). If this works, Kafka + producer + consumer are good.

### Step 4 — real run into S3
```powershell
python -m streaming.consumer --sink s3 --bucket YOUR-BUCKET-NAME --flush-seconds 20
python -m streaming.producer --orders-per-min 40 --duration-min 8 --speed 30
```
(`--speed 30` compresses time 30×: a 45-minute delivery plays out in 90 seconds, so 8 real minutes ≈ 4 simulated hours.)
Check: `aws s3 ls s3://YOUR-BUCKET-NAME/order_events/ --recursive`.
If you chose the fallback, run the `aws s3 sync` + `databricks fs cp` commands from Part 1 now.

### Step 5 — run the notebooks (in the Git folder, in this order)
| Notebook | Widgets | What to look at |
|---|---|---|
| `04_stream_bronze_events` | `source` = `s3` or `volume`; `s3_path` if s3 | `duplicate_rows` > 0 (the simulator injects ~3%) |
| `05_stream_silver_events` | – | reconciliation: `unaccounted` ≈ 0; quarantine reasons listed |
| `06_gold_order_sla` | – | `fact_order_sla` rows, % SLA met, `order_sla_dq_failures` |

### Step 6 — prove incremental behaviour (this is the interview story)
Run another producer batch (Step 4), run the consumer, re-run 04 → 05 → 06. Then confirm:
* bronze grows only by the new files (Auto Loader checkpoint),
* silver doesn't re-add old events (dedup state + idempotent writes),
* `fact_order_sla` has no duplicate `order_id`:
```sql
SELECT order_id, COUNT(*) FROM uber_eats_lakehouse.gold.fact_order_sla GROUP BY 1 HAVING COUNT(*) > 1;
```

### Step 7 — workflow + CI
Pushing to `main` redeploys the bundle, which now includes `uber_eats_streaming_pipeline`
(bronze → silver → gold). Run it once from **Workflows**, with the same `source` you used manually.

### Step 8 — README + GitHub
Add your real numbers to the README's streaming section (event counts, duplicates removed, quarantine
count, % SLA met) — take them from your own notebook output, not from this runbook. Add a screenshot of
the successful job run, then commit and push.

---

## Resetting and re-running from scratch
Dropping the tables is not enough — Auto Loader and the silver stream remember what they processed in
their checkpoints. In a notebook:
```python
for t in ["bronze.raw_order_events", "silver.order_events", "silver.order_events_quarantine",
          "gold.fact_order_sla", "gold.order_sla_dq_failures", "gold.agg_sla_by_hour_city"]:
    spark.sql(f"DROP TABLE IF EXISTS uber_eats_lakehouse.{t}")
dbutils.fs.rm("/Volumes/uber_eats_lakehouse/bronze/landing/_checkpoints/order_events", True)
dbutils.fs.rm("/Volumes/uber_eats_lakehouse/bronze/landing/_checkpoints/silver_order_events", True)
```
To reset Kafka: `docker compose -f streaming/docker-compose.yml down -v`.

## Troubleshooting
| Symptom | Likely cause / fix |
|---|---|
| Producer: `Connection refused` / hangs | Docker container not running → `docker ps`; check `docker compose logs kafka` |
| Consumer prints nothing | Producer not started yet, or a different `--group`; consumer starts from `earliest` so old messages are read too |
| `NoCredentialsError` / `AccessDenied` | Run `aws sts get-caller-identity`; check the IAM policy bucket name matches |
| 04: `PATH_NOT_FOUND` | Wrong `source`/`s3_path`, or files are not under `…/landing/order_events/` yet |
| 04: `_rescued_data` populated | Malformed JSON line; expected only if you edited files by hand |
| 05: `unaccounted` is large | Watermark too tight for your delays — keep `MAX_LATE_MIN + MAX_DUP_DELAY_MIN` below `WATERMARK_DELAY` (a test enforces this) |
| `ModuleNotFoundError: src` | Notebook is not running from the Git folder / bundle deploy (Step 0) |

## Cost safety
Set the $5 budget first. Everything here is tiny (a few MB in S3). When you are done for the day:
`docker compose -f streaming/docker-compose.yml down`. If you finish the project, you can delete the
bucket and the IAM access key.
