# Banking Transactions Lakehouse (PySpark)

A config-driven **PySpark medallion lakehouse** (bronze → silver → gold) for retail-banking
transactions, with data quality gates, source-to-target reconciliation, SCD Type 2 history,
PII protection, an ML feature table for fraud detection, Airflow orchestration, and AWS
deployment (S3, Glue, Lambda, IAM, KMS, CloudWatch, CloudTrail) defined in Terraform.

All data is **synthetic**, produced by a Spark-based generator that scales to tens of millions
of rows. The generator deliberately injects the defects real feeds have: duplicates, replays,
late events, nulls, bad codes, orphan keys, malformed records and a skewed "hot" merchant.

## What it demonstrates

| Area | Implementation |
|---|---|
| **PySpark ETL at scale** | Bronze → silver → gold pipeline. [Measured on 9M+ transactions](#measured-run) on a laptop; the same code runs on AWS Glue. |
| **Reusable framework** | Onboarding a source is a YAML change: schema, load type (`incremental` / `snapshot` / `cdc`), keys, DQ rules, PII policy ([`config/pipeline.yaml`](config/pipeline.yaml)). One generic ingest function handles every source. |
| **Data quality** | Declarative rules (`not_null`, `range`, `accepted_values`, `regex`, `unique`, `foreign_key`, `expression`) evaluated in **one pass**. Error rows are quarantined with reasons, warnings are counted, and a `max_error_rate` circuit breaker stops a bad batch ([`quality/rules.py`](src/lakehouse/quality/rules.py)). |
| **Reconciliation** | Every bronze row ends up in exactly one place (silver, quarantine or duplicates). Row counts **and monetary totals** are checked by re-reading storage, and a mismatch fails the run ([`quality/reconciliation.py`](src/lakehouse/quality/reconciliation.py)). |
| **Incremental & idempotent** | Partition-scoped dynamic overwrite: re-running any day gives identical results (asserted in tests). Late-arriving events are handled by recomputing every affected gold date. |
| **Deduplication** | In-batch duplicates via `row_number()`, cross-batch replays via `left_anti` against a configurable late-arrival window. |
| **SCD Type 2** | Customer dimension with effective dating. Ignores stale/out-of-order changes and is a no-op on replay ([`transform/scd2.py`](src/lakehouse/transform/scd2.py)). |
| **Security & controls** | Email SHA-256 hashed, phone masked, names dropped at bronze→silver. Lineage columns on every row. Audit tables for runs, DQ results and reconciliation. In AWS: SSE-KMS, TLS-only bucket policies, least-privilege IAM, CloudTrail S3 data events. |
| **Performance** | AQE (coalescing + skew join), broadcast joins for dimensions, explicit salting utility, partition pruning, and a [join-strategy benchmark](docs/performance.md) with measured results. |
| **ML collaboration** | `gold/txn_fraud_features`: rolling 1h/24h velocity, spend-vs-30-day-average ratio, first-time-merchant, time since previous transaction. Built **without look-ahead**, so training and scoring see the same values. |
| **Orchestration** | Airflow DAG with one task per stage, parallel branches, retries with backoff, and backfill-safe runs ([`dags/`](dags/lakehouse_daily.py)). Event-driven alternative: S3 `_SUCCESS` → Lambda → Glue. |
| **AWS / IaC** | Terraform for S3 (versioning, lifecycle tiering), KMS, Glue job + Data Catalog crawler, Lambda trigger, EventBridge → SNS failure alerts, CloudTrail ([`infra/terraform/`](infra/terraform)). |
| **Engineering practice** | 19 pytest tests (unit + end-to-end), ruff lint/format, and GitHub Actions CI that tests, validates Terraform and builds the Glue wheel. |

## Architecture

```
landing/ ──► bronze/ ──────────► silver/ ──────────────► gold/
 (files)     typed + lineage     validated, deduped,      customer_daily_spend
             per ingest_date     PII-protected, SCD2      merchant_category_daily
                 │                     │                  txn_fraud_features
                 ▼                     ▼
            quarantine/ (rows that failed, with reasons)    audit/ (runs, DQ, recon)
```

Design decisions and trade-offs, such as why silver is partitioned by `ingest_date` and how
late data flows through, are in [docs/architecture.md](docs/architecture.md).

## Project layout

```
config/pipeline.yaml          sources, DQ rules, PII policy, FX rates
src/lakehouse/
  common/                     Spark session, config loader, JSON logging + audit trail
  ingestion/bronze.py         generic config-driven ingestion
  quality/                    DQ rule engine, reconciliation
  transform/                  silver, SCD2, gold, shared transforms (dedupe, masking, salting)
  generate/synthetic.py       scalable synthetic data with realistic defects
  pipeline.py                 CLI: run one stage or all
dags/                         Airflow DAG
glue_jobs/                    AWS Glue entry point
lambda_functions/             S3 → Glue trigger
infra/terraform/              AWS infrastructure
scripts/                      join benchmark, audit report
tests/                        unit + end-to-end tests
```

## Run it locally

Requires Python 3.10+ and Java 17.

```bash
make setup                         # venv + pip install -e ".[dev]"
make test                          # 19 tests, ~30s
make data DAYS=3 TXNS=1000000      # synthetic landing files under ./data
make run                           # bronze → silver → gold for each generated day
.venv/bin/python scripts/show_audit.py --run-date <a generated date>
```

Run a single stage, as Airflow does:

```bash
python -m lakehouse.pipeline --run-date 2026-10-01 --stage silver_transactions
```

## Deploy to AWS

```bash
cd infra/terraform && terraform init && terraform apply
python -m build --wheel
aws s3 cp dist/lakehouse-0.1.0-py3-none-any.whl s3://<artifacts-bucket>/dist/
aws s3 cp glue_jobs/lakehouse_glue_job.py      s3://<artifacts-bucket>/glue_jobs/
aws s3 cp config/pipeline.yaml                 s3://<artifacts-bucket>/config/
```

Data arriving under `s3://<lake-bucket>/landing/transactions/ingest_date=YYYY-MM-DD/_SUCCESS`
triggers the Glue job. Alternatively, schedule it with the Airflow DAG.

## Measured run

3 days × 3M synthetic transactions (9.03M raw rows) on a 10-core laptop:

| Run date | Raw rows | Published to silver | Quarantined | Duplicates removed | Reconciled | Time |
|---|---:|---:|---:|---:|:-:|---:|
| 2026-09-29 | 3,009,095 | 2,989,431 | 10,569 | 9,095 | ✅ | 49 s |
| 2026-09-30 | 3,012,108 | 2,989,441 | 10,567 | 12,100 | ✅ | 95 s |
| 2026-10-01 | 3,012,209 | 2,989,423 | 10,584 | 12,202 | ✅ | 113 s |

The first attempt at this scale hit a driver OOM, caused by a DataFrame cache leak in the DQ
engine. The diagnosis, the fix and a join-strategy benchmark (broadcast vs AQE skew join vs
manual salting) are written up in [docs/performance.md](docs/performance.md).

## Roadmap

- Delta Lake / Iceberg table format for ACID `MERGE` and time travel
- Run on Databricks (the code is plain PySpark) and publish gold to Snowflake
- OpenLineage events for column-level lineage
