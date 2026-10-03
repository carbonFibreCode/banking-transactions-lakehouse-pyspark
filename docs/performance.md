# Performance notes

All numbers were measured on a laptop (10 cores, 16 GB RAM, Spark 4.2 in `local[*]` mode, 4 GB
driver). They show relative behaviour, not cluster throughput.

## End-to-end pipeline: 3 days × 3M transactions

Generated with `python -m lakehouse.generate.synthetic --days 3 --txns-per-day 3000000`.

| Run date | Raw rows (bronze) | Published to silver | Quarantined (DQ) | Duplicates removed | Wall time |
|---|---:|---:|---:|---:|---:|
| 2026-09-29 | 3,009,095 | 2,989,431 | 10,569 | 9,095 | 49 s |
| 2026-09-30 | 3,012,108 | 2,989,441 | 10,567 | 12,100 | 95 s |
| 2026-10-01 | 3,012,209 | 2,989,423 | 10,584 | 12,202 | 113 s |

Reconciliation balanced every day on both row count and amount (e.g. 3,012,209 rows and
£99,612,418.89 in = out on 2026-10-01). Days 2 and 3 cost more than day 1 for two reasons:
the cross-batch dedupe joins against earlier silver partitions, and the fraud features scan
the growing 30-day history.

### Issue found at scale: driver OOM

The first scale run failed in the gold stage with `java.lang.OutOfMemoryError: Java heap space`.
The 4,000-row tests had not triggered it. There were two causes:

1. **Cache leak:** the DQ engine persisted its checked DataFrame and never released it, so
   cached data built up across stages in the same JVM. Fixed with `QualityOutcome.release()`,
   called after the outputs are written.
2. **Local driver default of 1 GB**, shared by 10 concurrent tasks. Now configurable with
   `LAKEHOUSE_DRIVER_MEMORY` (default 4g). On Glue/EMR, memory comes from the worker type.

The run also showed that the audit wrapper's `finally` block could raise a second error when
the SparkContext died, which hid the original failure. The audit write is now guarded, and the
failure is always logged with its cause.

## Join strategies on a skewed key

`python scripts/benchmark_joins.py --rows 20000000`: 20M fact rows, 30% of them on a
single key, joined to a 2M-row dimension, then aggregated. Auto-broadcast is disabled so
the shuffle strategies can be compared.

| Strategy | Time (s) |
|---|---:|
| Sort-merge join, AQE off | 5.2 |
| Sort-merge join, AQE skew-join on | 3.6 |
| Manual salting (32 buckets), AQE skew-join off | 14.4 |
| Broadcast join | 1.6 |

What this shows:

* **Broadcast wins whenever the dimension fits in executor memory.** That's why the pipeline
  explicitly broadcasts accounts, merchants and FX rates.
* **AQE's skew-join handling cut the time by ~30% with zero code changes.** It splits the
  oversized partition at runtime. It is enabled by default in `get_spark()`.
* **Manual salting was the slowest option here.** It multiplies the dimension by the number of
  salt buckets (2M × 32 = 64M rows), which costs more than the skew it removes. Salting pays off
  when the dimension is small relative to the skew, or for skewed **aggregations**, which AQE
  doesn't split. `salted_join` stays in the toolkit for those cases. It's not the default.

## Other optimisations in the code

* **Explicit schemas on read**: no inference pass over raw files.
* **Partition pruning**: every stage reads only the `ingest_date` partitions it needs
  (the current batch, the late-arrival window, or the feature lookback).
* **Single-pass DQ**: all rules for a dataset are evaluated in one `select` plus one aggregate,
  so adding rules doesn't add scans.
* **Persist only where a DataFrame feeds several outputs** (bronze batch, DQ results),
  and release it straight afterwards.
* **Snappy-compressed Parquet**: the 1.6 GB of landing JSON becomes 194 MB in bronze.
