"""Silver layer: cleansed, validated, de-duplicated, PII-protected data."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.common.audit import StageMetrics
from lakehouse.common.config import PipelineConfig
from lakehouse.common.spark import read_table, replace_table, write_table
from lakehouse.quality.reconciliation import RECON_SCHEMA, reconcile
from lakehouse.quality.rules import apply_quality, results_to_df
from lakehouse.transform.common import convert_to_gbp, dedupe_latest, mask_pii, shift_date, standardise_codes
from lakehouse.transform.scd2 import apply_scd2

LINEAGE_COLS = ["_ingest_ts", "_source_file", "_batch_id"]


def _bronze_partition(
    spark: SparkSession, cfg: PipelineConfig, source: str, run_date: str
) -> DataFrame | None:
    bronze = read_table(spark, cfg.path("bronze", source))
    if bronze is None:
        return None
    return bronze.where(F.col("ingest_date") == run_date)


def _latest_snapshot(
    spark: SparkSession, cfg: PipelineConfig, source: str, run_date: str
) -> DataFrame | None:
    """For snapshot sources: the most recent snapshot delivered on or before run_date."""
    bronze = read_table(spark, cfg.path("bronze", source))
    if bronze is None:
        return None
    eligible = bronze.where(F.col("ingest_date") <= run_date)
    latest = eligible.agg(F.max("ingest_date")).first()[0]
    return None if latest is None else eligible.where(F.col("ingest_date") == latest)


def _write_dq(spark, cfg, outcome, batch_id, run_date) -> None:
    write_table(
        results_to_df(spark, outcome.results, batch_id, run_date),
        cfg.path("audit", "dq_results"),
        mode="append",
    )


def build_reference(
    spark: SparkSession, cfg: PipelineConfig, source: str, run_date: str, batch_id: str, metrics: StageMetrics
) -> None:
    """Snapshot sources (accounts, merchants): validate the latest snapshot and replace silver."""
    snapshot = _latest_snapshot(spark, cfg, source, run_date)
    if snapshot is None:
        return
    snapshot = snapshot.drop("ingest_date")
    code_cols = {"accounts": ["account_type", "status"], "merchants": ["merchant_category", "country"]}.get(
        source, []
    )
    snapshot = standardise_codes(snapshot, code_cols)

    outcome = apply_quality(snapshot, source, cfg.quality(source))
    write_table(outcome.valid.withColumn("_snapshot_date", F.lit(run_date)), cfg.path("silver", source))
    write_table(
        outcome.quarantined.withColumn("ingest_date", F.lit(run_date)),
        cfg.path("quarantine", source),
        partition_by=["ingest_date"],
    )
    _write_dq(spark, cfg, outcome, batch_id, run_date)
    outcome.release()
    metrics.rows_in, metrics.rows_quarantined = outcome.total_rows, outcome.error_rows
    metrics.rows_out = outcome.total_rows - outcome.error_rows


def build_customers(
    spark: SparkSession, cfg: PipelineConfig, run_date: str, batch_id: str, metrics: StageMetrics
) -> None:
    """CDC feed -> validated, PII-masked SCD Type 2 customer dimension."""
    src = cfg.source("customers")
    incoming = _bronze_partition(spark, cfg, "customers", run_date)
    if incoming is None:
        return
    incoming = standardise_codes(incoming, ["segment"])
    latest, _ = dedupe_latest(
        incoming, src["primary_key"], [F.col(src["change_ts_column"]).desc(), F.col("_ingest_ts").desc()]
    )
    outcome = apply_quality(latest, "customers", cfg.quality("customers"))

    tracked = src["scd2_tracked_columns"]
    changes = outcome.valid.withColumn(
        "row_hash",
        F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in tracked]), 256),
    )
    changes = mask_pii(changes, cfg.pii("customers")).drop("ingest_date", *LINEAGE_COLS)

    dim_path = cfg.path("silver", "customers_scd2")
    result = apply_scd2(
        current_dim=read_table(spark, dim_path),
        changes=changes,
        key=src["primary_key"][0],
        change_ts=src["change_ts_column"],
        batch_id=batch_id,
    )
    replace_table(spark, result.dimension, dim_path)
    write_table(outcome.quarantined, cfg.path("quarantine", "customers"), partition_by=["ingest_date"])
    _write_dq(spark, cfg, outcome, batch_id, run_date)
    outcome.release()
    metrics.rows_in, metrics.rows_quarantined = outcome.total_rows, outcome.error_rows
    metrics.rows_out = result.inserted + result.updated
    metrics.extra.update(inserted=result.inserted, updated=result.updated, stale_ignored=result.stale)


def build_transactions(
    spark: SparkSession, cfg: PipelineConfig, run_date: str, batch_id: str, metrics: StageMetrics
) -> None:
    """Incremental: process one ingest_date of bronze transactions into silver."""
    bronze = _bronze_partition(spark, cfg, "transactions", run_date)
    if bronze is None:
        return
    bronze = bronze.persist()

    txns = standardise_codes(bronze, ["currency", "channel", "status"])

    deduped, dup_in_batch = dedupe_latest(
        txns, ["txn_id"], [F.col("_ingest_ts").desc(), F.col("_source_file")]
    )

    silver_path = cfg.path("silver", "transactions")
    existing = read_table(spark, silver_path)
    dup_cross_batch = deduped.limit(0)
    if existing is not None:
        window_start = shift_date(run_date, -cfg.processing["late_arrival_days"])
        seen = existing.where(
            (F.col("ingest_date") >= window_start) & (F.col("ingest_date") < run_date)
        ).select("txn_id")
        dup_cross_batch = deduped.join(seen, "txn_id", "left_semi")
        deduped = deduped.join(seen, "txn_id", "left_anti")

    duplicates = dup_in_batch.unionByName(dup_cross_batch)

    refs = {name: read_table(spark, cfg.path("silver", name)) for name in ("accounts", "merchants")}
    outcome = apply_quality(
        deduped,
        "transactions",
        cfg.quality("transactions"),
        refs={k: v for k, v in refs.items() if v is not None},
    )

    silver = convert_to_gbp(spark, outcome.valid, cfg.fx_rates).withColumn("txn_date", F.to_date("txn_ts"))
    write_table(silver, silver_path, partition_by=["ingest_date"])
    write_table(outcome.quarantined, cfg.path("quarantine", "transactions"), partition_by=["ingest_date"])
    write_table(duplicates, cfg.path("quarantine", "transactions_duplicates"), partition_by=["ingest_date"])
    _write_dq(spark, cfg, outcome, batch_id, run_date)
    outcome.release()

    def _written(table: str) -> DataFrame:
        return spark.read.parquet(cfg.path(*table.split(":"))).where(F.col("ingest_date") == run_date)

    recon = reconcile(
        "transactions",
        source=bronze,
        targets=[
            _written("silver:transactions"),
            _written("quarantine:transactions"),
            _written("quarantine:transactions_duplicates"),
        ],
        amount_col="amount",
    )
    write_table(
        spark.createDataFrame([recon.as_row(batch_id, run_date)], RECON_SCHEMA),
        cfg.path("audit", "reconciliation"),
        mode="append",
    )
    bronze.unpersist()

    metrics.rows_in = recon.source_rows
    metrics.rows_quarantined = outcome.error_rows
    metrics.rows_out = outcome.total_rows - outcome.error_rows
    metrics.extra.update(duplicates=recon.source_rows - outcome.total_rows)
