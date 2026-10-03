"""Pipeline entry point. Each stage can be run on its own (Airflow / Glue) or all together.

python -m lakehouse.pipeline --run-date 2026-09-28 --stage all
python -m lakehouse.pipeline --run-date 2026-09-28 --stage silver_transactions
"""

from __future__ import annotations

import argparse
from collections.abc import Callable

from pyspark.sql import SparkSession

from lakehouse.common.audit import audited, get_logger, new_batch_id
from lakehouse.common.config import PipelineConfig, load_config
from lakehouse.common.spark import get_spark
from lakehouse.ingestion.bronze import ingest_source
from lakehouse.transform import gold, silver

STAGES = ["bronze", "silver_reference", "silver_customers", "silver_transactions", "gold"]


def run_bronze(spark: SparkSession, cfg: PipelineConfig, run_date: str, batch_id: str) -> None:
    for source in cfg.source_names:
        with audited(spark, cfg, batch_id, run_date, "bronze", source) as m:
            result = ingest_source(spark, cfg, source, run_date, batch_id)
            m.rows_in, m.rows_out, m.rows_quarantined = (
                result["rows_in"],
                result["rows_out"],
                result["rows_quarantined"],
            )


def run_silver_reference(spark: SparkSession, cfg: PipelineConfig, run_date: str, batch_id: str) -> None:
    for source in (s for s in cfg.source_names if cfg.source(s)["load_type"] == "snapshot"):
        with audited(spark, cfg, batch_id, run_date, "silver", source) as m:
            silver.build_reference(spark, cfg, source, run_date, batch_id, m)


def run_silver_customers(spark: SparkSession, cfg: PipelineConfig, run_date: str, batch_id: str) -> None:
    with audited(spark, cfg, batch_id, run_date, "silver", "customers_scd2") as m:
        silver.build_customers(spark, cfg, run_date, batch_id, m)


def run_silver_transactions(spark: SparkSession, cfg: PipelineConfig, run_date: str, batch_id: str) -> None:
    with audited(spark, cfg, batch_id, run_date, "silver", "transactions") as m:
        silver.build_transactions(spark, cfg, run_date, batch_id, m)


def run_gold(spark: SparkSession, cfg: PipelineConfig, run_date: str, batch_id: str) -> None:
    with audited(spark, cfg, batch_id, run_date, "gold", "transactions_marts") as m:
        gold.build_gold(spark, cfg, run_date, m)


RUNNERS: dict[str, Callable[[SparkSession, PipelineConfig, str, str], None]] = {
    "bronze": run_bronze,
    "silver_reference": run_silver_reference,
    "silver_customers": run_silver_customers,
    "silver_transactions": run_silver_transactions,
    "gold": run_gold,
}


def run(
    spark: SparkSession, cfg: PipelineConfig, run_date: str, stage: str = "all", batch_id: str | None = None
) -> str:
    batch_id = batch_id or new_batch_id()
    for name in STAGES if stage == "all" else [stage]:
        RUNNERS[name](spark, cfg, run_date, batch_id)
    return batch_id


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--run-date", required=True, help="Logical date to process (YYYY-MM-DD)")
    parser.add_argument("--stage", default="all", choices=["all", *STAGES])
    parser.add_argument("--config", default=None, help="Path or s3:// URI of pipeline.yaml")
    parser.add_argument("--base-path", default=None, help="Overrides base_path in config")
    parser.add_argument("--batch-id", default=None, help="Defaults to a new id; Airflow passes its run id")
    args = parser.parse_args(argv)

    cfg = load_config(args.config, args.base_path)
    spark = get_spark(f"lakehouse-{args.stage}", cfg.processing["shuffle_partitions"])
    batch_id = run(spark, cfg, args.run_date, args.stage, args.batch_id)
    get_logger().info(
        "pipeline finished", extra={"context": {"batch_id": batch_id, "run_date": args.run_date}}
    )
    spark.stop()


if __name__ == "__main__":
    main()
