"""Print the control tables for a run: stage audit, data quality results, reconciliation."""

from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from lakehouse.common.config import load_config
from lakehouse.common.spark import get_spark


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--base-path", default=None)
    args = parser.parse_args()

    cfg = load_config(base_path=args.base_path)
    spark = get_spark("show-audit", shuffle_partitions=4)

    def show(table: str, *cols: str, order: str) -> None:
        print(f"\n== {table}")
        df = spark.read.parquet(cfg.path("audit", table)).where(F.col("run_date") == args.run_date)
        df.select(*cols).orderBy(order).show(100, truncate=False)

    show(
        "pipeline_runs",
        "stage",
        "dataset",
        "status",
        "rows_in",
        "rows_out",
        "rows_quarantined",
        "duration_s",
        order="logged_at",
    )
    show(
        "dq_results", "dataset", "rule", "severity", "failed_rows", "total_rows", "pass_rate", order="dataset"
    )
    show(
        "reconciliation",
        "dataset",
        "source_rows",
        "target_rows",
        "source_amount",
        "target_amount",
        "balanced",
        order="dataset",
    )
    spark.stop()


if __name__ == "__main__":
    main()
