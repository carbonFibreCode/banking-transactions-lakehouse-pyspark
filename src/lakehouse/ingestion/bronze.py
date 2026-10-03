"""Generic, config-driven bronze ingestion."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.common.config import PipelineConfig
from lakehouse.common.spark import write_table

CORRUPT_COL = "_corrupt_record"


def read_landing(spark: SparkSession, cfg: PipelineConfig, source: str, run_date: str) -> DataFrame | None:
    src = cfg.source(source)
    path = f"{cfg.path('landing', source)}/ingest_date={run_date}"
    reader = (
        spark.read.format(src["format"])
        .schema(src["schema"])
        .options(**src.get("options", {}))
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COL)
    )
    try:
        return reader.load(path)
    except Exception as exc:
        if "PATH_NOT_FOUND" in str(exc) or "Path does not exist" in str(exc):
            return None
        raise


def ingest_source(
    spark: SparkSession, cfg: PipelineConfig, source: str, run_date: str, batch_id: str
) -> dict:
    """Land one source for one run date into bronze. Returns row metrics."""
    raw = read_landing(spark, cfg, source, run_date)
    if raw is None:
        return {"rows_in": 0, "rows_out": 0, "rows_quarantined": 0, "skipped": True}

    bronze = raw.select(
        *raw.columns,
        F.current_timestamp().alias("_ingest_ts"),
        F.col("_metadata.file_path").alias("_source_file"),
        F.lit(batch_id).alias("_batch_id"),
        F.lit(run_date).alias("ingest_date"),
    ).persist()

    total = bronze.count()
    corrupt = bronze.where(F.col(CORRUPT_COL).isNotNull())
    corrupt_count = corrupt.count()

    if corrupt_count:
        write_table(corrupt, cfg.path("quarantine", f"{source}_unparseable"), partition_by=["ingest_date"])

    write_table(
        bronze.where(F.col(CORRUPT_COL).isNull()).drop(CORRUPT_COL),
        cfg.path("bronze", source),
        partition_by=["ingest_date"],
    )
    bronze.unpersist()
    return {
        "rows_in": total,
        "rows_out": total - corrupt_count,
        "rows_quarantined": corrupt_count,
        "skipped": False,
    }
