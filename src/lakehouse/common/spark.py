"""SparkSession factory and table I/O helpers shared by every stage."""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException


def get_spark(
    app_name: str = "lakehouse", shuffle_partitions: int = 64, master: str | None = None
) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        # Adaptive Query Execution: coalesces small shuffle partitions and splits skewed
        # join partitions at runtime.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        # Only the partitions present in the written DataFrame are replaced, which makes
        # every stage safely re-runnable for a given run date.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # Keep partition values (ingest_date, txn_date) as strings so filters behave the
        # same whether a table was just written or read back from storage.
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        # stdout carries JSON logs only (CloudWatch-friendly); no console progress bars
        .config("spark.ui.showConsoleProgress", "false")
    )
    if master:
        builder = builder.master(master)
    # Only takes effect when this process starts the JVM (local runs, tests). On Glue/EMR,
    # memory is set by the cluster's worker type and spark-submit flags.
    builder = builder.config("spark.driver.memory", os.environ.get("LAKEHOUSE_DRIVER_MEMORY", "4g"))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def read_table(spark: SparkSession, path: str) -> DataFrame | None:
    """Read a Parquet table, returning None if it has never been written."""
    try:
        return spark.read.parquet(path)
    except AnalysisException as exc:
        if (
            "PATH_NOT_FOUND" in str(exc)
            or "Path does not exist" in str(exc)
            or "UNABLE_TO_INFER_SCHEMA" in str(exc)
        ):
            return None
        raise


def write_table(
    df: DataFrame, path: str, partition_by: list[str] | None = None, mode: str = "overwrite"
) -> None:
    writer = df.write.mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(path)


def replace_table(spark: SparkSession, df: DataFrame, path: str) -> None:
    """Fully replace a table whose new content is derived from the table itself.

    Spark cannot overwrite a Parquet path it is lazily reading from, so the result is staged
    first. On a table format with ACID MERGE (Delta Lake / Iceberg) this becomes a single
    MERGE statement.
    """
    staging = f"{path.rstrip('/')}__staging"
    df.write.mode("overwrite").parquet(staging)
    spark.read.parquet(staging).write.mode("overwrite").parquet(path)
    _delete_path(spark, staging)


def _delete_path(spark: SparkSession, path: str) -> None:
    jvm = spark.sparkContext._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = hadoop_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    fs.delete(hadoop_path, True)
