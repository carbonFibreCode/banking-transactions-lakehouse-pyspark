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
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if master:
        builder = builder.master(master)
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
    """Fully replace a table whose new content is derived from the table itself."""
    staging = f"{path.rstrip('/')}__staging"
    df.write.mode("overwrite").parquet(staging)
    spark.read.parquet(staging).write.mode("overwrite").parquet(path)
    _delete_path(spark, staging)


def _delete_path(spark: SparkSession, path: str) -> None:
    jvm = spark.sparkContext._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = hadoop_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    fs.delete(hadoop_path, True)
