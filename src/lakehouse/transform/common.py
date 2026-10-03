"""Reusable transformation building blocks shared by silver and gold jobs."""

from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def shift_date(run_date: str, days: int) -> str:
    return (date.fromisoformat(run_date) + timedelta(days=days)).isoformat()


def dedupe_latest(df: DataFrame, keys: list[str], order_by: list[Column]) -> tuple[DataFrame, DataFrame]:
    """Keep the latest row per business key. Returns (kept, duplicates).

    Rows with a null key are never treated as duplicates of each other; they flow on to the
    quality checks, which quarantine them with an explicit reason.
    """
    null_key = F.lit(False)
    for k in keys:
        null_key = null_key | F.col(k).isNull()

    w = Window.partitionBy(*keys).orderBy(*order_by)
    ranked = df.withColumn("__rn", F.when(null_key, F.lit(1)).otherwise(F.row_number().over(w)))
    return ranked.where("__rn = 1").drop("__rn"), ranked.where("__rn > 1").drop("__rn")


def standardise_codes(df: DataFrame, columns: list[str]) -> DataFrame:
    """Trim and upper-case code columns such as currency or channel."""
    return df.select(*[F.upper(F.trim(F.col(c))).alias(c) if c in columns else F.col(c) for c in df.columns])


def mask_pii(df: DataFrame, pii: dict[str, list[str]]) -> DataFrame:
    """Hash, mask, or drop PII columns as declared in config.

    hash -> <col>_hash  : SHA-256 of the normalised value. Still joinable, not readable.
    mask -> <col>_masked: all but the last 4 characters replaced.
    drop -> removed.
    """
    for c in pii.get("hash", []):
        df = df.withColumn(f"{c}_hash", F.sha2(F.lower(F.trim(F.col(c))), 256)).drop(c)
    for c in pii.get("mask", []):
        df = df.withColumn(
            f"{c}_masked",
            F.when(F.col(c).isNull(), None).otherwise(
                # SQL expression keeps this compatible with Spark 3.5 (AWS Glue 5.0)
                F.expr(f"concat(repeat('*', greatest(length(`{c}`) - 4, 0)), right(`{c}`, 4))")
            ),
        ).drop(c)
    return df.drop(*pii.get("drop", []))


def convert_to_gbp(spark: SparkSession, df: DataFrame, fx_rates: dict[str, float]) -> DataFrame:
    """Add amount_gbp via a broadcast join to the (tiny) FX rate table."""
    rates = spark.createDataFrame(list(fx_rates.items()), "fx_currency STRING, fx_rate DOUBLE")
    joined = df.join(F.broadcast(rates), df["currency"] == rates["fx_currency"], "left")
    return joined.withColumn(
        "amount_gbp",
        F.round(F.col("amount") * F.col("fx_rate").cast("decimal(18,6)"), 2).cast("decimal(18,2)"),
    ).drop("fx_currency", "fx_rate")


def salted_join(
    left: DataFrame, right: DataFrame, key: str, salt_buckets: int = 16, how: str = "inner"
) -> DataFrame:
    """Join on a heavily skewed key by spreading each hot key over `salt_buckets` partitions.

    The large (skewed) side gets a random salt; the smaller side is replicated once per salt
    value. AQE's skew-join handling usually covers this in Spark 3+/4, but explicit salting
    is still needed for aggregations on skewed keys and for engines without AQE.
    """
    salted_left = left.withColumn("__salt", (F.rand(seed=42) * salt_buckets).cast("int"))
    salted_right = right.withColumn("__salt", F.explode(F.sequence(F.lit(0), F.lit(salt_buckets - 1))))
    return salted_left.join(salted_right, [key, "__salt"], how).drop("__salt")
