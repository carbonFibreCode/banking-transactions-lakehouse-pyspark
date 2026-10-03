"""Gold layer: business-ready aggregates and an ML feature table.

* customer_daily_spend      - spend per customer per day, for reporting and BI
* merchant_category_daily   - spend by merchant category per day
* txn_fraud_features        - per-transaction behavioural features for the fraud model

Late-arriving transactions are handled by recomputing every txn_date that the current batch
touched, so gold always reflects all data received so far.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from lakehouse.common.audit import StageMetrics
from lakehouse.common.config import PipelineConfig
from lakehouse.common.spark import write_table
from lakehouse.transform.common import shift_date

SECONDS_PER_DAY = 86_400


def _silver(spark: SparkSession, cfg: PipelineConfig, table: str) -> DataFrame:
    return spark.read.parquet(cfg.path("silver", table))


def _touched_txns(spark: SparkSession, cfg: PipelineConfig, run_date: str) -> tuple[DataFrame, list[str]]:
    """All completed silver transactions on the txn_dates touched by this batch."""
    txns = _silver(spark, cfg, "transactions")
    touched = [
        r[0]
        for r in txns.where(F.col("ingest_date") == run_date)
        .select(F.date_format("txn_date", "yyyy-MM-dd"))
        .distinct()
        .collect()
    ]
    if not touched:
        return txns.limit(0), []
    # A transaction is always ingested on or after its txn_date, so this scan is complete.
    window = txns.where((F.col("ingest_date") >= min(touched)) & (F.col("ingest_date") <= run_date))
    affected = window.where(F.date_format("txn_date", "yyyy-MM-dd").isin(touched)).where(
        F.col("status") == "COMPLETED"
    )
    return affected, touched


def build_customer_daily_spend(txns: DataFrame, accounts: DataFrame) -> DataFrame:
    # Accounts is a small dimension: broadcasting avoids shuffling the transaction fact table.
    acct = F.broadcast(accounts.select("account_id", "customer_id"))
    return (
        txns.join(acct, "account_id")
        .groupBy("customer_id", "txn_date")
        .agg(
            F.count(F.lit(1)).alias("txn_count"),
            F.sum("amount_gbp").alias("total_spend_gbp"),
            F.round(F.avg("amount_gbp"), 2).cast("decimal(18,2)").alias("avg_txn_gbp"),
            F.max("amount_gbp").alias("max_txn_gbp"),
            F.sum(F.when(F.col("channel") == "ATM", F.col("amount_gbp")).otherwise(0)).alias(
                "atm_withdrawal_gbp"
            ),
            F.sum(F.when(F.col("channel") == "ONLINE", F.col("amount_gbp")).otherwise(0)).alias(
                "online_spend_gbp"
            ),
            F.countDistinct("merchant_id").alias("distinct_merchants"),
        )
        .withColumn("txn_date", F.date_format("txn_date", "yyyy-MM-dd"))
    )


def build_merchant_category_daily(txns: DataFrame, merchants: DataFrame) -> DataFrame:
    merch = F.broadcast(merchants.select("merchant_id", "merchant_category"))
    return (
        txns.join(merch, "merchant_id", "left")
        .fillna({"merchant_category": "UNKNOWN"})
        .groupBy("merchant_category", "txn_date")
        .agg(
            F.count(F.lit(1)).alias("txn_count"),
            F.sum("amount_gbp").alias("total_spend_gbp"),
            F.approx_count_distinct("account_id").alias("approx_unique_accounts"),
        )
        .withColumn("txn_date", F.date_format("txn_date", "yyyy-MM-dd"))
    )


def build_fraud_features(history: DataFrame, lookback_days: int) -> DataFrame:
    """Behavioural features per transaction, computed only from that account's PRIOR activity
    (no look-ahead), so the same features can be used for training and real-time scoring."""
    ts = F.col("txn_ts").cast("long")
    by_account = Window.partitionBy("account_id").orderBy(ts)
    last_1h = by_account.rangeBetween(-3600, -1)
    last_24h = by_account.rangeBetween(-SECONDS_PER_DAY, -1)
    lookback = by_account.rangeBetween(-lookback_days * SECONDS_PER_DAY, -1)
    first_seen = Window.partitionBy("account_id", "merchant_id").orderBy("txn_ts", "txn_id")

    avg_lookback = F.avg("amount_gbp").over(lookback)
    return history.select(
        "txn_id", "account_id", "merchant_id", "txn_ts", "amount_gbp", "channel", "ingest_date",
        F.count(F.lit(1)).over(last_1h).alias("txn_count_prev_1h"),
        F.count(F.lit(1)).over(last_24h).alias("txn_count_prev_24h"),
        F.coalesce(F.sum("amount_gbp").over(last_24h), F.lit(0)).alias("spend_prev_24h_gbp"),
        F.round(avg_lookback, 2).alias(f"avg_amount_prev_{lookback_days}d_gbp"),
        F.round(F.col("amount_gbp") / avg_lookback, 3).alias("amount_to_avg_ratio"),
        (ts - F.lag(ts).over(by_account)).alias("seconds_since_prev_txn"),
        (F.row_number().over(first_seen) == 1).alias("is_first_txn_at_merchant"),
        F.hour("txn_ts").between(0, 5).alias("is_night_txn"),
        (F.col("currency") != "GBP").alias("is_foreign_currency"),
    )  # fmt: skip


def build_gold(spark: SparkSession, cfg: PipelineConfig, run_date: str, metrics: StageMetrics) -> None:
    affected, touched = _touched_txns(spark, cfg, run_date)
    if not touched:
        return

    accounts = _silver(spark, cfg, "accounts")
    merchants = _silver(spark, cfg, "merchants")

    daily = build_customer_daily_spend(affected, accounts)
    write_table(daily, cfg.path("gold", "customer_daily_spend"), partition_by=["txn_date"])
    write_table(
        build_merchant_category_daily(affected, merchants),
        cfg.path("gold", "merchant_category_daily"),
        partition_by=["txn_date"],
    )

    lookback = cfg.processing["feature_lookback_days"]
    history_start = shift_date(run_date, -(lookback + cfg.processing["late_arrival_days"]))
    history = _silver(spark, cfg, "transactions").where(
        (F.col("ingest_date") >= history_start) & (F.col("ingest_date") <= run_date)
    )
    features = build_fraud_features(history, lookback).where(F.col("ingest_date") == run_date)
    write_table(features, cfg.path("gold", "txn_fraud_features"), partition_by=["ingest_date"])

    metrics.rows_out = (
        spark.read.parquet(cfg.path("gold", "txn_fraud_features"))
        .where(F.col("ingest_date") == run_date)
        .count()
    )
    metrics.extra.update(txn_dates_recomputed=sorted(touched))
