"""Synthetic retail-banking data generator, built on Spark so it scales to tens of millions of rows."""

from __future__ import annotations

import argparse
from datetime import date, timedelta

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.common.config import load_config
from lakehouse.common.spark import get_spark

FIRST_NAMES = [
    "Aarav",
    "Priya",
    "James",
    "Olivia",
    "Rohan",
    "Emma",
    "Liam",
    "Ananya",
    "Noah",
    "Sophia",
    "Arjun",
    "Mia",
]
LAST_NAMES = [
    "Sharma",
    "Smith",
    "Patel",
    "Jones",
    "Reddy",
    "Taylor",
    "Iyer",
    "Brown",
    "Khan",
    "Wilson",
    "Rao",
    "Evans",
]
CITIES = [
    "London",
    "Manchester",
    "Birmingham",
    "Leeds",
    "Glasgow",
    "Bengaluru",
    "Pune",
    "Chennai",
    "Edinburgh",
    "Bristol",
]
CATEGORIES = ["GROCERY", "FUEL", "TRAVEL", "DINING", "ONLINE_RETAIL", "UTILITIES", "ENTERTAINMENT", "HEALTH"]
HOT_MERCHANT = "M0000001"


def _pick(values: list[str], seed: int) -> Column:
    """Uniformly pick one of `values` per row."""
    idx = (F.rand(seed) * len(values)).cast("int") + 1
    return F.element_at(F.array(*[F.lit(v) for v in values]), idx)


def _weighted(choices: list[tuple[str, float]], seed: int) -> Column:
    r = F.rand(seed)
    expr, cumulative = None, 0.0
    for value, weight in choices:
        cumulative += weight
        expr = F.when(r < cumulative, value) if expr is None else expr.when(r < cumulative, value)
    return expr.otherwise(choices[-1][0])


def _id(prefix: str, col: Column, width: int) -> Column:
    return F.concat(F.lit(prefix), F.lpad(col.cast("string"), width, "0"))


def customers(spark: SparkSession, n: int, as_of: date, seed: int, id_offset: int = 0) -> DataFrame:
    df = spark.range(id_offset + 1, id_offset + n + 1).select(
        _id("C", F.col("id"), 8).alias("customer_id"),
        _pick(FIRST_NAMES, seed).alias("first_name"),
        _pick(LAST_NAMES, seed + 1).alias("last_name"),
        F.col("id"),
    )
    return df.select(
        "customer_id",
        "first_name",
        "last_name",
        F.when(F.rand(seed + 2) < 0.005, F.concat(F.lower("first_name"), F.lit(".at.example")))
        .otherwise(
            F.concat(
                F.lower("first_name"), F.lit("."), F.lower("last_name"), F.col("id"), F.lit("@example.com")
            )
        )
        .alias("email"),
        F.concat(F.lit("07"), F.lpad((F.rand(seed + 3) * 1e9).cast("long").cast("string"), 9, "0")).alias(
            "phone"
        ),
        _pick(CITIES, seed + 4).alias("city"),
        _weighted(
            [("RETAIL", 0.8), ("PREMIER", 0.15), ("PRIVATE", 0.03), ("BUSINESS", 0.02)], seed + 5
        ).alias("segment"),
        F.lit(f"{as_of} 00:00:00").cast("timestamp").alias("updated_at"),
    )


def customer_changes(spark: SparkSession, n: int, day: date, day_index: int, seed: int) -> DataFrame:
    """~1% of existing customers change city/segment; ~0.2% brand-new customers."""
    base = customers(spark, n, day, seed=7)
    changed = (
        base.where(F.rand(seed) < 0.01)
        .withColumn("city", _pick(CITIES, seed + 1))
        .withColumn("segment", _weighted([("RETAIL", 0.5), ("PREMIER", 0.4), ("PRIVATE", 0.1)], seed + 2))
    )
    new_count = max(1, n // 500)
    new = customers(spark, new_count, day, seed + 3, id_offset=n + (day_index - 1) * new_count)
    change_ts = F.lit(f"{day} 00:00:00").cast("timestamp") + F.make_interval(
        secs=(F.rand(seed + 4) * 86000).cast("int")
    )
    return changed.unionByName(new).withColumn("updated_at", change_ts)


def accounts(spark: SparkSession, n_accounts: int, n_customers: int, as_of: date, seed: int) -> DataFrame:
    return spark.range(1, n_accounts + 1).select(
        _id("A", F.col("id"), 8).alias("account_id"),
        _id("C", (F.rand(seed) * n_customers).cast("long") + 1, 8).alias("customer_id"),
        F.when(F.rand(seed + 1) < 0.001, "UNKNOWN")
        .otherwise(_weighted([("CURRENT", 0.6), ("SAVINGS", 0.25), ("CREDIT_CARD", 0.15)], seed + 2))
        .alias("account_type"),
        F.date_sub(F.lit(as_of), (F.rand(seed + 3) * 3650).cast("int")).alias("opened_date"),
        F.lit("ACTIVE").alias("status"),
    )


def merchants(spark: SparkSession, n: int, seed: int) -> DataFrame:
    return spark.range(1, n + 1).select(
        _id("M", F.col("id"), 7).alias("merchant_id"),
        F.concat(F.lit("Merchant "), F.col("id")).alias("merchant_name"),
        _pick(CATEGORIES, seed).alias("merchant_category"),
        _weighted([("GB", 0.85), ("IN", 0.1), ("US", 0.05)], seed + 1).alias("country"),
    )


def transactions(
    spark: SparkSession, n: int, day: date, n_accounts: int, n_merchants: int, seed: int, partitions: int
) -> DataFrame:
    day_tag = day.strftime("%Y%m%d")
    start = F.lit(f"{day} 00:00:00").cast("timestamp")
    offset_s = (F.rand(seed) * 86_399).cast("int")
    late_days = F.when(F.rand(seed + 1) < 0.02, (F.rand(seed + 2) * 2).cast("int") + 1).otherwise(0)
    merchant = F.when(F.rand(seed + 3) < 0.25, F.lit(HOT_MERCHANT)).otherwise(
        _id("M", (F.rand(seed + 4) * n_merchants).cast("long") + 1, 7)
    )
    amount = F.round(F.exp(F.randn(seed + 5) + F.lit(3.0)), 2).cast("decimal(18,2)")

    df = spark.range(0, n, numPartitions=partitions).select(
        F.concat(F.lit(f"T{day_tag}"), F.lpad(F.col("id").cast("string"), 10, "0")).alias("txn_id"),
        _id("A", (F.rand(seed + 6) * n_accounts).cast("long") + 1, 8).alias("account_id"),
        merchant.alias("merchant_id"),
        amount.alias("amount"),
        _weighted([("GBP", 0.9), ("EUR", 0.05), ("USD", 0.03), ("INR", 0.02)], seed + 7).alias("currency"),
        _weighted([("CARD", 0.6), ("ONLINE", 0.3), ("ATM", 0.07), ("TRANSFER", 0.03)], seed + 8).alias(
            "channel"
        ),
        _weighted([("COMPLETED", 0.95), ("DECLINED", 0.04), ("REVERSED", 0.01)], seed + 9).alias("status"),
        (start + F.make_interval(days=-late_days, secs=offset_s)).alias("txn_ts"),
        F.rand(seed + 10).alias("__defect"),
    )

    d = F.col("__defect")
    df = df.select(
        "txn_id",
        F.when(d < 0.0005, F.lit("A99999999")).otherwise(F.col("account_id")).alias("account_id"),
        "merchant_id",
        F.when(d.between(0.0005, 0.0015), None)
        .when(d.between(0.0015, 0.002), -F.col("amount"))
        .otherwise(F.col("amount"))
        .alias("amount"),
        F.when(d.between(0.002, 0.0025), F.lit("XXX"))
        .when(d.between(0.0025, 0.0035), F.concat(F.lit(" "), F.lower("currency"), F.lit(" ")))
        .otherwise(F.col("currency"))
        .alias("currency"),
        "channel",
        "status",
        "txn_ts",
    )
    duplicates = df.where(F.rand(seed + 11) < 0.003)
    return df.unionByName(duplicates)


def write_landing(df: DataFrame, path: str, fmt: str) -> None:
    writer = df.write.mode("overwrite")
    if fmt == "csv":
        writer.option("header", "true").csv(path)
    else:
        writer.json(path)


def generate(
    spark: SparkSession,
    base_path: str,
    start: date,
    days: int,
    txns_per_day: int,
    n_customers: int,
    n_accounts: int,
    n_merchants: int,
    partitions: int = 16,
) -> None:
    landing = f"{base_path.rstrip('/')}/landing"

    def _dir(source: str, day: date) -> str:
        return f"{landing}/{source}/ingest_date={day.isoformat()}"

    write_landing(customers(spark, n_customers, start, seed=7), _dir("customers", start), "csv")
    write_landing(accounts(spark, n_accounts, n_customers, start, seed=11), _dir("accounts", start), "csv")
    write_landing(merchants(spark, n_merchants, seed=13), _dir("merchants", start), "csv")

    previous: DataFrame | None = None
    for i in range(days):
        day = start + timedelta(days=i)
        if i > 0:
            write_landing(
                customer_changes(spark, n_customers, day, day_index=i, seed=100 + i),
                _dir("customers", day),
                "csv",
            )

        txns = transactions(
            spark, txns_per_day, day, n_accounts, n_merchants, seed=1000 + i, partitions=partitions
        )
        if previous is not None:
            txns = txns.unionByName(previous.where(F.rand(99) < 0.001))
        path = _dir("transactions", day)
        write_landing(txns, path, "json")
        malformed = ['{"txn_id": "BROKEN-1", "amount": ', "not json at all", '{"txn_id": "BROKEN-2"']
        spark.createDataFrame([(m,) for m in malformed], "value STRING").coalesce(1).write.mode(
            "append"
        ).text(path)
        previous = transactions(
            spark, txns_per_day, day, n_accounts, n_merchants, seed=1000 + i, partitions=partitions
        )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--start-date", default=None, help="Default: <days> days before today")
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--txns-per-day", type=int, default=1_000_000)
    parser.add_argument("--customers", type=int, default=100_000)
    parser.add_argument("--accounts", type=int, default=150_000)
    parser.add_argument("--merchants", type=int, default=5_000)
    parser.add_argument("--partitions", type=int, default=16)
    parser.add_argument("--base-path", default=None)
    args = parser.parse_args(argv)

    cfg = load_config(base_path=args.base_path)
    start = (
        date.fromisoformat(args.start_date) if args.start_date else date.today() - timedelta(days=args.days)
    )
    spark = get_spark("lakehouse-generate")
    generate(
        spark,
        cfg.base_path,
        start,
        args.days,
        args.txns_per_day,
        args.customers,
        args.accounts,
        args.merchants,
        args.partitions,
    )
    print(f"Generated {args.days} day(s) from {start} under {cfg.base_path}/landing")
    spark.stop()


if __name__ == "__main__":
    main()
