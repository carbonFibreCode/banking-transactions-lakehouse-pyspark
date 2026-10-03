from datetime import datetime
from decimal import Decimal

from conftest import rows
from pyspark.sql import functions as F

from lakehouse.transform.common import convert_to_gbp, dedupe_latest, mask_pii, salted_join, standardise_codes
from lakehouse.transform.gold import build_fraud_features
from lakehouse.transform.scd2 import apply_scd2


def test_dedupe_keeps_latest_and_never_merges_null_keys(spark):
    df = spark.createDataFrame(
        [("T1", 1), ("T1", 3), ("T2", 1), (None, 1), (None, 2)], "txn_id STRING, version INT"
    )
    kept, dups = dedupe_latest(df, ["txn_id"], [F.col("version").desc()])
    assert set(rows(kept)) == {("T1", 3), ("T2", 1), (None, 1), (None, 2)}
    assert rows(dups) == [("T1", 1)]


def test_standardise_codes(spark):
    df = spark.createDataFrame([(" gbp ", "card", "x")], "currency STRING, channel STRING, note STRING")
    assert rows(standardise_codes(df, ["currency", "channel"])) == [("GBP", "CARD", "x")]


def test_mask_pii(spark):
    df = spark.createDataFrame(
        [("C1", " Ann@Example.com", "07123456789", "Ann")],
        "id STRING, email STRING, phone STRING, name STRING",
    )
    out = mask_pii(df, {"hash": ["email"], "mask": ["phone"], "drop": ["name"]}).first()
    assert out.asDict().keys() == {"id", "email_hash", "phone_masked"}
    assert out.phone_masked == "*******6789"
    # hash is case/space-insensitive so the same customer still joins across systems
    assert out.email_hash == spark.sql("SELECT sha2('ann@example.com', 256)").first()[0]


def test_convert_to_gbp(spark):
    df = spark.createDataFrame(
        [(Decimal("100.00"), "USD"), (Decimal("10.00"), "GBP")], "amount DECIMAL(18,2), currency STRING"
    )
    out = rows(convert_to_gbp(spark, df, {"GBP": 1.0, "USD": 0.79}), "currency", "amount_gbp")
    assert out == [("GBP", Decimal("10.00")), ("USD", Decimal("79.00"))]


def test_salted_join_matches_plain_join(spark):
    facts = spark.range(1000).select((F.col("id") % 3).cast("string").alias("k"), F.col("id"))
    dim = spark.createDataFrame([("0", "a"), ("1", "b"), ("2", "c")], "k STRING, v STRING")
    assert rows(salted_join(facts, dim, "k", salt_buckets=8)) == rows(facts.join(dim, "k"))


def _cust(spark, data):
    return spark.createDataFrame(
        data, "customer_id STRING, city STRING, row_hash STRING, updated_at TIMESTAMP"
    )


def test_scd2_insert_update_unchanged_and_stale(spark):
    t0, t1, t2 = datetime(2026, 9, 1), datetime(2026, 9, 2), datetime(2026, 9, 3)
    first = apply_scd2(
        None,
        _cust(spark, [("C1", "London", "h1", t0), ("C2", "Leeds", "h2", t0)]),
        "customer_id",
        "updated_at",
        "b1",
    )
    assert first.inserted == 2
    dim = first.dimension.localCheckpoint()

    second = apply_scd2(
        dim,
        _cust(spark, [
            ("C1", "Pune", "h1b", t2),      # moved -> new version
            ("C2", "Leeds", "h2", t1),      # same attributes -> no-op
            ("C3", "Bristol", "h3", t1),    # new customer
        ]),
        "customer_id", "updated_at", "b2",
    )  # fmt: skip
    assert (second.inserted, second.updated, second.stale) == (1, 1, 0)
    out = rows(second.dimension, "customer_id", "city", "is_current", "effective_to")
    assert out == [
        ("C1", "London", False, t2),
        ("C1", "Pune", True, None),
        ("C2", "Leeds", True, None),
        ("C3", "Bristol", True, None),
    ]

    # an out-of-order record older than the current version must not rewrite history
    third = apply_scd2(
        second.dimension.localCheckpoint(),
        _cust(spark, [("C1", "Chennai", "hx", t1)]),
        "customer_id",
        "updated_at",
        "b3",
    )
    assert (third.inserted, third.updated, third.stale) == (0, 0, 1)
    assert third.dimension.where("is_current AND customer_id = 'C1'").first().city == "Pune"


def test_fraud_features_use_only_prior_activity(spark):
    df = (
        spark.createDataFrame(
            [
                ("T1", "A1", "M1", datetime(2026, 9, 1, 10, 0), Decimal("10.00")),
                ("T2", "A1", "M1", datetime(2026, 9, 1, 10, 30), Decimal("30.00")),
                ("T3", "A1", "M2", datetime(2026, 9, 2, 3, 0), Decimal("100.00")),
            ],
            "txn_id STRING, account_id STRING, merchant_id STRING, txn_ts TIMESTAMP, amount_gbp DECIMAL(18,2)",
        )
        .withColumn("channel", F.lit("CARD"))
        .withColumn("currency", F.lit("GBP"))
        .withColumn("ingest_date", F.lit("2026-09-02"))
    )

    f = {r.txn_id: r for r in build_fraud_features(df, lookback_days=30).collect()}
    assert f["T1"].txn_count_prev_24h == 0 and f["T1"].amount_to_avg_ratio is None
    assert f["T2"].txn_count_prev_1h == 1 and f["T2"].seconds_since_prev_txn == 1800
    assert f["T2"].is_first_txn_at_merchant is False
    assert f["T3"].txn_count_prev_1h == 0 and f["T3"].txn_count_prev_24h == 2
    assert f["T3"].amount_to_avg_ratio == 5.0  # 100 vs average of 10 and 30
    assert f["T3"].is_night_txn and f["T3"].is_first_txn_at_merchant
