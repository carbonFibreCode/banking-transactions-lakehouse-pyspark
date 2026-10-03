"""End-to-end: generate a small but messy dataset, run the full pipeline for several days, and check the guarantees the platform makes: completeness, uniqueness, idempotency, history."""

from datetime import date

import pytest
from pyspark.sql import functions as F

from lakehouse.generate.synthetic import generate
from lakehouse.pipeline import run

DAYS = ["2026-09-01", "2026-09-02", "2026-09-03"]


@pytest.fixture(scope="module")
def lake(spark, tmp_path_factory):
    from lakehouse.common.config import load_config

    cfg = load_config(base_path=str(tmp_path_factory.mktemp("e2e") / "lake"))
    generate(
        spark,
        cfg.base_path,
        date(2026, 9, 1),
        days=3,
        txns_per_day=4000,
        n_customers=500,
        n_accounts=800,
        n_merchants=50,
        partitions=2,
    )
    for d in DAYS:
        run(spark, cfg, d)
    return cfg


def _read(spark, cfg, layer, table):
    return spark.read.parquet(cfg.path(layer, table))


def test_every_stage_succeeded(spark, lake):
    audit = _read(spark, lake, "audit", "pipeline_runs")
    assert audit.where("status != 'SUCCESS'").count() == 0
    assert audit.select("run_date").distinct().count() == len(DAYS)


def test_reconciliation_balanced_every_day(spark, lake):
    recon = _read(spark, lake, "audit", "reconciliation")
    assert recon.count() == len(DAYS)
    assert recon.where("NOT balanced").count() == 0


def test_silver_transactions_are_unique_and_clean(spark, lake):
    silver = _read(spark, lake, "silver", "transactions")
    assert silver.count() == silver.select("txn_id").distinct().count()
    assert silver.where("amount <= 0 OR currency NOT IN ('GBP','USD','EUR','INR')").count() == 0
    dups = _read(spark, lake, "quarantine", "transactions_duplicates")
    assert (
        dups.where(
            "ingest_date > '2026-09-01' AND substring(txn_id, 2, 8) < date_format(ingest_date, 'yyyyMMdd')"
        ).count()
        > 0
    )


def test_defects_quarantined_with_reasons(spark, lake):
    q = _read(spark, lake, "quarantine", "transactions")
    reasons = {r[0] for r in q.select(F.explode("_dq_errors")).distinct().collect()}
    assert {
        "txn_required_fields",
        "txn_amount_positive",
        "txn_currency_valid",
        "txn_account_exists",
    } <= reasons
    assert _read(spark, lake, "quarantine", "transactions_unparseable").count() == 3 * len(DAYS)


def test_customer_dimension_has_one_current_row_and_no_raw_pii(spark, lake):
    dim = _read(spark, lake, "silver", "customers_scd2")
    current = dim.where("is_current")
    assert current.count() == current.select("customer_id").distinct().count()
    assert dim.where("NOT is_current").count() > 0
    assert not {"email", "phone", "first_name", "last_name"} & set(dim.columns)


def test_rerun_is_idempotent(spark, lake):
    def snapshot():
        return {
            t: _read(spark, lake, layer, t).count()
            for layer, t in [
                ("silver", "transactions"),
                ("silver", "customers_scd2"),
                ("gold", "customer_daily_spend"),
                ("gold", "txn_fraud_features"),
            ]
        }

    before = snapshot()
    run(spark, lake, DAYS[-1])
    assert snapshot() == before


def test_gold_matches_silver(spark, lake):
    silver = _read(spark, lake, "silver", "transactions").where("status = 'COMPLETED'")
    accounts = _read(spark, lake, "silver", "accounts")
    expected = silver.join(accounts, "account_id").agg(F.sum("amount_gbp")).first()[0]
    actual = _read(spark, lake, "gold", "customer_daily_spend").agg(F.sum("total_spend_gbp")).first()[0]
    assert actual == expected
