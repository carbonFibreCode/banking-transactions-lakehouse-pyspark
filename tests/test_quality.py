from decimal import Decimal

import pytest

from lakehouse.quality.reconciliation import ReconciliationError, reconcile
from lakehouse.quality.rules import DQ_ERRORS, DataQualityError, apply_quality

SCHEMA = "txn_id STRING, account_id STRING, amount DECIMAL(18,2), currency STRING"


def _txns(spark):
    return spark.createDataFrame(
        [
            ("T1", "A1", Decimal("10.00"), "GBP"),  # valid
            ("T2", None, Decimal("5.00"), "GBP"),  # null account
            ("T3", "A1", Decimal("-1.00"), "GBP"),  # negative
            ("T4", "A1", Decimal("3.00"), "XXX"),  # bad currency
            ("T5", "A9", Decimal("7.00"), "GBP"),  # orphan account
            ("T6", "A2", Decimal("8.00"), "GBP"),  # valid
        ],
        SCHEMA,
    )


RULES = {
    "max_error_rate": 1.0,
    "rules": [
        {"name": "required", "type": "not_null", "columns": ["txn_id", "account_id"], "severity": "error"},
        {"name": "positive", "type": "range", "column": "amount", "min": 0.01, "severity": "error"},
        {"name": "currency", "type": "accepted_values", "column": "currency", "values": ["GBP"], "severity": "error"},
        {"name": "fk", "type": "foreign_key", "column": "account_id", "ref_table": "accounts",
         "ref_column": "account_id", "severity": "error"},
        {"name": "big", "type": "expression", "expr": "amount < 9", "severity": "warn"},
    ],
}  # fmt: skip


def test_rows_are_split_with_reasons(spark):
    accounts = spark.createDataFrame([("A1",), ("A2",)], "account_id STRING")
    outcome = apply_quality(_txns(spark), "transactions", RULES, refs={"accounts": accounts})

    assert sorted(r.txn_id for r in outcome.valid.collect()) == ["T1", "T6"]
    reasons = {r.txn_id: r[DQ_ERRORS] for r in outcome.quarantined.collect()}
    assert reasons == {"T2": ["required"], "T3": ["positive"], "T4": ["currency"], "T5": ["fk"]}
    # validated output keeps the original schema
    assert outcome.valid.columns == _txns(spark).columns


def test_per_rule_counts_and_warnings(spark):
    accounts = spark.createDataFrame([("A1",), ("A2",)], "account_id STRING")
    outcome = apply_quality(_txns(spark), "transactions", RULES, refs={"accounts": accounts})
    counts = {r.rule: r.failed_rows for r in outcome.results}
    assert counts == {"required": 1, "positive": 1, "currency": 1, "fk": 1, "big": 1}
    assert outcome.error_rows == 4 and outcome.total_rows == 6


def test_circuit_breaker_stops_bad_batch(spark):
    accounts = spark.createDataFrame([("A1",), ("A2",)], "account_id STRING")
    with pytest.raises(DataQualityError, match="exceeds threshold"):
        apply_quality(
            _txns(spark), "transactions", {**RULES, "max_error_rate": 0.10}, refs={"accounts": accounts}
        )


def test_unique_and_regex_rules(spark):
    df = spark.createDataFrame(
        [("A1", "a@b.com"), ("A1", "bad"), ("A2", "c@d.co")], "id STRING, email STRING"
    )
    rules = {
        "rules": [
            {"name": "uniq", "type": "unique", "columns": ["id"], "severity": "error"},
            {"name": "email", "type": "regex", "column": "email", "pattern": r"^[^@\s]+@[^@\s]+\.[a-z]{2,}$",
             "severity": "warn"},
        ]
    }  # fmt: skip
    outcome = apply_quality(df, "x", rules)
    assert [r.id for r in outcome.valid.collect()] == ["A2"]
    assert {r.rule: r.failed_rows for r in outcome.results} == {"uniq": 2, "email": 1}


def test_reconciliation_balances_and_detects_loss(spark):
    src = _txns(spark)
    a, b = src.where("txn_id <= 'T3'"), src.where("txn_id > 'T3'")
    assert reconcile("t", src, [a, b], amount_col="amount").balanced

    with pytest.raises(ReconciliationError):
        reconcile("t", src, [a], amount_col="amount")
