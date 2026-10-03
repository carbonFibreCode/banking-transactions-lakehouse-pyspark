"""Declarative, config-driven data quality framework.

Each rule from the YAML config becomes a row-level check. All checks for a dataset are
evaluated in a single pass, so cost does not grow with the number of rules. Each row gets a
`_dq_errors` array naming the error-severity rules it failed:

* rows with any error  -> quarantine table (with the reasons attached)
* rows with only warns -> pass through, warnings counted in the DQ report
* if the error rate exceeds `max_error_rate` -> DataQualityError, nothing is published
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

DQ_ERRORS = "_dq_errors"
DQ_WARNINGS = "_dq_warnings"


class DataQualityError(RuntimeError):
    """Raised when a dataset breaches its error-rate threshold."""


@dataclass
class RuleResult:
    dataset: str
    rule: str
    rule_type: str
    severity: str
    failed_rows: int
    total_rows: int

    @property
    def pass_rate(self) -> float:
        return 1.0 if self.total_rows == 0 else 1 - self.failed_rows / self.total_rows


@dataclass
class QualityOutcome:
    valid: DataFrame
    quarantined: DataFrame
    results: list[RuleResult]
    total_rows: int
    error_rows: int
    _cached: DataFrame | None = None

    def release(self) -> None:
        """Free the cached check results once valid/quarantined have been written."""
        if self._cached is not None:
            self._cached.unpersist()

    @property
    def error_rate(self) -> float:
        return 0.0 if self.total_rows == 0 else self.error_rows / self.total_rows


def _failure_condition(
    df: DataFrame, rule: dict[str, Any], refs: dict[str, DataFrame]
) -> tuple[DataFrame, Column]:
    """Return (possibly enriched df, boolean column that is True when the row FAILS the rule)."""
    rtype = rule["type"]

    if rtype == "not_null":
        cond = F.lit(False)
        for c in rule["columns"]:
            cond = cond | F.col(c).isNull()
        return df, cond

    if rtype == "range":
        c = F.col(rule["column"])
        cond = F.lit(False)
        if "min" in rule:
            cond = cond | (c < F.lit(rule["min"]))
        if "max" in rule:
            cond = cond | (c > F.lit(rule["max"]))
        # nulls are the job of not_null rules, not range rules
        return df, F.coalesce(cond, F.lit(False))

    if rtype == "accepted_values":
        c = F.col(rule["column"])
        return df, c.isNotNull() & ~c.isin(rule["values"])

    if rtype == "regex":
        c = F.col(rule["column"])
        return df, c.isNotNull() & ~c.rlike(rule["pattern"])

    if rtype == "expression":
        return df, ~F.coalesce(F.expr(rule["expr"]), F.lit(True))

    if rtype == "unique":
        w = Window.partitionBy(*rule["columns"])
        flag = f"__dup_{rule['name']}"
        return df.withColumn(flag, F.count(F.lit(1)).over(w) > 1), F.col(flag)

    if rtype == "foreign_key":
        ref = refs.get(rule["ref_table"])
        if ref is None:
            raise ValueError(f"Rule {rule['name']} needs reference table '{rule['ref_table']}'")
        flag = f"__fk_{rule['name']}"
        keys = ref.select(F.col(rule["ref_column"]).alias(flag)).where(F.col(flag).isNotNull()).distinct()
        joined = df.join(F.broadcast(keys), df[rule["column"]] == keys[flag], "left")
        return joined, F.col(rule["column"]).isNotNull() & F.col(flag).isNull()

    raise ValueError(f"Unknown rule type '{rtype}' in rule {rule.get('name')}")


def apply_quality(
    df: DataFrame,
    dataset: str,
    config: dict[str, Any],
    refs: dict[str, DataFrame] | None = None,
    enforce_threshold: bool = True,
) -> QualityOutcome:
    refs = refs or {}
    original_cols = df.columns
    error_flags: list[Column] = []
    warn_flags: list[Column] = []
    rules = config.get("rules", [])

    for rule in rules:
        df, failed = _failure_condition(df, rule, refs)
        tagged = F.when(failed, F.lit(rule["name"]))
        (error_flags if rule.get("severity", "error") == "error" else warn_flags).append(tagged)

    def _collect(flags: list[Column]) -> Column:
        if not flags:
            return F.array().cast("array<string>")
        return F.filter(F.array(*flags), lambda x: x.isNotNull())

    checked = df.select(
        *original_cols, _collect(error_flags).alias(DQ_ERRORS), _collect(warn_flags).alias(DQ_WARNINGS)
    )
    # Cached because it feeds three outputs (stats, valid, quarantine); otherwise the source
    # and every join would be recomputed for each.
    checked = checked.persist()

    agg_exprs = [
        F.count(F.lit(1)).alias("__total"),
        F.sum((F.size(DQ_ERRORS) > 0).cast("long")).alias("__errors"),
    ]
    for rule in rules:
        col = DQ_ERRORS if rule.get("severity", "error") == "error" else DQ_WARNINGS
        agg_exprs.append(F.sum(F.array_contains(col, rule["name"]).cast("long")).alias(rule["name"]))
    stats = checked.agg(*agg_exprs).first().asDict()

    total = int(stats["__total"])
    error_rows = int(stats["__errors"] or 0)
    results = [
        RuleResult(
            dataset, r["name"], r["type"], r.get("severity", "error"), int(stats[r["name"]] or 0), total
        )
        for r in rules
    ]
    outcome = QualityOutcome(
        valid=checked.where(F.size(DQ_ERRORS) == 0).drop(DQ_ERRORS, DQ_WARNINGS),
        quarantined=checked.where(F.size(DQ_ERRORS) > 0).drop(DQ_WARNINGS),
        results=results,
        total_rows=total,
        error_rows=error_rows,
        _cached=checked,
    )

    max_rate = float(config.get("max_error_rate", 1.0))
    if enforce_threshold and outcome.error_rate > max_rate:
        outcome.release()
        failing = ", ".join(
            f"{r.rule}={r.failed_rows}" for r in results if r.failed_rows and r.severity == "error"
        )
        raise DataQualityError(
            f"{dataset}: error rate {outcome.error_rate:.2%} exceeds threshold {max_rate:.2%} ({failing})"
        )
    return outcome


def results_to_df(spark, results: list[RuleResult], batch_id: str, run_date: str) -> DataFrame:
    rows = [
        (
            batch_id,
            run_date,
            r.dataset,
            r.rule,
            r.rule_type,
            r.severity,
            r.failed_rows,
            r.total_rows,
            r.pass_rate,
        )
        for r in results
    ]
    schema = (
        "batch_id STRING, run_date STRING, dataset STRING, rule STRING, rule_type STRING, severity STRING, "
        "failed_rows LONG, total_rows LONG, pass_rate DOUBLE"
    )
    return spark.createDataFrame(rows, schema).withColumn("checked_at", F.current_timestamp())
