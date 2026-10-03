"""Source-to-target reconciliation.

Every bronze row for a batch must be accounted for in exactly one place: published to
silver, quarantined, or dropped as a duplicate. Row counts and monetary totals must
both balance; if they don't, the batch is failed rather than silently losing money.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class ReconciliationError(RuntimeError):
    pass


@dataclass
class ReconciliationResult:
    dataset: str
    source_rows: int
    target_rows: int
    source_amount: Decimal
    target_amount: Decimal

    @property
    def balanced(self) -> bool:
        return self.source_rows == self.target_rows and self.source_amount == self.target_amount

    def as_row(self, batch_id: str, run_date: str) -> tuple:
        return (
            batch_id, run_date, self.dataset, self.source_rows, self.target_rows,
            str(self.source_amount), str(self.target_amount), self.balanced,
        )  # fmt: skip


RECON_SCHEMA = (
    "batch_id STRING, run_date STRING, dataset STRING, source_rows LONG, target_rows LONG, "
    "source_amount STRING, target_amount STRING, balanced BOOLEAN"
)


def _profile(df: DataFrame, amount_col: str | None) -> tuple[int, Decimal]:
    amount_expr = F.coalesce(F.sum(amount_col), F.lit(0)) if amount_col else F.lit(0)
    row = df.agg(F.count(F.lit(1)).alias("n"), amount_expr.alias("amt")).first()
    return int(row["n"]), Decimal(str(row["amt"]))


def reconcile(
    dataset: str,
    source: DataFrame,
    targets: list[DataFrame],
    amount_col: str | None = None,
    raise_on_mismatch: bool = True,
) -> ReconciliationResult:
    src_rows, src_amt = _profile(source, amount_col)
    tgt_rows, tgt_amt = 0, Decimal(0)
    for t in targets:
        n, amt = _profile(t, amount_col)
        tgt_rows += n
        tgt_amt += amt

    result = ReconciliationResult(dataset, src_rows, tgt_rows, src_amt, tgt_amt)
    if raise_on_mismatch and not result.balanced:
        raise ReconciliationError(
            f"{dataset}: source {src_rows} rows / {src_amt} vs targets {tgt_rows} rows / {tgt_amt}"
        )
    return result
