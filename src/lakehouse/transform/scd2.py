"""Slowly Changing Dimension Type 2, implemented with plain DataFrame operations.

Each business key has exactly one current row (is_current = true, effective_to = null). When
a tracked attribute changes, the current row is closed at the change timestamp and a new
version is opened. A change feed record older than the current version is ignored as stale,
so out-of-order or replayed records cannot rewrite history. Re-applying the same feed is a
no-op because unchanged hashes produce no new versions.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SCD_COLS = ["customer_sk", "effective_from", "effective_to", "is_current", "_batch_id"]


@dataclass
class Scd2Result:
    dimension: DataFrame
    inserted: int
    updated: int
    stale: int


def _new_versions(df: DataFrame, key: str, change_ts: str, batch_id: str) -> DataFrame:
    return (
        df.withColumn("customer_sk", F.xxhash64(F.col(key), F.col(change_ts)))
        .withColumn("effective_from", F.col(change_ts))
        .withColumn("effective_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .withColumn("_batch_id", F.lit(batch_id))
    )


def apply_scd2(
    current_dim: DataFrame | None, changes: DataFrame, key: str, change_ts: str, batch_id: str
) -> Scd2Result:
    if current_dim is None:
        dim = _new_versions(changes, key, change_ts, batch_id)
        return Scd2Result(dim, inserted=changes.count(), updated=0, stale=0)

    current = current_dim.where("is_current")
    history = current_dim.where("NOT is_current")
    cols = current_dim.columns

    c = current.select(
        key, F.col("row_hash").alias("__cur_hash"), F.col("effective_from").alias("__cur_from")
    )
    matched = changes.join(c, key, "left")

    new_keys = matched.where(F.col("__cur_hash").isNull())
    differs = F.col("__cur_hash").isNotNull() & (F.col("row_hash") != F.col("__cur_hash"))
    updates = matched.where(differs & (F.col(change_ts) > F.col("__cur_from")))
    stale = matched.where(differs & (F.col(change_ts) <= F.col("__cur_from")))

    update_ts = updates.select(key, F.col(change_ts).alias("__closed_at"))
    expired = (
        current.join(update_ts, key, "inner")
        .withColumn("effective_to", F.col("__closed_at"))
        .withColumn("is_current", F.lit(False))
        .select(*cols)
    )
    untouched = current.join(update_ts, key, "left_anti")

    incoming = new_keys.unionByName(updates).drop("__cur_hash", "__cur_from")
    opened = _new_versions(incoming, key, change_ts, batch_id).select(*cols)

    dim = history.unionByName(expired).unionByName(untouched).unionByName(opened)
    return Scd2Result(dim, inserted=new_keys.count(), updated=updates.count(), stale=stale.count())
