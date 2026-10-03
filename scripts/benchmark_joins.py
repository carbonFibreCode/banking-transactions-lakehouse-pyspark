"""Compare join strategies on a skewed key and record the timings in docs/performance.md.

Scenario: a large fact table where one key holds `--hot-share` of all rows, joined to a
dimension that is too big to broadcast by default. Strategies compared:

1. sort-merge join, AQE off        - the naive baseline; one task gets the hot key
2. sort-merge join, AQE skew join  - Spark splits the skewed partition at runtime
3. manual salting                  - spread the hot key over N buckets ourselves
4. broadcast join                  - avoid the shuffle altogether (only if the dim fits in memory)

    python scripts/benchmark_joins.py --rows 20000000
"""

from __future__ import annotations

import argparse
import time

from pyspark.sql import functions as F

from lakehouse.common.spark import get_spark
from lakehouse.transform.common import salted_join


def timed(label: str, fn) -> float:
    start = time.perf_counter()
    fn()
    elapsed = time.perf_counter() - start
    print(f"{label:<40} {elapsed:8.2f}s")
    return elapsed


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=20_000_000)
    parser.add_argument("--dim-rows", type=int, default=2_000_000)
    parser.add_argument("--hot-share", type=float, default=0.3)
    parser.add_argument("--salt", type=int, default=32)
    args = parser.parse_args()

    spark = get_spark("join-benchmark", shuffle_partitions=64)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # force shuffle joins unless asked

    facts = spark.range(args.rows).select(
        F.when(F.rand(1) < args.hot_share, F.lit(0))
        .otherwise((F.rand(2) * args.dim_rows).cast("long"))
        .alias("k"),
        (F.rand(3) * 100).alias("amount"),
    )
    dim = spark.range(args.dim_rows).select(F.col("id").alias("k"), (F.col("id") % 50).alias("category"))

    # Materialise inputs once so every strategy reads the same cached data.
    facts = facts.cache()
    dim = dim.cache()
    facts.count(), dim.count()

    def agg(df):
        df.groupBy("category").agg(F.sum("amount")).collect()

    results = {}
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    results["Sort-merge join, AQE off"] = timed("sort-merge, AQE off", lambda: agg(facts.join(dim, "k")))

    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    results["Sort-merge join, AQE skew-join on"] = timed(
        "sort-merge, AQE skew join", lambda: agg(facts.join(dim, "k"))
    )

    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")
    results[f"Manual salting ({args.salt} buckets), AQE skew-join off"] = timed(
        "salted join", lambda: agg(salted_join(facts, dim, "k", salt_buckets=args.salt))
    )

    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    results["Broadcast join"] = timed("broadcast", lambda: agg(facts.join(F.broadcast(dim), "k")))

    print("\n| Strategy | Time (s) |\n|---|---|")
    for label, secs in results.items():
        print(f"| {label} | {secs:.1f} |")
    spark.stop()


if __name__ == "__main__":
    main()
