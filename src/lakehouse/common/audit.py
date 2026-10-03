"""Structured logging and the audit trail.

Every stage records one row per dataset in `audit/pipeline_runs`: who ran what, for which
run date, how many rows went in/out/quarantined, and whether it succeeded. Logs are JSON so
CloudWatch Logs Insights can query them directly.
"""

from __future__ import annotations

import json
import logging
import sys
import time
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from lakehouse.common.config import PipelineConfig


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        payload.update(getattr(record, "context", {}))
        return json.dumps(payload, default=str)


def get_logger(name: str = "lakehouse") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def new_batch_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8]


AUDIT_SCHEMA = T.StructType(
    [
        T.StructField("batch_id", T.StringType()),
        T.StructField("run_date", T.StringType()),
        T.StructField("stage", T.StringType()),
        T.StructField("dataset", T.StringType()),
        T.StructField("status", T.StringType()),
        T.StructField("rows_in", T.LongType()),
        T.StructField("rows_out", T.LongType()),
        T.StructField("rows_quarantined", T.LongType()),
        T.StructField("duration_s", T.DoubleType()),
        T.StructField("error", T.StringType()),
        T.StructField("logged_at", T.TimestampType()),
    ]
)


@dataclass
class StageMetrics:
    rows_in: int = 0
    rows_out: int = 0
    rows_quarantined: int = 0
    extra: dict = field(default_factory=dict)


@contextmanager
def audited(spark: SparkSession, cfg: PipelineConfig, batch_id: str, run_date: str, stage: str, dataset: str):
    """Time a unit of work and append its outcome to the audit table, even when it fails."""
    log = get_logger()
    metrics = StageMetrics()
    started = time.perf_counter()
    status, error = "SUCCESS", None
    ctx = {"batch_id": batch_id, "run_date": run_date, "stage": stage, "dataset": dataset}
    log.info("stage started", extra={"context": ctx})
    try:
        yield metrics
    except Exception as exc:
        status, error = "FAILED", f"{type(exc).__name__}: {exc}"[:2000]
        raise
    finally:
        duration = round(time.perf_counter() - started, 2)
        row = (
            batch_id, run_date, stage, dataset, status,
            metrics.rows_in, metrics.rows_out, metrics.rows_quarantined,
            duration, error, datetime.now(timezone.utc).replace(tzinfo=None),
        )  # fmt: skip
        try:
            spark.createDataFrame([row], AUDIT_SCHEMA).write.mode("append").parquet(
                cfg.path("audit", "pipeline_runs")
            )
        except Exception as audit_exc:  # never mask the original failure
            log.error("audit write failed", extra={"context": {**ctx, "audit_error": str(audit_exc)[:500]}})
        (log.error if error else log.info)(
            "stage finished",
            extra={
                "context": {
                    **ctx,
                    "status": status,
                    "duration_s": duration,
                    "error": error,
                    **asdict(metrics),
                }
            },
        )
