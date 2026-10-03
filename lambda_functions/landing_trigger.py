"""S3 -> Lambda -> Glue: run the pipeline as soon as a day's transaction feed is complete.

This is the event-driven alternative to the scheduled Airflow DAG: same Glue job, same
idempotent stages, triggered by data arrival instead of the clock.

Upstream writes files under landing/transactions/ingest_date=YYYY-MM-DD/ and finishes with a
_SUCCESS marker. The bucket notification is filtered to that suffix, so this function fires
once per delivered batch instead of once per file.
"""

from __future__ import annotations

import json
import logging
import os
import re
import urllib.parse

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

GLUE_JOB_NAME = os.environ["GLUE_JOB_NAME"]
DATE_PATTERN = re.compile(r"ingest_date=(\d{4}-\d{2}-\d{2})/")

glue = boto3.client("glue")


def handler(event, _context):
    started = []
    for record in event.get("Records", []):
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        match = DATE_PATTERN.search(key)
        if not match or not key.endswith("_SUCCESS"):
            logger.info(json.dumps({"msg": "ignored object", "key": key}))
            continue

        run_date = match.group(1)
        try:
            response = glue.start_job_run(
                JobName=GLUE_JOB_NAME,
                Arguments={"--run_date": run_date, "--stage": "all"},
            )
        except glue.exceptions.ConcurrentRunsExceededException:
            # A run for this job is already in progress. Let it fail so S3 retries the event.
            logger.warning(json.dumps({"msg": "glue busy, will retry", "run_date": run_date}))
            raise
        started.append({"run_date": run_date, "job_run_id": response["JobRunId"]})
        logger.info(json.dumps({"msg": "glue job started", **started[-1]}))
    return {"started": started}
