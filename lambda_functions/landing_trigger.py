"""S3 -> Lambda -> Glue: run the pipeline as soon as a day's transaction feed is complete."""

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
            logger.warning(json.dumps({"msg": "glue busy, will retry", "run_date": run_date}))
            raise
        started.append({"run_date": run_date, "job_run_id": response["JobRunId"]})
        logger.info(json.dumps({"msg": "glue job started", **started[-1]}))
    return {"started": started}
