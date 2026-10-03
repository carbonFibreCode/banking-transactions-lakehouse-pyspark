"""AWS Glue (PySpark) entry point.

The Glue job is a thin wrapper: it resolves job arguments, reuses Glue's SparkSession, and
calls the same `lakehouse.pipeline.run` that runs locally and in CI. The `lakehouse`
package is shipped as a wheel via --additional-python-modules / --extra-py-files.

Job parameters (set in infra/terraform/glue.tf):
    --run_date     YYYY-MM-DD (defaults to yesterday, UTC)
    --stage        all | bronze | silver_reference | silver_customers | silver_transactions | gold
    --config_path  s3://<artifacts-bucket>/config/pipeline.yaml
    --base_path    s3://<lake-bucket>

Glue job bookmarks are disabled on purpose: incremental state is tracked by run_date
partitions, which keeps reruns and backfills deterministic.
"""

import sys
from datetime import date, timedelta

from awsglue.context import GlueContext  # type: ignore[import-not-found]
from awsglue.job import Job  # type: ignore[import-not-found]
from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]
from pyspark.context import SparkContext

from lakehouse.common.config import load_config
from lakehouse.pipeline import run

REQUIRED = ["JOB_NAME", "stage", "config_path", "base_path"]
OPTIONAL = ["run_date"]

args = getResolvedOptions(sys.argv, REQUIRED + [o for o in OPTIONAL if f"--{o}" in sys.argv])
run_date = args.get("run_date") or (date.today() - timedelta(days=1)).isoformat()

glue_context = GlueContext(SparkContext.getOrCreate())
spark = glue_context.spark_session
for key, value in {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    "spark.sql.sources.partitionColumnTypeInference.enabled": "false",
    "spark.sql.session.timeZone": "UTC",
}.items():
    spark.conf.set(key, value)

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

cfg = load_config(args["config_path"], args["base_path"])
run(spark, cfg, run_date, stage=args["stage"], batch_id=f"glue-{args['JOB_NAME']}-{run_date}")

job.commit()
