"""AWS Glue (PySpark) entry point."""

import sys
from datetime import date, timedelta

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
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
