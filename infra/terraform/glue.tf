# ---------------------------------------------------------------------------
# IAM: least privilege. The Glue role can read landing data and write only to the
# layers it produces; it cannot delete from landing or touch other buckets.
# ---------------------------------------------------------------------------
data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${local.prefix}-glue"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_data" {
  statement {
    sid       = "ListLake"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn, aws_s3_bucket.artifacts.arn]
  }
  statement {
    sid       = "ReadLanding"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.lake.arn}/landing/*", "${aws_s3_bucket.artifacts.arn}/*"]
  }
  statement {
    sid     = "ReadWriteManagedLayers"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = [
      for layer in ["bronze", "silver", "gold", "quarantine", "audit", "tmp"] :
      "${aws_s3_bucket.lake.arn}/${layer}/*"
    ]
  }
  statement {
    sid       = "UseLakeKey"
    actions   = ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey"]
    resources = [aws_kms_key.lake.arn]
  }
}

resource "aws_iam_role_policy" "glue_data" {
  name   = "lake-access"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_data.json
}

# ---------------------------------------------------------------------------
# Glue job: the same code as local/CI, packaged as a wheel.
# ---------------------------------------------------------------------------
resource "aws_glue_job" "pipeline" {
  name              = "${local.prefix}-pipeline"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = var.glue_workers
  timeout           = 120
  max_retries       = 1

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.artifacts.bucket}/glue_jobs/lakehouse_glue_job.py"
  }

  execution_property {
    max_concurrent_runs = 1 # stages for consecutive days must not overlap
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.lake.bucket}/tmp/spark-ui/"
    "--TempDir"                          = "s3://${aws_s3_bucket.lake.bucket}/tmp/glue/"
    "--additional-python-modules"        = "pyyaml>=6.0"
    "--extra-py-files"                   = "s3://${aws_s3_bucket.artifacts.bucket}/dist/lakehouse-0.1.0-py3-none-any.whl"
    "--stage"                            = "all"
    "--config_path"                      = "s3://${aws_s3_bucket.artifacts.bucket}/config/pipeline.yaml"
    "--base_path"                        = "s3://${aws_s3_bucket.lake.bucket}"
  }
}

# Gold tables registered in the Glue Data Catalog for Athena / BI.
resource "aws_glue_catalog_database" "gold" {
  name = replace("${local.prefix}_gold", "-", "_")
}

resource "aws_glue_crawler" "gold" {
  name          = "${local.prefix}-gold"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.gold.name
  schedule      = "cron(0 4 * * ? *)"

  dynamic "s3_target" {
    for_each = ["customer_daily_spend", "merchant_category_daily", "txn_fraud_features"]
    content {
      path = "s3://${aws_s3_bucket.lake.bucket}/gold/${s3_target.value}/"
    }
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
}
