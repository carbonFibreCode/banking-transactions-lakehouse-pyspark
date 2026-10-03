# ---------------------------------------------------------------------------
# Event-driven trigger: S3 _SUCCESS marker -> Lambda -> Glue job
# ---------------------------------------------------------------------------
data "archive_file" "lambda" {
  type        = "zip"
  source_file = "${path.module}/../../lambda_functions/landing_trigger.py"
  output_path = "${path.module}/build/landing_trigger.zip"
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  name               = "${local.prefix}-landing-trigger"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

data "aws_iam_policy_document" "lambda" {
  statement {
    actions   = ["glue:StartJobRun"]
    resources = [aws_glue_job.pipeline.arn]
  }
  statement {
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.lambda.arn}:*"]
  }
}

resource "aws_iam_role_policy" "lambda" {
  name   = "start-glue"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda.json
}

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${local.prefix}-landing-trigger"
  retention_in_days = 90
}

resource "aws_lambda_function" "landing_trigger" {
  function_name    = "${local.prefix}-landing-trigger"
  role             = aws_iam_role.lambda.arn
  runtime          = "python3.12"
  handler          = "landing_trigger.handler"
  filename         = data.archive_file.lambda.output_path
  source_code_hash = data.archive_file.lambda.output_base64sha256
  timeout          = 30
  environment {
    variables = { GLUE_JOB_NAME = aws_glue_job.pipeline.name }
  }
  depends_on = [aws_cloudwatch_log_group.lambda]
}

resource "aws_lambda_permission" "s3" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.landing_trigger.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.lake.arn
}

resource "aws_s3_bucket_notification" "landing" {
  bucket = aws_s3_bucket.lake.id
  lambda_function {
    lambda_function_arn = aws_lambda_function.landing_trigger.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "landing/transactions/"
    filter_suffix       = "_SUCCESS"
  }
  depends_on = [aws_lambda_permission.s3]
}

# ---------------------------------------------------------------------------
# Alerting: any failed/timed-out Glue run -> SNS
# ---------------------------------------------------------------------------
# Unencrypted on purpose: EventBridge cannot publish to a topic encrypted with the AWS-managed
# SNS key. Use a customer-managed key with an events.amazonaws.com grant if alerts carry data.
resource "aws_sns_topic" "alerts" {
  name = "${local.prefix}-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alert_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_cloudwatch_event_rule" "glue_failed" {
  name = "${local.prefix}-glue-failed"
  event_pattern = jsonencode({
    source      = ["aws.glue"]
    detail-type = ["Glue Job State Change"]
    detail = {
      jobName = [aws_glue_job.pipeline.name]
      state   = ["FAILED", "TIMEOUT", "ERROR"]
    }
  })
}

resource "aws_cloudwatch_event_target" "glue_failed" {
  rule = aws_cloudwatch_event_rule.glue_failed.name
  arn  = aws_sns_topic.alerts.arn
}

data "aws_iam_policy_document" "sns_events" {
  statement {
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}

resource "aws_sns_topic_policy" "alerts" {
  arn    = aws_sns_topic.alerts.arn
  policy = data.aws_iam_policy_document.sns_events.json
}

# ---------------------------------------------------------------------------
# Audit: CloudTrail data events record every read/write on the lake bucket.
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "trail" {
  bucket = "${local.prefix}-cloudtrail-${local.account_id}"
}

resource "aws_s3_bucket_public_access_block" "trail" {
  bucket                  = aws_s3_bucket.trail.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

data "aws_iam_policy_document" "trail" {
  statement {
    sid       = "AclCheck"
    actions   = ["s3:GetBucketAcl"]
    resources = [aws_s3_bucket.trail.arn]
    principals {
      type        = "Service"
      identifiers = ["cloudtrail.amazonaws.com"]
    }
  }
  statement {
    sid       = "Write"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.trail.arn}/AWSLogs/${local.account_id}/*"]
    principals {
      type        = "Service"
      identifiers = ["cloudtrail.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "s3:x-amz-acl"
      values   = ["bucket-owner-full-control"]
    }
  }
}

resource "aws_s3_bucket_policy" "trail" {
  bucket = aws_s3_bucket.trail.id
  policy = data.aws_iam_policy_document.trail.json
}

resource "aws_cloudtrail" "lake" {
  name                       = "${local.prefix}-lake-data-events"
  s3_bucket_name             = aws_s3_bucket.trail.id
  enable_log_file_validation = true
  is_multi_region_trail      = false

  event_selector {
    read_write_type           = "All"
    include_management_events = false
    data_resource {
      type   = "AWS::S3::Object"
      values = ["${aws_s3_bucket.lake.arn}/"]
    }
  }
  depends_on = [aws_s3_bucket_policy.trail]
}
