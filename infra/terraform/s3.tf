# Lake bucket: landing/ bronze/ silver/ gold/ quarantine/ audit/ prefixes.
# Artifacts bucket: the lakehouse wheel, Glue scripts, pipeline.yaml.

resource "aws_s3_bucket" "lake" {
  bucket = "${local.prefix}-lake-${local.account_id}"
}

resource "aws_s3_bucket" "artifacts" {
  bucket = "${local.prefix}-artifacts-${local.account_id}"
}

locals {
  buckets = {
    lake      = aws_s3_bucket.lake
    artifacts = aws_s3_bucket.artifacts
  }
}

resource "aws_s3_bucket_public_access_block" "all" {
  for_each                = local.buckets
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "all" {
  for_each = local.buckets
  bucket   = each.value.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "all" {
  for_each = local.buckets
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.lake.arn
    }
    bucket_key_enabled = true
  }
}

# Deny any request that is not over TLS.
data "aws_iam_policy_document" "tls_only" {
  for_each = local.buckets
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [each.value.arn, "${each.value.arn}/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "tls_only" {
  for_each = local.buckets
  bucket   = each.value.id
  policy   = data.aws_iam_policy_document.tls_only[each.key].json
}

# Retention: raw data moves to cheaper storage tiers; quarantine is kept for a year for investigation.
resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    id     = "landing-expire"
    status = "Enabled"
    filter { prefix = "landing/" }
    expiration { days = 30 }
  }

  rule {
    id     = "bronze-tiering"
    status = "Enabled"
    filter { prefix = "bronze/" }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 180
      storage_class = "GLACIER"
    }
  }

  rule {
    id     = "quarantine-expire"
    status = "Enabled"
    filter { prefix = "quarantine/" }
    expiration { days = 365 }
  }

  rule {
    id     = "old-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration { noncurrent_days = 30 }
  }
}
