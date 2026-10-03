output "lake_bucket" {
  value = aws_s3_bucket.lake.bucket
}

output "artifacts_bucket" {
  value = aws_s3_bucket.artifacts.bucket
}

output "glue_job_name" {
  value = aws_glue_job.pipeline.name
}

output "alerts_topic_arn" {
  value = aws_sns_topic.alerts.arn
}
