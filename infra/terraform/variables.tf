variable "project" {
  type    = string
  default = "banking-lakehouse"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "region" {
  type    = string
  default = "ap-south-1"
}

variable "glue_workers" {
  description = "Number of G.1X workers for the pipeline job"
  type        = number
  default     = 10
}

variable "alert_email" {
  description = "Where Glue job failure alerts are sent (leave empty to skip the subscription)"
  type        = string
  default     = ""
}
