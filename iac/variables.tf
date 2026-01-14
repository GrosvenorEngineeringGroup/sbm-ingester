variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "ap-southeast-2"
}

variable "project_name" {
  description = "Project name for tagging"
  type        = string
  default     = "SBM-Ingestion"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "production"
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 30
}

variable "input_bucket" {
  description = "S3 bucket for file ingestion"
  type        = string
  default     = "sbm-file-ingester"
}

variable "deployment_bucket" {
  description = "S3 bucket for Lambda code"
  type        = string
  default     = "gega-code-deployment-bucket"
}

variable "neptune_endpoint" {
  description = "Neptune database endpoint"
  type        = string
  default     = "bw-1-instance-1.cov3fflnpa7n.ap-southeast-2.neptune.amazonaws.com"
}

variable "neptune_port" {
  description = "Neptune database port"
  type        = string
  default     = "8182"
}
