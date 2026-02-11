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

variable "aurora_db_name" {
  description = "Name of the default database to create"
  type        = string
  default     = "sbm"
}

variable "aurora_master_username" {
  description = "Master username for the Aurora cluster"
  type        = string
  default     = "postgres"
}

variable "aurora_master_password" {
  description = "Master password for the Aurora cluster"
  type        = string
  sensitive   = true
}

# -----------------------------
# Optima Exporter Credentials
# -----------------------------
variable "optima_bunnings_username" {
  description = "BidEnergy username for Bunnings"
  type        = string
  sensitive   = true
}

variable "optima_bunnings_password" {
  description = "BidEnergy password for Bunnings"
  type        = string
  sensitive   = true
}

variable "optima_racv_username" {
  description = "BidEnergy username for RACV"
  type        = string
  sensitive   = true
}

variable "optima_racv_password" {
  description = "BidEnergy password for RACV"
  type        = string
  sensitive   = true
}

# -----------------------------
# CIM Exporter Credentials
# -----------------------------
variable "cim_username" {
  description = "CIM platform login username"
  type        = string
  sensitive   = true
}

variable "cim_password" {
  description = "CIM platform login password"
  type        = string
  sensitive   = true
}

variable "smtp_username" {
  description = "SES SMTP IAM access key"
  type        = string
  sensitive   = true
}

variable "smtp_password" {
  description = "SES SMTP IAM secret key"
  type        = string
  sensitive   = true
}
