# ================================
# CloudWatch Log Groups
# ================================

# -----------------------------
# Default Lambda Log Groups
# -----------------------------
resource "aws_cloudwatch_log_group" "sbm_files_ingester_default" {
  name              = "/aws/lambda/sbm-files-ingester"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "sbm_files_ingester_redrive_default" {
  name              = "/aws/lambda/sbm-files-ingester-redrive"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "sbm_files_ingester_nem12_mappings_default" {
  name              = "/aws/lambda/sbm-files-ingester-nem12-mappings-to-s3"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "weekly_archiver" {
  name              = "/aws/lambda/sbm-weekly-archiver"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "glue_trigger" {
  name              = "/aws/lambda/sbm-glue-trigger"
  retention_in_days = var.log_retention_days
}

# -----------------------------
# Custom Application Log Groups
# -----------------------------
resource "aws_cloudwatch_log_group" "sbm_ingester_error_log" {
  name              = "sbm-ingester-error-log"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "sbm_ingester_execution_log" {
  name              = "sbm-ingester-execution-log"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "sbm_ingester_metrics_log" {
  name              = "sbm-ingester-metrics-log"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "sbm_ingester_parse_error_log" {
  name              = "sbm-ingester-parse-error-log"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "sbm_ingester_runtime_error_log" {
  name              = "sbm-ingester-runtime-error-log"
  retention_in_days = var.log_retention_days
}
