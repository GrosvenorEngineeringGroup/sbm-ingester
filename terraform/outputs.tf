# -----------------------------
# Outputs
# -----------------------------
output "sbm_api_invoke_url" {
  value       = "https://${aws_api_gateway_rest_api.sbm_api.id}.execute-api.${var.aws_region}.amazonaws.com/${aws_api_gateway_stage.api_stage.stage_name}/nem12-mappings"
  description = "Invoke URL for the API."
}

output "sbm_api_key_value" {
  value       = aws_api_gateway_api_key.sbm_api_key.value
  description = "API key to access the API."
  sensitive   = true
}

output "sbm_dlq_url" {
  value       = aws_sqs_queue.sbm_files_ingester_dlq.url
  description = "Dead Letter Queue URL for monitoring failed messages."
}

output "sbm_alerts_topic_arn" {
  value       = aws_sns_topic.sbm_alerts.arn
  description = "SNS topic ARN for subscribing to ingester alerts."
}

output "sbm_idempotency_table_name" {
  value       = aws_dynamodb_table.sbm_ingester_idempotency.name
  description = "DynamoDB table name for idempotency tracking."
}

output "glue_job_name" {
  value       = aws_glue_job.hudi_import.name
  description = "Glue job name for Hudi data import."
}

# Note: Glue job is triggered by Step Function "MyStateMachine"
# via EventBridge Scheduler "scanForNewDfs" (rate: 1 hour)

output "aurora_connection_url" {
  value       = "postgresql://${var.aurora_master_username}:${local.aurora_credentials["password"]}@${aws_rds_cluster.sbm_aurora.endpoint}:5432/${var.aurora_db_name}"
  description = "Full Aurora PostgreSQL connection URL."
  sensitive   = true
}

# -----------------------------
# CI/CD Policy Drift Guard
# -----------------------------
# The AWS IAM managed policy `sbm-ingester-cicd-policy` (attached to IAM user
# `sbm-ingester-github-actions`) grants `lambda:UpdateFunctionCode` on a
# hard-coded list of Lambda ARNs. That policy is managed MANUALLY (not in
# Terraform) to avoid policy-version churn and accidental permission loss.
#
# This output surfaces the CANONICAL set of ARNs that policy SHOULD contain.
# Whenever a Lambda is renamed / added / removed in Terraform, run
# `scripts/check_cicd_policy_drift.sh` to compare this list against the live
# IAM policy and patch the policy manually if they diverge.
output "cicd_managed_lambda_arns" {
  description = "Lambda ARNs the sbm-ingester-cicd-policy IAM policy must allow lambda:UpdateFunctionCode on. Keep the live policy in sync with this list (scripts/check_cicd_policy_drift.sh)."
  value = sort([
    aws_lambda_function.sbm_files_ingester.arn,
    aws_lambda_function.sbm_files_ingester_redrive.arn,
    aws_lambda_function.sbm_files_ingester_nem12_mappings.arn,
    aws_lambda_function.weekly_archiver.arn,
    aws_lambda_function.glue_trigger.arn,
    aws_lambda_function.optima_nem12_exporter.arn,
    aws_lambda_function.optima_billing_exporter.arn,
    aws_lambda_function.cim_report_exporter.arn,
  ])
}
