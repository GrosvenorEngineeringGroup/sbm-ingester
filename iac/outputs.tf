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
