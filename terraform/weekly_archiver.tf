# ================================
# Weekly Archiver: Archive Processed Files
# ================================
# Runs every Monday at UTC 00:00 (AEST 11:00)

# -----------------------------
# Lambda: Weekly Archiver
# -----------------------------
resource "aws_lambda_function" "weekly_archiver" {
  function_name = "sbm-weekly-archiver"
  role          = data.aws_iam_role.ingester_role.arn
  handler       = "app.lambda_handler"
  runtime       = "python3.13"
  timeout       = 600
  memory_size   = 1024
  s3_bucket     = var.deployment_bucket
  s3_key        = "${local.lambda_s3_prefix}/weekly_archiver.zip"

  environment {
    variables = {
      POWERTOOLS_SERVICE_NAME = "weekly-archiver"
      LOG_LEVEL               = "INFO"
    }
  }

  tracing_config {
    mode = "Active"
  }

  depends_on = [aws_cloudwatch_log_group.weekly_archiver]
}

# -----------------------------
# EventBridge: Weekly Schedule
# -----------------------------
resource "aws_cloudwatch_event_rule" "weekly_archive" {
  name                = "sbm-weekly-archive-schedule"
  schedule_expression = "cron(0 0 ? * MON *)"
}

resource "aws_cloudwatch_event_target" "weekly_archive_target" {
  rule      = aws_cloudwatch_event_rule.weekly_archive.name
  target_id = "weekly-archiver-lambda"
  arn       = aws_lambda_function.weekly_archiver.arn
}

resource "aws_lambda_permission" "allow_eventbridge_weekly" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.weekly_archiver.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.weekly_archive.arn
}
