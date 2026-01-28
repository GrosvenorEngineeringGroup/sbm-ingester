# ================================
# Optima Exporter: BidEnergy Data Export
# ================================
# Exports meter data from Optima/BidEnergy via web login
# and uploads CSV reports to S3 for ingestion pipeline
#
# Supports scheduled daily exports via EventBridge and
# on-demand invocation with specific parameters.

# -----------------------------
# DynamoDB: Site Configuration
# -----------------------------
resource "aws_dynamodb_table" "optima_config" {
  name         = "sbm-optima-config"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "project"
  range_key    = "nmi"

  attribute {
    name = "project"
    type = "S"
  }

  attribute {
    name = "nmi"
    type = "S"
  }

  tags = local.common_tags
}

# -----------------------------
# IAM: DynamoDB Access Policy
# -----------------------------
resource "aws_iam_role_policy" "optima_dynamodb_access" {
  name = "sbm-optima-exporter-dynamodb-access"
  role = data.aws_iam_role.ingester_role.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["dynamodb:Scan", "dynamodb:Query", "dynamodb:GetItem"]
      Resource = aws_dynamodb_table.optima_config.arn
    }]
  })
}

# -----------------------------
# Note: S3 permissions inherited from getIdFromNem12Id-role-153b7a0a
# No additional IAM policies needed for S3 upload
# -----------------------------

# -----------------------------
# CloudWatch Log Group
# -----------------------------
resource "aws_cloudwatch_log_group" "optima_exporter" {
  name              = "/aws/lambda/sbm-optima-exporter"
  retention_in_days = var.log_retention_days

  tags = local.common_tags
}

# -----------------------------
# Lambda: Optima Exporter
# -----------------------------
resource "aws_lambda_function" "optima_exporter" {
  function_name = "sbm-optima-exporter"
  description   = "Exports Optima NMI data to S3 for Bunnings and RACV projects"
  role          = data.aws_iam_role.ingester_role.arn
  handler       = "app.lambda_handler"
  runtime       = "python3.13"
  timeout       = 900 # 15 minutes - Bunnings has 50+ sites, each takes up to 120s for CSV download
  memory_size   = 256
  s3_bucket     = var.deployment_bucket
  s3_key        = "${local.lambda_s3_prefix}/optima_exporter.zip"

  environment {
    variables = {
      POWERTOOLS_SERVICE_NAME = "optima-exporter"
      POWERTOOLS_LOG_LEVEL    = "INFO"

      # S3 upload configuration
      S3_UPLOAD_BUCKET = "sbm-file-ingester"
      S3_UPLOAD_PREFIX = "newTBP/"

      # DynamoDB configuration
      OPTIMA_CONFIG_TABLE = aws_dynamodb_table.optima_config.name
      OPTIMA_PROJECTS     = "bunnings" # Test with bunnings first, then add racv
      OPTIMA_DAYS_BACK    = "7"        # Export past 7 days of data
      BIDENERGY_BASE_URL  = "https://app.bidenergy.com"

      # Parallel processing configuration
      OPTIMA_MAX_WORKERS = "10" # Number of concurrent site downloads

      # Bunnings credentials
      OPTIMA_BUNNINGS_USERNAME  = "optimaBunningsEnergy@verdeos.com"
      OPTIMA_BUNNINGS_PASSWORD  = "3?JSBPKrbFF6rr"
      OPTIMA_BUNNINGS_CLIENT_ID = "Visualisation"

      # RACV credentials
      OPTIMA_RACV_USERNAME  = "kes@gegroup.com.au"
      OPTIMA_RACV_PASSWORD  = "35JPdgJsKNKVgCs"
      OPTIMA_RACV_CLIENT_ID = "BidEnergy"
    }
  }

  tracing_config {
    mode = "PassThrough"
  }

  depends_on = [aws_cloudwatch_log_group.optima_exporter]

  tags = local.common_tags
}

# -----------------------------
# EventBridge Scheduler: Daily Schedule
# -----------------------------
resource "aws_scheduler_schedule" "optima_daily" {
  name       = "sbm-optima-daily-export"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "cron(0 7 * * ? *)" # 7:00 AM Sydney
  schedule_expression_timezone = "Australia/Sydney"  # Auto handles AEDT/AEST

  target {
    arn      = aws_lambda_function.optima_exporter.arn
    role_arn = aws_iam_role.optima_scheduler_role.arn
    input    = "{}"
  }
}

# IAM Role for EventBridge Scheduler
resource "aws_iam_role" "optima_scheduler_role" {
  name = "sbm-optima-scheduler-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "scheduler.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "optima_scheduler_invoke_lambda" {
  name = "sbm-optima-scheduler-invoke-lambda"
  role = aws_iam_role.optima_scheduler_role.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "lambda:InvokeFunction"
      Resource = aws_lambda_function.optima_exporter.arn
    }]
  })
}

# -----------------------------
# CloudWatch: Error Alarm
# -----------------------------
resource "aws_cloudwatch_metric_alarm" "optima_errors" {
  alarm_name          = "sbm-optima-exporter-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 3600 # 1 hour
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Optima exporter Lambda errors"

  dimensions = {
    FunctionName = aws_lambda_function.optima_exporter.function_name
  }

  alarm_actions = [data.aws_sns_topic.sbm_alerts.arn]
  ok_actions    = [data.aws_sns_topic.sbm_alerts.arn]

  tags = local.common_tags
}

data "aws_sns_topic" "sbm_alerts" {
  name = "sbm-ingester-alerts"
}
