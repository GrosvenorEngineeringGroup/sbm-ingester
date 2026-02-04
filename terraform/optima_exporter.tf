# ================================
# Optima Exporter: BidEnergy Data Export
# ================================
# Two Lambda functions for different export types:
# - Interval Exporter: Downloads CSV interval data, uploads to S3
# - Billing Exporter: Triggers monthly usage report (email delivery)
#
# Each project (bunnings, racv) is triggered separately via EventBridge.

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
# Shared Environment Variables
# -----------------------------
locals {
  optima_common_env = {
    POWERTOOLS_LOG_LEVEL = "INFO"
    BIDENERGY_BASE_URL   = "https://app.bidenergy.com"

    # DynamoDB configuration
    OPTIMA_CONFIG_TABLE = aws_dynamodb_table.optima_config.name

    # Bunnings credentials
    OPTIMA_BUNNINGS_USERNAME  = "optimaBunningsEnergy@verdeos.com"
    OPTIMA_BUNNINGS_PASSWORD  = "3?JSBPKrbFF6rr"
    OPTIMA_BUNNINGS_CLIENT_ID = "BidEnergy"

    # RACV credentials
    OPTIMA_RACV_USERNAME  = "kes@gegroup.com.au"
    OPTIMA_RACV_PASSWORD  = "35JPdgJsKNKVgCs"
    OPTIMA_RACV_CLIENT_ID = "BidEnergy"
  }
}

# ================================
# Lambda 1: Interval Exporter
# ================================

resource "aws_cloudwatch_log_group" "optima_interval_exporter" {
  name              = "/aws/lambda/optima-interval-exporter"
  retention_in_days = var.log_retention_days

  tags = local.common_tags
}

resource "aws_lambda_function" "optima_interval_exporter" {
  function_name = "optima-interval-exporter"
  description   = "Exports Optima interval data to S3 for ingestion pipeline"
  role          = data.aws_iam_role.ingester_role.arn
  handler       = "interval_exporter.app.lambda_handler"
  runtime       = "python3.13"
  timeout       = 900 # 15 minutes - Bunnings has 400+ sites
  memory_size   = 256
  s3_bucket     = var.deployment_bucket
  s3_key        = "${local.lambda_s3_prefix}/optima_exporter.zip"

  environment {
    variables = merge(local.optima_common_env, {
      POWERTOOLS_SERVICE_NAME = "optima-interval-exporter"

      # S3 upload configuration
      S3_UPLOAD_BUCKET = "sbm-file-ingester"
      S3_UPLOAD_PREFIX = "newTBP/"

      # Interval export configuration
      OPTIMA_DAYS_BACK   = "7"
      OPTIMA_MAX_WORKERS = "10"
    })
  }

  tracing_config {
    mode = "PassThrough"
  }

  depends_on = [aws_cloudwatch_log_group.optima_interval_exporter]

  tags = local.common_tags
}

# ================================
# Lambda 2: Billing Exporter
# ================================

resource "aws_cloudwatch_log_group" "optima_billing_exporter" {
  name              = "/aws/lambda/optima-billing-exporter"
  retention_in_days = var.log_retention_days

  tags = local.common_tags
}

resource "aws_lambda_function" "optima_billing_exporter" {
  function_name = "optima-billing-exporter"
  description   = "Triggers Optima billing report generation (email delivery)"
  role          = data.aws_iam_role.ingester_role.arn
  handler       = "billing_exporter.app.lambda_handler"
  runtime       = "python3.13"
  timeout       = 120 # 2 minutes - triggers report for multiple countries
  memory_size   = 128
  s3_bucket     = var.deployment_bucket
  s3_key        = "${local.lambda_s3_prefix}/optima_exporter.zip"

  environment {
    variables = merge(local.optima_common_env, {
      POWERTOOLS_SERVICE_NAME = "optima-billing-exporter"

      # Billing export configuration - supported countries per project
      OPTIMA_BUNNINGS_COUNTRIES = "AU,NZ"
      OPTIMA_RACV_COUNTRIES     = "AU"
      OPTIMA_BILLING_MONTHS     = "12"
    })
  }

  tracing_config {
    mode = "PassThrough"
  }

  depends_on = [aws_cloudwatch_log_group.optima_billing_exporter]

  tags = local.common_tags
}

# ================================
# EventBridge Scheduler: Interval (Daily)
# ================================

# Bunnings Interval - Daily 7:00 AM Sydney
resource "aws_scheduler_schedule" "optima_bunnings_interval" {
  name       = "optima-bunnings-interval-daily"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "cron(0 7 * * ? *)"
  schedule_expression_timezone = "Australia/Sydney"

  target {
    arn      = aws_lambda_function.optima_interval_exporter.arn
    role_arn = aws_iam_role.optima_scheduler_role.arn
    input    = jsonencode({ project = "bunnings" })
  }
}

# RACV Interval - Daily 7:00 AM Sydney
resource "aws_scheduler_schedule" "optima_racv_interval" {
  name       = "optima-racv-interval-daily"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "cron(0 7 * * ? *)"
  schedule_expression_timezone = "Australia/Sydney"

  target {
    arn      = aws_lambda_function.optima_interval_exporter.arn
    role_arn = aws_iam_role.optima_scheduler_role.arn
    input    = jsonencode({ project = "racv" })
  }
}

# ================================
# EventBridge Scheduler: Billing (Monthly)
# ================================

# Bunnings Billing - Monthly 1st 7:00 AM Sydney
resource "aws_scheduler_schedule" "optima_bunnings_billing" {
  name       = "optima-bunnings-billing-monthly"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "cron(0 7 1 * ? *)"
  schedule_expression_timezone = "Australia/Sydney"

  target {
    arn      = aws_lambda_function.optima_billing_exporter.arn
    role_arn = aws_iam_role.optima_scheduler_role.arn
    input    = jsonencode({ project = "bunnings" })
  }
}

# RACV Billing - Monthly 1st 7:00 AM Sydney
resource "aws_scheduler_schedule" "optima_racv_billing" {
  name       = "optima-racv-billing-monthly"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "cron(0 7 1 * ? *)"
  schedule_expression_timezone = "Australia/Sydney"

  target {
    arn      = aws_lambda_function.optima_billing_exporter.arn
    role_arn = aws_iam_role.optima_scheduler_role.arn
    input    = jsonencode({ project = "racv" })
  }
}

# ================================
# EventBridge Scheduler: Interval (Weekly)
# ================================

# Bunnings Interval Weekly - Sunday 8:00 AM Sydney (full history export)
resource "aws_scheduler_schedule" "optima_bunnings_interval_weekly" {
  name       = "optima-bunnings-interval-weekly"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "cron(0 8 ? * SUN *)"
  schedule_expression_timezone = "Australia/Sydney"

  target {
    arn      = aws_lambda_function.optima_interval_exporter.arn
    role_arn = aws_iam_role.optima_scheduler_role.arn
    input = jsonencode({
      project   = "bunnings"
      startDate = "2024-01-01"
    })
  }
}

# ================================
# IAM Role for EventBridge Scheduler
# ================================

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
      Effect = "Allow"
      Action = "lambda:InvokeFunction"
      Resource = [
        aws_lambda_function.optima_interval_exporter.arn,
        aws_lambda_function.optima_billing_exporter.arn,
      ]
    }]
  })
}

# ================================
# CloudWatch Alarms
# ================================

resource "aws_cloudwatch_metric_alarm" "optima_interval_errors" {
  alarm_name          = "optima-interval-exporter-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 3600 # 1 hour
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Optima interval exporter Lambda errors"

  dimensions = {
    FunctionName = aws_lambda_function.optima_interval_exporter.function_name
  }

  alarm_actions = [data.aws_sns_topic.sbm_alerts.arn]
  ok_actions    = [data.aws_sns_topic.sbm_alerts.arn]

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "optima_billing_errors" {
  alarm_name          = "optima-billing-exporter-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 3600 # 1 hour
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Optima billing exporter Lambda errors"

  dimensions = {
    FunctionName = aws_lambda_function.optima_billing_exporter.function_name
  }

  alarm_actions = [data.aws_sns_topic.sbm_alerts.arn]
  ok_actions    = [data.aws_sns_topic.sbm_alerts.arn]

  tags = local.common_tags
}

data "aws_sns_topic" "sbm_alerts" {
  name = "sbm-ingester-alerts"
}
