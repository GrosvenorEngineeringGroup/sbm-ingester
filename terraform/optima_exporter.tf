# ================================
# Optima Exporter: BidEnergy Data Export
# ================================
# Two Lambda functions for different export types:
# - NEM12 Exporter: Downloads NEM12 CSV files from BidEnergy, uploads to S3
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
    OPTIMA_BUNNINGS_USERNAME  = var.optima_bunnings_username
    OPTIMA_BUNNINGS_PASSWORD  = var.optima_bunnings_password
    OPTIMA_BUNNINGS_CLIENT_ID = "BidEnergy"

    # RACV credentials
    OPTIMA_RACV_USERNAME  = var.optima_racv_username
    OPTIMA_RACV_PASSWORD  = var.optima_racv_password
    OPTIMA_RACV_CLIENT_ID = "BidEnergy"
  }
}

# ================================
# Lambda 1: NEM12 Exporter
# ================================

resource "aws_cloudwatch_log_group" "optima_nem12_exporter" {
  name              = "/aws/lambda/optima-nem12-exporter"
  retention_in_days = var.log_retention_days

  tags = local.common_tags
}

resource "aws_lambda_function" "optima_nem12_exporter" {
  function_name = "optima-nem12-exporter"
  description   = "Exports Optima NEM12 files to S3 for ingestion pipeline"
  role          = data.aws_iam_role.ingester_role.arn
  handler       = "nem12_exporter.app.lambda_handler"
  runtime       = "python3.13"
  timeout       = 900 # 15 minutes - Bunnings has 400+ sites
  memory_size   = 256
  s3_bucket     = var.deployment_bucket
  s3_key        = "${local.lambda_s3_prefix}/optima_exporter.zip"

  environment {
    variables = merge(local.optima_common_env, {
      POWERTOOLS_SERVICE_NAME = "optima-nem12-exporter"

      # S3 upload configuration
      S3_UPLOAD_BUCKET = "sbm-file-ingester"
      S3_UPLOAD_PREFIX = "newTBP/"

      # NEM12 export configuration
      OPTIMA_DAYS_BACK   = "1"
      OPTIMA_MAX_WORKERS = "20"
    })
  }

  tracing_config {
    mode = "PassThrough"
  }

  depends_on = [aws_cloudwatch_log_group.optima_nem12_exporter]

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
# EventBridge Scheduler: NEM12 (Daily)
# ================================

# === DISABLED 2026-05-06 ===
# Replaced by optima-interval-exporter (uses POST /BuyerReport/exportdailyusagecsv).
# The optima-nem12-exporter Lambda function, log group, and alarm are intentionally
# kept for manual invoke / backup / debug. To re-enable: uncomment these two
# resource blocks + run `terraform apply`.
#
# # Bunnings NEM12 - Daily 2:00 PM Sydney
# resource "aws_scheduler_schedule" "optima_bunnings_nem12" {
#   name       = "optima-bunnings-nem12-daily"
#   group_name = "default"
#
#   flexible_time_window {
#     mode = "OFF"
#   }
#
#   schedule_expression          = "cron(0 14 * * ? *)"
#   schedule_expression_timezone = "Australia/Sydney"
#
#   target {
#     arn      = aws_lambda_function.optima_nem12_exporter.arn
#     role_arn = aws_iam_role.optima_scheduler_role.arn
#     input    = jsonencode({ project = "bunnings" })
#   }
# }
#
# # RACV NEM12 - Daily 2:00 PM Sydney
# resource "aws_scheduler_schedule" "optima_racv_nem12" {
#   name       = "optima-racv-nem12-daily"
#   group_name = "default"
#
#   flexible_time_window {
#     mode = "OFF"
#   }
#
#   schedule_expression          = "cron(0 14 * * ? *)"
#   schedule_expression_timezone = "Australia/Sydney"
#
#   target {
#     arn      = aws_lambda_function.optima_nem12_exporter.arn
#     role_arn = aws_iam_role.optima_scheduler_role.arn
#     input    = jsonencode({ project = "racv" })
#   }
# }

# ================================
# EventBridge Scheduler: Billing (Monthly)
# ================================

# Bunnings Billing - Weekly Saturday 7:00 AM Sydney
resource "aws_scheduler_schedule" "optima_bunnings_billing" {
  name       = "optima-bunnings-billing-weekly"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "cron(0 7 ? * SAT *)"
  schedule_expression_timezone = "Australia/Sydney"

  target {
    arn      = aws_lambda_function.optima_billing_exporter.arn
    role_arn = aws_iam_role.optima_scheduler_role.arn
    input    = jsonencode({ project = "bunnings" })
  }
}

# RACV Billing - Weekly Saturday 7:00 AM Sydney
resource "aws_scheduler_schedule" "optima_racv_billing" {
  name       = "optima-racv-billing-weekly"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "cron(0 7 ? * SAT *)"
  schedule_expression_timezone = "Australia/Sydney"

  target {
    arn      = aws_lambda_function.optima_billing_exporter.arn
    role_arn = aws_iam_role.optima_scheduler_role.arn
    input    = jsonencode({ project = "racv" })
  }
}

# ================================
# EventBridge Scheduler: NEM12 (Weekly)
# ================================

# Bunnings NEM12 Weekly - SUSPENDED
# resource "aws_scheduler_schedule" "optima_bunnings_nem12_weekly" {
#   name       = "optima-bunnings-nem12-weekly"
#   group_name = "default"
#
#   flexible_time_window {
#     mode = "OFF"
#   }
#
#   schedule_expression          = "cron(0 8 ? * SUN *)"
#   schedule_expression_timezone = "Australia/Sydney"
#
#   target {
#     arn      = aws_lambda_function.optima_nem12_exporter.arn
#     role_arn = aws_iam_role.optima_scheduler_role.arn
#     input = jsonencode({
#       project   = "bunnings"
#       startDate = "2024-01-01"
#     })
#   }
# }

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
        aws_lambda_function.optima_nem12_exporter.arn,
        aws_lambda_function.optima_billing_exporter.arn,
        aws_lambda_function.optima_demand_exporter.arn,
        aws_lambda_function.optima_interval_exporter.arn,
      ]
    }]
  })
}

# ================================
# CloudWatch Alarms
# ================================

resource "aws_cloudwatch_metric_alarm" "optima_nem12_errors" {
  alarm_name          = "optima-nem12-exporter-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 3600 # 1 hour
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Optima NEM12 exporter Lambda errors"

  dimensions = {
    FunctionName = aws_lambda_function.optima_nem12_exporter.function_name
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

# ================================
# Lambda 3: Demand Exporter
# ================================

resource "aws_cloudwatch_log_group" "optima_demand_exporter" {
  name              = "/aws/lambda/optima-demand-exporter"
  retention_in_days = var.log_retention_days

  tags = local.common_tags
}

resource "aws_lambda_function" "optima_demand_exporter" {
  function_name = "optima-demand-exporter"
  description   = "Exports Optima Demand Profile CSVs to S3 for ingestion pipeline"
  role          = data.aws_iam_role.ingester_role.arn
  handler       = "demand_exporter.app.lambda_handler"
  runtime       = "python3.13"
  timeout       = 900
  memory_size   = 256
  s3_bucket     = var.deployment_bucket
  s3_key        = "${local.lambda_s3_prefix}/optima_exporter.zip"

  environment {
    variables = merge(local.optima_common_env, {
      POWERTOOLS_SERVICE_NAME = "optima-demand-exporter"

      # S3 upload configuration
      S3_UPLOAD_BUCKET = "sbm-file-ingester"
      S3_UPLOAD_PREFIX = "newTBP/"

      # Demand export configuration
      OPTIMA_DAYS_BACK   = "3"
      OPTIMA_MAX_WORKERS = "20"
    })
  }

  tracing_config {
    mode = "PassThrough"
  }

  depends_on = [aws_cloudwatch_log_group.optima_demand_exporter]

  tags = local.common_tags
}

# ================================
# EventBridge Scheduler: Demand (Daily, 14:30 Sydney — staggered 30min after nem12)
# ================================

resource "aws_scheduler_schedule" "optima_bunnings_demand" {
  name       = "optima-bunnings-demand-daily"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "cron(30 14 * * ? *)"
  schedule_expression_timezone = "Australia/Sydney"

  target {
    arn      = aws_lambda_function.optima_demand_exporter.arn
    role_arn = aws_iam_role.optima_scheduler_role.arn
    input    = jsonencode({ project = "bunnings" })
  }
}

resource "aws_scheduler_schedule" "optima_racv_demand" {
  name       = "optima-racv-demand-daily"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "cron(30 14 * * ? *)"
  schedule_expression_timezone = "Australia/Sydney"

  target {
    arn      = aws_lambda_function.optima_demand_exporter.arn
    role_arn = aws_iam_role.optima_scheduler_role.arn
    input    = jsonencode({ project = "racv" })
  }
}

# Monthly re-ingest of the previous calendar month — fires 01:00 Sydney on the 1st
resource "aws_scheduler_schedule" "optima_bunnings_demand_monthly" {
  name       = "optima-bunnings-demand-monthly"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "cron(0 1 1 * ? *)"
  schedule_expression_timezone = "Australia/Sydney"

  target {
    arn      = aws_lambda_function.optima_demand_exporter.arn
    role_arn = aws_iam_role.optima_scheduler_role.arn
    input    = jsonencode({ project = "bunnings", mode = "previous_month" })
  }
}

resource "aws_scheduler_schedule" "optima_racv_demand_monthly" {
  name       = "optima-racv-demand-monthly"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  # Staggered 1h after bunnings to avoid 4 concurrent BidEnergy pulls
  schedule_expression          = "cron(0 2 1 * ? *)"
  schedule_expression_timezone = "Australia/Sydney"

  target {
    arn      = aws_lambda_function.optima_demand_exporter.arn
    role_arn = aws_iam_role.optima_scheduler_role.arn
    input    = jsonencode({ project = "racv", mode = "previous_month" })
  }
}

# CloudWatch alarm — mirror existing optima_nem12_errors alarm
resource "aws_cloudwatch_metric_alarm" "optima_demand_errors" {
  alarm_name          = "optima-demand-exporter-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 3600 # 1 hour — matches optima_nem12_errors
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Optima demand exporter Lambda errors"

  dimensions = {
    FunctionName = aws_lambda_function.optima_demand_exporter.function_name
  }

  alarm_actions = [data.aws_sns_topic.sbm_alerts.arn]
  ok_actions    = [data.aws_sns_topic.sbm_alerts.arn]

  tags = local.common_tags
}

# ================================
# Lambda 4: Interval Exporter (NEW primary interval data source)
# ================================

resource "aws_cloudwatch_log_group" "optima_interval_exporter" {
  name              = "/aws/lambda/optima-interval-exporter"
  retention_in_days = var.log_retention_days

  tags = local.common_tags
}

resource "aws_lambda_function" "optima_interval_exporter" {
  function_name = "optima-interval-exporter"
  description   = "Exports Optima interval CSVs (POST exportdailyusagecsv) to S3 - primary source"
  role          = data.aws_iam_role.ingester_role.arn
  handler       = "interval_exporter.app.lambda_handler"
  runtime       = "python3.13"
  timeout       = 900
  memory_size   = 256
  s3_bucket     = var.deployment_bucket
  s3_key        = "${local.lambda_s3_prefix}/optima_exporter.zip"

  environment {
    variables = merge(local.optima_common_env, {
      POWERTOOLS_SERVICE_NAME = "optima-interval-exporter"
      S3_UPLOAD_BUCKET        = "sbm-file-ingester"
      S3_UPLOAD_PREFIX        = "newTBP/"
      OPTIMA_DAYS_BACK        = "3"
      OPTIMA_MAX_WORKERS      = "20"
    })
  }

  tracing_config {
    mode = "PassThrough"
  }

  depends_on = [aws_cloudwatch_log_group.optima_interval_exporter]

  tags = local.common_tags
}

# Bunnings Interval - Daily 2:00 PM Sydney (taking the slot vacated by NEM12)
resource "aws_scheduler_schedule" "optima_bunnings_interval" {
  name       = "optima-bunnings-interval-daily"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "cron(0 14 * * ? *)"
  schedule_expression_timezone = "Australia/Sydney"

  target {
    arn      = aws_lambda_function.optima_interval_exporter.arn
    role_arn = aws_iam_role.optima_scheduler_role.arn
    input    = jsonencode({ project = "bunnings" })
  }

  depends_on = [aws_iam_role_policy.optima_scheduler_invoke_lambda]
}

# RACV Interval - Daily 2:00 PM Sydney
resource "aws_scheduler_schedule" "optima_racv_interval" {
  name       = "optima-racv-interval-daily"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "cron(0 14 * * ? *)"
  schedule_expression_timezone = "Australia/Sydney"

  target {
    arn      = aws_lambda_function.optima_interval_exporter.arn
    role_arn = aws_iam_role.optima_scheduler_role.arn
    input    = jsonencode({ project = "racv" })
  }

  depends_on = [aws_iam_role_policy.optima_scheduler_invoke_lambda]
}

# Monthly re-ingest of the previous calendar month — fires 01:00 Sydney on the 1st
resource "aws_scheduler_schedule" "optima_bunnings_interval_monthly" {
  name       = "optima-bunnings-interval-monthly"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "cron(0 1 1 * ? *)"
  schedule_expression_timezone = "Australia/Sydney"

  target {
    arn      = aws_lambda_function.optima_interval_exporter.arn
    role_arn = aws_iam_role.optima_scheduler_role.arn
    input    = jsonencode({ project = "bunnings", mode = "previous_month" })
  }

  depends_on = [aws_iam_role_policy.optima_scheduler_invoke_lambda]
}

resource "aws_scheduler_schedule" "optima_racv_interval_monthly" {
  name       = "optima-racv-interval-monthly"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  # Staggered 1h after bunnings to avoid 4 concurrent BidEnergy pulls
  schedule_expression          = "cron(0 2 1 * ? *)"
  schedule_expression_timezone = "Australia/Sydney"

  target {
    arn      = aws_lambda_function.optima_interval_exporter.arn
    role_arn = aws_iam_role.optima_scheduler_role.arn
    input    = jsonencode({ project = "racv", mode = "previous_month" })
  }

  depends_on = [aws_iam_role_policy.optima_scheduler_invoke_lambda]
}

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

data "aws_sns_topic" "sbm_alerts" {
  name = "sbm-ingester-alerts"
}
