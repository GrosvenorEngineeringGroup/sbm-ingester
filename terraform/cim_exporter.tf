# ================================
# CIM Exporter: AFDD Report Export
# ================================
# Lambda function using Playwright for browser automation.
# Downloads AFDD ticket reports from CIM and sends via email.
#
# Uses Docker container image due to Playwright browser dependencies.

# -----------------------------
# ECR Repository
# -----------------------------
resource "aws_ecr_repository" "cim_exporter" {
  name                 = "cim-exporter"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = local.common_tags
}

# Lifecycle policy to keep only recent images
resource "aws_ecr_lifecycle_policy" "cim_exporter" {
  repository = aws_ecr_repository.cim_exporter.name

  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep only 5 most recent images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 5
      }
      action = {
        type = "expire"
      }
    }]
  })
}

# -----------------------------
# CloudWatch Log Group
# -----------------------------
resource "aws_cloudwatch_log_group" "cim_report_exporter" {
  name              = "/aws/lambda/cim-report-exporter"
  retention_in_days = var.log_retention_days

  tags = local.common_tags
}

# -----------------------------
# Lambda Function (Container Image)
# -----------------------------
resource "aws_lambda_function" "cim_report_exporter" {
  function_name = "cim-report-exporter"
  description   = "Exports CIM AFDD reports using Playwright browser automation"
  role          = data.aws_iam_role.ingester_role.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.cim_exporter.repository_url}:latest"
  timeout       = 300 # 5 minutes - browser automation and download
  memory_size   = 1024

  environment {
    variables = {
      POWERTOOLS_SERVICE_NAME = "cim-report-exporter"
      POWERTOOLS_LOG_LEVEL    = "INFO"

      # CIM Authentication
      CIM_LOGIN_URL = "https://login.cimenviro.com/auth/realms/cimenviro/protocol/openid-connect/auth?client_id=dashboard-website&redirect_uri=https://ace.cimenviro.com/&response_type=code"
      CIM_BASE_URL  = "https://ace.cimenviro.com"
      CIM_USERNAME  = "CharterHallAFDD@gegroup.com.au"
      CIM_PASSWORD  = "Afdd@2025"

      # Site IDs (comma-separated)
      CIM_SITE_IDS = "232,239,242,480,268,233,228,234,248,244,748,249,547,401,237,743,301,550,29,269,632,247,250,230"

      # SMTP Configuration
      SMTP_HOST     = "email-smtp.ap-southeast-2.amazonaws.com"
      SMTP_PORT     = "587"
      SMTP_USERNAME = "AKIA56UE5WHAJENUB5PD"
      SMTP_PASSWORD = "BGqWbktM06akdXLLyu1+xcdMY+g6kw8R1AAvQORe++g3"

      # Email Settings (EMAIL_TO supports comma-separated list for multiple recipients)
      EMAIL_FROM    = "zyc@gegroup.com.au"
      EMAIL_TO      = "zyc@gegroup.com.au,CharterHallAFDD@gegroup.com.au"
      EMAIL_SUBJECT = "CIM AFDD Report - Charter Hall"
    }
  }

  depends_on = [aws_cloudwatch_log_group.cim_report_exporter]

  tags = local.common_tags

  # Ignore changes to image_uri as it's managed by deploy script
  lifecycle {
    ignore_changes = [image_uri]
  }
}

# -----------------------------
# EventBridge Scheduler: Daily
# -----------------------------
resource "aws_scheduler_schedule" "cim_report_daily" {
  name       = "cim-report-exporter-daily"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  # Daily at 8:00 AM Sydney time
  schedule_expression          = "cron(0 8 * * ? *)"
  schedule_expression_timezone = "Australia/Sydney"

  target {
    arn      = aws_lambda_function.cim_report_exporter.arn
    role_arn = aws_iam_role.cim_scheduler_role.arn
  }
}

# -----------------------------
# IAM Role for EventBridge Scheduler
# -----------------------------
resource "aws_iam_role" "cim_scheduler_role" {
  name = "sbm-cim-scheduler-role"

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

resource "aws_iam_role_policy" "cim_scheduler_invoke_lambda" {
  name = "sbm-cim-scheduler-invoke-lambda"
  role = aws_iam_role.cim_scheduler_role.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "lambda:InvokeFunction"
      Resource = aws_lambda_function.cim_report_exporter.arn
    }]
  })
}

# -----------------------------
# CloudWatch Alarm: Lambda Errors
# -----------------------------
resource "aws_cloudwatch_metric_alarm" "cim_exporter_errors" {
  alarm_name          = "cim-report-exporter-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 3600 # 1 hour
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "CIM Report Exporter Lambda errors"

  dimensions = {
    FunctionName = aws_lambda_function.cim_report_exporter.function_name
  }

  alarm_actions = [data.aws_sns_topic.sbm_alerts.arn]
  ok_actions    = [data.aws_sns_topic.sbm_alerts.arn]

  tags = local.common_tags
}

# -----------------------------
# Outputs
# -----------------------------
output "cim_exporter_ecr_url" {
  description = "ECR repository URL for CIM Exporter"
  value       = aws_ecr_repository.cim_exporter.repository_url
}

output "cim_exporter_function_name" {
  description = "CIM Report Exporter Lambda function name"
  value       = aws_lambda_function.cim_report_exporter.function_name
}
