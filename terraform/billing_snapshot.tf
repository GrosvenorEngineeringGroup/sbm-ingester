# =====================================================
# Bunnings Billing Snapshot Lambda
# =====================================================
# Weekly Lambda exports Bunnings billing data from the Hudi data lake to a
# wide-format CSV at s3://gegoptimareports/bunnings-billing/billing-latest.csv
# for SkySpark consumption.
#
# Spec: docs/superpowers/specs/2026-05-18-bunnings-billing-snapshot-design.md
# Plan: docs/superpowers/plans/2026-05-18-bunnings-billing-snapshot.md

data "aws_caller_identity" "current" {}

# -----------------------------
# Athena Workgroup (dedicated)
# -----------------------------
# A dedicated workgroup isolates this Lambda's concurrent-DML slots from the
# account-level "primary" workgroup and lets us cap per-query data scanned.
resource "aws_athena_workgroup" "billing_snapshot" {
  name        = "sbm-billing-snapshot"
  description = "Dedicated workgroup for sbm-bunnings-billing-snapshot Lambda"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = 2 * 1024 * 1024 * 1024 # 2 GB safety cap

    result_configuration {
      output_location = "s3://sbm-file-ingester/athena-results/"
    }
  }

  force_destroy = true

  tags = local.common_tags
}

# -----------------------------
# CloudWatch Log Group
# -----------------------------
resource "aws_cloudwatch_log_group" "billing_snapshot" {
  name              = "/aws/lambda/sbm-bunnings-billing-snapshot"
  retention_in_days = var.log_retention_days

  tags = local.common_tags
}

# -----------------------------
# IAM Role: Lambda Execution
# -----------------------------
resource "aws_iam_role" "billing_snapshot" {
  name = "sbm-bunnings-billing-snapshot-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "billing_snapshot_basic" {
  role       = aws_iam_role.billing_snapshot.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "billing_snapshot_inline" {
  name = "sbm-billing-snapshot-inline"
  role = aws_iam_role.billing_snapshot.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # s3:GetObject also authorises HEAD requests against the same key —
        # no separate s3:HeadObject action exists in IAM.
        Sid      = "ReadMappingsJson"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = "arn:aws:s3:::sbm-file-ingester/nem12_mappings.json"
      },
      {
        Sid    = "AthenaQuery"
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
        ]
        Resource = aws_athena_workgroup.billing_snapshot.arn
      },
      {
        Sid    = "GlueHudiCatalog"
        Effect = "Allow"
        Action = [
          "glue:GetTable",
          "glue:GetDatabase",
          "glue:GetPartitions",
        ]
        Resource = [
          "arn:aws:glue:ap-southeast-2:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:ap-southeast-2:${data.aws_caller_identity.current.account_id}:database/default",
          "arn:aws:glue:ap-southeast-2:${data.aws_caller_identity.current.account_id}:table/default/sensordata_default",
        ]
      },
      {
        Sid      = "AthenaResultsBucket"
        Effect   = "Allow"
        Action   = ["s3:GetBucketAcl", "s3:ListBucket"]
        Resource = "arn:aws:s3:::sbm-file-ingester"
      },
      {
        Sid      = "AthenaResultsObjects"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"]
        Resource = "arn:aws:s3:::sbm-file-ingester/athena-results/*"
      },
      {
        Sid      = "WriteBillingSnapshot"
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = "arn:aws:s3:::gegoptimareports/bunnings-billing/*"
      },
      {
        Sid      = "EmitMetrics"
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData"]
        Resource = "*"
        Condition = {
          StringEquals = {
            "cloudwatch:namespace" = "BillingSnapshot"
          }
        }
      },
    ]
  })
}

# -----------------------------
# Lambda Function
# -----------------------------
resource "aws_lambda_function" "billing_snapshot" {
  function_name = "sbm-bunnings-billing-snapshot"
  description   = "Weekly Bunnings billing snapshot from Hudi via Athena → CSV in gegoptimareports"
  role          = aws_iam_role.billing_snapshot.arn
  handler       = "app.lambda_handler"
  runtime       = "python3.13"
  timeout       = 900
  memory_size   = 512
  s3_bucket     = var.deployment_bucket
  s3_key        = "${local.lambda_s3_prefix}/billing_snapshot.zip"

  environment {
    variables = {
      POWERTOOLS_LOG_LEVEL    = "INFO"
      POWERTOOLS_SERVICE_NAME = "sbm-bunnings-billing-snapshot"
      ATHENA_WORKGROUP        = aws_athena_workgroup.billing_snapshot.name
      ATHENA_DATABASE         = "default"
      ATHENA_TABLE            = "sensordata_default"
      MAPPINGS_BUCKET         = "sbm-file-ingester"
      MAPPINGS_KEY            = "nem12_mappings.json"
      OUTPUT_BUCKET           = "gegoptimareports"
      OUTPUT_KEY              = "bunnings-billing/billing-latest.csv"
      HISTORY_START_DATE      = "2025-01-01"
      CHUNK_COUNT             = "8"
      MAX_WORKERS             = "3"
      POLL_INTERVAL_SECONDS   = "2"
      POLL_TIMEOUT_SECONDS    = "240"
    }
  }

  tracing_config {
    mode = "PassThrough"
  }

  depends_on = [aws_cloudwatch_log_group.billing_snapshot]

  tags = local.common_tags
}

# -----------------------------
# EventBridge Scheduler Role
# -----------------------------
resource "aws_iam_role" "billing_snapshot_scheduler" {
  name = "sbm-billing-snapshot-scheduler-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "billing_snapshot_scheduler_invoke" {
  name = "sbm-billing-snapshot-scheduler-invoke"
  role = aws_iam_role.billing_snapshot_scheduler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "lambda:InvokeFunction"
      Resource = aws_lambda_function.billing_snapshot.arn
    }]
  })
}

# -----------------------------
# EventBridge Schedule (Sun 08:00 Sydney)
# -----------------------------
resource "aws_scheduler_schedule" "billing_snapshot_weekly" {
  name        = "sbm-bunnings-billing-snapshot-weekly"
  description = "Weekly Bunnings billing snapshot — Sunday 08:00 Sydney"
  group_name  = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "cron(0 8 ? * SUN *)"
  schedule_expression_timezone = "Australia/Sydney"

  target {
    arn      = aws_lambda_function.billing_snapshot.arn
    role_arn = aws_iam_role.billing_snapshot_scheduler.arn

    retry_policy {
      maximum_retry_attempts = 0
    }
  }
}

# -----------------------------
# CloudWatch Alarm: Lambda Errors
# -----------------------------
resource "aws_cloudwatch_metric_alarm" "billing_snapshot_errors" {
  alarm_name          = "sbm-bunnings-billing-snapshot-errors"
  alarm_description   = "Bunnings billing snapshot Lambda errored"
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  statistic           = "Sum"
  period              = 86400 # 24 h
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.billing_snapshot.function_name
  }

  alarm_actions = [aws_sns_topic.sbm_alerts.arn]
  ok_actions    = [aws_sns_topic.sbm_alerts.arn]

  tags = local.common_tags
}
