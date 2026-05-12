# ================================
# Monitoring: SNS + CloudWatch Alarms
# ================================

# -----------------------------
# SNS Topic for Alerts
# -----------------------------
resource "aws_sns_topic" "sbm_alerts" {
  name = "sbm-ingester-alerts"

  tags = {
    Name = "sbm-ingester-alerts"
  }
}

# -----------------------------
# CloudWatch Alarms
# -----------------------------
resource "aws_cloudwatch_metric_alarm" "dlq_messages" {
  alarm_name          = "sbm-ingester-dlq-messages"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Average"
  threshold           = 0
  alarm_description   = "Alert when messages appear in DLQ"
  alarm_actions       = [aws_sns_topic.sbm_alerts.arn]

  dimensions = {
    QueueName = aws_sqs_queue.sbm_files_ingester_dlq.name
  }
}

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "sbm-ingester-lambda-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Alert when Lambda function errors occur"
  alarm_actions       = [aws_sns_topic.sbm_alerts.arn]

  dimensions = {
    FunctionName = aws_lambda_function.sbm_files_ingester.function_name
  }
}

# -----------------------------
# File Processor: extended alarms
# -----------------------------
resource "aws_cloudwatch_metric_alarm" "max_retries_exceeded" {
  alarm_name          = "FileProcessor-MaxRetriesExceeded"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "MaxRetriesExceeded"
  namespace           = "SBM/Ingester"
  period              = 86400 # 1 day
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "File-stability retry budget exhausted on at least one file in 24h."
  alarm_actions       = [aws_sns_topic.sbm_alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "parse_error_spike" {
  alarm_name          = "FileProcessor-ParseErrorSpike"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ParseErrorFiles"
  namespace           = "SBM/Ingester"
  period              = 3600
  statistic           = "Sum"
  threshold           = 5 # baseline placeholder; tune after 1-2 weeks
  alarm_description   = "Parse-error file count exceeded threshold over 1 hour."
  alarm_actions       = [aws_sns_topic.sbm_alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "file_processor_error_rate" {
  alarm_name          = "FileProcessor-ErrorRate"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  threshold           = 1 # 1% — Lambda Errors / Invocations
  alarm_description   = "Lambda error rate > 1% over 5 min."
  alarm_actions       = [aws_sns_topic.sbm_alerts.arn]

  metric_query {
    id          = "errorRate"
    expression  = "100 * errors / invocations"
    label       = "Error rate (%)"
    return_data = "true"
  }

  metric_query {
    id = "errors"
    metric {
      metric_name = "Errors"
      namespace   = "AWS/Lambda"
      period      = 300
      stat        = "Sum"
      dimensions = {
        FunctionName = aws_lambda_function.sbm_files_ingester.function_name
      }
    }
  }

  metric_query {
    id = "invocations"
    metric {
      metric_name = "Invocations"
      namespace   = "AWS/Lambda"
      period      = 300
      stat        = "Sum"
      dimensions = {
        FunctionName = aws_lambda_function.sbm_files_ingester.function_name
      }
    }
  }
}

resource "aws_cloudwatch_metric_alarm" "idempotent_skip_spike" {
  alarm_name          = "FileProcessor-IdempotentSkipSpike"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ConditionalCheckFailedRequests"
  namespace           = "AWS/DynamoDB"
  period              = 3600
  statistic           = "Sum"
  threshold           = 50 # placeholder; tune after baseline measured
  alarm_description   = "Cache-hit rate on idempotency table is unusually high (> threshold over 1 hour)."
  alarm_actions       = [aws_sns_topic.sbm_alerts.arn]

  dimensions = {
    TableName = aws_dynamodb_table.sbm_ingester_idempotency.name
  }
}

# Anomaly detector for the S3DuplicateEvent metric introduced 2026-05-12.
# Normal baseline expected: ~30 events/day (~5% of invocations) from
# S3's at-least-once ObjectCreated delivery. Alarm fires when the
# duplicates/invocations ratio crosses 50% over a 2-hour evaluation —
# that level indicates either an S3 misbehavior or that our
# move-after-process logic stopped working.
resource "aws_cloudwatch_metric_alarm" "file_processor_duplicate_event_spike" {
  alarm_name          = "FileProcessor-DuplicateEventSpike"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  threshold           = 0.5
  treat_missing_data  = "notBreaching"
  alarm_description   = "S3DuplicateEvent / Invocations ratio above 50% — investigate move-after-process logic or S3 event configuration"
  alarm_actions       = [aws_sns_topic.sbm_alerts.arn]

  metric_query {
    id          = "ratio"
    expression  = "duplicates / invocations"
    label       = "S3DuplicateEvent ratio"
    return_data = true
  }

  metric_query {
    id          = "duplicates"
    return_data = false
    metric {
      namespace   = "SBM/Ingester"
      metric_name = "S3DuplicateEvent"
      period      = 3600
      stat        = "Sum"
    }
  }

  metric_query {
    id          = "invocations"
    return_data = false
    metric {
      namespace   = "AWS/Lambda"
      metric_name = "Invocations"
      period      = 3600
      stat        = "Sum"
      dimensions = {
        FunctionName = aws_lambda_function.sbm_files_ingester.function_name
      }
    }
  }

  tags = {
    Name = "FileProcessor-DuplicateEventSpike"
  }
}
