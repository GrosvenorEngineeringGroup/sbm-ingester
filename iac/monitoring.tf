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
