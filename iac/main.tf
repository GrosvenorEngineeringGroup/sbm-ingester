# ================================
# Default Lambda Log Groups (Terraform-managed)
# ================================

resource "aws_cloudwatch_log_group" "sbm_files_ingester_default" {
  name              = "/aws/lambda/sbm-files-ingester"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "sbm_files_ingester_redrive_default" {
  name              = "/aws/lambda/sbm-files-ingester-redrive"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "sbm_files_ingester_nem12_mappings_default" {
  name              = "/aws/lambda/sbm-files-ingester-nem12-mappings-to-s3"
  retention_in_days = var.log_retention_days
}

# ================================
# Custom Log Groups (Application-level)
# ================================

resource "aws_cloudwatch_log_group" "sbm_ingester_error_log" {
  name              = "sbm-ingester-error-log"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "sbm_ingester_execution_log" {
  name              = "sbm-ingester-execution-log"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "sbm_ingester_metrics_log" {
  name              = "sbm-ingester-metrics-log"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "sbm_ingester_parse_error_log" {
  name              = "sbm-ingester-parse-error-log"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "sbm_ingester_runtime_error_log" {
  name              = "sbm-ingester-runtime-error-log"
  retention_in_days = var.log_retention_days
}

# -----------------------------
# IAM role (already exists, re-use)
# -----------------------------
data "aws_iam_role" "ingester_role" {
  name = "getIdFromNem12Id-role-153b7a0a"
}

# -----------------------------
# Lambda: sbm-files-ingester
# -----------------------------
resource "aws_lambda_function" "sbm_files_ingester" {
  function_name                  = "sbm-files-ingester"
  role                           = data.aws_iam_role.ingester_role.arn
  handler                        = "functions.file_processor.app.lambda_handler"
  runtime                        = "python3.13"
  memory_size                    = 512
  timeout                        = 300
  reserved_concurrent_executions = 5
  s3_bucket                      = var.deployment_bucket
  s3_key                         = "${local.lambda_s3_prefix}/ingester.zip"

  tracing_config {
    mode = "Active"
  }
}

# -----------------------------
# DynamoDB Idempotency Table
# -----------------------------
resource "aws_dynamodb_table" "sbm_ingester_idempotency" {
  name         = "sbm-ingester-idempotency"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "file_key"

  attribute {
    name = "file_key"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  tags = {
    Name = "sbm-ingester-idempotency"
  }
}

# Add DynamoDB permissions to Lambda role
resource "aws_iam_role_policy" "idempotency_access" {
  name = "sbm-ingester-idempotency-access"
  role = data.aws_iam_role.ingester_role.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "dynamodb:PutItem",
          "dynamodb:GetItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem"
        ]
        Resource = aws_dynamodb_table.sbm_ingester_idempotency.arn
      }
    ]
  })
}

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
# SQS Queues (Main + DLQ)
# -----------------------------
resource "aws_sqs_queue" "sbm_files_ingester_dlq" {
  name                       = "sbm-files-ingester-dlq"
  message_retention_seconds  = 1209600 # 14 days
  visibility_timeout_seconds = 300

  tags = {
    Name = "sbm-files-ingester-dlq"
  }
}

resource "aws_sqs_queue" "sbm_files_ingester_queue" {
  name                       = "sbm-files-ingester-queue"
  visibility_timeout_seconds = 300

  tags = {
    Name = "sbm-files-ingester-queue"
  }
}

# SQS Redrive Policy (Main Queue -> DLQ)
resource "aws_sqs_queue_redrive_policy" "main_queue_redrive" {
  queue_url = aws_sqs_queue.sbm_files_ingester_queue.id

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.sbm_files_ingester_dlq.arn
    maxReceiveCount     = 3
  })
}

# SQS Redrive Allow Policy (DLQ accepts from Main Queue)
resource "aws_sqs_queue_redrive_allow_policy" "dlq_allow" {
  queue_url = aws_sqs_queue.sbm_files_ingester_dlq.id

  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.sbm_files_ingester_queue.arn]
  })
}

# CloudWatch Alarm for DLQ
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

# CloudWatch Alarm for Lambda Errors
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
# S3 -> SQS Event Notifications
# -----------------------------
resource "aws_s3_bucket_notification" "sbm_file_ingester_notifications" {
  bucket = var.input_bucket

  queue {
    queue_arn     = aws_sqs_queue.sbm_files_ingester_queue.arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "newTBP/"
  }
}

# -----------------------------
# SQS Queue Policy
# -----------------------------
resource "aws_sqs_queue_policy" "queue_policy" {
  queue_url = aws_sqs_queue.sbm_files_ingester_queue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowS3Send"
        Effect = "Allow"
        Principal = {
          Service = "s3.amazonaws.com"
        }
        Action   = "sqs:SendMessage"
        Resource = aws_sqs_queue.sbm_files_ingester_queue.arn
        Condition = {
          ArnEquals = {
            "aws:SourceArn" = "arn:aws:s3:::${var.input_bucket}"
          }
        }
      },
      {
        Sid    = "AllowLambdaPoll"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = [
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes"
        ]
        Resource = aws_sqs_queue.sbm_files_ingester_queue.arn
      }
    ]
  })
}

# Lambda event source mapping (SQS -> Lambda)
resource "aws_lambda_event_source_mapping" "sqs_trigger" {
  event_source_arn = aws_sqs_queue.sbm_files_ingester_queue.arn
  function_name    = aws_lambda_function.sbm_files_ingester.arn
  batch_size       = 1

  scaling_config {
    maximum_concurrency = 5
  }
}

# -----------------------------
# Lambda: sbm-files-ingester-redrive
# -----------------------------
resource "aws_lambda_function" "sbm_files_ingester_redrive" {
  function_name = "sbm-files-ingester-redrive"
  role          = data.aws_iam_role.ingester_role.arn
  handler       = "redrive.lambda_handler"
  runtime       = "python3.13"
  memory_size   = 128
  timeout       = 600
  s3_bucket     = var.deployment_bucket
  s3_key        = "${local.lambda_s3_prefix}/redrive.zip"

  tracing_config {
    mode = "Active"
  }
}

# -----------------------------
# Lambda: sbm-files-ingester-nem12-mappings-to-s3
# -----------------------------
resource "aws_lambda_function" "sbm_files_ingester_nem12_mappings" {
  function_name = "sbm-files-ingester-nem12-mappings-to-s3"
  role          = data.aws_iam_role.ingester_role.arn
  handler       = "nem12_mappings_to_s3.lambda_handler"
  runtime       = "python3.13"
  memory_size   = 128
  timeout       = 60
  s3_bucket     = var.deployment_bucket
  s3_key        = "${local.lambda_s3_prefix}/nem12-mappings-to-s3.zip"

  layers = [
    "arn:aws:lambda:ap-southeast-2:318396632821:layer:aenumLayer:1",
    "arn:aws:lambda:ap-southeast-2:318396632821:layer:aiohhtpReqLayer:1",
    "arn:aws:lambda:ap-southeast-2:318396632821:layer:idnaLayer:1",
    "arn:aws:lambda:ap-southeast-2:318396632821:layer:isodateLayer:1",
    "arn:aws:lambda:ap-southeast-2:318396632821:layer:neptuneLayer:4",
  ]

  vpc_config {
    subnet_ids         = local.neptune_subnet_ids
    security_group_ids = local.neptune_security_group_ids
  }

  environment {
    variables = {
      neptuneEndpoint = var.neptune_endpoint
      neptunePort     = var.neptune_port
    }
  }

  tracing_config {
    mode = "Active"
  }
}

# -----------------------------
# Schedule rule (every hour)
# -----------------------------
resource "aws_cloudwatch_event_rule" "nem12_schedule" {
  name                = "sbm-nem12-mappings-schedule"
  schedule_expression = "rate(1 hour)"
}

resource "aws_cloudwatch_event_target" "schedule_target" {
  rule      = aws_cloudwatch_event_rule.nem12_schedule.name
  target_id = "lambda"
  arn       = aws_lambda_function.sbm_files_ingester_nem12_mappings.arn
}

resource "aws_lambda_permission" "allow_schedule" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.sbm_files_ingester_nem12_mappings.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.nem12_schedule.arn
}

# -----------------------------
# API Gateway (REST API) with API Key + Usage Plan
# -----------------------------
resource "aws_api_gateway_rest_api" "sbm_api" {
  name        = "sbm-files-ingester-api"
  description = "API Gateway for sbm-files-ingester-nem12-mappings-to-s3"
}

resource "aws_api_gateway_resource" "nem12_resource" {
  rest_api_id = aws_api_gateway_rest_api.sbm_api.id
  parent_id   = aws_api_gateway_rest_api.sbm_api.root_resource_id
  path_part   = "nem12-mappings"
}

resource "aws_api_gateway_method" "get_method" {
  rest_api_id      = aws_api_gateway_rest_api.sbm_api.id
  resource_id      = aws_api_gateway_resource.nem12_resource.id
  http_method      = "GET"
  authorization    = "NONE"
  api_key_required = true
}

resource "aws_api_gateway_integration" "lambda_integration" {
  rest_api_id             = aws_api_gateway_rest_api.sbm_api.id
  resource_id             = aws_api_gateway_resource.nem12_resource.id
  http_method             = aws_api_gateway_method.get_method.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = aws_lambda_function.sbm_files_ingester_nem12_mappings.invoke_arn
}

resource "aws_lambda_permission" "apigw_lambda" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.sbm_files_ingester_nem12_mappings.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.sbm_api.execution_arn}/*/*"
}

resource "aws_api_gateway_deployment" "api_deployment" {
  rest_api_id = aws_api_gateway_rest_api.sbm_api.id

  triggers = {
    redeploy = sha1(jsonencode(aws_api_gateway_integration.lambda_integration))
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_api_gateway_stage" "api_stage" {
  rest_api_id   = aws_api_gateway_rest_api.sbm_api.id
  deployment_id = aws_api_gateway_deployment.api_deployment.id
  stage_name    = "prod"
}

# API Key + Usage Plan
resource "aws_api_gateway_api_key" "sbm_api_key" {
  name    = "sbm-ingester-api-key"
  enabled = true
}

resource "aws_api_gateway_usage_plan" "sbm_usage_plan" {
  name        = "sbm-ingester-usage-plan"
  description = "Limit API calls to 500 per day"

  quota_settings {
    limit  = 500
    period = "DAY"
  }

  api_stages {
    api_id = aws_api_gateway_rest_api.sbm_api.id
    stage  = aws_api_gateway_stage.api_stage.stage_name
  }
}

resource "aws_api_gateway_usage_plan_key" "sbm_usage_plan_key" {
  key_id        = aws_api_gateway_api_key.sbm_api_key.id
  key_type      = "API_KEY"
  usage_plan_id = aws_api_gateway_usage_plan.sbm_usage_plan.id
}

# ================================
# Weekly Archiver Lambda
# ================================
resource "aws_cloudwatch_log_group" "weekly_archiver" {
  name              = "/aws/lambda/sbm-weekly-archiver"
  retention_in_days = var.log_retention_days
}

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

# EventBridge Rule - Every Monday at UTC 00:00 (AEST 11:00)
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
