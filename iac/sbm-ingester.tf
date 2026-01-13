terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.6"
    }
  }

  required_version = ">= 1.6.0"
}

provider "aws" {
  region = "ap-southeast-2"

  default_tags {
    tags = {
      project     = "SBM-Ingestion"
      managed_by  = "terraform"
    }
  }
}

# ================================
# Default Lambda Log Groups (Terraform-managed)
# ================================

resource "aws_cloudwatch_log_group" "sbm_files_ingester_default" {
  name              = "/aws/lambda/sbm-files-ingester"
  retention_in_days = 30
  tags = {
    project     = "SBM-Ingestion"
    managed_by  = "terraform"
  }
}

resource "aws_cloudwatch_log_group" "sbm_files_ingester_redrive_default" {
  name              = "/aws/lambda/sbm-files-ingester-redrive"
  retention_in_days = 30
  tags = {
    project     = "SBM-Ingestion"
    managed_by  = "terraform"
  }
}

resource "aws_cloudwatch_log_group" "sbm_files_ingester_nem12_mappings_default" {
  name              = "/aws/lambda/sbm-files-ingester-nem12-mappings-to-s3"
  retention_in_days = 30
  tags = {
    project     = "SBM-Ingestion"
    managed_by  = "terraform"
  }
}

# ================================
# Custom Log Groups (Application-level)
# ================================

resource "aws_cloudwatch_log_group" "sbm_ingester_error_log" {
  name              = "sbm-ingester-error-log"
  retention_in_days = 30
  tags = {
    project     = "SBM-Ingestion"
    managed_by  = "terraform"
  }
}

resource "aws_cloudwatch_log_group" "sbm_ingester_execution_log" {
  name              = "sbm-ingester-execution-log"
  retention_in_days = 30
  tags = {
    project     = "SBM-Ingestion"
    managed_by  = "terraform"
  }
}

resource "aws_cloudwatch_log_group" "sbm_ingester_metrics_log" {
  name              = "sbm-ingester-metrics-log"
  retention_in_days = 30
  tags = {
    project     = "SBM-Ingestion"
    managed_by  = "terraform"
  }
}

resource "aws_cloudwatch_log_group" "sbm_ingester_parse_error_log" {
  name              = "sbm-ingester-parse-error-log"
  retention_in_days = 30
  tags = {
    project     = "SBM-Ingestion"
    managed_by  = "terraform"
  }
}

resource "aws_cloudwatch_log_group" "sbm_ingester_runtime_error_log" {
  name              = "sbm-ingester-runtime-error-log"
  retention_in_days = 30
  tags = {
    project     = "SBM-Ingestion"
    managed_by  = "terraform"
  }
}

# -----------------------------
# IAM role (already exists, re-use)
# -----------------------------
data "aws_iam_role" "ingester_role" {
  name = "getIdFromNem12Id-role-153b7a0a"
}

# -----------------------------
# Lambda: sbm-files-ingester-2
# -----------------------------
resource "aws_lambda_function" "sbm_files_ingester" {
  function_name = "sbm-files-ingester"
  role          = data.aws_iam_role.ingester_role.arn
  handler       = "gemsDataParseAndWrite.lambda_handler"
  runtime       = "python3.12"
  memory_size   = 1024  # Increased from 512 for better performance
  timeout       = 300   # Increased from 120 to handle larger files
  reserved_concurrent_executions = 5
  s3_bucket = "gega-code-deployment-bucket"
  s3_key    = "sbm-files-ingester/ingester.zip"
}

# -----------------------------
# SQS Queue
# -----------------------------
resource "aws_sqs_queue" "sbm_files_ingester_queue" {
  name                       = "sbm-files-ingester-queue"
  visibility_timeout_seconds = 300
}

# -----------------------------
# S3 -> SQS Event Notifications
# -----------------------------
resource "aws_s3_bucket_notification" "sbm_file_ingester_notifications" {
  bucket = "sbm-file-ingester"

  queue {
    queue_arn     = aws_sqs_queue.sbm_files_ingester_queue.arn
    events        = ["s3:ObjectCreated:*"]

    filter_prefix = "newTBP/"
  }
}

# -----------------------------
# Update SQS Queue Policy (secure)
# -----------------------------
resource "aws_sqs_queue_policy" "queue_policy" {
  queue_url = aws_sqs_queue.sbm_files_ingester_queue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowS3Send"
        Effect    = "Allow"
        Principal = {
          Service = "s3.amazonaws.com"
        }
        Action    = "sqs:SendMessage"
        Resource  = aws_sqs_queue.sbm_files_ingester_queue.arn
        Condition = {
          ArnEquals = {
            "aws:SourceArn" = "arn:aws:s3:::sbm-file-ingester"
          }
        }
      },
      {
        Sid       = "AllowLambdaPoll"
        Effect    = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action    = [
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
  runtime       = "python3.12"
  memory_size   = 128
  timeout       = 600
  s3_bucket = "gega-code-deployment-bucket"
  s3_key    = "sbm-files-ingester/redrive.zip"
}

# -----------------------------
# Lambda: sbm-files-ingester-nem12-mappings-to-s3
# -----------------------------
resource "aws_lambda_function" "sbm_files_ingester_nem12_mappings" {
  function_name = "sbm-files-ingester-nem12-mappings-to-s3"
  role          = data.aws_iam_role.ingester_role.arn
  handler       = "nem12_mappings_to_s3.lambda_handler"
  runtime       = "python3.9"
  memory_size   = 128
  timeout       = 60
  s3_bucket = "gega-code-deployment-bucket"
  s3_key    = "sbm-files-ingester/nem12-mappings-to-s3.zip"

  layers = [
    "arn:aws:lambda:ap-southeast-2:318396632821:layer:aenumLayer:1",
    "arn:aws:lambda:ap-southeast-2:318396632821:layer:aiohhtpReqLayer:1",
    "arn:aws:lambda:ap-southeast-2:318396632821:layer:idnaLayer:1",
    "arn:aws:lambda:ap-southeast-2:318396632821:layer:isodateLayer:1",
    "arn:aws:lambda:ap-southeast-2:318396632821:layer:neptuneLayer:4",
  ]

  vpc_config {
    subnet_ids         = ["subnet-0b7ffe958514b2615", "subnet-02306ea93a94a2fcf", "subnet-0928e926296546e03"]
    security_group_ids = ["sg-02ece37ea391fba00"]
  }

  environment {
    variables = {
      neptuneEndpoint = "bw-1-instance-1.cov3fflnpa7n.ap-southeast-2.neptune.amazonaws.com"
      neptunePort     = "8182"
    }
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
  rest_api_id     = aws_api_gateway_rest_api.sbm_api.id
  resource_id     = aws_api_gateway_resource.nem12_resource.id
  http_method     = "GET"
  authorization   = "NONE"
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

# -----------------------------
# Outputs
# -----------------------------
output "sbm_api_invoke_url" {
  value = "${aws_api_gateway_rest_api.sbm_api.execution_arn}/${aws_api_gateway_stage.api_stage.stage_name}"
  description = "Invoke URL for the API (append /nem12-mappings)."
}

output "sbm_api_key_value" {
  value       = aws_api_gateway_api_key.sbm_api_key.value
  description = "API key to access the API."
  sensitive   = true
}
