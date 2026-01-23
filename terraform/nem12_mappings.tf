# ================================
# NEM12 Mappings: Export to S3 + API Gateway
# ================================

# -----------------------------
# Lambda: NEM12 Mappings Exporter
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

  vpc_config {
    subnet_ids         = local.neptune_subnet_ids
    security_group_ids = local.neptune_security_group_ids
  }

  environment {
    variables = {
      NEPTUNEENDPOINT = var.neptune_endpoint
      NEPTUNEPORT     = var.neptune_port
    }
  }

  tracing_config {
    mode = "Active"
  }
}

# -----------------------------
# EventBridge: Hourly Schedule
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
# API Gateway: REST API
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

# -----------------------------
# API Key + Usage Plan
# -----------------------------
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
