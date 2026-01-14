# ================================
# File Ingester: Main Processing Pipeline
# ================================
# S3 -> SQS -> Lambda -> Data Lake

# -----------------------------
# IAM Role (existing, re-use)
# -----------------------------
data "aws_iam_role" "ingester_role" {
  name = "getIdFromNem12Id-role-153b7a0a"
}

# -----------------------------
# Lambda: Main File Processor
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
# Lambda: Redrive Handler
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
# DynamoDB: Idempotency Table
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
# SQS: Main Queue + DLQ
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

resource "aws_sqs_queue_redrive_policy" "main_queue_redrive" {
  queue_url = aws_sqs_queue.sbm_files_ingester_queue.id

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.sbm_files_ingester_dlq.arn
    maxReceiveCount     = 3
  })
}

resource "aws_sqs_queue_redrive_allow_policy" "dlq_allow" {
  queue_url = aws_sqs_queue.sbm_files_ingester_dlq.id

  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.sbm_files_ingester_queue.arn]
  })
}

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

# -----------------------------
# S3 -> SQS Event Notification
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
# SQS -> Lambda Trigger
# -----------------------------
resource "aws_lambda_event_source_mapping" "sqs_trigger" {
  event_source_arn = aws_sqs_queue.sbm_files_ingester_queue.arn
  function_name    = aws_lambda_function.sbm_files_ingester.arn
  batch_size       = 1

  scaling_config {
    maximum_concurrency = 5
  }
}
