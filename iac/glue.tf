# ================================
# Glue: Hudi Data Import Pipeline
# ================================
# Imports sensor data from CSV files into Apache Hudi data lake
#
# Flow: EventBridge (hourly) -> Lambda (check file count) -> Glue Job
# Condition: Only runs when ≥2 files exist in hudibucketsrc/sensorDataFiles/

# -----------------------------
# IAM Role (existing)
# -----------------------------
data "aws_iam_role" "glue_role" {
  name = "hudiTrial-2-GlueRoleStack-1VGL72HA871CN-myGlueRole-D7MCD81KE1FV"
}

# -----------------------------
# Glue Job
# -----------------------------
# Note: Hudi connector "Apache Hudi Connector 0.10.1 for AWS Glue 3.0"
# was created via AWS Console/Marketplace.

resource "aws_glue_job" "hudi_import" {
  name        = "DataImportIntoLake"
  description = "Imports data from CSVs into Datalake"
  role_arn    = data.aws_iam_role.glue_role.arn

  glue_version      = "4.0"
  worker_type       = "G.2X"
  number_of_workers = 5
  timeout           = 1440 # 24 hours
  max_retries       = 0

  command {
    name            = "glueetl"
    script_location = "s3://${local.glue_assets_bucket}/scripts/${local.glue_script_name}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--enable-metrics"                   = "true"
    "--enable-spark-ui"                  = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"          = "true"
    "--enable-auto-scaling"              = "true"
    "--enable-job-insights"              = "false"
    "--TempDir"                          = "s3://${local.glue_assets_bucket}/temporary/"
    "--spark-event-logs-path"            = "s3://${local.glue_assets_bucket}/sparkHistoryLogs/"

    # Job-specific arguments
    "--HUDI_TABLE_NAME"       = "sensorData"
    "--HUDI_DB_NAME"          = "Default"
    "--HUDI_INIT_SORT_OPTION" = "DEFAULT"
    "--OUTPUT_BUCKET"         = local.hudi_output_bucket
  }

  connections = ["Apache Hudi Connector 0.10.1 for AWS Glue 3.0"]

  execution_property {
    max_concurrent_runs = 1
  }

  tags = local.common_tags
}

# -----------------------------
# Lambda: Glue Trigger
# -----------------------------
# Checks S3 file count and triggers Glue job if ≥2 files exist

resource "aws_lambda_function" "glue_trigger" {
  function_name = "sbm-glue-trigger"
  role          = data.aws_iam_role.ingester_role.arn
  handler       = "app.lambda_handler"
  runtime       = "python3.13"
  memory_size   = 128
  timeout       = 30
  s3_bucket     = var.deployment_bucket
  s3_key        = "${local.lambda_s3_prefix}/glue-trigger.zip"

  environment {
    variables = {
      BUCKET_NAME     = local.hudi_source_bucket
      PREFIX          = "sensorDataFiles/"
      FILES_THRESHOLD = "10"
      GLUE_JOB_NAME   = aws_glue_job.hudi_import.name
    }
  }

  depends_on = [aws_cloudwatch_log_group.glue_trigger]

  tags = local.common_tags
}

resource "aws_iam_role_policy" "glue_trigger_access" {
  name = "sbm-glue-trigger-access"
  role = data.aws_iam_role.ingester_role.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["glue:StartJobRun"]
        Resource = aws_glue_job.hudi_import.arn
      }
    ]
  })
}

# -----------------------------
# EventBridge: Hourly Schedule
# -----------------------------
resource "aws_cloudwatch_event_rule" "glue_trigger_schedule" {
  name                = "sbm-glue-trigger-schedule"
  description         = "Trigger Glue job check every hour"
  schedule_expression = "rate(1 hour)"
}

resource "aws_cloudwatch_event_target" "glue_trigger_target" {
  rule      = aws_cloudwatch_event_rule.glue_trigger_schedule.name
  target_id = "glue-trigger-lambda"
  arn       = aws_lambda_function.glue_trigger.arn
}

resource "aws_lambda_permission" "allow_eventbridge_glue_trigger" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.glue_trigger.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.glue_trigger_schedule.arn
}

# -----------------------------
# Note: CloudWatch Logs
# -----------------------------
# Glue 4.0 uses shared log groups:
# - /aws-glue/jobs/error
# - /aws-glue/jobs/logs-v2
# - /aws-glue/jobs/output
# No job-specific log group is needed
