"""
Lambda function to check S3 file count and trigger Glue job.

Replaces the previous Step Function + getBucketSize Lambda setup.
Triggered hourly by EventBridge to process sensor data files.
"""

import os
from typing import Any

import boto3
from aws_lambda_powertools import Logger, Tracer
from botocore.exceptions import ClientError

tracer = Tracer()
logger = Logger()

# Configuration from environment variables
BUCKET_NAME = os.environ.get("BUCKET_NAME", "hudibucketsrc")
PREFIX = os.environ.get("PREFIX", "sensorDataFiles/")
FILES_THRESHOLD = int(os.environ.get("FILES_THRESHOLD", "2"))
GLUE_JOB_NAME = os.environ.get("GLUE_JOB_NAME", "DataImportIntoLake")


@tracer.capture_method
def count_files_in_prefix(s3_client: Any, bucket: str, prefix: str) -> int:
    """
    Count files in an S3 prefix, excluding directory markers.

    Args:
        s3_client: boto3 S3 client
        bucket: S3 bucket name
        prefix: S3 prefix to search

    Returns:
        Number of files (excluding directory markers)
    """
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    contents = response.get("Contents", [])

    # Filter out directory markers (keys ending with /)
    actual_files = [obj for obj in contents if not obj["Key"].endswith("/")]
    return len(actual_files)


@tracer.capture_method
def start_glue_job(glue_client: Any, job_name: str) -> dict[str, Any]:
    """
    Start a Glue job run.

    Args:
        glue_client: boto3 Glue client
        job_name: Name of the Glue job to start

    Returns:
        dict with status and optional job run ID or reason

    Raises:
        ClientError: For unexpected AWS errors
    """
    try:
        response = glue_client.start_job_run(JobName=job_name)
        return {"started": True, "job_run_id": response.get("JobRunId")}
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code == "ConcurrentRunsExceededException":
            logger.info(f"Glue job '{job_name}' is already running, skipping")
            return {"started": False, "reason": "already_running"}
        raise


@logger.inject_lambda_context
@tracer.capture_lambda_handler
def lambda_handler(event: dict, context: Any) -> dict:
    """
    Check if enough files exist in S3, then trigger Glue job.

    Args:
        event: Lambda event (from EventBridge)
        context: Lambda context

    Returns:
        dict with triggered status and file count
    """
    s3 = boto3.client("s3")
    glue = boto3.client("glue")

    # Count files in the source directory
    file_count = count_files_in_prefix(s3, BUCKET_NAME, PREFIX)
    logger.info(f"Found {file_count} files in s3://{BUCKET_NAME}/{PREFIX}")

    if file_count >= FILES_THRESHOLD:
        logger.info(f"File count ({file_count}) >= threshold ({FILES_THRESHOLD}), triggering Glue job")
        result = start_glue_job(glue, GLUE_JOB_NAME)

        if result["started"]:
            logger.info(f"Glue job '{GLUE_JOB_NAME}' started, run_id={result.get('job_run_id')}")
            return {"triggered": True, "file_count": file_count, "job_run_id": result.get("job_run_id")}
        return {"triggered": False, "file_count": file_count, "reason": result.get("reason")}

    logger.info(f"File count ({file_count}) < threshold ({FILES_THRESHOLD}), skipping Glue job")
    return {"triggered": False, "file_count": file_count, "reason": "below_threshold"}
