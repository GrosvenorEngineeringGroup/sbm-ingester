"""SQS-triggered Lambda handler for the SBM file ingester.

This module is a thin SQS adapter. All business logic lives in
functions.file_processor.pipeline.ingest_file.

Pre-conditions (enforced at import time):
  - SQS_QUEUE_URL env var is set; KeyError on import otherwise so deploy
    fails fast rather than silently targeting the production queue.

Per-record flow:
  1. Decode the SQS record → bucket, key.
  2. Check file stability (S3 size stable for 2 consecutive checks).
  3. If unstable: requeue with backoff (up to MAX_REQUEUE_RETRIES); skip otherwise.
  4. If stable: build SourceFile, call ingest_file (which is idempotent +
     traced + emits per-file structured log + metrics).
  5. Return statusCode 200.
"""

from __future__ import annotations

import json
import os
import time
from typing import Any
from urllib.parse import unquote

import boto3
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.metrics import MetricUnit

from functions.file_processor.pipeline import ingest_file
from shared.source_file import SourceFile

# Required env var — KeyError on import if missing.
SQS_QUEUE_URL = os.environ["SQS_QUEUE_URL"]

# File-stability tuning (preserved from previous shape).
FILE_STABILITY_CHECK_INTERVAL = 5  # seconds between checks
FILE_STABILITY_MAX_WAIT = 30  # max seconds to wait for stabilisation
FILE_STABILITY_REQUIRED_CHECKS = 2  # consecutive stable checks required
MAX_REQUEUE_RETRIES = 3  # aligned with SQS maxReceiveCount = 3 (per spec)
REQUEUE_DELAY_SECONDS = 60

logger = Logger(service="file-processor")
tracer = Tracer(service="file-processor")
metrics = Metrics(namespace="SBM/Ingester")

s3_client = boto3.client("s3")
sqs_client = boto3.client("sqs")


@tracer.capture_method
def check_file_stability(bucket: str, key: str) -> tuple[bool, int]:
    """Wait for an S3 object's size to stabilise across 2 consecutive HEADs."""
    last_size = -1
    stable_count = 0
    total_wait = 0

    while total_wait < FILE_STABILITY_MAX_WAIT:
        try:
            response = s3_client.head_object(Bucket=bucket, Key=key)
            current_size = response["ContentLength"]

            if current_size == 0:
                logger.debug(
                    "File is empty, waiting",
                    extra={"bucket": bucket, "key": key, "waited": total_wait},
                )
                time.sleep(FILE_STABILITY_CHECK_INTERVAL)
                total_wait += FILE_STABILITY_CHECK_INTERVAL
                continue

            if current_size == last_size:
                stable_count += 1
                if stable_count >= FILE_STABILITY_REQUIRED_CHECKS:
                    logger.info(
                        "File is stable",
                        extra={"bucket": bucket, "key": key, "size": current_size},
                    )
                    return True, current_size
            else:
                stable_count = 0

            last_size = current_size
            time.sleep(FILE_STABILITY_CHECK_INTERVAL)
            total_wait += FILE_STABILITY_CHECK_INTERVAL
        except Exception as e:
            if hasattr(e, "response") and e.response.get("Error", {}).get("Code") == "NoSuchKey":
                logger.warning("File not found", extra={"bucket": bucket, "key": key})
                return False, 0
            logger.error(
                "Error checking file stability",
                exc_info=True,
                extra={"bucket": bucket, "key": key, "error": str(e)},
            )
            return False, 0

    logger.warning(
        "File stability check timed out",
        extra={"bucket": bucket, "key": key, "last_size": last_size, "waited": total_wait},
    )
    return False, 0


def requeue_message(original_body: dict, retry_count: int) -> bool:
    """Re-publish an SQS message with `_retry_count` incremented.

    Returns True if SQS accepted the message; False on send error (caller
    continues — the in-flight message has been received and will be deleted
    by the Lambda runtime).
    """
    try:
        new_body = original_body.copy()
        new_body["_retry_count"] = retry_count + 1
        sqs_client.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps(new_body),
            DelaySeconds=REQUEUE_DELAY_SECONDS,
        )
        logger.info(
            "Message requeued for later processing",
            extra={"retry_count": retry_count + 1, "delay_seconds": REQUEUE_DELAY_SECONDS},
        )
        return True
    except Exception as e:
        logger.error("Failed to requeue message", exc_info=True, extra={"error": str(e)})
        return False


@tracer.capture_lambda_handler
@metrics.log_metrics(capture_cold_start_metric=True)
@logger.inject_lambda_context(correlation_id_path="Records[0].messageId")
def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    requeued_count = 0
    skipped_count = 0
    processed_count = 0

    for record in event["Records"]:
        try:
            message_body = json.loads(record["body"])
            retry_count = message_body.get("_retry_count", 0)

            s3_event = message_body["Records"][0]
            bucket_name = s3_event["s3"]["bucket"]["name"]
            file_key = s3_event["s3"]["object"]["key"]
            decoded_key = unquote(file_key.replace("+", "%20"))

            logger.info(
                "Processing file",
                extra={"bucket": bucket_name, "key": decoded_key, "retry_count": retry_count},
            )

            is_stable, _ = check_file_stability(bucket_name, decoded_key)
            if not is_stable:
                if retry_count >= MAX_REQUEUE_RETRIES:
                    logger.error(
                        "Max retries exceeded for unstable file",
                        extra={"bucket": bucket_name, "key": decoded_key, "retry_count": retry_count},
                    )
                    metrics.add_metric(name="MaxRetriesExceeded", unit=MetricUnit.Count, value=1)
                    skipped_count += 1
                    continue
                if requeue_message(message_body, retry_count):
                    requeued_count += 1
                    metrics.add_metric(name="MessagesRequeued", unit=MetricUnit.Count, value=1)
                continue

            ingest_file(source_file=SourceFile(bucket=bucket_name, key=decoded_key))
            processed_count += 1
        except Exception:
            logger.error("Error processing SQS record", exc_info=True)
            continue

    return {
        "statusCode": 200,
        "body": "Successfully processed files.",
        "processed": processed_count,
        "requeued": requeued_count,
        "skipped": skipped_count,
    }
