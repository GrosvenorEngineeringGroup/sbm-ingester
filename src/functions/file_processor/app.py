"""SQS-triggered Lambda handler for the SBM file ingester.

This module is a thin SQS adapter. All business logic lives in
functions.file_processor.pipeline.ingest_file.

Pre-conditions (enforced at import time):
  - SQS_QUEUE_URL env var is set; KeyError on import otherwise so deploy
    fails fast rather than silently targeting the production queue.

Per-record flow:
  1. Decode the SQS record → bucket, key.
  2. Check file stability (S3 size stable for 2 consecutive checks).
  3. If vanished (HEAD 404): emit S3DuplicateEvent metric, log, skip silently
     (a duplicate S3 event arrived after a prior delivery already moved the
     file). No requeue, no MaxRetriesExceeded.
  4. If unstable but present: requeue with backoff (up to MAX_REQUEUE_RETRIES).
  5. If stable: build SourceFile, call ingest_file (which is idempotent +
     traced + emits per-file structured log + metrics).
  6. Return statusCode 200.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote

import boto3
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.metrics import MetricUnit
from botocore.exceptions import ClientError

from functions.file_processor.pipeline import ingest_file
from shared.source_file import SourceFile

# Required env var — KeyError on import if missing.
SQS_QUEUE_URL = os.environ["SQS_QUEUE_URL"]

# File-stability tuning (preserved from previous shape).
FILE_STABILITY_CHECK_INTERVAL = 5  # seconds between checks
FILE_STABILITY_MAX_WAIT = 30  # max seconds to wait for stabilisation
FILE_STABILITY_REQUIRED_CHECKS = 2  # consecutive stable checks required
MAX_REQUEUE_RETRIES = 3  # aligned with SQS maxReceiveCount = 3 (per spec)
# Delay before a requeued message becomes visible. Reverted 90 -> 60
# (2026-05-12): the 90s bump in f8282f4 was based on the wrong root cause.
# The MaxRetriesExceeded alarms it tried to suppress were actually caused by
# HEAD 404 on duplicate S3 events (now handled via StabilityResult.vanished),
# not by slow stability convergence. Real slow uploads stabilise in <12s.
REQUEUE_DELAY_SECONDS = 60

logger = Logger(service="file-processor")
tracer = Tracer(service="file-processor")
metrics = Metrics(namespace="SBM/Ingester")

s3_client = boto3.client("s3")
sqs_client = boto3.client("sqs")


@dataclass(frozen=True, slots=True)
class StabilityResult:
    """Result of a file-stability probe.

    ``vanished`` distinguishes "S3 HEAD returned 404 — the key no longer
    exists" from "we tried but the file is not yet stable / unreadable for
    another reason". Vanished keys are the expected outcome for a duplicate
    S3 event that arrived after a prior delivery already moved the file out
    of newTBP/; callers should skip them silently (no requeue, no error).
    """

    stable: bool
    size: int
    vanished: bool = False


def _is_object_missing(err: ClientError) -> bool:
    """Return True iff a ClientError represents a missing S3 object.

    HeadObject returns ``Code="404"`` / ``Message="Not Found"`` (and
    ``HTTPStatusCode == 404``) when the key does not exist; older code paths
    and tests may surface ``NoSuchKey`` (which is GetObject semantics, but
    some moto / boto3 combinations still raise it on HEAD).
    """
    code = err.response.get("Error", {}).get("Code")
    status = err.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return code in {"NoSuchKey", "404"} or status == 404


@tracer.capture_method
def check_file_stability(bucket: str, key: str) -> StabilityResult:
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
                    return StabilityResult(stable=True, size=current_size)
            else:
                stable_count = 0

            last_size = current_size
            time.sleep(FILE_STABILITY_CHECK_INTERVAL)
            total_wait += FILE_STABILITY_CHECK_INTERVAL
        except ClientError as e:
            if _is_object_missing(e):
                logger.info(
                    "s3_duplicate_event",
                    extra={"bucket": bucket, "key": key, "reason": "head_404"},
                )
                return StabilityResult(stable=False, size=0, vanished=True)
            logger.error(
                "Error checking file stability",
                exc_info=True,
                extra={"bucket": bucket, "key": key, "error": str(e)},
            )
            return StabilityResult(stable=False, size=0)
        except Exception as e:
            logger.error(
                "Error checking file stability",
                exc_info=True,
                extra={"bucket": bucket, "key": key, "error": str(e)},
            )
            return StabilityResult(stable=False, size=0)

    logger.warning(
        "File stability check timed out",
        extra={"bucket": bucket, "key": key, "last_size": last_size, "waited": total_wait},
    )
    return StabilityResult(stable=False, size=0)


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
    duplicate_count = 0

    for record in event["Records"]:
        try:
            message_body = json.loads(record["body"])
            retry_count = message_body.get("_retry_count", 0)
            # Bind retry_count to every log line emitted inside ingest_file
            # (including the parser_outcome structured log emitted by
            # pipeline._emit_parser_outcome_log). SQS batch_size=1 in
            # production so loop runs at most once per invocation —
            # remove_keys cleanup is omitted as moot.
            logger.append_keys(retry_count=retry_count)

            s3_event = message_body["Records"][0]
            bucket_name = s3_event["s3"]["bucket"]["name"]
            file_key = s3_event["s3"]["object"]["key"]
            decoded_key = unquote(file_key.replace("+", "%20"))

            logger.info(
                "Processing file",
                extra={"bucket": bucket_name, "key": decoded_key, "retry_count": retry_count},
            )

            stability = check_file_stability(bucket_name, decoded_key)

            if stability.vanished:
                # Duplicate S3 event: a prior delivery already moved this key
                # out of newTBP/. Silent skip — no requeue, no MaxRetriesExceeded.
                logger.info(
                    "s3_duplicate_event",
                    extra={
                        "source_bucket": bucket_name,
                        "source_key": decoded_key,
                        "retry_count": retry_count,
                    },
                )
                metrics.add_metric(name="S3DuplicateEvent", unit=MetricUnit.Count, value=1)
                duplicate_count += 1
                continue

            if not stability.stable:
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
        "duplicate": duplicate_count,
    }
