"""
Weekly Archiver Lambda: Archives processed files to weekly directories.
Triggered by EventBridge every Monday at UTC 00:00 (AEST 11:00).

Can also be manually invoked with a specific target_week:
  aws lambda invoke --function-name sbm-weekly-archiver \
    --payload '{"target_week": "2026-W01"}' output.json
"""

import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any

import boto3
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.metrics import MetricUnit
from botocore.config import Config
from botocore.exceptions import ClientError

logger = Logger(service="weekly-archiver")
tracer = Tracer(service="weekly-archiver")
metrics = Metrics(namespace="SBM/Ingester")

BUCKET_NAME = "sbm-file-ingester"
PREFIXES = ["newP/", "newIrrevFiles/", "newParseErr/"]
MAX_WORKERS = 50
TARGET_WEEK_PATTERN = re.compile(r"^\d{4}-W(0[1-9]|[1-4]\d|5[0-3])$")

# Increase connection pool to match worker count for better concurrency
s3 = boto3.client("s3", config=Config(max_pool_connections=MAX_WORKERS))


class ArchiveResult(Enum):
    """Result status for archive operation."""

    SUCCESS = "success"
    SKIPPED = "skipped"  # File no longer exists (already moved/deleted)
    ERROR = "error"


def get_iso_week(dt: datetime) -> str:
    """Return ISO week format: 2026-W03"""
    return f"{dt.isocalendar().year}-W{dt.isocalendar().week:02d}"


def validate_target_week(target_week: str) -> bool:
    """Validate target_week format (YYYY-WXX where XX is 01-53)."""
    return bool(TARGET_WEEK_PATTERN.match(target_week))


def archive_single_file(key: str, prefix: str, target_week: str) -> tuple[ArchiveResult, str]:
    """Archive a single file.

    Returns:
        Tuple of (result, message) where result is SUCCESS, SKIPPED, or ERROR.
    """
    filename = key.split("/")[-1]
    dest_key = f"{prefix}archived/{target_week}/{filename}"

    try:
        s3.copy_object(
            Bucket=BUCKET_NAME,
            CopySource={"Bucket": BUCKET_NAME, "Key": key},
            Key=dest_key,
        )
        s3.delete_object(Bucket=BUCKET_NAME, Key=key)
        return ArchiveResult.SUCCESS, key

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")

        if error_code in ("NoSuchKey", "404"):
            # File was already moved/deleted - skip silently
            logger.debug(
                "File no longer exists, skipping",
                extra={"key": key, "target_week": target_week},
            )
            return ArchiveResult.SKIPPED, key

        # Real error - log with full context
        logger.error(
            "Failed to archive file",
            extra={
                "key": key,
                "dest_key": dest_key,
                "target_week": target_week,
                "error_code": error_code,
                "error": str(e),
            },
        )
        return ArchiveResult.ERROR, f"{key}: {e}"

    except Exception as e:
        logger.error(
            "Unexpected error archiving file",
            extra={
                "key": key,
                "dest_key": dest_key,
                "target_week": target_week,
                "error": str(e),
            },
        )
        return ArchiveResult.ERROR, f"{key}: {e}"


@tracer.capture_method
def archive_files_for_prefix(prefix: str, target_week: str) -> dict[str, int]:
    """Move files belonging to target_week from prefix to archived/ using concurrent processing."""
    paginator = s3.get_paginator("list_objects_v2")

    # Collect files to archive
    files_to_archive: list[str] = []
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix, Delimiter="/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if "/archived/" in key:
                continue

            last_modified = obj["LastModified"]
            file_week = get_iso_week(last_modified)

            if file_week == target_week:
                filename = key.split("/")[-1]
                if filename:
                    files_to_archive.append(key)

    if not files_to_archive:
        return {"archived": 0, "skipped": 0, "errors": 0}

    logger.info(
        "Found files to archive",
        extra={"prefix": prefix, "target_week": target_week, "count": len(files_to_archive)},
    )

    # Process concurrently
    archived = 0
    skipped = 0
    errors = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(archive_single_file, key, prefix, target_week): key for key in files_to_archive}

        for future in as_completed(futures):
            result, _ = future.result()
            if result == ArchiveResult.SUCCESS:
                archived += 1
            elif result == ArchiveResult.SKIPPED:
                skipped += 1
            else:
                errors += 1

    return {"archived": archived, "skipped": skipped, "errors": errors}


@tracer.capture_lambda_handler
@metrics.log_metrics(capture_cold_start_metric=True)
@logger.inject_lambda_context
def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Archive files from the previous week or a specified target_week.

    Args:
        event: Can contain optional 'target_week' (e.g., '2026-W01') for manual invocation.
               If not provided, archives files from the previous week.

    Returns:
        - statusCode 200: All files archived successfully
        - statusCode 207: Partial success (some files skipped or errored)
        - statusCode 400: Invalid target_week format
    """
    # Support manual invocation with specific target_week
    if "target_week" in event:
        target_week = event["target_week"]

        if not validate_target_week(target_week):
            logger.error(
                "Invalid target_week format",
                extra={"target_week": target_week, "expected_format": "YYYY-WXX (e.g., 2026-W01)"},
            )
            return {
                "statusCode": 400,
                "error": f"Invalid target_week format: {target_week}. Expected YYYY-WXX (e.g., 2026-W01)",
            }

        logger.info("Manual archive triggered", extra={"target_week": target_week})
    else:
        # Calculate last week's ISO week number (default behavior for EventBridge)
        today = datetime.now(tz=UTC)
        last_week = today - timedelta(weeks=1)
        target_week = get_iso_week(last_week)

    logger.info("Starting weekly archive", extra={"target_week": target_week})

    total_archived = 0
    total_skipped = 0
    total_errors = 0

    for prefix in PREFIXES:
        result = archive_files_for_prefix(prefix, target_week)

        # Record metrics per prefix
        prefix_name = prefix.rstrip("/")
        metrics.add_metric(name=f"Archived_{prefix_name}", unit=MetricUnit.Count, value=result["archived"])

        total_archived += result["archived"]
        total_skipped += result["skipped"]
        total_errors += result["errors"]

        logger.info(
            "Archived prefix",
            extra={
                "prefix": prefix,
                "target_week": target_week,
                "archived": result["archived"],
                "skipped": result["skipped"],
                "errors": result["errors"],
            },
        )

    # Record aggregate metrics
    metrics.add_metric(name="TotalArchived", unit=MetricUnit.Count, value=total_archived)
    metrics.add_metric(name="TotalSkipped", unit=MetricUnit.Count, value=total_skipped)
    metrics.add_metric(name="ArchiveErrors", unit=MetricUnit.Count, value=total_errors)

    # Determine status code
    if total_errors > 0:
        status_code = 207  # Partial success
    else:
        status_code = 200

    logger.info(
        "Weekly archive completed",
        extra={
            "target_week": target_week,
            "total_archived": total_archived,
            "total_skipped": total_skipped,
            "total_errors": total_errors,
            "status_code": status_code,
        },
    )

    return {
        "statusCode": status_code,
        "archived": total_archived,
        "skipped": total_skipped,
        "errors": total_errors,
        "week": target_week,
    }
