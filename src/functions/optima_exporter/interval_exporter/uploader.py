"""S3 upload utilities for interval data export."""

from typing import Any

import boto3
from aws_lambda_powertools import Logger
from optima_shared.config import S3_UPLOAD_BUCKET, S3_UPLOAD_PREFIX

logger = Logger(service="optima-interval-exporter")

# S3 client (lazy initialization)
_s3_client = None


def get_s3_client() -> Any:
    """Get S3 client with lazy initialization."""
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3", region_name="ap-southeast-2")
    return _s3_client


def upload_to_s3(
    file_content: bytes,
    filename: str,
    bucket: str | None = None,
    prefix: str | None = None,
) -> bool:
    """
    Upload CSV file to S3 for ingestion pipeline.

    Args:
        file_content: CSV file content as bytes
        filename: Filename for S3 object
        bucket: S3 bucket name (default: S3_UPLOAD_BUCKET)
        prefix: S3 prefix/folder (default: S3_UPLOAD_PREFIX)

    Returns:
        True if upload successful, False otherwise
    """
    bucket = bucket or S3_UPLOAD_BUCKET
    prefix = prefix or S3_UPLOAD_PREFIX
    s3_key = f"{prefix}{filename}"

    logger.info(
        "Uploading CSV to S3",
        extra={
            "bucket": bucket,
            "key": s3_key,
            "size_bytes": len(file_content),
            "file_name": filename,
        },
    )

    try:
        s3 = get_s3_client()
        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=file_content,
            ContentType="text/csv",
        )
        logger.info(
            "CSV uploaded successfully to S3",
            extra={"bucket": bucket, "key": s3_key, "file_name": filename},
        )
        return True

    except Exception as e:
        logger.error(
            "S3 upload failed",
            exc_info=True,
            extra={
                "error": str(e),
                "bucket": bucket,
                "key": s3_key,
                "file_name": filename,
            },
        )
        return False
