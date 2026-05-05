"""Shared loader for nem12_mappings.json (cached per Lambda container).

Used by any parser that needs to resolve nem12-style sensor keys to Neptune
IDs without going through file_processor's standard NMI-mapping flow.
"""

from __future__ import annotations

import json

import boto3
from aws_lambda_powertools import Logger

logger = Logger(service="nem12-mappings-loader", child=True)

MAPPINGS_BUCKET = "sbm-file-ingester"
MAPPINGS_KEY = "nem12_mappings.json"

_cache: dict | None = None


def get_nem12_mappings() -> dict:
    """Lazy-load nem12_mappings.json from S3 once per Lambda container.

    Cached at module level; lives for the container's warm lifetime.
    Cold starts pay one ~1 MB S3 GET. Mapping refresh happens hourly via
    the sbm-files-ingester-nem12-mappings-to-s3 Lambda — stale containers
    miss new NMIs until they recycle, which is acceptable.
    """
    global _cache
    if _cache is None:
        logger.info(
            "Loading nem12_mappings.json from S3",
            extra={"bucket": MAPPINGS_BUCKET, "key": MAPPINGS_KEY},
        )
        obj = boto3.client("s3").get_object(Bucket=MAPPINGS_BUCKET, Key=MAPPINGS_KEY)
        _cache = json.loads(obj["Body"].read())
    return _cache
