"""Bunnings billing snapshot Lambda — orchestration layer.

Reads sensor mappings, runs Athena chunks in parallel, pivots results, and
writes a single CSV to S3 for SkySpark consumption. See
``docs/superpowers/specs/2026-05-18-bunnings-billing-snapshot-design.md``.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any


def load_mappings(s3_client: Any, *, bucket: str, key: str) -> tuple[dict[str, str], float]:
    """Download nem12_mappings.json and compute its age in hours.

    Returns ``(mappings_dict, age_in_hours)`` where age is derived from S3
    ``LastModified``. Lambda emits this as ``MappingJsonAgeHours`` metric.
    """
    head = s3_client.head_object(Bucket=bucket, Key=key)
    last_modified: datetime = head["LastModified"]
    age = (datetime.now(UTC) - last_modified).total_seconds() / 3600.0

    obj = s3_client.get_object(Bucket=bucket, Key=key)
    mappings = json.loads(obj["Body"].read().decode("utf-8"))
    return mappings, age
