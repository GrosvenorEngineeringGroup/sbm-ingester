"""Sidecar audit log writer for partial-data-loss signal.

Per spec lines 605-621, when a file has skipped or unmapped rows we emit a
JSON sidecar to ``s3://hudibucketsrc/audit/<batch_ts>/<source>.skipped.json``.
The sidecar carries skip counters, unmapped identifiers, unsupported
suffixes, and up to 100 sample skip rows for operator drill-down.

The writer is best-effort: callers are expected to swallow any exception
raised here and continue processing — failure to emit an audit sidecar
must never fail a file's primary disposition.
"""

from __future__ import annotations

import json
from typing import Any

import boto3

from shared.common import HUDI_BUCKET

AUDIT_BUCKET = HUDI_BUCKET
AUDIT_PREFIX = "audit"
SAMPLE_CAP = 100


def _safe_filename(source_filename: str) -> str:
    """Replace path separators so the audit key stays single-segment."""
    return source_filename.replace("/", "_").replace("\\", "_")


def write_audit_sidecar(
    batch_ts: str,
    source_filename: str,
    outcome_summary: dict[str, Any],
    skip_reasons: dict[str, int],
    unmapped_identifiers: list[tuple[str, str]],
    unsupported_suffixes: list[str],
    skipped_samples: list[dict[str, Any]],
    s3_client: Any | None = None,
    total_skipped: int | None = None,
) -> str:
    """Write audit sidecar JSON to S3. Returns the S3 key written.

    The writer enforces ``SAMPLE_CAP`` (100) on ``skipped_samples`` defensively
    even when the caller has already capped. A trailing
    ``{"truncated": true, "total_skipped": <N>}`` marker is appended whenever
    truncation occurred — either because the caller supplied more than
    ``SAMPLE_CAP`` entries, or because the explicit ``total_skipped`` count
    exceeds the number of supplied samples (i.e. caller pre-capped sample
    collection but knows the true skipped total).
    """
    if s3_client is None:
        s3_client = boto3.client("s3")

    safe_filename = _safe_filename(source_filename)
    key = f"{AUDIT_PREFIX}/{batch_ts}/{safe_filename}.skipped.json"

    if len(skipped_samples) > SAMPLE_CAP:
        samples_payload: list[dict[str, Any]] = list(skipped_samples[:SAMPLE_CAP])
        samples_payload.append(
            {"truncated": True, "total_skipped": len(skipped_samples)},
        )
    else:
        samples_payload = list(skipped_samples)
        if total_skipped is not None and total_skipped > len(skipped_samples):
            samples_payload.append(
                {"truncated": True, "total_skipped": total_skipped},
            )

    payload = {
        "source_file": source_filename,
        "outcome": outcome_summary,
        "skip_reasons": dict(skip_reasons),
        "unmapped_identifiers": [list(pair) for pair in unmapped_identifiers],
        "unsupported_suffixes": list(unsupported_suffixes),
        "skipped_samples": samples_payload,
    }

    body = json.dumps(payload, default=str).encode("utf-8")
    s3_client.put_object(
        Bucket=AUDIT_BUCKET,
        Key=key,
        Body=body,
        ContentType="application/json",
    )
    return key
