"""Bunnings billing snapshot Lambda — orchestration layer.

Reads sensor mappings, runs Athena chunks in parallel, pivots results, and
writes a single CSV to S3 for SkySpark consumption. See
``docs/superpowers/specs/2026-05-18-bunnings-billing-snapshot-design.md``.
"""

from __future__ import annotations

import io
import json
from datetime import UTC, datetime
from typing import Any

import athena
import boto3
import config as cfg
import pivot
from aws_lambda_powertools import Logger
from aws_lambda_powertools.metrics import Metrics, MetricUnit

logger = Logger(service="billing-snapshot")
metrics = Metrics(namespace="BillingSnapshot")


def load_mappings(s3_client: Any, *, bucket: str, key: str) -> tuple[dict[str, str], float]:
    """Download nem12_mappings.json and compute its age in hours.

    Returns ``(mappings_dict, age_in_hours)`` where age is derived from S3
    ``LastModified``. Lambda emits this as ``MappingJsonAgeHours`` metric.
    """
    head = s3_client.head_object(Bucket=bucket, Key=key)
    last_modified: datetime = head["LastModified"]
    age = (datetime.now(UTC) - last_modified).total_seconds() / 3600.0

    obj = s3_client.get_object(Bucket=bucket, Key=key)
    body_stream = obj["Body"]
    try:
        mappings = json.loads(body_stream.read().decode("utf-8"))
    finally:
        body_stream.close()
    return mappings, age


@logger.inject_lambda_context
@metrics.log_metrics
def lambda_handler(event: dict, context: Any) -> dict[str, Any]:
    """Main entry point — orchestrates Athena chunks → pivot → S3 PUT.

    Raises on any failure (chunk error, empty pivot, S3 failure). EventBridge
    Scheduler has ``maximum_retry_attempts=0`` so a single failure surfaces
    immediately via the CloudWatch error alarm.
    """
    start = datetime.now(UTC)

    s3 = boto3.client("s3")
    athena_client = boto3.client("athena")

    mappings, age_hours = load_mappings(s3, bucket=cfg.MAPPINGS_BUCKET, key=cfg.MAPPINGS_KEY)
    # MetricUnit.NoUnit because "hours" isn't a Powertools-supported EMF unit.
    metrics.add_metric(name="MappingJsonAgeHours", unit=MetricUnit.NoUnit, value=age_hours)

    reverse_map = pivot.build_reverse_map(mappings)
    sensor_ids = list(reverse_map.keys())
    logger.info(
        "mappings_loaded",
        extra={"billing_sensors": len(sensor_ids), "mapping_age_hours": age_hours},
    )

    # Spec invariant: if mapping JSON is missing/empty/has zero billing keys,
    # raise loudly without overwriting `billing-latest.csv`. Without this guard
    # we'd submit empty `WHERE sensorid IN ()` SQL, which Athena rejects as a
    # syntax error and surfaces as `AthenaQueryFailed` instead of the intended
    # `EmptyPivotError`.
    if not sensor_ids:
        raise pivot.EmptyPivotError(
            "nem12_mappings.json has zero billing keys; refusing to query Athena and overwrite billing-latest.csv"
        )

    chunks = athena.chunk_sensor_ids(sensor_ids, chunk_count=cfg.CHUNK_COUNT)
    rows = athena.run_chunks_parallel(
        athena_client=athena_client,
        s3_client=s3,
        chunks=chunks,
        workgroup=cfg.ATHENA_WORKGROUP,
        database=cfg.ATHENA_DATABASE,
        table=cfg.ATHENA_TABLE,
        start_date=cfg.HISTORY_START_DATE,
        max_workers=cfg.MAX_WORKERS,
        poll_interval=cfg.POLL_INTERVAL_SECONDS,
        poll_timeout=cfg.POLL_TIMEOUT_SECONDS,
    )
    metrics.add_metric(name="ChunkSuccessCount", unit=MetricUnit.Count, value=len(chunks))

    pivoted = pivot.build_pivot(rows, reverse_map)
    currencies, currency_stats = pivot.derive_currencies(pivoted)
    metrics.add_metric(name="CurrencyConflictNMIs", unit=MetricUnit.Count, value=currency_stats.conflict)
    metrics.add_metric(name="UnknownCurrencyNMIs", unit=MetricUnit.Count, value=currency_stats.unknown)
    metrics.add_metric(name="SuspectCurrencyNMIs", unit=MetricUnit.Count, value=currency_stats.suspect)

    buf = io.StringIO()
    pivot.write_csv(buf, pivoted, currencies)  # raises EmptyPivotError if 0 rows

    s3.put_object(
        Bucket=cfg.OUTPUT_BUCKET,
        Key=cfg.OUTPUT_KEY,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv; charset=utf-8",
    )

    duration = (datetime.now(UTC) - start).total_seconds()
    metrics.add_metric(name="Duration", unit=MetricUnit.Seconds, value=duration)
    metrics.add_metric(name="RowCount", unit=MetricUnit.Count, value=len(pivoted))
    metrics.add_metric(name="NMICount", unit=MetricUnit.Count, value=len({nmi for (nmi, _) in pivoted}))
    metrics.add_metric(name="MonthCount", unit=MetricUnit.Count, value=len({m for (_, m) in pivoted}))

    logger.info(
        "billing_snapshot_complete",
        extra={
            "row_count": len(pivoted),
            "nmi_count": len({nmi for (nmi, _) in pivoted}),
            "duration_seconds": duration,
            "s3_key": cfg.OUTPUT_KEY,
        },
    )
    return {"status": "ok", "row_count": len(pivoted), "duration_seconds": duration}
