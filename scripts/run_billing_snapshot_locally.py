"""Run the billing snapshot Lambda end-to-end against live AWS.

Useful for incident response, pivot debugging, and ad-hoc re-runs after a
production failure. Reads ``nem12_mappings.json`` from S3, runs Athena
queries against the dedicated workgroup, and writes the CSV either to a
local path or to S3 (controlled by ``--output``).

Usage:
    uv run scripts/run_billing_snapshot_locally.py --output local /tmp/billing.csv
    uv run scripts/run_billing_snapshot_locally.py --output s3
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from pathlib import Path

# Make the billing_snapshot module importable when run from repo root.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "functions" / "billing_snapshot"))

import athena
import boto3
import config as cfg
import pivot
from app import load_mappings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", choices=["local", "s3"], default="local")
    parser.add_argument("path", nargs="?", default="/tmp/billing-latest.csv")
    args = parser.parse_args()

    s3 = boto3.client("s3", region_name="ap-southeast-2")
    athena_client = boto3.client("athena", region_name="ap-southeast-2")

    mappings, age = load_mappings(s3, bucket=cfg.MAPPINGS_BUCKET, key=cfg.MAPPINGS_KEY)
    log.info("Loaded %d mappings (age=%.2fh)", len(mappings), age)

    rmap = pivot.build_reverse_map(mappings)
    log.info("Reverse map: %d billing sensors", len(rmap))

    chunks = athena.chunk_sensor_ids(list(rmap.keys()), chunk_count=cfg.CHUNK_COUNT)
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
    log.info("Athena merged rows: %d", len(rows))

    pivoted = pivot.build_pivot(rows, rmap)
    currencies, stats = pivot.derive_currencies(pivoted)
    log.info("Pivot: %d (nmi, month) rows | currency stats: %s", len(pivoted), stats)

    buf = io.StringIO()
    pivot.write_csv(buf, pivoted, currencies)

    if args.output == "local":
        Path(args.path).write_text(buf.getvalue(), encoding="utf-8")
        log.info("Wrote CSV to %s (%d bytes)", args.path, len(buf.getvalue()))
    else:
        s3.put_object(
            Bucket=cfg.OUTPUT_BUCKET,
            Key=cfg.OUTPUT_KEY,
            Body=buf.getvalue().encode("utf-8"),
            ContentType="text/csv; charset=utf-8",
        )
        log.info("Wrote CSV to s3://%s/%s", cfg.OUTPUT_BUCKET, cfg.OUTPUT_KEY)


if __name__ == "__main__":
    main()
