#!/usr/bin/env python3
"""
Export billing data from Aurora PostgreSQL to Hudi data lake via S3.

Reads billing_point_ids.csv (point_vertex_id -> nem12_id mapping) and all bills
from Aurora, generates a single Hudi-format CSV, and uploads it to S3.

Usage:
    PYTHONPATH=src uv run scripts/export_billing_to_hudi.py --dry-run
    PYTHONPATH=src uv run scripts/export_billing_to_hudi.py

Output CSV (Hudi format):
    sensorId,ts,val,unit,its
"""

from __future__ import annotations

import argparse
import csv
import sys
from io import StringIO
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import date

import boto3
from sqlmodel import Session, create_engine, select

from scripts.import_billing_csv import get_db_url
from shared.db.models.bill import Bill
from shared.db.models.meter import Meter

# S3 destination (same as process_nem12_locally.py)
OUTPUT_BUCKET = "hudibucketsrc"
OUTPUT_PREFIX = "sensorDataFiles"
AWS_PROFILE = "geg"

# 23 billing fields grouped by unit source
# (field_name, nem12_id_suffix, unit_source)
# unit_source: "usage" -> bills.usage_unit, "spend" -> bills.spend_currency
BILLING_FIELDS: list[tuple[str, str, str]] = [
    ("peak_usage", "billing-peak-usage", "usage"),
    ("off_peak_usage", "billing-off-peak-usage", "usage"),
    ("shoulder_usage", "billing-shoulder-usage", "usage"),
    ("total_usage", "billing-total-usage", "usage"),
    ("total_greenpower_usage", "billing-total-greenpower-usage", "usage"),
    ("estimated_peak_usage", "billing-estimated-peak-usage", "usage"),
    ("estimated_off_peak_usage", "billing-estimated-off-peak-usage", "usage"),
    ("estimated_shoulder_usage", "billing-estimated-shoulder-usage", "usage"),
    ("total_estimated_usage", "billing-total-estimated-usage", "usage"),
    ("total_estimated_greenpower_usage", "billing-total-estimated-greenpower-usage", "usage"),
    ("energy_charge", "billing-energy-charge", "spend"),
    ("network_charge", "billing-network-charge", "spend"),
    ("environmental_charge", "billing-environmental-charge", "spend"),
    ("metering_charge", "billing-metering-charge", "spend"),
    ("other_charge", "billing-other-charge", "spend"),
    ("total_spend", "billing-total-spend", "spend"),
    ("greenpower_spend", "billing-greenpower-spend", "spend"),
    ("estimated_energy_charge", "billing-estimated-energy-charge", "spend"),
    ("estimated_network_charge", "billing-estimated-network-charge", "spend"),
    ("estimated_environmental_charge", "billing-estimated-environmental-charge", "spend"),
    ("estimated_metering_charge", "billing-estimated-metering-charge", "spend"),
    ("estimated_other_charge", "billing-estimated-other-charge", "spend"),
    ("total_estimated_spend", "billing-total-estimated-spend", "spend"),
]


def load_point_id_mapping(csv_path: str) -> dict[tuple[str, str], str]:
    """Load billing_point_ids.csv and build (identifier, field) -> point_vertex_id mapping.

    Parses nem12_id format: {identifier}-billing-{suffix} to extract identifier and field.
    """
    mapping: dict[tuple[str, str], str] = {}

    # Build suffix -> field_name lookup
    suffix_to_field = {suffix: field_name for field_name, suffix, _ in BILLING_FIELDS}

    with Path(csv_path).open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            point_vertex_id = row["point_vertex_id"]
            nem12_id = row["nem12_id"]

            # Parse: {identifier}-billing-{suffix}
            # Find the first occurrence of "-billing-" to split
            billing_idx = nem12_id.find("-billing-")
            if billing_idx == -1:
                continue

            identifier = nem12_id[:billing_idx]
            suffix = nem12_id[billing_idx + 1 :]  # "billing-peak-usage" etc.
            field_name = suffix_to_field.get(suffix)
            if field_name:
                mapping[(identifier, field_name)] = point_vertex_id

    return mapping


def format_ts(bill_date: date) -> str:
    """Format bill date as Hudi timestamp: YYYY-MM-DD 00:00:00."""
    return f"{bill_date.isoformat()} 00:00:00"


def main(argv: list[str] | None = None) -> int:
    """Export billing data to Hudi CSV and upload to S3."""
    parser = argparse.ArgumentParser(
        description="Export billing data from Aurora to Hudi data lake.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    PYTHONPATH=src uv run scripts/export_billing_to_hudi.py --dry-run
    PYTHONPATH=src uv run scripts/export_billing_to_hudi.py
        """,
    )
    parser.add_argument(
        "--point-ids",
        default="data/billing_point_ids.csv",
        help="Path to billing_point_ids.csv (default: data/billing_point_ids.csv)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Generate CSV locally, no S3 upload")
    args = parser.parse_args(argv)

    print("=" * 60)
    print("Billing Data -> Hudi Export")
    print("=" * 60)
    print(f"  Point IDs: {args.point_ids}")
    print(f"  Mode:      {'DRY RUN' if args.dry_run else 'LIVE'}")
    print("=" * 60)
    print()

    # Step 1: Load point ID mapping
    print("Loading point ID mapping...")
    mapping = load_point_id_mapping(args.point_ids)
    unique_identifiers = len({k[0] for k in mapping})
    print(f"  Loaded {len(mapping)} mappings for {unique_identifiers} meters")
    print()

    # Step 2: Load bills from Aurora (with meter identifier)
    print("Loading bills from Aurora...")
    db_url = get_db_url()
    engine = create_engine(db_url, echo=False)

    with Session(engine) as session:
        stmt = select(Bill, Meter.identifier).join(Meter, Bill.meter_id == Meter.id)
        results = session.exec(stmt).all()

    print(f"  Loaded {len(results)} bills")
    print()

    # Step 3: Generate Hudi CSV rows
    print("Generating Hudi CSV...")
    buf = StringIO()
    writer = csv.writer(buf)
    writer.writerow(["sensorId", "ts", "val", "unit", "its"])

    rows_written = 0
    bills_skipped = 0

    for bill, identifier in results:
        ts = format_ts(bill.bill_date)
        usage_unit = bill.usage_unit.lower()
        spend_unit = bill.spend_currency.lower()

        for field_name, _suffix, unit_source in BILLING_FIELDS:
            point_id = mapping.get((identifier, field_name))
            if not point_id:
                bills_skipped += 1
                continue

            val = getattr(bill, field_name)
            unit = usage_unit if unit_source == "usage" else spend_unit
            writer.writerow([point_id, ts, val, unit, ts])
            rows_written += 1

    csv_content = buf.getvalue()
    csv_size_mb = len(csv_content.encode()) / (1024 * 1024)

    print(f"  Rows:     {rows_written}")
    print(f"  Size:     {csv_size_mb:.1f} MB")
    if bills_skipped:
        print(f"  Skipped:  {bills_skipped} (no point ID mapping)")
    print()

    # Step 4: Upload or save locally
    if args.dry_run:
        local_path = Path("data/billing_hudi_preview.csv")
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_text(csv_content)
        print(f"[DRY RUN] CSV written to {local_path}")
        print(f"[DRY RUN] Would upload to s3://{OUTPUT_BUCKET}/{OUTPUT_PREFIX}/")
    else:
        s3_key = f"{OUTPUT_PREFIX}/billing_export.csv"
        print(f"Uploading to s3://{OUTPUT_BUCKET}/{s3_key}...")
        s3_session = boto3.Session(profile_name=AWS_PROFILE)
        s3_client = s3_session.client("s3")
        s3_client.put_object(
            Bucket=OUTPUT_BUCKET,
            Key=s3_key,
            Body=csv_content,
        )
        print("  Upload complete.")

    # Summary
    print()
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"  Bills:          {len(results)}")
    print(f"  Fields/bill:    {len(BILLING_FIELDS)}")
    print(f"  Hudi rows:      {rows_written}")
    print(f"  CSV size:       {csv_size_mb:.1f} MB")
    if not args.dry_run:
        print(f"  S3 location:    s3://{OUTPUT_BUCKET}/{OUTPUT_PREFIX}/billing_export.csv")
        print()
        print("Next: trigger Glue job to import into Hudi:")
        print("  aws glue start-job-run --job-name DataImportIntoLake --region ap-southeast-2")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
