#!/usr/bin/env python3
"""
Generate billing points CSV from Aurora meters + S3 nem12_mappings.

Reads all 477 meters from Aurora PostgreSQL, downloads nem12_mappings.json from S3,
resolves meter vertex IDs via Neptune Gremlin queries, and outputs a CSV with one
row per billing field per meter (23 fields x 477 meters = ~10,971 rows).

Usage:
    PYTHONPATH=src uv run scripts/generate_billing_points_csv.py
    PYTHONPATH=src uv run scripts/generate_billing_points_csv.py --output data/billing_points.csv

Output CSV columns:
    identifier, field, nem12_id, label, point_category, meter_vertex_id
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

import boto3
from sqlmodel import Session, create_engine, select

from scripts.billing_neptune_helper import gremlin_query
from scripts.import_billing_csv import get_db_url
from shared.db.models.meter import Meter

# S3 constants (same as process_nem12_locally.py)
MAPPINGS_BUCKET = "sbm-file-ingester"
MAPPINGS_KEY = "nem12_mappings.json"
AWS_PROFILE = "geg"

# 23 billing fields: (field_name, nem12_id_suffix, label_display_name)
BILLING_FIELDS: list[tuple[str, str, str]] = [
    ("peak_usage", "billing-peak-usage", "Peak Usage"),
    ("off_peak_usage", "billing-off-peak-usage", "Off Peak Usage"),
    ("shoulder_usage", "billing-shoulder-usage", "Shoulder Usage"),
    ("total_usage", "billing-total-usage", "Total Usage"),
    ("total_greenpower_usage", "billing-total-greenpower-usage", "Total Greenpower Usage"),
    ("estimated_peak_usage", "billing-estimated-peak-usage", "Estimated Peak Usage"),
    ("estimated_off_peak_usage", "billing-estimated-off-peak-usage", "Estimated Off Peak Usage"),
    ("estimated_shoulder_usage", "billing-estimated-shoulder-usage", "Estimated Shoulder Usage"),
    ("total_estimated_usage", "billing-total-estimated-usage", "Total Estimated Usage"),
    (
        "total_estimated_greenpower_usage",
        "billing-total-estimated-greenpower-usage",
        "Total Estimated Greenpower Usage",
    ),
    ("energy_charge", "billing-energy-charge", "Energy Charge"),
    ("network_charge", "billing-network-charge", "Network Charge"),
    ("environmental_charge", "billing-environmental-charge", "Environmental Charge"),
    ("metering_charge", "billing-metering-charge", "Metering Charge"),
    ("other_charge", "billing-other-charge", "Other Charge"),
    ("total_spend", "billing-total-spend", "Total Spend"),
    ("greenpower_spend", "billing-greenpower-spend", "Greenpower Spend"),
    ("estimated_energy_charge", "billing-estimated-energy-charge", "Estimated Energy Charge"),
    ("estimated_network_charge", "billing-estimated-network-charge", "Estimated Network Charge"),
    ("estimated_environmental_charge", "billing-estimated-environmental-charge", "Estimated Environmental Charge"),
    ("estimated_metering_charge", "billing-estimated-metering-charge", "Estimated Metering Charge"),
    ("estimated_other_charge", "billing-estimated-other-charge", "Estimated Other Charge"),
    ("total_estimated_spend", "billing-total-estimated-spend", "Total Estimated Spend"),
]

# Neptune query batch size (number of vertex IDs per query)
NEPTUNE_BATCH_SIZE = 50


def load_nem12_mappings() -> dict[str, str]:
    """Load NEM12 to Neptune ID mappings from S3."""
    print(f"Loading mappings from s3://{MAPPINGS_BUCKET}/{MAPPINGS_KEY}...")
    session = boto3.Session(profile_name=AWS_PROFILE)
    s3_client = session.client("s3")
    response = s3_client.get_object(Bucket=MAPPINGS_BUCKET, Key=MAPPINGS_KEY)
    mappings = json.loads(response["Body"].read().decode("utf-8"))
    print(f"  Loaded {len(mappings)} mappings")
    return mappings


def load_meters_from_aurora() -> list[Meter]:
    """Load all meters from Aurora PostgreSQL."""
    print("Loading meters from Aurora...")
    db_url = get_db_url()
    engine = create_engine(db_url, echo=False)
    with Session(engine) as session:
        meters = list(session.exec(select(Meter)).all())
    print(f"  Loaded {len(meters)} meters")
    return meters


def find_sensor_point_id(identifier: str, nem12_mappings: dict[str, str]) -> str | None:
    """Find any sensor point ID for a meter identifier in nem12_mappings.

    Looks for keys matching Optima_{identifier}-* (e.g. Optima_3052218678-E1).

    Returns:
        The first matching sensor point vertex ID, or None if not found.
    """
    prefix = f"Optima_{identifier}-"
    for key, point_id in nem12_mappings.items():
        if key.startswith(prefix):
            return point_id
    return None


def resolve_meter_vertex_ids(sensor_point_ids: list[str]) -> dict[str, str]:
    """Batch-resolve sensor point IDs to meter vertex IDs via Neptune.

    For each sensor point, traverses the equipRef edge to find the parent meter vertex.

    Args:
        sensor_point_ids: List of sensor point vertex IDs.

    Returns:
        Dict mapping sensor_point_id -> meter_vertex_id.
    """
    result: dict[str, str] = {}

    for i in range(0, len(sensor_point_ids), NEPTUNE_BATCH_SIZE):
        batch = sensor_point_ids[i : i + NEPTUNE_BATCH_SIZE]
        batch_num = i // NEPTUNE_BATCH_SIZE + 1
        total_batches = (len(sensor_point_ids) + NEPTUNE_BATCH_SIZE - 1) // NEPTUNE_BATCH_SIZE
        print(f"  Querying Neptune batch {batch_num}/{total_batches} ({len(batch)} points)...")

        # Build batch query: for each sensor point, get the meter vertex via equipRef
        id_list = ", ".join(f"'{pid}'" for pid in batch)
        query = f"g.V({id_list}).project('sensor', 'meter').by(id()).by(out('equipRef').id()).toList()"

        try:
            data = gremlin_query(query)
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        sensor_id = item.get("sensor")
                        meter_id = item.get("meter")
                        if sensor_id and meter_id:
                            result[sensor_id] = meter_id
        except RuntimeError as e:
            print(f"    WARNING: Batch query failed: {e}")
            # Try individual queries as fallback
            for pid in batch:
                try:
                    fallback_query = f"g.V('{pid}').out('equipRef').id().next()"
                    meter_id = gremlin_query(fallback_query)
                    if isinstance(meter_id, str):
                        result[pid] = meter_id
                except RuntimeError:
                    print(f"    WARNING: Failed to resolve meter for sensor point {pid}")

    return result


def main(argv: list[str] | None = None) -> int:
    """Generate billing_points.csv."""
    parser = argparse.ArgumentParser(
        description="Generate billing points CSV from Aurora meters + nem12_mappings.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    PYTHONPATH=src uv run scripts/generate_billing_points_csv.py
    PYTHONPATH=src uv run scripts/generate_billing_points_csv.py --output data/billing_points.csv
        """,
    )
    parser.add_argument(
        "--output",
        default="data/billing_points.csv",
        help="Output CSV path (default: data/billing_points.csv)",
    )
    args = parser.parse_args(argv)

    print("=" * 60)
    print("Billing Points CSV Generator")
    print("=" * 60)
    print()

    # Step 1: Load meters from Aurora
    meters = load_meters_from_aurora()

    # Step 2: Load nem12_mappings from S3
    nem12_mappings = load_nem12_mappings()

    # Step 3: For each meter, find sensor point ID from mappings
    print("\nResolving sensor point IDs from mappings...")
    sensor_point_map: dict[str, str] = {}  # identifier -> sensor_point_id
    missing_identifiers: list[str] = []

    for meter in meters:
        sensor_point_id = find_sensor_point_id(meter.identifier, nem12_mappings)
        if sensor_point_id:
            sensor_point_map[meter.identifier] = sensor_point_id
        else:
            missing_identifiers.append(meter.identifier)

    print(f"  Found sensor points: {len(sensor_point_map)}")
    if missing_identifiers:
        print(f"  Missing from mappings: {len(missing_identifiers)}")
        for ident in missing_identifiers[:10]:
            print(f"    - {ident}")
        if len(missing_identifiers) > 10:
            print(f"    ... and {len(missing_identifiers) - 10} more")

    # Step 4: Batch query Neptune to resolve meter vertex IDs
    print("\nResolving meter vertex IDs from Neptune...")
    unique_sensor_ids = list(set(sensor_point_map.values()))
    sensor_to_meter = resolve_meter_vertex_ids(unique_sensor_ids)
    print(f"  Resolved {len(sensor_to_meter)} meter vertex IDs")

    # Build identifier -> meter_vertex_id map
    identifier_to_meter: dict[str, str] = {}
    unresolved: list[str] = []
    for identifier, sensor_id in sensor_point_map.items():
        meter_vertex_id = sensor_to_meter.get(sensor_id)
        if meter_vertex_id:
            identifier_to_meter[identifier] = meter_vertex_id
        else:
            unresolved.append(identifier)

    if unresolved:
        print(f"\n  WARNING: Could not resolve meter vertex for {len(unresolved)} identifiers:")
        for ident in unresolved[:10]:
            print(f"    - {ident}")
        if len(unresolved) > 10:
            print(f"    ... and {len(unresolved) - 10} more")

    # Step 5: Generate CSV rows
    print(f"\nGenerating CSV with {len(BILLING_FIELDS)} fields per meter...")
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    rows_written = 0
    meters_skipped = 0

    with output_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["identifier", "field", "nem12_id", "label", "point_category", "meter_vertex_id"])

        for meter in sorted(meters, key=lambda m: m.identifier):
            meter_vertex_id = identifier_to_meter.get(meter.identifier)
            if not meter_vertex_id:
                meters_skipped += 1
                continue

            for field_name, nem12_suffix, label_display in BILLING_FIELDS:
                nem12_id = f"{meter.identifier}-{nem12_suffix}"
                label = f"{meter.identifier} {label_display}"
                writer.writerow(
                    [
                        meter.identifier,
                        field_name,
                        nem12_id,
                        label,
                        "billing",
                        meter_vertex_id,
                    ]
                )
                rows_written += 1

    # Summary
    meters_included = len(identifier_to_meter)
    print()
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"  Total meters in Aurora:     {len(meters)}")
    print(f"  Meters with mapping:        {len(sensor_point_map)}")
    print(f"  Meters with vertex ID:      {meters_included}")
    print(f"  Meters skipped (no vertex): {meters_skipped}")
    print(f"  Billing fields per meter:   {len(BILLING_FIELDS)}")
    print(f"  Total CSV rows:             {rows_written}")
    print(f"  Output file:                {output_path}")
    print("=" * 60)

    if meters_skipped:
        print(f"\nWARNING: {meters_skipped} meters skipped due to missing mapping or vertex ID.")
        print("Run meter-importer for missing meters, then refresh nem12_mappings and re-run.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
