#!/usr/bin/env python3
"""
Fetch billing point vertex IDs from Neptune for existing billing points.

Reads billing_points.csv, batch-queries Neptune for points matching the nem12Id
values, and outputs a CSV mapping point_vertex_id to nem12_id.

Usage:
    PYTHONPATH=src uv run scripts/fetch_billing_point_ids.py
    PYTHONPATH=src uv run scripts/fetch_billing_point_ids.py --input data/billing_points.csv --output data/billing_point_ids.csv
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

from scripts.billing_neptune_helper import gremlin_query

# Batch size for Neptune queries
BATCH_SIZE = 200


def fetch_point_ids(nem12_ids: list[str]) -> dict[str, str]:
    """Batch-fetch Neptune vertex IDs for given nem12Ids.

    Returns:
        Dict mapping nem12_id -> point_vertex_id.
    """
    result: dict[str, str] = {}

    for i in range(0, len(nem12_ids), BATCH_SIZE):
        batch = nem12_ids[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (len(nem12_ids) + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"  Querying batch {batch_num}/{total_batches} ({len(batch)} IDs)...")

        id_list = ", ".join(f"'{nid}'" for nid in batch)
        query = (
            f"g.V().has('nem12Id', within({id_list})).project('vid', 'nem12Id').by(id()).by(values('nem12Id')).toList()"
        )

        try:
            data = gremlin_query(query)
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        vid = item.get("vid")
                        nem12_id = item.get("nem12Id")
                        if vid and nem12_id:
                            result[nem12_id] = vid
        except RuntimeError as e:
            print(f"    WARNING: Batch query failed: {e}")

    return result


def main(argv: list[str] | None = None) -> int:
    """Fetch billing point IDs from Neptune."""
    parser = argparse.ArgumentParser(
        description="Fetch billing point vertex IDs from Neptune.",
    )
    parser.add_argument(
        "--input",
        default="data/billing_points.csv",
        dest="input_file",
        help="Input billing_points.csv (default: data/billing_points.csv)",
    )
    parser.add_argument(
        "--output",
        default="data/billing_point_ids.csv",
        help="Output CSV (default: data/billing_point_ids.csv)",
    )
    args = parser.parse_args(argv)

    print("=" * 60)
    print("Fetch Billing Point IDs from Neptune")
    print("=" * 60)
    print()

    # Read input CSV
    print("Reading input CSV...")
    nem12_ids: list[str] = []
    with Path(args.input_file).open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            nem12_ids.append(row["nem12_id"])
    print(f"  Loaded {len(nem12_ids)} nem12_ids")
    print()

    # Query Neptune
    print("Querying Neptune...")
    mapping = fetch_point_ids(nem12_ids)
    print(f"  Found {len(mapping)} points")
    print()

    # Write output
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["point_vertex_id", "nem12_id"])
        for nem12_id in nem12_ids:
            vid = mapping.get(nem12_id)
            if vid:
                writer.writerow([vid, nem12_id])

    found = len(mapping)
    missing = len(nem12_ids) - found

    print("=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"  Total nem12_ids:  {len(nem12_ids)}")
    print(f"  Found in Neptune: {found}")
    print(f"  Missing:          {missing}")
    print(f"  Output:           {output_path}")
    print("=" * 60)

    if missing:
        missing_ids = [nid for nid in nem12_ids if nid not in mapping]
        print(f"\nMissing nem12_ids ({missing}):")
        for nid in missing_ids[:10]:
            print(f"  - {nid}")
        if missing > 10:
            print(f"  ... and {missing - 10} more")

    return 0


if __name__ == "__main__":
    sys.exit(main())
