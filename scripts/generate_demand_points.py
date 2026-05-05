#!/usr/bin/env python3
"""Generate a demand_points CSV for Optima sites of a given project.

Scans DynamoDB sbm-optima-config for the chosen project's sites with NMIs
starting with 'Optima_', then queries Neptune to find each site's
meter_vertex_id by walking the equipRef edge from any existing E1 (or B1
fallback) point.

Output: a CSV with 3 rows per NMI (kw/kva/pf), suitable for input to
scripts/import_demand_points.py.

Usage:
    PYTHONPATH=src uv run scripts/generate_demand_points.py \\
        --output data/demand_points.csv \\
        [--project bunnings] [--dry-run]
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter
from pathlib import Path

import boto3

from scripts.billing_neptune_helper import gremlin_query

AWS_PROFILE = "geg"
AWS_REGION = "ap-southeast-2"
DYNAMODB_TABLE = "sbm-optima-config"

DEMAND_FIELDS = [
    # (field_short, suffix_in_nem12_id, label_descriptor)
    ("kw", "kw", "Demand kW"),
    ("kva", "kva", "Demand kVA"),
    ("pf", "pf", "Demand Power Factor"),
]


def scan_optima_sites_for_project(project: str) -> list[dict]:
    """Scan DynamoDB for Optima sites belonging to the given project."""
    session = boto3.Session(profile_name=AWS_PROFILE)
    ddb = session.client("dynamodb", region_name=AWS_REGION)

    items: list[dict] = []
    kwargs = {
        "TableName": DYNAMODB_TABLE,
        "ProjectionExpression": "nmi,country,#p",
        "ExpressionAttributeNames": {"#p": "project"},
    }
    while True:
        resp = ddb.scan(**kwargs)
        items.extend(resp["Items"])
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    return [
        {
            "nmi_full": i["nmi"]["S"],
            "nmi_bare": i["nmi"]["S"].replace("Optima_", ""),
            "country": i["country"]["S"],
        }
        for i in items
        if i["project"]["S"] == project and i["nmi"]["S"].startswith("Optima_")
    ]


def find_meter_vertex_id(nmi_full: str) -> tuple[str | None, str]:
    """Find the meter_vertex_id for an NMI.

    Returns (vertex_id, strategy) where strategy is one of 'E1', 'B1', or
    'missing'. Strategy A: walk equipRef from <nmi_full>-E1 point. Strategy B
    (fallback): try -B1 instead. None if neither works.
    """
    for channel in ("E1", "B1"):
        nem12_id = f"{nmi_full}-{channel}"
        # Escape single quotes — NMIs are alphanumeric so this is just defensive
        escaped = nem12_id.replace("'", "\\'")
        # Edge direction: point --equipRef--> meter (see import_billing_points.py
        # which creates the edge with .addE('equipRef').from('pt')). To walk
        # from a point to its meter we follow OUTGOING edges with .out().
        query = f"g.V().has('nem12Id', '{escaped}').out('equipRef').id().limit(1).toList()"
        result = gremlin_query(query)
        if result:
            return result[0], channel
    return None, "missing"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        default="data/demand_points.csv",
        help="Output CSV path (default: data/demand_points.csv)",
    )
    parser.add_argument(
        "--project",
        default="bunnings",
        help="DynamoDB project filter (default: bunnings)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip writing the output file; print the summary only",
    )
    args = parser.parse_args(argv)

    print("=" * 60)
    print("Generate Demand Points CSV")
    print("=" * 60)
    print(f"  Project: {args.project}")
    print(f"  Output:  {args.output}")
    print(f"  Mode:    {'DRY RUN' if args.dry_run else 'LIVE'}")
    print("=" * 60)

    print("\nScanning DynamoDB...")
    sites = scan_optima_sites_for_project(args.project)
    print(f"Found {len(sites)} {args.project} Optima sites.")

    print("\nResolving meter_vertex_id from Neptune (this may take a few minutes)...")
    rows: list[dict] = []
    strategy_counter: Counter[str] = Counter()
    missing_nmis: list[str] = []

    for i, site in enumerate(sites, 1):
        if i % 50 == 0:
            print(f"  Processed {i}/{len(sites)}...")
        meter_id, strategy = find_meter_vertex_id(site["nmi_full"])
        strategy_counter[strategy] += 1
        if meter_id is None:
            missing_nmis.append(site["nmi_bare"])

        for field_short, suffix, label_desc in DEMAND_FIELDS:
            rows.append(
                {
                    "identifier": site["nmi_bare"],
                    "field": field_short,
                    "nem12_id": f"{site['nmi_full']}-demand-{suffix}",
                    "label": f"{site['nmi_bare']} {label_desc}",
                    "point_category": "demand",
                    "meter_vertex_id": meter_id or "",
                }
            )

    print("\nNeptune lookup summary:")
    print(f"  Found via E1: {strategy_counter['E1']}")
    print(f"  Found via B1: {strategy_counter['B1']}")
    print(f"  Missing:      {strategy_counter['missing']}")
    if missing_nmis:
        sample = sorted(missing_nmis)[:10]
        ellipsis = "..." if len(missing_nmis) > 10 else ""
        print(f"  Missing NMIs: {', '.join(sample)}{ellipsis}")

    print(f"\nTotal rows: {len(rows)} (= {len(sites)} NMIs * 3 fields)")
    print(f"Rows with empty meter_vertex_id: {strategy_counter['missing'] * 3}")

    if args.dry_run:
        print("\n[DRY RUN] Skipping CSV write.")
        return 0

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["identifier", "field", "nem12_id", "label", "point_category", "meter_vertex_id"]
    with output_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n[OK] Wrote {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
