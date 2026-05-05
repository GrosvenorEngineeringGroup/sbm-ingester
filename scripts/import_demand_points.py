#!/usr/bin/env python3
"""
Import demand points into Neptune from a CSV file.

Reads data/demand_points.csv and creates Neptune point vertices with equipRef
edges to their parent meter vertices. Each point is created idempotently
using nem12Id as the deduplication key.

Rows with empty meter_vertex_id are skipped and logged as orphans (the
NMI's meter vertex doesn't exist in Neptune yet — manual investigation
needed).

Usage:
    PYTHONPATH=src uv run scripts/import_demand_points.py --csv data/demand_points.csv --dry-run
    PYTHONPATH=src uv run scripts/import_demand_points.py --csv data/demand_points.csv
"""

from __future__ import annotations

import argparse
import csv
import secrets
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from scripts.billing_neptune_helper import gremlin_query

EXISTENCE_CHECK_BATCH_SIZE = 200


def generate_point_id() -> str:
    """Generate a Neptune point ID matching existing convention.

    Format: p:bunnings:{hex_timestamp}-{hex_random}
    """
    hex_ts = format(int(time.time() * 1000), "x")
    hex_rand = secrets.token_hex(3)
    return f"p:bunnings:{hex_ts}-{hex_rand}"


def batch_check_existing_nem12_ids(nem12_ids: list[str]) -> set[str]:
    """Batch check which nem12Ids already exist in Neptune."""
    existing: set[str] = set()
    for i in range(0, len(nem12_ids), EXISTENCE_CHECK_BATCH_SIZE):
        chunk = nem12_ids[i : i + EXISTENCE_CHECK_BATCH_SIZE]
        id_list = "[" + ",".join(f"'{nid}'" for nid in chunk) + "]"
        try:
            query = f"g.V().has('nem12Id', within({id_list})).values('nem12Id').toList()"
            result = gremlin_query(query)
            existing.update(result)
        except Exception as e:
            print(f"  WARN: batch existence check failed ({e}); falling back to per-id checks")
            for nid in chunk:
                try:
                    single_query = f"g.V().has('nem12Id', '{nid}').hasNext()"
                    if gremlin_query(single_query):
                        existing.add(nid)
                except Exception:
                    pass
    return existing


_print_lock = threading.Lock()


def create_demand_point(point_id: str, label: str, nem12_id: str, meter_vertex_id: str) -> bool:
    """Create a single demand point vertex with equipRef edge to its meter."""
    label_escaped = label.replace("'", "\\'")
    nem12_id_escaped = nem12_id.replace("'", "\\'")

    query = (
        f"g.addV('point')"
        f".property(id, '{point_id}')"
        f".property('label', '{label_escaped}')"
        f".property('nem12Id', '{nem12_id_escaped}')"
        f".property('pointCategory', 'demand')"
        f".as('pt')"
        f".V('{meter_vertex_id}')"
        f".addE('equipRef').from('pt')"
    )
    gremlin_query(query)
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", required=True, dest="csv_file", help="Path to demand_points.csv")
    parser.add_argument("--dry-run", action="store_true", help="Preview mode, no Neptune writes")
    parser.add_argument(
        "--output",
        default="data/demand_point_ids.csv",
        help="Output CSV mapping point_vertex_id to nem12_id (default: data/demand_point_ids.csv)",
    )
    parser.add_argument("--workers", type=int, default=10, help="Number of parallel workers (default: 10)")
    args = parser.parse_args(argv)

    print("=" * 60)
    print("Demand Points Neptune Import")
    print("=" * 60)
    print(f"  CSV File:  {args.csv_file}")
    print(f"  Mode:      {'DRY RUN' if args.dry_run else 'LIVE'}")
    print("=" * 60)
    print()

    print("Reading CSV...")
    with Path(args.csv_file).open() as f:
        reader = csv.DictReader(f)
        all_rows = list(reader)
    print(f"  Read {len(all_rows)} rows.")

    # Filter out orphans (empty meter_vertex_id)
    orphan_rows = [r for r in all_rows if not r.get("meter_vertex_id", "").strip()]
    valid_rows = [r for r in all_rows if r.get("meter_vertex_id", "").strip()]
    print(f"  Valid rows: {len(valid_rows)}")
    print(f"  Orphan rows (empty meter_vertex_id, skipped): {len(orphan_rows)}")
    if orphan_rows:
        orphan_nmis = sorted({r["identifier"] for r in orphan_rows})
        sample = orphan_nmis[:10]
        ellipsis = "..." if len(orphan_nmis) > 10 else ""
        print(f"  Orphan NMIs ({len(orphan_nmis)}): {', '.join(sample)}{ellipsis}")

    print("\nBatch-checking existing nem12Ids in Neptune...")
    nem12_ids = [r["nem12_id"] for r in valid_rows]
    existing = batch_check_existing_nem12_ids(nem12_ids)
    print(f"  Already exist: {len(existing)}")
    new_rows = [r for r in valid_rows if r["nem12_id"] not in existing]
    print(f"  To create:     {len(new_rows)}")

    if args.dry_run:
        print("\n[DRY RUN] Would create the above points. Exiting.")
        return 0

    if not new_rows:
        print("\n[OK] Nothing to do — all nem12Ids already exist.")
        return 0

    print(f"\nCreating {len(new_rows)} points with {args.workers} workers...")
    created: list[tuple[str, str]] = []
    failed = 0

    def _create(row: dict) -> tuple[str, str] | None:
        try:
            point_id = generate_point_id()
            create_demand_point(
                point_id=point_id,
                label=row["label"],
                nem12_id=row["nem12_id"],
                meter_vertex_id=row["meter_vertex_id"],
            )
        except Exception as e:
            with _print_lock:
                print(f"  FAIL nem12Id={row['nem12_id']}: {e}")
            return None
        else:
            return point_id, row["nem12_id"]

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(_create, row) for row in new_rows]
        for i, fut in enumerate(as_completed(futures), 1):
            result = fut.result()
            if result:
                created.append(result)
            else:
                failed += 1
            if i % 100 == 0:
                with _print_lock:
                    print(f"  Progress: {i}/{len(new_rows)} (created={len(created)}, failed={failed})")

    print(f"\n[OK] Created {len(created)} points; {failed} failed.")

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["point_vertex_id", "nem12_id"])
        writer.writerows(created)
    print(f"[OK] Wrote mapping to {output_path}")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
