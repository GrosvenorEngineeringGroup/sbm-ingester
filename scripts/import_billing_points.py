#!/usr/bin/env python3
"""
Import billing points into Neptune from a CSV file.

Reads billing_points.csv and creates Neptune point vertices with equipRef edges
to their parent meter vertices. Each point is created idempotently using nem12Id
as the deduplication key.

Usage:
    PYTHONPATH=src uv run scripts/import_billing_points.py --csv data/billing_points.csv --dry-run
    PYTHONPATH=src uv run scripts/import_billing_points.py --csv data/billing_points.csv
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

# Batch size for existence checks
EXISTENCE_CHECK_BATCH_SIZE = 200


def generate_point_id() -> str:
    """Generate a Neptune point ID matching existing convention.

    Format: p:bunnings:{hex_timestamp}-{hex_random}
    """
    hex_ts = format(int(time.time() * 1000), "x")
    hex_rand = secrets.token_hex(3)
    return f"p:bunnings:{hex_ts}-{hex_rand}"


def batch_check_existing_nem12_ids(nem12_ids: list[str]) -> set[str]:
    """Batch check which nem12Ids already exist in Neptune.

    Args:
        nem12_ids: List of nem12Id values to check.

    Returns:
        Set of nem12Ids that already exist.
    """
    existing: set[str] = set()

    for i in range(0, len(nem12_ids), EXISTENCE_CHECK_BATCH_SIZE):
        batch = nem12_ids[i : i + EXISTENCE_CHECK_BATCH_SIZE]
        batch_num = i // EXISTENCE_CHECK_BATCH_SIZE + 1
        total_batches = (len(nem12_ids) + EXISTENCE_CHECK_BATCH_SIZE - 1) // EXISTENCE_CHECK_BATCH_SIZE
        print(f"  Checking existence batch {batch_num}/{total_batches} ({len(batch)} IDs)...")

        id_list = ", ".join(f"'{nid}'" for nid in batch)
        query = f"g.V().has('nem12Id', within({id_list})).values('nem12Id').toList()"

        try:
            data = gremlin_query(query)
            if isinstance(data, list):
                existing.update(data)
        except RuntimeError as e:
            print(f"    WARNING: Batch existence check failed: {e}")
            print("    Falling back to individual checks for this batch...")
            for nid in batch:
                try:
                    single_query = f"g.V().has('nem12Id', '{nid}').hasNext()"
                    result = gremlin_query(single_query)
                    if result is True:
                        existing.add(nid)
                except RuntimeError:
                    pass  # Assume not existing on error

    return existing


def create_billing_point(
    point_id: str,
    label: str,
    nem12_id: str,
    meter_vertex_id: str,
) -> bool:
    """Create a single billing point vertex with equipRef edge to its meter.

    Returns True on success, False on failure.
    """
    # Escape single quotes in label and nem12_id
    label_escaped = label.replace("'", "\\'")
    nem12_id_escaped = nem12_id.replace("'", "\\'")

    query = (
        f"g.addV('point')"
        f".property(id, '{point_id}')"
        f".property('label', '{label_escaped}')"
        f".property('nem12Id', '{nem12_id_escaped}')"
        f".property('pointCategory', 'billing')"
        f".as('pt')"
        f".V('{meter_vertex_id}')"
        f".addE('equipRef').from('pt')"
    )

    gremlin_query(query)
    return True


def main(argv: list[str] | None = None) -> int:
    """Import billing points from CSV into Neptune."""
    parser = argparse.ArgumentParser(
        description="Import billing points into Neptune from CSV.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    PYTHONPATH=src uv run scripts/import_billing_points.py --csv data/billing_points.csv --dry-run
    PYTHONPATH=src uv run scripts/import_billing_points.py --csv data/billing_points.csv
        """,
    )
    parser.add_argument("--csv", required=True, dest="csv_file", help="Path to billing_points.csv")
    parser.add_argument("--dry-run", action="store_true", help="Preview mode, no Neptune writes")
    parser.add_argument(
        "--output",
        default="data/billing_point_ids.csv",
        help="Output CSV mapping point_vertex_id to nem12_id (default: data/billing_point_ids.csv)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=10,
        help="Number of parallel workers (default: 10)",
    )
    args = parser.parse_args(argv)

    print("=" * 60)
    print("Billing Points Neptune Import")
    print("=" * 60)
    print(f"  CSV File:  {args.csv_file}")
    print(f"  Mode:      {'DRY RUN' if args.dry_run else 'LIVE'}")
    print("=" * 60)
    print()

    # Step 1: Read CSV
    print("Reading CSV...")
    rows: list[dict[str, str]] = []
    with Path(args.csv_file).open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    print(f"  Loaded {len(rows)} rows")

    if not rows:
        print("No rows to process.")
        return 0

    # Count unique meters
    unique_meters = len({row["identifier"] for row in rows})
    print(f"  Unique meters: {unique_meters}")
    print()

    # Step 2: Batch check existing nem12Ids
    print("Checking existing points in Neptune...")
    all_nem12_ids = [row["nem12_id"] for row in rows]
    existing_nem12_ids = batch_check_existing_nem12_ids(all_nem12_ids)
    print(f"  Already existing: {len(existing_nem12_ids)}")
    print(f"  To create: {len(rows) - len(existing_nem12_ids)}")
    print()

    # Step 3: Create or skip
    to_create = [row for row in rows if row["nem12_id"] not in existing_nem12_ids]
    to_skip = len(rows) - len(to_create)

    if args.dry_run:
        print(f"[DRY RUN] Would create {len(to_create)} billing points for {unique_meters} meters")
        if to_skip:
            print(f"[DRY RUN] Would skip {to_skip} already-existing points")
        if to_create:
            print("\n  Sample points to create:")
            for row in to_create[:5]:
                print(f"    {row['label']} -> meter {row['meter_vertex_id']}")
            if len(to_create) > 5:
                print(f"    ... and {len(to_create) - 5} more")
        print()
        print("=" * 60)
        print("Summary (DRY RUN)")
        print("=" * 60)
        print(f"  Total:   {len(rows)}")
        print(f"  Create:  {len(to_create)}")
        print(f"  Skip:    {to_skip}")
        print("=" * 60)
        return 0

    # Live run
    if not to_create:
        print("All points already exist. Nothing to create.")
        return 0

    workers = args.workers
    print(f"Creating {len(to_create)} billing points with {workers} workers...")
    skipped = to_skip
    created_mappings: list[dict[str, str]] = []
    failed_rows: list[tuple[dict[str, str], str]] = []
    lock = threading.Lock()
    completed_count = 0
    start_time = time.time()

    def _create_one(row: dict[str, str]) -> tuple[dict[str, str], str | None]:
        """Create a single point. Returns (row, error_or_none)."""
        point_id = generate_point_id()
        try:
            create_billing_point(point_id, row["label"], row["nem12_id"], row["meter_vertex_id"])
            return row, None, point_id  # type: ignore[return-value]
        except RuntimeError as e:
            return row, str(e), None  # type: ignore[return-value]

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_create_one, row): row for row in to_create}

        for future in as_completed(futures):
            row, error, point_id = future.result()
            with lock:
                completed_count += 1
                if error:
                    failed_rows.append((row, error))
                    if len(failed_rows) <= 5:
                        print(f"  [{completed_count}/{len(to_create)}] FAILED: {row['label']} - {error}")
                else:
                    created_mappings.append({"point_vertex_id": point_id, "nem12_id": row["nem12_id"]})

                if completed_count % 100 == 0 or completed_count == len(to_create):
                    elapsed = time.time() - start_time
                    rate = completed_count / elapsed if elapsed > 0 else 0
                    eta = (len(to_create) - completed_count) / rate if rate > 0 else 0
                    print(
                        f"  [{completed_count}/{len(to_create)}] "
                        f"{len(created_mappings)} created, {len(failed_rows)} failed "
                        f"({rate:.1f}/s, ETA {eta:.0f}s)"
                    )

    created = len(created_mappings)
    failed = len(failed_rows)

    # Write output mapping CSV
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["point_vertex_id", "nem12_id"])
        writer.writeheader()
        writer.writerows(created_mappings)
    print(f"\nMapping written to {output_path} ({len(created_mappings)} rows)")

    # Summary
    print()
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"  Created: {created}")
    print(f"  Skipped: {skipped}")
    print(f"  Failed:  {failed}")
    print(f"  Output:  {output_path}")
    print("=" * 60)

    if failed_rows:
        print(f"\nFailed rows ({len(failed_rows)}):")
        for row, error in failed_rows[:20]:
            print(f"  - {row['nem12_id']}: {error}")
        if len(failed_rows) > 20:
            print(f"  ... and {len(failed_rows) - 20} more")
        print("\nRe-running is safe (idempotent). Failed rows will be retried.")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
