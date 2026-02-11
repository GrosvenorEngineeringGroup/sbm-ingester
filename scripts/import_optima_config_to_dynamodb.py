#!/usr/bin/env python3
"""
Import Optima site configuration from CSV to DynamoDB.

Supports two CSV formats:
1. Sites CSV (from export_optima_sites.py):
   - SiteIdStr, NmiOrIcp, SiteName, ...
   - NmiOrIcp will be prefixed with "Optima_"

2. Legacy format:
   - nmi, siteIdStr, siteName

Usage:
    uv run scripts/import_optima_config_to_dynamodb.py <csv_file> <project> [options]

Options:
    --dry-run   Preview mode, no writes executed
    --force     Overwrite records with differences (default: skip)

Examples:
    uv run scripts/import_optima_config_to_dynamodb.py output/optima-bunnings-sites-2026-01-28.csv bunnings --dry-run
    uv run scripts/import_optima_config_to_dynamodb.py output/optima-bunnings-sites-2026-01-28.csv bunnings --force
"""

import argparse
import csv
import sys
from enum import Enum
from pathlib import Path

import boto3


class ImportStatus(Enum):
    """Status of an import item."""

    NEW = "new"
    IDENTICAL = "identical"
    CONFLICT = "conflict"


def fetch_existing_items(
    table: "boto3.resources.factory.dynamodb.Table",  # type: ignore[name-defined]
    project: str,
    nmis: list[str],
) -> dict[str, dict]:
    """Batch get existing items from DynamoDB.

    Args:
        table: DynamoDB table resource
        project: Project name (partition key)
        nmis: List of NMI values to fetch

    Returns:
        Dict mapping NMI to existing item data
    """
    existing = {}

    # DynamoDB batch_get_item has a limit of 100 items per request
    batch_size = 100
    for i in range(0, len(nmis), batch_size):
        batch_nmis = nmis[i : i + batch_size]
        keys = [{"project": project, "nmi": nmi} for nmi in batch_nmis]

        response = table.meta.client.batch_get_item(RequestItems={table.name: {"Keys": keys}})

        for item in response.get("Responses", {}).get(table.name, []):
            existing[item["nmi"]] = item

        # Handle unprocessed keys (retry)
        unprocessed = response.get("UnprocessedKeys", {}).get(table.name, {})
        while unprocessed:
            response = table.meta.client.batch_get_item(RequestItems={table.name: unprocessed})
            for item in response.get("Responses", {}).get(table.name, []):
                existing[item["nmi"]] = item
            unprocessed = response.get("UnprocessedKeys", {}).get(table.name, {})

    return existing


def compare_item(csv_item: dict, existing: dict | None) -> tuple[ImportStatus, dict | None]:
    """Compare CSV item with existing DynamoDB item.

    Args:
        csv_item: Item from CSV file
        existing: Existing item from DynamoDB (or None)

    Returns:
        Tuple of (status, diff) where diff is a dict of field differences
    """
    if existing is None:
        return ImportStatus.NEW, None

    # Fields to compare (excluding partition/sort keys)
    compare_fields = ["siteIdStr", "siteName"]

    diff = {}
    for field in compare_fields:
        csv_value = csv_item.get(field, "")
        existing_value = existing.get(field, "")
        if csv_value != existing_value:
            diff[field] = {"csv": csv_value, "existing": existing_value}

    if not diff:
        return ImportStatus.IDENTICAL, None

    return ImportStatus.CONFLICT, diff


def normalize_item(row: dict[str, str]) -> dict[str, str] | None:
    """Normalize a CSV row to standard format.

    Supports two formats:
    1. Sites CSV: SiteIdStr, NmiOrIcp, SiteName (NmiOrIcp gets Optima_ prefix)
    2. Legacy: nmi, siteIdStr, siteName

    Args:
        row: CSV row as dict

    Returns:
        Normalized dict with nmi, siteIdStr, siteName or None if invalid
    """
    # Detect format by checking for SiteIdStr (sites format) or siteIdStr (legacy)
    if "SiteIdStr" in row:
        # Sites CSV format
        nmi_raw = row.get("NmiOrIcp", "").strip()
        site_id_str = row.get("SiteIdStr", "").strip()
        site_name = row.get("SiteName", "").strip()

        # Add Optima_ prefix if not already present
        if nmi_raw and not nmi_raw.startswith("Optima_"):
            nmi = f"Optima_{nmi_raw}"
        else:
            nmi = nmi_raw
    else:
        # Legacy format
        nmi = row.get("nmi", "").strip()
        site_id_str = row.get("siteIdStr", "").strip()
        site_name = row.get("siteName", "").strip()

    # Validate required fields are non-empty
    if not nmi:
        return None
    if not site_id_str:
        return None

    return {
        "nmi": nmi,
        "siteIdStr": site_id_str,
        "siteName": site_name,
    }


def import_csv_to_dynamodb(
    csv_path: str,
    project: str,
    dry_run: bool = False,
    force: bool = False,
) -> int:
    """Import CSV data to DynamoDB sbm-optima-config table.

    Args:
        csv_path: Path to CSV file
        project: Project name (partition key)
        dry_run: If True, only preview changes without writing
        force: If True, overwrite records with differences (default: skip)

    Returns:
        Exit code (0 for success, 1 for error)
    """
    dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
    table = dynamodb.Table("sbm-optima-config")

    csv_file = Path(csv_path)
    if not csv_file.exists():
        print(f"Error: File not found: {csv_path}")
        return 1

    with csv_file.open() as f:
        reader = csv.DictReader(f)
        raw_items = list(reader)

    if not raw_items:
        print("Error: CSV file is empty")
        return 1

    # Detect format and validate columns
    csv_cols = set(raw_items[0].keys())
    is_sites_format = "SiteIdStr" in csv_cols

    if is_sites_format:
        required_cols = {"SiteIdStr", "NmiOrIcp"}
        format_name = "Sites CSV"
    else:
        required_cols = {"nmi", "siteIdStr"}
        format_name = "Legacy"

    missing = required_cols - csv_cols
    if missing:
        print(f"Error: Missing required columns: {', '.join(missing)}")
        return 1

    # Normalize items and filter invalid rows
    items = []
    skipped_empty = 0
    for row in raw_items:
        normalized = normalize_item(row)
        if normalized:
            items.append(normalized)
        else:
            skipped_empty += 1

    # Print header
    print("=" * 60)
    print("Optima Config Import")
    print("=" * 60)
    print(f"CSV File:    {csv_file.absolute()}")
    print(f"Format:      {format_name}")
    print(f"Project:     {project}")
    print(f"Mode:        {'DRY RUN' if dry_run else 'LIVE'}")
    if force:
        print("Option:      --force (overwrite conflicts)")
    print("=" * 60)
    if skipped_empty > 0:
        print(f"Warning: Skipped {skipped_empty} rows with empty nmi or siteIdStr")
    print()

    # Fetch existing items
    nmis = [item["nmi"] for item in items]
    print(f"Fetching existing records for {len(nmis)} NMIs...")
    existing_items = fetch_existing_items(table, project, nmis)
    print(f"Found {len(existing_items)} existing records in DynamoDB")
    print()

    # Analyze and categorize items
    print(f"Analyzing {len(items)} records...")
    print()

    new_items = []
    identical_items = []
    conflict_items = []

    for item in items:
        nmi = item["nmi"]
        existing = existing_items.get(nmi)
        status, diff = compare_item(item, existing)

        site_name = item.get("siteName", "N/A")

        if status == ImportStatus.NEW:
            new_items.append(item)
            print(f"  ✓ NEW:       {nmi} - {site_name}")
        elif status == ImportStatus.IDENTICAL:
            identical_items.append(item)
            print(f"  ○ IDENTICAL: {nmi} - {site_name}")
        else:  # CONFLICT
            conflict_items.append((item, diff))
            print(f"  ⚠ CONFLICT:  {nmi} - {site_name}")
            for field, values in diff.items():
                print(f'    └─ {field}: "{values["csv"]}" → "{values["existing"]}" (existing)')

    print()
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"New:         {len(new_items)}")
    print(f"Identical:   {len(identical_items)}  (will skip)")
    print(f"Conflicts:   {len(conflict_items)}  ", end="")
    if conflict_items:
        if force:
            print("(will overwrite)")
        else:
            print("(will skip, use --force to overwrite)")
    else:
        print()
    print("=" * 60)
    print()

    if dry_run:
        print("Dry run complete. No changes made.")
        return 0

    # Confirm before writing if there are conflicts and not forcing
    if conflict_items and not force:
        print(f"Note: {len(conflict_items)} conflicting record(s) will be skipped.")
        print()

    # Execute writes
    items_to_write = new_items.copy()
    if force:
        items_to_write.extend([item for item, _ in conflict_items])

    if not items_to_write:
        print("No items to import.")
        return 0

    print(f"Importing {len(items_to_write)} items...")

    success_count = 0
    error_count = 0
    for item in items_to_write:
        try:
            table.put_item(
                Item={
                    "project": project,
                    "nmi": item["nmi"],
                    "siteIdStr": item["siteIdStr"],
                    "siteName": item.get("siteName", ""),
                }
            )
            success_count += 1
        except Exception as e:
            print(f"  ✗ {item['nmi']} - Error: {e}")
            error_count += 1

    print()
    print(f"Import complete: {success_count} succeeded, {error_count} failed")

    return 0 if error_count == 0 else 1


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Import Optima site configuration from CSV to DynamoDB.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    uv run scripts/import_optima_config_to_dynamodb.py bunnings-optima.csv bunnings --dry-run
    uv run scripts/import_optima_config_to_dynamodb.py bunnings-optima.csv bunnings --force
        """,
    )
    parser.add_argument("csv_file", help="Path to CSV file")
    parser.add_argument("project", help="Project name (e.g., bunnings, racv)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview mode, no writes executed",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite records with differences (default: skip)",
    )

    args = parser.parse_args()

    return import_csv_to_dynamodb(
        csv_path=args.csv_file,
        project=args.project.lower(),
        dry_run=args.dry_run,
        force=args.force,
    )


if __name__ == "__main__":
    sys.exit(main())
