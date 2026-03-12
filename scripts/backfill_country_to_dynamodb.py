#!/usr/bin/env python3
"""
Backfill `country` field for existing items in the sbm-optima-config DynamoDB table.

Detection logic (in priority order):
1. siteName contains "NZ" → country = "NZ"
2. siteName contains "AUS" → country = "AU"
3. Identifier (strip Optima_ prefix) length == 15 → country = "NZ" (NZ ICP format)
4. Otherwise → country = "AU"

Usage:
    uv run scripts/backfill_country_to_dynamodb.py --dry-run
    uv run scripts/backfill_country_to_dynamodb.py
"""

import argparse
import sys

import boto3


def detect_country(nmi: str, site_name: str) -> str:
    """Detect country from site name and NMI/ICP identifier.

    Args:
        nmi: NMI value (e.g., "Optima_3051488345" or "Optima_0000005438UN02B")
        site_name: Site name (e.g., "BUN AUS Rivervale" or "BUN NZ Tokoroa")

    Returns:
        Country code: "AU" or "NZ"
    """
    # Priority 1: Check siteName for country keywords
    name_parts = site_name.split()
    if len(name_parts) >= 2:
        if name_parts[1] == "NZ":
            return "NZ"
        if name_parts[1] == "AUS":
            return "AU"

    # Priority 2: Check identifier length (NZ ICP = 15 chars)
    raw = nmi.removeprefix("Optima_")
    if len(raw) == 15:
        return "NZ"

    return "AU"


def backfill_country(dry_run: bool = False) -> int:
    """Backfill country field for all items in sbm-optima-config.

    Args:
        dry_run: If True, only preview changes without writing

    Returns:
        Exit code (0 for success, 1 for error)
    """
    dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
    table = dynamodb.Table("sbm-optima-config")

    # Scan all items
    print("Scanning sbm-optima-config table...")
    items = []
    response = table.scan()
    items.extend(response.get("Items", []))
    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items.extend(response.get("Items", []))

    print(f"Found {len(items)} items")
    print()

    # Categorize
    already_set = []
    to_update = []

    for item in items:
        nmi = item["nmi"]
        site_name = item.get("siteName", "")
        existing_country = item.get("country")
        detected_country = detect_country(nmi, site_name)

        if existing_country:
            already_set.append((nmi, existing_country))
        else:
            to_update.append((item, detected_country))

    # Print summary
    print("=" * 60)
    print(f"Mode:          {'DRY RUN' if dry_run else 'LIVE'}")
    print(f"Already set:   {len(already_set)}  (will skip)")
    print(f"To update:     {len(to_update)}")
    print("=" * 60)
    print()

    # Count by country
    au_count = sum(1 for _, c in to_update if c == "AU")
    nz_count = sum(1 for _, c in to_update if c == "NZ")
    print(f"  AU: {au_count}")
    print(f"  NZ: {nz_count}")
    print()

    # Show NZ items for verification
    nz_items = [(item, c) for item, c in to_update if c == "NZ"]
    if nz_items:
        print("NZ items to update:")
        for item, _ in nz_items:
            print(f"  {item['nmi']} - {item.get('siteName', '(no name)')}")
        print()

    if dry_run:
        print("Dry run complete. No changes made.")
        return 0

    if not to_update:
        print("No items to update.")
        return 0

    # Execute updates
    print(f"Updating {len(to_update)} items...")
    success_count = 0
    error_count = 0

    for item, country in to_update:
        try:
            table.update_item(
                Key={"project": item["project"], "nmi": item["nmi"]},
                UpdateExpression="SET country = :c",
                ExpressionAttributeValues={":c": country},
            )
            success_count += 1
        except Exception as e:
            print(f"  Error updating {item['nmi']}: {e}")
            error_count += 1

    print()
    print(f"Update complete: {success_count} succeeded, {error_count} failed")

    return 0 if error_count == 0 else 1


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Backfill country field in sbm-optima-config DynamoDB table.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview mode, no writes executed",
    )

    args = parser.parse_args()
    return backfill_country(dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
