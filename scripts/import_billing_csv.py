#!/usr/bin/env python3
"""
Import Bunnings "Usage and Spend Report" billing CSV into Aurora PostgreSQL.

Reads a UTF-16-LE encoded CSV (with 7 metadata rows before the column header),
parses site/meter/bill data, and upserts into the sites, meters, and bills tables
using the existing SQLModel ORM layer.

Usage:
    uv run scripts/import_billing_csv.py <csv_file> [options]

Options:
    --dry-run        Preview mode, no database writes
    --database-url   Override DATABASE_URL (default: env var or Secrets Manager)

Examples:
    uv run scripts/import_billing_csv.py billing-report.csv --dry-run
    uv run scripts/import_billing_csv.py billing-report.csv --database-url postgresql://...
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sys
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from sqlalchemy.dialects.postgresql import insert
from sqlmodel import Session, create_engine, select

from shared.db.models.bill import Bill
from shared.db.models.meter import Meter
from shared.db.models.site import Site


def parse_bill_date(date_str: str) -> date:
    """Parse 'Mon YYYY' format to first day of the month.

    Args:
        date_str: Date string like 'Jan 2026', 'Feb 2026'.

    Returns:
        Date representing the first of that month.

    Raises:
        ValueError: If the date string cannot be parsed.
    """
    dt = datetime.strptime(date_str.strip(), "%b %Y")
    return dt.date().replace(day=1)


def _to_decimal(value: str) -> Decimal:
    """Convert string to Decimal, defaulting to Decimal('0') on failure.

    Args:
        value: Numeric string to convert.

    Returns:
        Decimal value, or Decimal('0') if conversion fails.
    """
    cleaned = value.strip() if value else ""
    if not cleaned:
        return Decimal("0")
    try:
        return Decimal(cleaned.replace(",", ""))
    except InvalidOperation:
        return Decimal("0")


def parse_billing_csv(
    filepath: str,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], list[dict[str, Any]]]:
    """Read UTF-16-LE encoded billing CSV and extract sites, meters, and bills.

    The CSV has 7 metadata rows before the column header on row 8.

    Args:
        filepath: Path to the CSV file.

    Returns:
        Tuple of (sites_dict, meters_dict, bills_list):
        - sites_dict: keyed by building_id (or site name if building_id is empty)
        - meters_dict: keyed by NMI identifier
        - bills_list: list of bill dicts with all mapped fields
    """
    path = Path(filepath)
    raw_bytes = path.read_bytes()
    text = raw_bytes.decode("utf-16-le")

    # Remove BOM if present
    if text.startswith("\ufeff"):
        text = text[1:]

    # Normalize line endings (file mixes \r\n and bare \r)
    text = text.replace("\r\n", "\n").replace("\r", "\n")

    lines = text.splitlines(keepends=True)

    # Skip 7 metadata rows; row 8 (index 7) is the column header
    data_text = "".join(lines[7:])
    reader = csv.DictReader(io.StringIO(data_text))

    sites: dict[str, dict[str, Any]] = {}
    meters: dict[str, dict[str, Any]] = {}
    bills: list[dict[str, Any]] = []

    for row in reader:
        building_id_raw = row.get("Site Reference 3", "").strip()
        site_name = row.get("Site Name", "").strip()
        building_id: str | None = building_id_raw if building_id_raw else None
        site_key = building_id if building_id else site_name

        # Collect site
        if site_key not in sites:
            sites[site_key] = {
                "name": site_name,
                "address": row.get("Address", "").strip() or None,
                "building_id": building_id,
                "client_id": row.get("BuyerShortName", "").strip() or None,
                "country": row.get("Country", "").strip(),
                "state": row.get("State", "").strip() or None,
            }

        # Collect meter
        identifier = row.get("Identifier", "").strip()
        if identifier and identifier not in meters:
            meters[identifier] = {
                "identifier": identifier,
                "identifier_type": row.get("IdentifierType", "").strip(),
                "building_id": building_id,
                "site_key": site_key,
            }

        # Collect bill (skip rows with empty identifier)
        if not identifier:
            continue
        retailer_raw = row.get("Retailer", "").strip()
        bills.append(
            {
                "identifier": identifier,
                "bill_date": parse_bill_date(row.get("Date", "")),
                "retailer": retailer_raw if retailer_raw else None,
                # Actual usage
                "peak_usage": _to_decimal(row.get("Peak", "")),
                "off_peak_usage": _to_decimal(row.get("OffPeak", "")),
                "shoulder_usage": _to_decimal(row.get("Shoulder", "")),
                "total_usage": _to_decimal(row.get("Total Usage", "")),
                "total_greenpower_usage": _to_decimal(row.get("Total GreenPower", "")),
                # Estimated usage
                "estimated_peak_usage": _to_decimal(row.get("Estimated Peak", "")),
                "estimated_off_peak_usage": _to_decimal(row.get("Estimated OffPeak", "")),
                "estimated_shoulder_usage": _to_decimal(row.get("Estimated Shoulder", "")),
                "total_estimated_usage": _to_decimal(row.get("Total Estimated Usage", "")),
                "total_estimated_greenpower_usage": _to_decimal(row.get("Total Estimated GreenPower", "")),
                # Usage unit
                "usage_unit": row.get("Usage Measurement Unit", "kWh").strip(),
                # Actual spend
                "energy_charge": _to_decimal(row.get("Energy Charge", "")),
                "network_charge": _to_decimal(row.get("Total Network Charge", "")),
                "environmental_charge": _to_decimal(row.get("Environmental Charge", "")),
                "metering_charge": _to_decimal(row.get("Metering Charge", "")),
                "other_charge": _to_decimal(row.get("Other Charge", "")),
                "total_spend": _to_decimal(row.get("Total Spend", "")),
                "greenpower_spend": _to_decimal(row.get("GreenPower Spend", "")),
                # Estimated spend
                "estimated_energy_charge": _to_decimal(row.get("Estimated Energy Charge", "")),
                "estimated_network_charge": _to_decimal(row.get("Estimated Network Charge", "")),
                "estimated_environmental_charge": _to_decimal(row.get("Estimated Environmental Charge", "")),
                "estimated_metering_charge": _to_decimal(row.get("Estimated Metering Charge", "")),
                "estimated_other_charge": _to_decimal(row.get("Estimated Other Charge", "")),
                "total_estimated_spend": _to_decimal(row.get("Total Estimated Spend", "")),
                # Currency
                "spend_currency": row.get("Spend Currency", "AUD").strip(),
            }
        )

    return sites, meters, bills


def get_db_url() -> str:
    """Get database URL from environment or AWS Secrets Manager.

    Tries DATABASE_URL environment variable first. If not set, reads
    credentials from AWS Secrets Manager (prod/db/sbm-aurora in ap-southeast-2)
    and constructs a PostgreSQL connection URL.

    Returns:
        PostgreSQL connection URL string.
    """
    url = os.environ.get("DATABASE_URL")
    if url:
        return url

    import boto3

    client = boto3.client("secretsmanager", region_name="ap-southeast-2")
    response = client.get_secret_value(SecretId="prod/db/sbm-aurora")
    secret = json.loads(response["SecretString"])

    user = secret["username"]
    password = secret["password"]
    host = secret["host"]
    port = secret.get("port", 5432)
    dbname = secret.get("dbname", "sbm")

    return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"


def upsert_sites(session: Session, sites: dict[str, dict[str, Any]]) -> dict[str, int]:
    """Upsert sites and return a mapping of site_key to site id.

    For sites with a building_id: uses ON CONFLICT (building_id) WHERE
    building_id IS NOT NULL DO UPDATE SET name, address, state, updated_at.

    For sites without building_id: uses SELECT-first pattern (select by
    name + country, insert only if not found) to avoid duplicates since
    there is no unique constraint on name.

    Args:
        session: SQLModel database session.
        sites: Dict of site_key -> site data.

    Returns:
        Dict mapping site_key to database site id.
    """
    site_id_map: dict[str, int] = {}
    now = datetime.now()

    # Separate sites with and without building_id
    sites_with_bid: list[tuple[str, dict[str, Any]]] = []
    sites_without_bid: list[tuple[str, dict[str, Any]]] = []

    for site_key, site_data in sites.items():
        if site_data["building_id"]:
            sites_with_bid.append((site_key, site_data))
        else:
            sites_without_bid.append((site_key, site_data))

    # Upsert sites WITH building_id using ON CONFLICT
    for site_key, site_data in sites_with_bid:
        stmt = (
            insert(Site.__table__)
            .values(
                name=site_data["name"],
                address=site_data["address"],
                building_id=site_data["building_id"],
                client_id=site_data["client_id"],
                country=site_data["country"],
                state=site_data["state"],
                created_at=now,
                updated_at=now,
            )
            .on_conflict_do_update(
                index_elements=["building_id"],
                index_where=Site.__table__.c.building_id.isnot(None),
                set_={
                    "name": site_data["name"],
                    "address": site_data["address"],
                    "state": site_data["state"],
                    "updated_at": now,
                },
            )
            .returning(Site.__table__.c.id)
        )
        result = session.execute(stmt)
        site_id_map[site_key] = result.scalar_one()

    # Handle sites WITHOUT building_id using SELECT-first
    for site_key, site_data in sites_without_bid:
        stmt_select = select(Site).where(
            Site.name == site_data["name"],
            Site.country == site_data["country"],
        )
        existing = session.exec(stmt_select).first()

        if existing:
            existing.address = site_data["address"]
            existing.state = site_data["state"]
            existing.updated_at = now
            session.add(existing)
            session.flush()
            site_id_map[site_key] = existing.id  # type: ignore[assignment]
        else:
            new_site = Site(
                name=site_data["name"],
                address=site_data["address"],
                building_id=None,
                client_id=site_data["client_id"],
                country=site_data["country"],
                state=site_data["state"],
                created_at=now,
                updated_at=now,
            )
            session.add(new_site)
            session.flush()
            site_id_map[site_key] = new_site.id  # type: ignore[assignment]

    return site_id_map


def upsert_meters(
    session: Session,
    meters: dict[str, dict[str, Any]],
    site_id_map: dict[str, int],
) -> dict[str, int]:
    """Upsert meters and return a mapping of identifier to meter id.

    Uses ON CONFLICT (identifier) DO UPDATE SET site_id, updated_at.

    Args:
        session: SQLModel database session.
        meters: Dict of identifier -> meter data.
        site_id_map: Dict mapping site_key to site id.

    Returns:
        Dict mapping meter identifier to database meter id.
    """
    meter_id_map: dict[str, int] = {}
    now = datetime.now()

    for identifier, meter_data in meters.items():
        site_key = meter_data["site_key"]
        site_id = site_id_map[site_key]

        stmt = (
            insert(Meter.__table__)
            .values(
                identifier=meter_data["identifier"],
                identifier_type=meter_data["identifier_type"],
                site_id=site_id,
                created_at=now,
                updated_at=now,
            )
            .on_conflict_do_update(
                index_elements=["identifier"],
                set_={
                    "site_id": site_id,
                    "updated_at": now,
                },
            )
            .returning(Meter.__table__.c.id)
        )
        result = session.execute(stmt)
        meter_id_map[identifier] = result.scalar_one()

    return meter_id_map


def upsert_bills(
    session: Session,
    bills: list[dict[str, Any]],
    meter_id_map: dict[str, int],
) -> int:
    """Upsert bills and return the count of upserted rows.

    Uses ON CONFLICT (meter_id, bill_date) DO UPDATE SET all usage/charge/spend fields.

    Args:
        session: SQLModel database session.
        bills: List of bill dicts with an 'identifier' key for meter lookup.
        meter_id_map: Dict mapping meter identifier to meter id.

    Returns:
        Number of bills upserted.
    """
    count = 0
    now = datetime.now()

    for bill in bills:
        identifier = bill["identifier"]
        meter_id = meter_id_map[identifier]

        values = {
            "meter_id": meter_id,
            "bill_date": bill["bill_date"],
            "retailer": bill["retailer"],
            "peak_usage": bill["peak_usage"],
            "off_peak_usage": bill["off_peak_usage"],
            "shoulder_usage": bill["shoulder_usage"],
            "total_usage": bill["total_usage"],
            "total_greenpower_usage": bill["total_greenpower_usage"],
            "estimated_peak_usage": bill["estimated_peak_usage"],
            "estimated_off_peak_usage": bill["estimated_off_peak_usage"],
            "estimated_shoulder_usage": bill["estimated_shoulder_usage"],
            "total_estimated_usage": bill["total_estimated_usage"],
            "total_estimated_greenpower_usage": bill["total_estimated_greenpower_usage"],
            "usage_unit": bill["usage_unit"],
            "energy_charge": bill["energy_charge"],
            "network_charge": bill["network_charge"],
            "environmental_charge": bill["environmental_charge"],
            "metering_charge": bill["metering_charge"],
            "other_charge": bill["other_charge"],
            "total_spend": bill["total_spend"],
            "greenpower_spend": bill["greenpower_spend"],
            "estimated_energy_charge": bill["estimated_energy_charge"],
            "estimated_network_charge": bill["estimated_network_charge"],
            "estimated_environmental_charge": bill["estimated_environmental_charge"],
            "estimated_metering_charge": bill["estimated_metering_charge"],
            "estimated_other_charge": bill["estimated_other_charge"],
            "total_estimated_spend": bill["total_estimated_spend"],
            "spend_currency": bill["spend_currency"],
            "created_at": now,
        }

        # Build update set (all fields except the PK columns and created_at)
        update_set = {k: v for k, v in values.items() if k not in ("meter_id", "bill_date", "created_at")}

        stmt = (
            insert(Bill.__table__)
            .values(**values)
            .on_conflict_do_update(
                constraint="bills_pkey",
                set_=update_set,
            )
        )
        session.execute(stmt)
        count += 1

    return count


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for billing CSV import.

    Args:
        argv: Command line arguments (defaults to sys.argv[1:]).

    Returns:
        Exit code (0 for success, 1 for error).
    """
    parser = argparse.ArgumentParser(
        description="Import Bunnings billing CSV into Aurora PostgreSQL.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    uv run scripts/import_billing_csv.py billing-report.csv --dry-run
    uv run scripts/import_billing_csv.py billing-report.csv --database-url postgresql://...
        """,
    )
    parser.add_argument("csv_file", help="Path to UTF-16-LE encoded billing CSV")
    parser.add_argument("--dry-run", action="store_true", help="Preview mode, no database writes")
    parser.add_argument("--database-url", help="Override DATABASE_URL")

    args = parser.parse_args(argv)

    # Parse CSV
    print("=" * 60)
    print("Billing CSV Import")
    print("=" * 60)
    print(f"CSV File:  {args.csv_file}")
    print(f"Mode:      {'DRY RUN' if args.dry_run else 'LIVE'}")
    print("=" * 60)
    print()

    try:
        sites, meters, bills = parse_billing_csv(args.csv_file)
    except (ValueError, FileNotFoundError) as e:
        print(f"Error parsing CSV: {e}")
        return 1

    print(f"Parsed {len(sites)} sites, {len(meters)} meters, {len(bills)} bills")
    print()

    if args.dry_run:
        print("Dry run complete. No database changes made.")
        return 0

    # Connect to database
    if args.database_url:
        db_url = args.database_url
    else:
        db_url = get_db_url()

    engine = create_engine(db_url, echo=False)

    try:
        with Session(engine) as session:
            print("Upserting sites...")
            site_id_map = upsert_sites(session, sites)
            print(f"  {len(site_id_map)} sites upserted")

            print("Upserting meters...")
            meter_id_map = upsert_meters(session, meters, site_id_map)
            print(f"  {len(meter_id_map)} meters upserted")

            print("Upserting bills...")
            bill_count = upsert_bills(session, bills, meter_id_map)
            print(f"  {bill_count} bills upserted")

            session.commit()
            print()
            print("Import complete. All changes committed.")
    except Exception as e:
        print(f"Error: {e}")
        print("All changes rolled back.")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
