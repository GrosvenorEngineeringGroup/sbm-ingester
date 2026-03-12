# Billing CSV Import Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a reusable Python script that imports Bunnings "Usage and Spend Report" CSVs into the `sbm-aurora` PostgreSQL database (sites → meters → bills).

**Architecture:** A single CLI script (`scripts/import_billing_csv.py`) that reads a UTF-16-LE CSV, extracts unique sites and meters, then upserts data into three tables with foreign key relationships. Database credentials come from AWS Secrets Manager (`prod/db/sbm-aurora`) with `DATABASE_URL` env var override.

**Tech Stack:** Python 3.13, psycopg2-binary, boto3, csv stdlib, argparse

**Design doc:** `docs/plans/2026-02-23-billing-csv-import-design.md`

**Data quirks discovered during exploration:**
- CSV is UTF-16-LE encoded with 7 metadata rows before the column header (row 8)
- 9 NMIs (108 rows) have empty `Site Reference 3` (building_id) — use Site Name as fallback key
- 34 building_ids have multiple site names because NMI is appended to name (e.g., `BUN AUS Mirrabooka - 8002444113`) — first occurrence wins
- Date format is `Mon YYYY` (e.g., `Feb 2026`) — parse to first of month (`2026-02-01`)
- `sites.name` has no unique constraint, so sites without building_id must use SELECT-first pattern to avoid duplicates on re-run

---

### Task 1: Add psycopg2-binary dependency

**Files:**
- Modify: `pyproject.toml`

**Step 1: Add dependency**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv add psycopg2-binary
```

**Step 2: Add `.` to pythonpath so tests can import `scripts.*`**

In `pyproject.toml`, update `pythonpath`:

```toml
pythonpath = ["src/functions/optima_exporter", "."]
```

**Step 3: Verify installation**

```bash
uv run python -c "import psycopg2; print(psycopg2.__version__)"
```

Expected: prints version number without error.

**Step 4: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "feat: add psycopg2-binary dependency for Aurora PostgreSQL access"
```

---

### Task 2: Apply schema migration — add 3 GreenPower columns to bills table

**Files:**
- None (SQL executed against live database)

**Step 1: Run ALTER TABLE statements**

```bash
PGPASSWORD='TooRQSTMjZ19L12Dl2TO' psql \
  -h sbm-aurora.cluster-cov3fflnpa7n.ap-southeast-2.rds.amazonaws.com \
  -U postgres -d sbm --no-password -c "
ALTER TABLE bills ADD COLUMN IF NOT EXISTS total_greenpower_usage numeric(14,2) NOT NULL DEFAULT 0;
ALTER TABLE bills ADD COLUMN IF NOT EXISTS total_estimated_greenpower_usage numeric(14,2) NOT NULL DEFAULT 0;
ALTER TABLE bills ADD COLUMN IF NOT EXISTS greenpower_spend numeric(14,2) NOT NULL DEFAULT 0;
"
```

**Step 2: Verify columns exist**

```bash
PGPASSWORD='TooRQSTMjZ19L12Dl2TO' psql \
  -h sbm-aurora.cluster-cov3fflnpa7n.ap-southeast-2.rds.amazonaws.com \
  -U postgres -d sbm --no-password -c "\d bills"
```

Expected: `total_greenpower_usage`, `total_estimated_greenpower_usage`, `greenpower_spend` appear in output.

---

### Task 3: Write CSV parser and tests

**Files:**
- Create: `scripts/import_billing_csv.py`
- Create: `tests/unit/test_import_billing_csv.py`

This task builds the CSV parsing logic only (no DB code yet). The script will have a `parse_billing_csv(filepath)` function that returns structured data.

**Step 1: Write failing tests for CSV parsing**

Create `tests/unit/test_import_billing_csv.py`:

```python
"""Unit tests for billing CSV import script."""

import csv
import json
import os
import tempfile
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# ================================
# Fixtures
# ================================
SAMPLE_HEADER = [
    "BuyerShortName", "Country", "Commodity", "Identifier", "IdentifierType",
    "DistributorId", "Site Name", "Site Reference", "Site Reference 2",
    "Site Reference 3", "Site Reference 4", "Site Reference 5", "Business Unit",
    "Address", "City", "State", "Postcode", "Cost Code", "GL Code", "Tags",
    "Site Move in Date", "Status", "Closed Date", "Closed Reason", "Date",
    "Retailer", "Peak", "OffPeak", "Shoulder", "Total Usage", "Total GreenPower",
    "Estimated Peak", "Estimated OffPeak", "Estimated Shoulder",
    "Total Estimated Usage", "Total Estimated GreenPower",
    "Usage Measurement Unit", "Energy Charge", "Total Network Charge",
    "Environmental Charge", "Metering Charge", "Other Charge", "Total Spend",
    "GreenPower Spend", "Estimated Energy Charge", "Estimated Network Charge",
    "Estimated Environmental Charge", "Estimated Metering Charge",
    "Estimated Other Charge", "Total Estimated Spend", "Spend Currency",
]

SAMPLE_ROW = [
    "Bunnings", "AU", "Electricity", "3052218678", "NMI", "ERGONETP",
    "BUN AUS Cairns (Portsmith)", "109", "Bunnings QLD", "8471", "4231", "",
    "Bunnings Australia", "71-83 Kenny Street", "Portsmith", "AU:QLD", "4870",
    "", "", "8471;bun-aus", "17 Apr 2015", "Active", "", "",
    "Jan 2026", "CleanCo", "185512.99", "0.00", "0.00", "185512.99", "0.00",
    "0.00", "0.00", "0.00", "0.00", "0.00", "kWh",
    "13433.23", "23615.18", "0.00", "33.17", "-41.23", "37040.35", "0.00",
    "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "AUD",
]


@pytest.fixture
def create_billing_csv():
    """Factory fixture to create temporary billing CSV files in UTF-16-LE."""
    def _create(rows=None, metadata_lines=None):
        if metadata_lines is None:
            metadata_lines = [
                'Commodities:,"Electricity"',
                'Status:,"Active"',
                'Country:, Australia',
                'Start:,01 Mar 2025',
                'End:,28 Feb 2026',
                '',
                '',
            ]
        if rows is None:
            rows = [SAMPLE_ROW]

        lines = metadata_lines + [",".join(SAMPLE_HEADER)]
        for row in rows:
            # Quote fields that contain commas
            quoted = []
            for val in row:
                if "," in val:
                    quoted.append(f'"{val}"')
                else:
                    quoted.append(val)
            lines.append(",".join(quoted))

        content = "\n".join(lines)
        tmp = tempfile.NamedTemporaryFile(
            mode="wb", suffix=".csv", delete=False
        )
        tmp.write(content.encode("utf-16-le"))
        tmp.close()
        return tmp.name
    return _create


# ================================
# Tests: parse_bill_date
# ================================
class TestParseBillDate:
    def test_standard_date(self):
        from scripts.import_billing_csv import parse_bill_date
        assert parse_bill_date("Jan 2026") == date(2026, 1, 1)

    def test_various_months(self):
        from scripts.import_billing_csv import parse_bill_date
        assert parse_bill_date("Feb 2026") == date(2026, 2, 1)
        assert parse_bill_date("Dec 2025") == date(2025, 12, 1)
        assert parse_bill_date("Mar 2025") == date(2025, 3, 1)


# ================================
# Tests: parse_billing_csv
# ================================
class TestParseBillingCsv:
    def test_parses_single_row(self, create_billing_csv):
        from scripts.import_billing_csv import parse_billing_csv
        filepath = create_billing_csv()
        sites, meters, bills = parse_billing_csv(filepath)

        assert len(sites) == 1
        site = sites["8471"]
        assert site["name"] == "BUN AUS Cairns (Portsmith)"
        assert site["address"] == "71-83 Kenny Street"
        assert site["country"] == "AU"
        assert site["state"] == "AU:QLD"
        assert site["client_id"] == "Bunnings"

    def test_parses_meter(self, create_billing_csv):
        from scripts.import_billing_csv import parse_billing_csv
        filepath = create_billing_csv()
        sites, meters, bills = parse_billing_csv(filepath)

        assert len(meters) == 1
        meter = meters["3052218678"]
        assert meter["identifier"] == "3052218678"
        assert meter["identifier_type"] == "NMI"
        assert meter["building_id"] == "8471"

    def test_parses_bill(self, create_billing_csv):
        from scripts.import_billing_csv import parse_billing_csv
        filepath = create_billing_csv()
        sites, meters, bills = parse_billing_csv(filepath)

        assert len(bills) == 1
        bill = bills[0]
        assert bill["identifier"] == "3052218678"
        assert bill["bill_date"] == date(2026, 1, 1)
        assert bill["retailer"] == "CleanCo"
        assert bill["peak_usage"] == 185512.99
        assert bill["total_spend"] == 37040.35
        assert bill["spend_currency"] == "AUD"

    def test_deduplicates_sites(self, create_billing_csv):
        from scripts.import_billing_csv import parse_billing_csv
        # Two rows with same building_id (different months)
        row2 = SAMPLE_ROW.copy()
        row2[24] = "Feb 2026"
        filepath = create_billing_csv(rows=[SAMPLE_ROW, row2])
        sites, meters, bills = parse_billing_csv(filepath)

        assert len(sites) == 1
        assert len(bills) == 2

    def test_empty_building_id_uses_site_name(self, create_billing_csv):
        from scripts.import_billing_csv import parse_billing_csv
        row = SAMPLE_ROW.copy()
        row[9] = ""  # empty Site Reference 3
        row[6] = "BUN AUS Oxley WH - 3120914378"
        filepath = create_billing_csv(rows=[row])
        sites, meters, bills = parse_billing_csv(filepath)

        assert len(sites) == 1
        # Key should be site name since building_id is empty
        site = list(sites.values())[0]
        assert site["name"] == "BUN AUS Oxley WH - 3120914378"
        assert site["building_id"] is None

    def test_empty_retailer_stored_as_none(self, create_billing_csv):
        from scripts.import_billing_csv import parse_billing_csv
        row = SAMPLE_ROW.copy()
        row[25] = ""  # empty Retailer
        filepath = create_billing_csv(rows=[row])
        _, _, bills = parse_billing_csv(filepath)
        assert bills[0]["retailer"] is None
```

**Step 2: Run tests to verify they fail**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv run pytest tests/unit/test_import_billing_csv.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'scripts.import_billing_csv'`

**Step 3: Implement parse_billing_csv**

Create `scripts/import_billing_csv.py`:

```python
#!/usr/bin/env python3
"""
Import Bunnings "Usage and Spend Report" CSV into Aurora PostgreSQL.

Reads a UTF-16-LE encoded billing CSV and upserts data into three tables:
sites → meters → bills.

Usage:
    uv run python scripts/import_billing_csv.py <csv_file> [options]

Options:
    --dry-run   Preview mode, no database writes
    --database-url  Override Secrets Manager with a connection URL

Examples:
    uv run python scripts/import_billing_csv.py "/path/to/report.csv" --dry-run
    uv run python scripts/import_billing_csv.py "/path/to/report.csv"
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any

METADATA_ROWS = 7  # Lines before column header


def parse_bill_date(date_str: str) -> date:
    """Parse 'Mon YYYY' format (e.g., 'Feb 2026') to first of month."""
    return datetime.strptime(date_str.strip(), "%b %Y").replace(day=1).date()


def _to_decimal(value: str) -> float:
    """Convert string to float, defaulting to 0.0 for empty strings."""
    value = value.strip()
    return float(value) if value else 0.0


def parse_billing_csv(
    filepath: str,
) -> tuple[dict[str, dict], dict[str, dict], list[dict]]:
    """Parse a Bunnings billing CSV file.

    Args:
        filepath: Path to the UTF-16-LE encoded CSV file.

    Returns:
        Tuple of (sites_dict, meters_dict, bills_list):
        - sites_dict: keyed by building_id (or site name if building_id empty)
        - meters_dict: keyed by identifier (NMI)
        - bills_list: list of bill dicts
    """
    path = Path(filepath)
    raw = path.read_bytes()
    text = raw.decode("utf-16-le").replace("\r\n", "\n").replace("\r", "\n")
    lines = text.strip().split("\n")

    # Skip metadata rows, parse from column header
    reader = csv.DictReader(lines[METADATA_ROWS:])

    sites: dict[str, dict] = {}
    meters: dict[str, dict] = {}
    bills: list[dict] = []

    for row in reader:
        building_id = row["Site Reference 3"].strip() or None
        site_key = building_id if building_id else row["Site Name"].strip()
        identifier = row["Identifier"].strip()

        # Deduplicate sites
        if site_key not in sites:
            sites[site_key] = {
                "name": row["Site Name"].strip(),
                "address": row["Address"].strip(),
                "building_id": building_id,
                "client_id": row["BuyerShortName"].strip(),
                "country": row["Country"].strip(),
                "state": row["State"].strip(),
            }

        # Deduplicate meters
        if identifier not in meters:
            meters[identifier] = {
                "identifier": identifier,
                "identifier_type": row["IdentifierType"].strip(),
                "building_id": building_id,
                "site_key": site_key,
            }

        # Parse bill
        retailer = row["Retailer"].strip() or None
        bills.append({
            "identifier": identifier,
            "bill_date": parse_bill_date(row["Date"]),
            "retailer": retailer,
            "peak_usage": _to_decimal(row["Peak"]),
            "off_peak_usage": _to_decimal(row["OffPeak"]),
            "shoulder_usage": _to_decimal(row["Shoulder"]),
            "total_usage": _to_decimal(row["Total Usage"]),
            "total_greenpower_usage": _to_decimal(row["Total GreenPower"]),
            "estimated_peak_usage": _to_decimal(row["Estimated Peak"]),
            "estimated_off_peak_usage": _to_decimal(row["Estimated OffPeak"]),
            "estimated_shoulder_usage": _to_decimal(row["Estimated Shoulder"]),
            "total_estimated_usage": _to_decimal(row["Total Estimated Usage"]),
            "total_estimated_greenpower_usage": _to_decimal(row["Total Estimated GreenPower"]),
            "usage_unit": row["Usage Measurement Unit"].strip(),
            "energy_charge": _to_decimal(row["Energy Charge"]),
            "network_charge": _to_decimal(row["Total Network Charge"]),
            "environmental_charge": _to_decimal(row["Environmental Charge"]),
            "metering_charge": _to_decimal(row["Metering Charge"]),
            "other_charge": _to_decimal(row["Other Charge"]),
            "total_spend": _to_decimal(row["Total Spend"]),
            "greenpower_spend": _to_decimal(row["GreenPower Spend"]),
            "estimated_energy_charge": _to_decimal(row["Estimated Energy Charge"]),
            "estimated_network_charge": _to_decimal(row["Estimated Network Charge"]),
            "estimated_environmental_charge": _to_decimal(row["Estimated Environmental Charge"]),
            "estimated_metering_charge": _to_decimal(row["Estimated Metering Charge"]),
            "estimated_other_charge": _to_decimal(row["Estimated Other Charge"]),
            "total_estimated_spend": _to_decimal(row["Total Estimated Spend"]),
            "spend_currency": row["Spend Currency"].strip(),
        })

    return sites, meters, bills
```

**Step 4: Run tests to verify they pass**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv run pytest tests/unit/test_import_billing_csv.py -v
```

Expected: all tests PASS.

**Step 5: Lint**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv run ruff check scripts/import_billing_csv.py tests/unit/test_import_billing_csv.py && uv run ruff format scripts/import_billing_csv.py tests/unit/test_import_billing_csv.py
```

**Step 6: Commit**

```bash
git add scripts/import_billing_csv.py tests/unit/test_import_billing_csv.py
git commit -m "feat: add billing CSV parser with tests"
```

---

### Task 4: Write database connection helper and upsert functions

**Files:**
- Modify: `scripts/import_billing_csv.py`
- Modify: `tests/unit/test_import_billing_csv.py`

This task adds the `get_db_connection()` function (Secrets Manager / DATABASE_URL) and the three upsert functions: `upsert_sites()`, `upsert_meters()`, `upsert_bills()`.

**Step 1: Write failing tests for DB functions**

Append to `tests/unit/test_import_billing_csv.py` (imports already at top of file from Task 3):

```python
# ================================
# Tests: get_db_connection
# ================================
class TestGetDbConnection:
    @patch("scripts.import_billing_csv.psycopg2")
    def test_uses_database_url_env_var(self, mock_pg):
        from scripts.import_billing_csv import get_db_connection
        with patch.dict(os.environ, {"DATABASE_URL": "postgresql://user:pass@host:5432/db"}):
            get_db_connection()
            mock_pg.connect.assert_called_once_with("postgresql://user:pass@host:5432/db")

    @patch("scripts.import_billing_csv.psycopg2")
    @patch("scripts.import_billing_csv.boto3")
    def test_falls_back_to_secrets_manager(self, mock_boto, mock_pg):
        from scripts.import_billing_csv import get_db_connection
        # Remove DATABASE_URL if present
        env = {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}
        mock_client = MagicMock()
        mock_boto.client.return_value = mock_client
        mock_client.get_secret_value.return_value = {
            "SecretString": json.dumps({
                "host": "myhost", "port": 5432,
                "dbname": "mydb", "username": "user", "password": "pass"
            })
        }
        with patch.dict(os.environ, env, clear=True):
            get_db_connection()
            mock_pg.connect.assert_called_once_with(
                host="myhost", port=5432,
                dbname="mydb", user="user", password="pass"
            )


# ================================
# Tests: upsert functions
# ================================
class TestUpsertSites:
    def test_generates_correct_sql(self):
        from scripts.import_billing_csv import upsert_sites
        mock_cursor = MagicMock()
        sites = {
            "8471": {
                "name": "BUN AUS Cairns", "address": "71 Kenny St",
                "building_id": "8471", "client_id": "Bunnings",
                "country": "AU", "state": "AU:QLD",
            }
        }
        result = upsert_sites(mock_cursor, sites)
        assert mock_cursor.execute.called
        assert result == {"8471": mock_cursor.fetchone.return_value[0]}


class TestUpsertMeters:
    def test_generates_correct_sql(self):
        from scripts.import_billing_csv import upsert_meters
        mock_cursor = MagicMock()
        meters = {
            "3052218678": {
                "identifier": "3052218678", "identifier_type": "NMI",
                "building_id": "8471", "site_key": "8471",
            }
        }
        site_id_map = {"8471": 1}
        result = upsert_meters(mock_cursor, meters, site_id_map)
        assert mock_cursor.execute.called
        assert result == {"3052218678": mock_cursor.fetchone.return_value[0]}


class TestUpsertBills:
    def test_generates_correct_sql(self):
        from scripts.import_billing_csv import upsert_bills
        mock_cursor = MagicMock()
        bills = [{
            "identifier": "3052218678",
            "bill_date": date(2026, 1, 1),
            "retailer": "CleanCo",
            "peak_usage": 185512.99, "off_peak_usage": 0.0,
            "shoulder_usage": 0.0, "total_usage": 185512.99,
            "total_greenpower_usage": 0.0,
            "estimated_peak_usage": 0.0, "estimated_off_peak_usage": 0.0,
            "estimated_shoulder_usage": 0.0, "total_estimated_usage": 0.0,
            "total_estimated_greenpower_usage": 0.0,
            "usage_unit": "kWh",
            "energy_charge": 13433.23, "network_charge": 23615.18,
            "environmental_charge": 0.0, "metering_charge": 33.17,
            "other_charge": -41.23, "total_spend": 37040.35,
            "greenpower_spend": 0.0,
            "estimated_energy_charge": 0.0, "estimated_network_charge": 0.0,
            "estimated_environmental_charge": 0.0,
            "estimated_metering_charge": 0.0, "estimated_other_charge": 0.0,
            "total_estimated_spend": 0.0, "spend_currency": "AUD",
        }]
        meter_id_map = {"3052218678": 42}
        count = upsert_bills(mock_cursor, bills, meter_id_map)
        assert mock_cursor.execute.called
        assert count == 1
```

**Step 2: Run tests to verify they fail**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv run pytest tests/unit/test_import_billing_csv.py -v -k "TestGetDb or TestUpsert"
```

Expected: FAIL — `ImportError`

**Step 3: Implement DB functions**

Add to `scripts/import_billing_csv.py` (after `parse_billing_csv`). Note: `os` and `json` are already imported from Task 3. Add `import boto3` and `import psycopg2` to the import block at the top of the file.

```python
import boto3
import psycopg2

SECRET_ID = "prod/db/sbm-aurora"
AWS_REGION = "ap-southeast-2"


def get_db_connection():
    """Get a PostgreSQL connection from DATABASE_URL or Secrets Manager."""
    database_url = os.environ.get("DATABASE_URL")
    if database_url:
        return psycopg2.connect(database_url)

    client = boto3.client("secretsmanager", region_name=AWS_REGION)
    secret = json.loads(
        client.get_secret_value(SecretId=SECRET_ID)["SecretString"]
    )
    return psycopg2.connect(
        host=secret["host"],
        port=secret["port"],
        dbname=secret["dbname"],
        user=secret["username"],
        password=secret["password"],
    )


def upsert_sites(
    cursor: Any,
    sites: dict[str, dict],
) -> dict[str, int]:
    """Upsert sites and return {site_key: site_id} mapping."""
    site_id_map: dict[str, int] = {}
    for site_key, site in sites.items():
        if site["building_id"]:
            cursor.execute(
                """
                INSERT INTO sites (name, address, building_id, client_id, country, state)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (building_id) WHERE building_id IS NOT NULL
                DO UPDATE SET name = EXCLUDED.name, address = EXCLUDED.address,
                             state = EXCLUDED.state, updated_at = NOW()
                RETURNING id
                """,
                (site["name"], site["address"], site["building_id"],
                 site["client_id"], site["country"], site["state"]),
            )
        else:
            # No building_id — SELECT first to avoid duplicates (no unique constraint on name)
            cursor.execute(
                "SELECT id FROM sites WHERE name = %s AND country = %s",
                (site["name"], site["country"]),
            )
            existing = cursor.fetchone()
            if existing:
                site_id_map[site_key] = existing[0]
                continue
            cursor.execute(
                """
                INSERT INTO sites (name, address, client_id, country, state)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
                """,
                (site["name"], site["address"],
                 site["client_id"], site["country"], site["state"]),
            )
        site_id_map[site_key] = cursor.fetchone()[0]
    return site_id_map


def upsert_meters(
    cursor: Any,
    meters: dict[str, dict],
    site_id_map: dict[str, int],
) -> dict[str, int]:
    """Upsert meters and return {identifier: meter_id} mapping."""
    meter_id_map: dict[str, int] = {}
    for identifier, meter in meters.items():
        site_id = site_id_map[meter["site_key"]]
        cursor.execute(
            """
            INSERT INTO meters (identifier, identifier_type, site_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (identifier)
            DO UPDATE SET site_id = EXCLUDED.site_id, updated_at = NOW()
            RETURNING id
            """,
            (meter["identifier"], meter["identifier_type"], site_id),
        )
        meter_id_map[identifier] = cursor.fetchone()[0]
    return meter_id_map


def upsert_bills(
    cursor: Any,
    bills: list[dict],
    meter_id_map: dict[str, int],
) -> int:
    """Upsert bills. Returns number of rows upserted."""
    count = 0
    for bill in bills:
        meter_id = meter_id_map.get(bill["identifier"])
        if meter_id is None:
            continue
        cursor.execute(
            """
            INSERT INTO bills (
                meter_id, bill_date, retailer,
                peak_usage, off_peak_usage, shoulder_usage, total_usage,
                total_greenpower_usage,
                estimated_peak_usage, estimated_off_peak_usage,
                estimated_shoulder_usage, total_estimated_usage,
                total_estimated_greenpower_usage,
                usage_unit,
                energy_charge, network_charge, environmental_charge,
                metering_charge, other_charge, total_spend,
                greenpower_spend,
                estimated_energy_charge, estimated_network_charge,
                estimated_environmental_charge, estimated_metering_charge,
                estimated_other_charge, total_estimated_spend,
                spend_currency
            ) VALUES (
                %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s,
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s
            )
            ON CONFLICT (meter_id, bill_date)
            DO UPDATE SET
                retailer = EXCLUDED.retailer,
                peak_usage = EXCLUDED.peak_usage,
                off_peak_usage = EXCLUDED.off_peak_usage,
                shoulder_usage = EXCLUDED.shoulder_usage,
                total_usage = EXCLUDED.total_usage,
                total_greenpower_usage = EXCLUDED.total_greenpower_usage,
                estimated_peak_usage = EXCLUDED.estimated_peak_usage,
                estimated_off_peak_usage = EXCLUDED.estimated_off_peak_usage,
                estimated_shoulder_usage = EXCLUDED.estimated_shoulder_usage,
                total_estimated_usage = EXCLUDED.total_estimated_usage,
                total_estimated_greenpower_usage = EXCLUDED.total_estimated_greenpower_usage,
                usage_unit = EXCLUDED.usage_unit,
                energy_charge = EXCLUDED.energy_charge,
                network_charge = EXCLUDED.network_charge,
                environmental_charge = EXCLUDED.environmental_charge,
                metering_charge = EXCLUDED.metering_charge,
                other_charge = EXCLUDED.other_charge,
                total_spend = EXCLUDED.total_spend,
                greenpower_spend = EXCLUDED.greenpower_spend,
                estimated_energy_charge = EXCLUDED.estimated_energy_charge,
                estimated_network_charge = EXCLUDED.estimated_network_charge,
                estimated_environmental_charge = EXCLUDED.estimated_environmental_charge,
                estimated_metering_charge = EXCLUDED.estimated_metering_charge,
                estimated_other_charge = EXCLUDED.estimated_other_charge,
                total_estimated_spend = EXCLUDED.total_estimated_spend,
                spend_currency = EXCLUDED.spend_currency
            """,
            (
                meter_id, bill["bill_date"], bill["retailer"],
                bill["peak_usage"], bill["off_peak_usage"],
                bill["shoulder_usage"], bill["total_usage"],
                bill["total_greenpower_usage"],
                bill["estimated_peak_usage"], bill["estimated_off_peak_usage"],
                bill["estimated_shoulder_usage"], bill["total_estimated_usage"],
                bill["total_estimated_greenpower_usage"],
                bill["usage_unit"],
                bill["energy_charge"], bill["network_charge"],
                bill["environmental_charge"], bill["metering_charge"],
                bill["other_charge"], bill["total_spend"],
                bill["greenpower_spend"],
                bill["estimated_energy_charge"], bill["estimated_network_charge"],
                bill["estimated_environmental_charge"],
                bill["estimated_metering_charge"],
                bill["estimated_other_charge"], bill["total_estimated_spend"],
                bill["spend_currency"],
            ),
        )
        count += 1
    return count
```

**Step 4: Run tests to verify they pass**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv run pytest tests/unit/test_import_billing_csv.py -v
```

Expected: all tests PASS.

**Step 5: Lint and commit**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv run ruff check scripts/import_billing_csv.py tests/unit/test_import_billing_csv.py --fix && uv run ruff format scripts/import_billing_csv.py tests/unit/test_import_billing_csv.py
git add scripts/import_billing_csv.py tests/unit/test_import_billing_csv.py
git commit -m "feat: add database upsert functions for billing import"
```

---

### Task 5: Write CLI main function and --dry-run support

**Files:**
- Modify: `scripts/import_billing_csv.py`
- Modify: `tests/unit/test_import_billing_csv.py`

**Step 1: Write failing test for main**

Append to `tests/unit/test_import_billing_csv.py`:

```python
class TestMain:
    @patch("scripts.import_billing_csv.get_db_connection")
    def test_dry_run_does_not_write(self, mock_conn, create_billing_csv):
        from scripts.import_billing_csv import main
        filepath = create_billing_csv()
        main([filepath, "--dry-run"])
        # Should not call commit
        mock_conn.return_value.commit.assert_not_called()

    @patch("scripts.import_billing_csv.get_db_connection")
    def test_normal_run_commits(self, mock_conn, create_billing_csv):
        from scripts.import_billing_csv import main
        mock_cursor = MagicMock()
        mock_conn.return_value.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.return_value.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchone.return_value = (1,)

        filepath = create_billing_csv()
        main([filepath])
        mock_conn.return_value.commit.assert_called_once()
```

**Step 2: Run tests to verify they fail**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv run pytest tests/unit/test_import_billing_csv.py::TestMain -v
```

**Step 3: Implement main**

Add to `scripts/import_billing_csv.py`:

```python
def main(argv: list[str] | None = None) -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Import Bunnings billing CSV into Aurora PostgreSQL"
    )
    parser.add_argument("csv_file", help="Path to the billing CSV file (UTF-16-LE)")
    parser.add_argument("--dry-run", action="store_true", help="Preview mode, no database writes")
    parser.add_argument("--database-url", help="Override Secrets Manager with a connection URL")
    args = parser.parse_args(argv)

    if args.database_url:
        os.environ["DATABASE_URL"] = args.database_url

    # Parse CSV
    print(f"Parsing {args.csv_file}...")
    sites, meters, bills = parse_billing_csv(args.csv_file)
    print(f"  Sites:  {len(sites)}")
    print(f"  Meters: {len(meters)}")
    print(f"  Bills:  {len(bills)}")

    if args.dry_run:
        print("\n[DRY RUN] No database changes made.")
        return

    # Connect and upsert
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            print("\nUpserting sites...")
            site_id_map = upsert_sites(cur, sites)
            print(f"  {len(site_id_map)} sites upserted")

            print("Upserting meters...")
            meter_id_map = upsert_meters(cur, meters, site_id_map)
            print(f"  {len(meter_id_map)} meters upserted")

            print("Upserting bills...")
            bill_count = upsert_bills(cur, bills, meter_id_map)
            print(f"  {bill_count} bills upserted")

        conn.commit()
        print("\nDone. All changes committed.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
```

**Step 4: Run all tests**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv run pytest tests/unit/test_import_billing_csv.py -v
```

Expected: all tests PASS.

**Step 5: Lint and commit**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv run ruff check scripts/import_billing_csv.py tests/unit/test_import_billing_csv.py --fix && uv run ruff format scripts/import_billing_csv.py tests/unit/test_import_billing_csv.py
git add scripts/import_billing_csv.py tests/unit/test_import_billing_csv.py
git commit -m "feat: add CLI entry point with --dry-run support"
```

---

### Task 6: Run against real database

**Step 1: Dry run against real CSV**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv run python scripts/import_billing_csv.py "/Users/zeyu/Downloads/20260223.091747-Bunnings-Usage and Spend Report.csv" --dry-run
```

Expected: prints counts (Sites: ~410, Meters: ~413, Bills: ~4956), no DB changes.

**Step 2: Real import**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv run python scripts/import_billing_csv.py "/Users/zeyu/Downloads/20260223.091747-Bunnings-Usage and Spend Report.csv"
```

Expected: prints upsert counts and "Done. All changes committed."

**Step 3: Verify data in database**

```bash
PGPASSWORD='TooRQSTMjZ19L12Dl2TO' psql \
  -h sbm-aurora.cluster-cov3fflnpa7n.ap-southeast-2.rds.amazonaws.com \
  -U postgres -d sbm --no-password -c "
SELECT 'sites' as tbl, count(*) FROM sites
UNION ALL SELECT 'meters', count(*) FROM meters
UNION ALL SELECT 'bills', count(*) FROM bills;
"
```

Expected: sites ~410, meters ~413, bills ~4956.

**Step 4: Verify idempotency — run again**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv run python scripts/import_billing_csv.py "/Users/zeyu/Downloads/20260223.091747-Bunnings-Usage and Spend Report.csv"
```

Then re-check counts — should be identical (upsert, not duplicate insert).

**Step 5: Final commit**

```bash
git add -A
git commit -m "feat: complete billing CSV import script"
```
