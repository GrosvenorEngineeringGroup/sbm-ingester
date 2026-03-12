"""Unit tests for import_billing_csv script.

Tests the Bunnings billing CSV import script that upserts site/meter/bill
data into Aurora PostgreSQL.
"""

import contextlib
import json
import os
import tempfile
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Constants for sample data
SAMPLE_HEADER = [
    "BuyerShortName",
    "Country",
    "Commodity",
    "Identifier",
    "IdentifierType",
    "DistributorId",
    "Site Name",
    "Site Reference",
    "Site Reference 2",
    "Site Reference 3",
    "Site Reference 4",
    "Site Reference 5",
    "Business Unit",
    "Address",
    "City",
    "State",
    "Postcode",
    "Cost Code",
    "GL Code",
    "Tags",
    "Site Move in Date",
    "Status",
    "Closed Date",
    "Closed Reason",
    "Date",
    "Retailer",
    "Peak",
    "OffPeak",
    "Shoulder",
    "Total Usage",
    "Total GreenPower",
    "Estimated Peak",
    "Estimated OffPeak",
    "Estimated Shoulder",
    "Total Estimated Usage",
    "Total Estimated GreenPower",
    "Usage Measurement Unit",
    "Energy Charge",
    "Total Network Charge",
    "Environmental Charge",
    "Metering Charge",
    "Other Charge",
    "Total Spend",
    "GreenPower Spend",
    "Estimated Energy Charge",
    "Estimated Network Charge",
    "Estimated Environmental Charge",
    "Estimated Metering Charge",
    "Estimated Other Charge",
    "Total Estimated Spend",
    "Spend Currency",
]

SAMPLE_ROW = [
    "Bunnings",
    "AU",
    "Electricity",
    "3052218678",
    "NMI",
    "ERGONETP",
    "BUN AUS Cairns (Portsmith)",
    "109",
    "Bunnings QLD",
    "8471",
    "4231",
    "",
    "Bunnings Australia",
    "71-83 Kenny Street",
    "Portsmith",
    "AU:QLD",
    "4870",
    "",
    "",
    "8471;bun-aus",
    "17 Apr 2015",
    "Active",
    "",
    "",
    "Jan 2026",
    "CleanCo",
    "185512.99",
    "0.00",
    "0.00",
    "185512.99",
    "0.00",
    "0.00",
    "0.00",
    "0.00",
    "0.00",
    "0.00",
    "kWh",
    "13433.23",
    "23615.18",
    "0.00",
    "33.17",
    "-41.23",
    "37040.35",
    "0.00",
    "0.00",
    "0.00",
    "0.00",
    "0.00",
    "0.00",
    "0.00",
    "AUD",
]


def _build_csv_content(rows, header=None):
    """Build UTF-16-LE encoded CSV bytes with 7 metadata rows."""
    if header is None:
        header = SAMPLE_HEADER

    lines = []
    # 7 metadata rows
    for i in range(7):
        lines.append(f"metadata row {i + 1}")
    # Column header
    lines.append(",".join(header))
    # Data rows
    for row in rows:
        lines.append(",".join(str(v) for v in row))

    text = "\n".join(lines)
    return text.encode("utf-16-le")


@pytest.fixture
def create_billing_csv():
    """Factory fixture that creates temp UTF-16-LE CSV files."""
    created_files = []

    def _create(rows=None, header=None):
        if rows is None:
            rows = [SAMPLE_ROW]
        content = _build_csv_content(rows, header)
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            f.write(content)
            created_files.append(f.name)
            return f.name

    yield _create

    for path in created_files:
        with contextlib.suppress(OSError):
            Path(path).unlink()


# ================================
# TestParseBillDate
# ================================
class TestParseBillDate:
    def test_standard_date(self):
        from scripts.import_billing_csv import parse_bill_date

        result = parse_bill_date("Jan 2026")
        assert result == date(2026, 1, 1)

    def test_various_months(self):
        from scripts.import_billing_csv import parse_bill_date

        assert parse_bill_date("Feb 2026") == date(2026, 2, 1)
        assert parse_bill_date("Dec 2025") == date(2025, 12, 1)
        assert parse_bill_date("Mar 2024") == date(2024, 3, 1)


# ================================
# TestToDecimal
# ================================
class TestToDecimal:
    def test_valid_number(self):
        from scripts.import_billing_csv import _to_decimal

        assert _to_decimal("185512.99") == Decimal("185512.99")

    def test_empty_string(self):
        from scripts.import_billing_csv import _to_decimal

        assert _to_decimal("") == Decimal("0")

    def test_negative(self):
        from scripts.import_billing_csv import _to_decimal

        assert _to_decimal("-41.23") == Decimal("-41.23")

    def test_invalid(self):
        from scripts.import_billing_csv import _to_decimal

        assert _to_decimal("abc") == Decimal("0")

    def test_comma_formatted_number(self):
        from scripts.import_billing_csv import _to_decimal

        assert _to_decimal("185,512.99") == Decimal("185512.99")


# ================================
# TestParseBillingCsv
# ================================
class TestParseBillingCsv:
    def test_parses_single_row(self, create_billing_csv):
        from scripts.import_billing_csv import parse_billing_csv

        filepath = create_billing_csv()
        sites, meters, bills = parse_billing_csv(filepath)

        assert len(sites) == 1
        assert len(meters) == 1
        assert len(bills) == 1

        # Site keyed by building_id "8471"
        site = sites["8471"]
        assert site["name"] == "BUN AUS Cairns (Portsmith)"
        assert site["address"] == "71-83 Kenny Street"
        assert site["building_id"] == "8471"
        assert site["client_id"] == "Bunnings"
        assert site["country"] == "AU"
        assert site["state"] == "AU:QLD"

    def test_parses_meter(self, create_billing_csv):
        from scripts.import_billing_csv import parse_billing_csv

        filepath = create_billing_csv()
        _sites, meters, _bills = parse_billing_csv(filepath)

        meter = meters["3052218678"]
        assert meter["identifier"] == "3052218678"
        assert meter["identifier_type"] == "NMI"
        assert meter["building_id"] == "8471"

    def test_parses_bill(self, create_billing_csv):
        from scripts.import_billing_csv import parse_billing_csv

        filepath = create_billing_csv()
        _sites, _meters, bills = parse_billing_csv(filepath)

        bill = bills[0]
        assert bill["bill_date"] == date(2026, 1, 1)
        assert bill["retailer"] == "CleanCo"
        assert bill["peak_usage"] == Decimal("185512.99")
        assert bill["off_peak_usage"] == Decimal("0.00")
        assert bill["total_usage"] == Decimal("185512.99")
        assert bill["energy_charge"] == Decimal("13433.23")
        assert bill["network_charge"] == Decimal("23615.18")
        assert bill["other_charge"] == Decimal("-41.23")
        assert bill["total_spend"] == Decimal("37040.35")
        assert bill["spend_currency"] == "AUD"
        assert bill["usage_unit"] == "kWh"

    def test_deduplicates_sites(self, create_billing_csv):
        from scripts.import_billing_csv import parse_billing_csv

        # Two rows with same building_id but different NMIs/dates
        row2 = list(SAMPLE_ROW)
        row2[3] = "9999999999"  # Different NMI
        row2[24] = "Feb 2026"  # Different date
        filepath = create_billing_csv(rows=[SAMPLE_ROW, row2])
        sites, meters, bills = parse_billing_csv(filepath)

        assert len(sites) == 1
        assert len(meters) == 2
        assert len(bills) == 2

    def test_empty_building_id_uses_site_name(self, create_billing_csv):
        from scripts.import_billing_csv import parse_billing_csv

        row = list(SAMPLE_ROW)
        row[9] = ""  # Empty Site Reference 3
        filepath = create_billing_csv(rows=[row])
        sites, meters, bills = parse_billing_csv(filepath)

        # Should be keyed by site name
        assert "BUN AUS Cairns (Portsmith)" in sites
        site = sites["BUN AUS Cairns (Portsmith)"]
        assert site["building_id"] is None

    def test_empty_identifier_skips_bill(self, create_billing_csv):
        from scripts.import_billing_csv import parse_billing_csv

        row = list(SAMPLE_ROW)
        row[3] = ""  # Empty Identifier
        filepath = create_billing_csv(rows=[row])
        sites, meters, bills = parse_billing_csv(filepath)

        # Site is still collected, but no meter or bill
        assert len(sites) == 1
        assert len(meters) == 0
        assert len(bills) == 0

    def test_empty_retailer_stored_as_none(self, create_billing_csv):
        from scripts.import_billing_csv import parse_billing_csv

        row = list(SAMPLE_ROW)
        row[25] = ""  # Empty Retailer
        filepath = create_billing_csv(rows=[row])
        _sites, _meters, bills = parse_billing_csv(filepath)

        assert bills[0]["retailer"] is None


# ================================
# TestGetDbUrl
# ================================
class TestGetDbUrl:
    def test_uses_database_url_env_var(self):
        from scripts.import_billing_csv import get_db_url

        with patch.dict(os.environ, {"DATABASE_URL": "postgresql://user:pass@localhost/testdb"}):
            result = get_db_url()
            assert result == "postgresql://user:pass@localhost/testdb"

    def test_falls_back_to_secrets_manager(self):
        from scripts.import_billing_csv import get_db_url

        # Ensure DATABASE_URL is not set
        env = {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}

        secret = {
            "username": "admin",
            "password": "secret123",
            "host": "mydb.cluster.ap-southeast-2.rds.amazonaws.com",
            "port": 5432,
            "dbname": "sbm",
        }

        mock_client = MagicMock()
        mock_client.get_secret_value.return_value = {"SecretString": json.dumps(secret)}

        with patch.dict(os.environ, env, clear=True), patch("boto3.client") as mock_boto3_client:
            mock_boto3_client.return_value = mock_client
            result = get_db_url()

        assert result == "postgresql://admin:secret123@mydb.cluster.ap-southeast-2.rds.amazonaws.com:5432/sbm"
        mock_boto3_client.assert_called_once_with("secretsmanager", region_name="ap-southeast-2")
        mock_client.get_secret_value.assert_called_once_with(SecretId="prod/db/sbm-aurora")


# ================================
# TestMain
# ================================
class TestMain:
    def test_dry_run_does_not_connect(self, create_billing_csv):
        from scripts.import_billing_csv import main

        filepath = create_billing_csv()

        with patch("scripts.import_billing_csv.create_engine") as mock_engine:
            result = main([filepath, "--dry-run"])

        assert result == 0
        mock_engine.assert_not_called()

    def test_normal_run_commits(self, create_billing_csv):
        from scripts.import_billing_csv import main

        filepath = create_billing_csv()

        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        with (
            patch("scripts.import_billing_csv.get_db_url", return_value="postgresql://fake/db"),
            patch("scripts.import_billing_csv.create_engine"),
            patch("scripts.import_billing_csv.Session", return_value=mock_session),
            patch("scripts.import_billing_csv.upsert_sites", return_value={"8471": 1}),
            patch("scripts.import_billing_csv.upsert_meters", return_value={"3052218678": 1}),
            patch("scripts.import_billing_csv.upsert_bills", return_value=1),
        ):
            result = main([filepath])

        assert result == 0
        mock_session.commit.assert_called_once()
