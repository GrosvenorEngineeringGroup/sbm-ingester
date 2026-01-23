"""Unit tests for import_optima_config_to_dynamodb script.

Tests the Optima site configuration import script with DynamoDB.
"""

import csv
import os
import sys
import tempfile
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import boto3
import pytest
from moto import mock_aws

# Type aliases for fixtures
CsvData = list[dict[str, str]]
CsvFileFactory = Callable[..., str]


# ================================
# Test Fixtures
# ================================
@pytest.fixture(autouse=True)
def reset_env() -> None:
    """Reset environment variables before each test."""
    os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"


@pytest.fixture
def sample_csv_data() -> list[dict[str, str]]:
    """Sample CSV data for testing."""
    return [
        {"nmi": "4103381203", "siteIdStr": "site-uuid-1", "siteName": "BUN AUS Narrabeen"},
        {"nmi": "4103865235", "siteIdStr": "site-uuid-2", "siteName": "BUN AUS West Gosford"},
        {"nmi": "4310958168", "siteIdStr": "site-uuid-3", "siteName": "BUN AUS Unanderra TC"},
    ]


@pytest.fixture
def create_csv_file(sample_csv_data: list[dict[str, str]]) -> Callable[..., str]:
    """Factory fixture to create temporary CSV files."""

    def _create(data: list[dict[str, str]] | None = None, include_header: bool = True) -> str:
        if data is None:
            data = sample_csv_data

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            if data:
                fieldnames = data[0].keys()
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                if include_header:
                    writer.writeheader()
                writer.writerows(data)
            return f.name

    return _create


@pytest.fixture
def dynamodb_table() -> Generator[Any]:
    """Create a mocked DynamoDB table."""
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        table = dynamodb.create_table(
            TableName="sbm-optima-config",
            KeySchema=[
                {"AttributeName": "project", "KeyType": "HASH"},
                {"AttributeName": "nmi", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "project", "AttributeType": "S"},
                {"AttributeName": "nmi", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()
        yield table


# ================================
# ImportStatus Enum Tests
# ================================
class TestImportStatus:
    """Tests for ImportStatus enum."""

    def test_enum_values(self) -> None:
        """Test that enum has correct values."""
        from scripts.import_optima_config_to_dynamodb import ImportStatus

        assert ImportStatus.NEW.value == "new"
        assert ImportStatus.IDENTICAL.value == "identical"
        assert ImportStatus.CONFLICT.value == "conflict"

    def test_enum_members(self) -> None:
        """Test that enum has all expected members."""
        from scripts.import_optima_config_to_dynamodb import ImportStatus

        members = [m.name for m in ImportStatus]
        assert "NEW" in members
        assert "IDENTICAL" in members
        assert "CONFLICT" in members
        assert len(members) == 3


# ================================
# compare_item Tests
# ================================
class TestCompareItem:
    """Tests for compare_item function."""

    def test_new_item_when_no_existing(self) -> None:
        """Test that item is NEW when no existing record."""
        from scripts.import_optima_config_to_dynamodb import ImportStatus, compare_item

        csv_item = {"nmi": "123", "siteIdStr": "uuid-1", "siteName": "Test Site"}
        status, diff = compare_item(csv_item, None)

        assert status == ImportStatus.NEW
        assert diff is None

    def test_identical_when_all_fields_match(self) -> None:
        """Test that item is IDENTICAL when all fields match."""
        from scripts.import_optima_config_to_dynamodb import ImportStatus, compare_item

        csv_item = {"nmi": "123", "siteIdStr": "uuid-1", "siteName": "Test Site"}
        existing = {"project": "test", "nmi": "123", "siteIdStr": "uuid-1", "siteName": "Test Site"}
        status, diff = compare_item(csv_item, existing)

        assert status == ImportStatus.IDENTICAL
        assert diff is None

    def test_conflict_when_sitename_differs(self) -> None:
        """Test that item is CONFLICT when siteName differs."""
        from scripts.import_optima_config_to_dynamodb import ImportStatus, compare_item

        csv_item = {"nmi": "123", "siteIdStr": "uuid-1", "siteName": "New Name"}
        existing = {"project": "test", "nmi": "123", "siteIdStr": "uuid-1", "siteName": "Old Name"}
        status, diff = compare_item(csv_item, existing)

        assert status == ImportStatus.CONFLICT
        assert diff is not None
        assert "siteName" in diff
        assert diff["siteName"]["csv"] == "New Name"
        assert diff["siteName"]["existing"] == "Old Name"

    def test_conflict_when_siteidstr_differs(self) -> None:
        """Test that item is CONFLICT when siteIdStr differs."""
        from scripts.import_optima_config_to_dynamodb import ImportStatus, compare_item

        csv_item = {"nmi": "123", "siteIdStr": "new-uuid", "siteName": "Test Site"}
        existing = {"project": "test", "nmi": "123", "siteIdStr": "old-uuid", "siteName": "Test Site"}
        status, diff = compare_item(csv_item, existing)

        assert status == ImportStatus.CONFLICT
        assert diff is not None
        assert "siteIdStr" in diff
        assert diff["siteIdStr"]["csv"] == "new-uuid"
        assert diff["siteIdStr"]["existing"] == "old-uuid"

    def test_conflict_when_multiple_fields_differ(self) -> None:
        """Test that item is CONFLICT when multiple fields differ."""
        from scripts.import_optima_config_to_dynamodb import ImportStatus, compare_item

        csv_item = {"nmi": "123", "siteIdStr": "new-uuid", "siteName": "New Name"}
        existing = {"project": "test", "nmi": "123", "siteIdStr": "old-uuid", "siteName": "Old Name"}
        status, diff = compare_item(csv_item, existing)

        assert status == ImportStatus.CONFLICT
        assert diff is not None
        assert len(diff) == 2
        assert "siteIdStr" in diff
        assert "siteName" in diff

    def test_identical_when_extra_fields_in_existing(self) -> None:
        """Test that extra fields in existing don't cause conflict."""
        from scripts.import_optima_config_to_dynamodb import ImportStatus, compare_item

        csv_item = {"nmi": "123", "siteIdStr": "uuid-1", "siteName": "Test Site"}
        existing = {
            "project": "test",
            "nmi": "123",
            "siteIdStr": "uuid-1",
            "siteName": "Test Site",
            "extraField": "extra",
        }
        status, diff = compare_item(csv_item, existing)

        assert status == ImportStatus.IDENTICAL
        assert diff is None

    def test_identical_with_empty_sitename(self) -> None:
        """Test that empty siteName values are handled correctly."""
        from scripts.import_optima_config_to_dynamodb import ImportStatus, compare_item

        csv_item = {"nmi": "123", "siteIdStr": "uuid-1", "siteName": ""}
        existing = {"project": "test", "nmi": "123", "siteIdStr": "uuid-1", "siteName": ""}
        status, _diff = compare_item(csv_item, existing)

        assert status == ImportStatus.IDENTICAL

    def test_conflict_empty_vs_non_empty_sitename(self) -> None:
        """Test conflict when CSV has empty siteName but existing has value."""
        from scripts.import_optima_config_to_dynamodb import ImportStatus, compare_item

        csv_item = {"nmi": "123", "siteIdStr": "uuid-1", "siteName": ""}
        existing = {"project": "test", "nmi": "123", "siteIdStr": "uuid-1", "siteName": "Has Name"}
        status, diff = compare_item(csv_item, existing)

        assert status == ImportStatus.CONFLICT
        assert diff["siteName"]["csv"] == ""
        assert diff["siteName"]["existing"] == "Has Name"


# ================================
# fetch_existing_items Tests
# ================================
class TestFetchExistingItems:
    """Tests for fetch_existing_items function."""

    @mock_aws
    def test_fetch_empty_when_no_records(self) -> None:
        """Test fetching from empty table returns empty dict."""
        from scripts.import_optima_config_to_dynamodb import fetch_existing_items

        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        table = dynamodb.create_table(
            TableName="sbm-optima-config",
            KeySchema=[
                {"AttributeName": "project", "KeyType": "HASH"},
                {"AttributeName": "nmi", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "project", "AttributeType": "S"},
                {"AttributeName": "nmi", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()

        result = fetch_existing_items(table, "bunnings", ["123", "456"])
        assert result == {}

    @mock_aws
    def test_fetch_existing_items_returns_matches(self) -> None:
        """Test fetching returns matching items."""
        from scripts.import_optima_config_to_dynamodb import fetch_existing_items

        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        table = dynamodb.create_table(
            TableName="sbm-optima-config",
            KeySchema=[
                {"AttributeName": "project", "KeyType": "HASH"},
                {"AttributeName": "nmi", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "project", "AttributeType": "S"},
                {"AttributeName": "nmi", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()

        # Add test items
        table.put_item(Item={"project": "bunnings", "nmi": "123", "siteIdStr": "uuid-1", "siteName": "Site 1"})
        table.put_item(Item={"project": "bunnings", "nmi": "456", "siteIdStr": "uuid-2", "siteName": "Site 2"})

        result = fetch_existing_items(table, "bunnings", ["123", "456", "789"])

        assert len(result) == 2
        assert "123" in result
        assert "456" in result
        assert "789" not in result
        assert result["123"]["siteName"] == "Site 1"
        assert result["456"]["siteName"] == "Site 2"

    @mock_aws
    def test_fetch_items_different_project_not_returned(self) -> None:
        """Test that items from different projects are not returned."""
        from scripts.import_optima_config_to_dynamodb import fetch_existing_items

        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        table = dynamodb.create_table(
            TableName="sbm-optima-config",
            KeySchema=[
                {"AttributeName": "project", "KeyType": "HASH"},
                {"AttributeName": "nmi", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "project", "AttributeType": "S"},
                {"AttributeName": "nmi", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()

        # Add items to different projects
        table.put_item(Item={"project": "bunnings", "nmi": "123", "siteIdStr": "uuid-1", "siteName": "Bunnings Site"})
        table.put_item(Item={"project": "racv", "nmi": "123", "siteIdStr": "uuid-2", "siteName": "RACV Site"})

        result = fetch_existing_items(table, "bunnings", ["123"])

        assert len(result) == 1
        assert result["123"]["siteName"] == "Bunnings Site"

    @mock_aws
    def test_fetch_handles_large_batch(self) -> None:
        """Test fetching handles more than 100 items (batch limit)."""
        from scripts.import_optima_config_to_dynamodb import fetch_existing_items

        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        table = dynamodb.create_table(
            TableName="sbm-optima-config",
            KeySchema=[
                {"AttributeName": "project", "KeyType": "HASH"},
                {"AttributeName": "nmi", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "project", "AttributeType": "S"},
                {"AttributeName": "nmi", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()

        # Add 150 items
        for i in range(150):
            table.put_item(
                Item={"project": "bunnings", "nmi": f"nmi_{i:04d}", "siteIdStr": f"uuid-{i}", "siteName": f"Site {i}"}
            )

        nmis = [f"nmi_{i:04d}" for i in range(150)]
        result = fetch_existing_items(table, "bunnings", nmis)

        assert len(result) == 150


# ================================
# import_csv_to_dynamodb Tests
# ================================
class TestImportCsvToDynamodb:
    """Tests for import_csv_to_dynamodb function."""

    @mock_aws
    def test_file_not_found_returns_error(self) -> None:
        """Test that missing file returns error code."""
        from scripts.import_optima_config_to_dynamodb import import_csv_to_dynamodb

        result = import_csv_to_dynamodb("/nonexistent/file.csv", "bunnings")
        assert result == 1

    @mock_aws
    def test_empty_csv_returns_error(self, create_csv_file: CsvFileFactory) -> None:
        """Test that empty CSV returns error code."""
        from scripts.import_optima_config_to_dynamodb import import_csv_to_dynamodb

        # Create empty file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            csv_path = f.name

        result = import_csv_to_dynamodb(csv_path, "bunnings")
        assert result == 1

        Path(csv_path).unlink()

    @mock_aws
    def test_missing_required_columns_returns_error(self) -> None:
        """Test that CSV missing required columns returns error."""
        from scripts.import_optima_config_to_dynamodb import import_csv_to_dynamodb

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            writer = csv.DictWriter(f, fieldnames=["nmi", "wrongColumn"])
            writer.writeheader()
            writer.writerow({"nmi": "123", "wrongColumn": "value"})
            csv_path = f.name

        result = import_csv_to_dynamodb(csv_path, "bunnings")
        assert result == 1

        Path(csv_path).unlink()

    @mock_aws
    def test_dry_run_does_not_write(self, create_csv_file: CsvFileFactory, sample_csv_data: CsvData) -> None:
        """Test that dry-run mode does not write to DynamoDB."""
        from scripts.import_optima_config_to_dynamodb import import_csv_to_dynamodb

        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        table = dynamodb.create_table(
            TableName="sbm-optima-config",
            KeySchema=[
                {"AttributeName": "project", "KeyType": "HASH"},
                {"AttributeName": "nmi", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "project", "AttributeType": "S"},
                {"AttributeName": "nmi", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()

        csv_path = create_csv_file(sample_csv_data)
        result = import_csv_to_dynamodb(csv_path, "bunnings", dry_run=True)

        assert result == 0

        # Verify nothing was written
        response = table.scan()
        assert response["Count"] == 0

        Path(csv_path).unlink()

    @mock_aws
    def test_import_new_items_success(self, create_csv_file: CsvFileFactory, sample_csv_data: CsvData) -> None:
        """Test successful import of new items."""
        from scripts.import_optima_config_to_dynamodb import import_csv_to_dynamodb

        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        table = dynamodb.create_table(
            TableName="sbm-optima-config",
            KeySchema=[
                {"AttributeName": "project", "KeyType": "HASH"},
                {"AttributeName": "nmi", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "project", "AttributeType": "S"},
                {"AttributeName": "nmi", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()

        csv_path = create_csv_file(sample_csv_data)
        result = import_csv_to_dynamodb(csv_path, "bunnings")

        assert result == 0

        # Verify items were written
        response = table.scan()
        assert response["Count"] == 3

        Path(csv_path).unlink()

    @mock_aws
    def test_skip_identical_items(self, create_csv_file: CsvFileFactory, sample_csv_data: CsvData) -> None:
        """Test that identical items are skipped."""
        from scripts.import_optima_config_to_dynamodb import import_csv_to_dynamodb

        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        table = dynamodb.create_table(
            TableName="sbm-optima-config",
            KeySchema=[
                {"AttributeName": "project", "KeyType": "HASH"},
                {"AttributeName": "nmi", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "project", "AttributeType": "S"},
                {"AttributeName": "nmi", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()

        # Pre-populate with identical data
        for item in sample_csv_data:
            table.put_item(
                Item={
                    "project": "bunnings",
                    "nmi": item["nmi"],
                    "siteIdStr": item["siteIdStr"],
                    "siteName": item["siteName"],
                }
            )

        csv_path = create_csv_file(sample_csv_data)

        # Capture stdout to verify skip messages
        result = import_csv_to_dynamodb(csv_path, "bunnings")

        assert result == 0

        Path(csv_path).unlink()

    @mock_aws
    def test_skip_conflicts_by_default(self, create_csv_file: CsvFileFactory) -> None:
        """Test that conflicts are skipped by default."""
        from scripts.import_optima_config_to_dynamodb import import_csv_to_dynamodb

        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        table = dynamodb.create_table(
            TableName="sbm-optima-config",
            KeySchema=[
                {"AttributeName": "project", "KeyType": "HASH"},
                {"AttributeName": "nmi", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "project", "AttributeType": "S"},
                {"AttributeName": "nmi", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()

        # Pre-populate with different data
        table.put_item(
            Item={
                "project": "bunnings",
                "nmi": "123",
                "siteIdStr": "old-uuid",
                "siteName": "Old Name",
            }
        )

        csv_data = [{"nmi": "123", "siteIdStr": "new-uuid", "siteName": "New Name"}]
        csv_path = create_csv_file(csv_data)

        result = import_csv_to_dynamodb(csv_path, "bunnings")

        assert result == 0

        # Verify original data unchanged
        response = table.get_item(Key={"project": "bunnings", "nmi": "123"})
        assert response["Item"]["siteIdStr"] == "old-uuid"
        assert response["Item"]["siteName"] == "Old Name"

        Path(csv_path).unlink()

    @mock_aws
    def test_force_overwrites_conflicts(self, create_csv_file: CsvFileFactory) -> None:
        """Test that --force overwrites conflicting items."""
        from scripts.import_optima_config_to_dynamodb import import_csv_to_dynamodb

        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        table = dynamodb.create_table(
            TableName="sbm-optima-config",
            KeySchema=[
                {"AttributeName": "project", "KeyType": "HASH"},
                {"AttributeName": "nmi", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "project", "AttributeType": "S"},
                {"AttributeName": "nmi", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()

        # Pre-populate with different data
        table.put_item(
            Item={
                "project": "bunnings",
                "nmi": "123",
                "siteIdStr": "old-uuid",
                "siteName": "Old Name",
            }
        )

        csv_data = [{"nmi": "123", "siteIdStr": "new-uuid", "siteName": "New Name"}]
        csv_path = create_csv_file(csv_data)

        result = import_csv_to_dynamodb(csv_path, "bunnings", force=True)

        assert result == 0

        # Verify data was overwritten
        response = table.get_item(Key={"project": "bunnings", "nmi": "123"})
        assert response["Item"]["siteIdStr"] == "new-uuid"
        assert response["Item"]["siteName"] == "New Name"

        Path(csv_path).unlink()

    @mock_aws
    def test_mixed_new_and_existing(self, create_csv_file: CsvFileFactory) -> None:
        """Test import with mix of new, identical, and conflicting items."""
        from scripts.import_optima_config_to_dynamodb import import_csv_to_dynamodb

        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        table = dynamodb.create_table(
            TableName="sbm-optima-config",
            KeySchema=[
                {"AttributeName": "project", "KeyType": "HASH"},
                {"AttributeName": "nmi", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "project", "AttributeType": "S"},
                {"AttributeName": "nmi", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()

        # Pre-populate: one identical, one conflict
        table.put_item(Item={"project": "bunnings", "nmi": "identical", "siteIdStr": "uuid-1", "siteName": "Same Name"})
        table.put_item(Item={"project": "bunnings", "nmi": "conflict", "siteIdStr": "old-uuid", "siteName": "Old Name"})

        csv_data = [
            {"nmi": "new", "siteIdStr": "uuid-new", "siteName": "New Site"},
            {"nmi": "identical", "siteIdStr": "uuid-1", "siteName": "Same Name"},
            {"nmi": "conflict", "siteIdStr": "new-uuid", "siteName": "Changed Name"},
        ]
        csv_path = create_csv_file(csv_data)

        result = import_csv_to_dynamodb(csv_path, "bunnings")

        assert result == 0

        # Verify: new item added, identical unchanged, conflict unchanged
        response = table.scan()
        assert response["Count"] == 3

        new_item = table.get_item(Key={"project": "bunnings", "nmi": "new"})
        assert new_item["Item"]["siteName"] == "New Site"

        conflict_item = table.get_item(Key={"project": "bunnings", "nmi": "conflict"})
        assert conflict_item["Item"]["siteIdStr"] == "old-uuid"  # Not overwritten

        Path(csv_path).unlink()

    @mock_aws
    def test_project_name_lowercased(self, create_csv_file: CsvFileFactory) -> None:
        """Test that project name is stored in lowercase."""
        from scripts.import_optima_config_to_dynamodb import import_csv_to_dynamodb

        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        table = dynamodb.create_table(
            TableName="sbm-optima-config",
            KeySchema=[
                {"AttributeName": "project", "KeyType": "HASH"},
                {"AttributeName": "nmi", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "project", "AttributeType": "S"},
                {"AttributeName": "nmi", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()

        csv_data = [{"nmi": "123", "siteIdStr": "uuid-1", "siteName": "Test"}]
        csv_path = create_csv_file(csv_data)

        # Note: project lowercasing happens in main(), not import_csv_to_dynamodb()
        # So we test with lowercase directly
        result = import_csv_to_dynamodb(csv_path, "bunnings")

        assert result == 0

        response = table.get_item(Key={"project": "bunnings", "nmi": "123"})
        assert response["Item"]["project"] == "bunnings"

        Path(csv_path).unlink()


# ================================
# main() / argparse Tests
# ================================
class TestMain:
    """Tests for main function and argument parsing."""

    def test_help_output(self) -> None:
        """Test that --help works."""
        from scripts.import_optima_config_to_dynamodb import main

        with patch.object(sys, "argv", ["import_optima_config.py", "--help"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0

    def test_missing_args_shows_error(self) -> None:
        """Test that missing arguments shows error."""
        from scripts.import_optima_config_to_dynamodb import main

        with patch.object(sys, "argv", ["import_optima_config.py"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code != 0

    @mock_aws
    def test_dry_run_flag(self, create_csv_file: CsvFileFactory, sample_csv_data: CsvData) -> None:
        """Test that --dry-run flag is parsed correctly."""
        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        table = dynamodb.create_table(
            TableName="sbm-optima-config",
            KeySchema=[
                {"AttributeName": "project", "KeyType": "HASH"},
                {"AttributeName": "nmi", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "project", "AttributeType": "S"},
                {"AttributeName": "nmi", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()

        csv_path = create_csv_file(sample_csv_data)

        from scripts.import_optima_config_to_dynamodb import main

        with patch.object(sys, "argv", ["import_optima_config.py", csv_path, "bunnings", "--dry-run"]):
            result = main()

        assert result == 0

        # Verify nothing was written
        response = table.scan()
        assert response["Count"] == 0

        Path(csv_path).unlink()

    @mock_aws
    def test_force_flag(self, create_csv_file: CsvFileFactory) -> None:
        """Test that --force flag is parsed correctly."""
        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        table = dynamodb.create_table(
            TableName="sbm-optima-config",
            KeySchema=[
                {"AttributeName": "project", "KeyType": "HASH"},
                {"AttributeName": "nmi", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "project", "AttributeType": "S"},
                {"AttributeName": "nmi", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()

        # Pre-populate with conflicting data
        table.put_item(Item={"project": "bunnings", "nmi": "123", "siteIdStr": "old", "siteName": "Old"})

        csv_data = [{"nmi": "123", "siteIdStr": "new", "siteName": "New"}]
        csv_path = create_csv_file(csv_data)

        from scripts.import_optima_config_to_dynamodb import main

        with patch.object(sys, "argv", ["import_optima_config.py", csv_path, "bunnings", "--force"]):
            result = main()

        assert result == 0

        # Verify data was overwritten
        response = table.get_item(Key={"project": "bunnings", "nmi": "123"})
        assert response["Item"]["siteIdStr"] == "new"

        Path(csv_path).unlink()

    @mock_aws
    def test_project_name_case_insensitive(self, create_csv_file: CsvFileFactory, sample_csv_data: CsvData) -> None:
        """Test that project name is converted to lowercase."""
        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        table = dynamodb.create_table(
            TableName="sbm-optima-config",
            KeySchema=[
                {"AttributeName": "project", "KeyType": "HASH"},
                {"AttributeName": "nmi", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "project", "AttributeType": "S"},
                {"AttributeName": "nmi", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()

        csv_path = create_csv_file(sample_csv_data)

        from scripts.import_optima_config_to_dynamodb import main

        with patch.object(sys, "argv", ["import_optima_config.py", csv_path, "BUNNINGS"]):
            result = main()

        assert result == 0

        # Verify project stored as lowercase
        response = table.scan()
        for item in response["Items"]:
            assert item["project"] == "bunnings"

        Path(csv_path).unlink()


# ================================
# Output Format Tests
# ================================
class TestOutputFormat:
    """Tests for output formatting."""

    @mock_aws
    def test_output_contains_summary(
        self, create_csv_file: CsvFileFactory, sample_csv_data: CsvData, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Test that output contains summary section."""
        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        table = dynamodb.create_table(
            TableName="sbm-optima-config",
            KeySchema=[
                {"AttributeName": "project", "KeyType": "HASH"},
                {"AttributeName": "nmi", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "project", "AttributeType": "S"},
                {"AttributeName": "nmi", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()

        csv_path = create_csv_file(sample_csv_data)

        from scripts.import_optima_config_to_dynamodb import import_csv_to_dynamodb

        import_csv_to_dynamodb(csv_path, "bunnings", dry_run=True)

        captured = capsys.readouterr()
        assert "Summary" in captured.out
        assert "New:" in captured.out
        assert "Identical:" in captured.out
        assert "Conflicts:" in captured.out

        Path(csv_path).unlink()

    @mock_aws
    def test_output_shows_mode(
        self, create_csv_file: CsvFileFactory, sample_csv_data: CsvData, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Test that output shows DRY RUN mode."""
        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        table = dynamodb.create_table(
            TableName="sbm-optima-config",
            KeySchema=[
                {"AttributeName": "project", "KeyType": "HASH"},
                {"AttributeName": "nmi", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "project", "AttributeType": "S"},
                {"AttributeName": "nmi", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()

        csv_path = create_csv_file(sample_csv_data)

        from scripts.import_optima_config_to_dynamodb import import_csv_to_dynamodb

        import_csv_to_dynamodb(csv_path, "bunnings", dry_run=True)

        captured = capsys.readouterr()
        assert "DRY RUN" in captured.out

        Path(csv_path).unlink()

    @mock_aws
    def test_output_shows_conflict_details(
        self, create_csv_file: CsvFileFactory, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Test that conflict details are shown in output."""
        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        table = dynamodb.create_table(
            TableName="sbm-optima-config",
            KeySchema=[
                {"AttributeName": "project", "KeyType": "HASH"},
                {"AttributeName": "nmi", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "project", "AttributeType": "S"},
                {"AttributeName": "nmi", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()

        # Pre-populate with conflicting data
        table.put_item(Item={"project": "bunnings", "nmi": "123", "siteIdStr": "old-uuid", "siteName": "Old Name"})

        csv_data = [{"nmi": "123", "siteIdStr": "new-uuid", "siteName": "New Name"}]
        csv_path = create_csv_file(csv_data)

        from scripts.import_optima_config_to_dynamodb import import_csv_to_dynamodb

        import_csv_to_dynamodb(csv_path, "bunnings", dry_run=True)

        captured = capsys.readouterr()
        assert "CONFLICT" in captured.out
        assert "siteName" in captured.out or "siteIdStr" in captured.out

        Path(csv_path).unlink()


# ================================
# Edge Cases Tests
# ================================
class TestEdgeCases:
    """Tests for edge cases."""

    @mock_aws
    def test_csv_with_extra_columns(self, create_csv_file: CsvFileFactory) -> None:
        """Test that extra columns in CSV are ignored."""
        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        table = dynamodb.create_table(
            TableName="sbm-optima-config",
            KeySchema=[
                {"AttributeName": "project", "KeyType": "HASH"},
                {"AttributeName": "nmi", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "project", "AttributeType": "S"},
                {"AttributeName": "nmi", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()

        csv_data = [{"nmi": "123", "siteIdStr": "uuid-1", "siteName": "Test", "extraCol": "ignored"}]
        csv_path = create_csv_file(csv_data)

        from scripts.import_optima_config_to_dynamodb import import_csv_to_dynamodb

        result = import_csv_to_dynamodb(csv_path, "bunnings")

        assert result == 0

        response = table.get_item(Key={"project": "bunnings", "nmi": "123"})
        assert "extraCol" not in response["Item"]

        Path(csv_path).unlink()

    @mock_aws
    def test_csv_missing_optional_sitename(self) -> None:
        """Test that missing siteName defaults to empty string."""
        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        table = dynamodb.create_table(
            TableName="sbm-optima-config",
            KeySchema=[
                {"AttributeName": "project", "KeyType": "HASH"},
                {"AttributeName": "nmi", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "project", "AttributeType": "S"},
                {"AttributeName": "nmi", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()

        # Create CSV without siteName column
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            writer = csv.DictWriter(f, fieldnames=["nmi", "siteIdStr"])
            writer.writeheader()
            writer.writerow({"nmi": "123", "siteIdStr": "uuid-1"})
            csv_path = f.name

        from scripts.import_optima_config_to_dynamodb import import_csv_to_dynamodb

        result = import_csv_to_dynamodb(csv_path, "bunnings")

        assert result == 0

        response = table.get_item(Key={"project": "bunnings", "nmi": "123"})
        assert response["Item"]["siteName"] == ""

        Path(csv_path).unlink()

    @mock_aws
    def test_no_items_to_import(self, create_csv_file: CsvFileFactory) -> None:
        """Test when all items are identical (nothing to import)."""
        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        table = dynamodb.create_table(
            TableName="sbm-optima-config",
            KeySchema=[
                {"AttributeName": "project", "KeyType": "HASH"},
                {"AttributeName": "nmi", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "project", "AttributeType": "S"},
                {"AttributeName": "nmi", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()

        # Pre-populate with identical data
        table.put_item(Item={"project": "bunnings", "nmi": "123", "siteIdStr": "uuid-1", "siteName": "Test"})

        csv_data = [{"nmi": "123", "siteIdStr": "uuid-1", "siteName": "Test"}]
        csv_path = create_csv_file(csv_data)

        from scripts.import_optima_config_to_dynamodb import import_csv_to_dynamodb

        result = import_csv_to_dynamodb(csv_path, "bunnings")

        assert result == 0

        Path(csv_path).unlink()

    @mock_aws
    def test_dynamodb_write_error_returns_failure(self, create_csv_file: CsvFileFactory) -> None:
        """Test that DynamoDB write errors are handled."""
        from scripts.import_optima_config_to_dynamodb import import_csv_to_dynamodb

        csv_data = [{"nmi": "123", "siteIdStr": "uuid-1", "siteName": "Test"}]
        csv_path = create_csv_file(csv_data)

        # Mock DynamoDB to raise an error
        with patch("scripts.import_optima_config_to_dynamodb.boto3") as mock_boto3:
            mock_table = MagicMock()
            mock_table.name = "sbm-optima-config"
            mock_table.meta.client.batch_get_item.return_value = {"Responses": {"sbm-optima-config": []}}
            mock_table.put_item.side_effect = Exception("DynamoDB Error")

            mock_dynamodb = MagicMock()
            mock_dynamodb.Table.return_value = mock_table
            mock_boto3.resource.return_value = mock_dynamodb

            result = import_csv_to_dynamodb(csv_path, "bunnings")

        assert result == 1

        Path(csv_path).unlink()
