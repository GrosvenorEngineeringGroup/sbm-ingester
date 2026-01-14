"""Tests for scripts/process_nem12_locally.py - Local NEM12 file processor."""

import json
import sys
from collections.abc import Generator
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws
from mypy_boto3_s3 import S3Client

# Add scripts to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))


# ==================== Fixtures ====================


@pytest.fixture
def aws_credentials_local() -> None:
    """Mock AWS credentials for moto."""
    import os

    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"


@pytest.fixture
def mock_s3_local(aws_credentials_local: None) -> Generator[S3Client]:
    """Create mock S3 service with required buckets and mappings."""
    with mock_aws():
        s3 = boto3.client("s3", region_name="ap-southeast-2")

        # Create required buckets
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )
        s3.create_bucket(
            Bucket="hudibucketsrc",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        # Upload sample mappings
        mappings = {
            "VABD000163-E1": "neptune-id-001",
            "VABD000163-Q1": "neptune-id-002",
            "NEM1234567890-E1": "neptune-id-003",
            "NEM1234567890-B1": "neptune-id-004",
        }
        s3.put_object(
            Bucket="sbm-file-ingester",
            Key="nem12_mappings.json",
            Body=json.dumps(mappings),
        )

        yield s3


@pytest.fixture
def nem12_sample_path() -> str:
    """Return path to NEM12 sample file."""
    return str(Path(__file__).parent / "fixtures" / "nem12_sample.csv")


@pytest.fixture
def nem12_multiple_meters_path() -> str:
    """Return path to NEM12 multiple meters sample file."""
    return str(Path(__file__).parent / "fixtures" / "nem12_multiple_meters.csv")


# ==================== Tests for load_nem12_mappings ====================


class TestLoadNem12Mappings:
    """Tests for load_nem12_mappings function."""

    def test_load_mappings_success(self, mock_s3_local: S3Client) -> None:
        """Test successfully loading mappings from S3."""
        from process_nem12_locally import load_nem12_mappings

        mappings = load_nem12_mappings(mock_s3_local)

        assert len(mappings) == 4
        assert mappings["VABD000163-E1"] == "neptune-id-001"
        assert mappings["VABD000163-Q1"] == "neptune-id-002"

    def test_load_mappings_empty(self, mock_s3_local: S3Client) -> None:
        """Test loading empty mappings file."""
        from process_nem12_locally import load_nem12_mappings

        # Override with empty mappings
        mock_s3_local.put_object(
            Bucket="sbm-file-ingester",
            Key="nem12_mappings.json",
            Body=json.dumps({}),
        )

        mappings = load_nem12_mappings(mock_s3_local)

        assert mappings == {}

    def test_load_mappings_not_found(self, mock_s3_local: S3Client) -> None:
        """Test error when mappings file not found."""
        from process_nem12_locally import load_nem12_mappings

        # Delete the mappings file
        mock_s3_local.delete_object(
            Bucket="sbm-file-ingester",
            Key="nem12_mappings.json",
        )

        with pytest.raises(ClientError):
            load_nem12_mappings(mock_s3_local)


# ==================== Tests for process_nem12_file ====================


class TestProcessNem12File:
    """Tests for process_nem12_file function."""

    def test_process_file_dry_run(self, mock_s3_local: S3Client, nem12_sample_path: str) -> None:
        """Test processing NEM12 file in dry-run mode (no uploads)."""
        from process_nem12_locally import process_nem12_file

        mappings = {
            "VABD000163-E1": "neptune-id-001",
            "VABD000163-Q1": "neptune-id-002",
        }

        stats = process_nem12_file(nem12_sample_path, mappings, mock_s3_local, dry_run=True)

        # Check stats
        assert stats["nmis_total"] == 1
        assert stats["nmis_mapped"] == 1
        assert stats["nmis_unmapped"] == 0
        assert stats["monitor_points"] == 2  # E1 and Q1
        assert stats["readings_total"] > 0
        assert stats["files_uploaded"] == 0  # Dry run, no uploads
        assert stats["unmapped_nmis"] == []

    def test_process_file_actual_upload(self, mock_s3_local: S3Client, nem12_sample_path: str) -> None:
        """Test processing NEM12 file with actual S3 uploads."""
        from process_nem12_locally import process_nem12_file

        mappings = {
            "VABD000163-E1": "neptune-id-001",
            "VABD000163-Q1": "neptune-id-002",
        }

        stats = process_nem12_file(nem12_sample_path, mappings, mock_s3_local, dry_run=False)

        # Check stats
        assert stats["nmis_total"] == 1
        assert stats["nmis_mapped"] == 1
        assert stats["files_uploaded"] == 2  # E1 and Q1

        # Verify files were uploaded to S3
        response = mock_s3_local.list_objects_v2(Bucket="hudibucketsrc", Prefix="sensorDataFiles/")
        assert "Contents" in response
        assert len(response["Contents"]) == 2

        # Verify file content structure
        for obj in response["Contents"]:
            file_content = (
                mock_s3_local.get_object(Bucket="hudibucketsrc", Key=obj["Key"])["Body"].read().decode("utf-8")
            )
            # Check CSV header
            assert "sensorId,ts,val,unit,its" in file_content

    def test_process_file_unmapped_nmis(self, mock_s3_local: S3Client, nem12_sample_path: str) -> None:
        """Test processing NEM12 file with unmapped NMIs."""
        from process_nem12_locally import process_nem12_file

        # Empty mappings - no NMIs will be mapped
        mappings: dict[str, str] = {}

        stats = process_nem12_file(nem12_sample_path, mappings, mock_s3_local, dry_run=True)

        # Check stats
        assert stats["nmis_total"] == 1
        assert stats["nmis_mapped"] == 0
        assert stats["nmis_unmapped"] == 1
        assert stats["files_uploaded"] == 0
        assert len(stats["unmapped_nmis"]) > 0
        # Check that unmapped NMI is recorded
        unmapped_nmi_ids = [nmi for nmi, _ in stats["unmapped_nmis"]]
        assert "VABD000163" in unmapped_nmi_ids

    def test_process_file_partial_mapping(self, mock_s3_local: S3Client, nem12_sample_path: str) -> None:
        """Test processing with only some channels mapped."""
        from process_nem12_locally import process_nem12_file

        # Only map E1, not Q1
        mappings = {
            "VABD000163-E1": "neptune-id-001",
        }

        stats = process_nem12_file(nem12_sample_path, mappings, mock_s3_local, dry_run=False)

        # E1 mapped, Q1 not mapped
        assert stats["nmis_total"] == 1
        assert stats["monitor_points"] == 1  # Only E1
        assert stats["files_uploaded"] == 1

    def test_process_file_multiple_meters(self, mock_s3_local: S3Client, nem12_multiple_meters_path: str) -> None:
        """Test processing NEM12 file with multiple meters."""
        from process_nem12_locally import process_nem12_file

        # NMIs in nem12_multiple_meters.csv: NCDE001111, NDDD001888
        mappings = {
            "NCDE001111-E1": "neptune-id-001",
            "NCDE001111-B1": "neptune-id-002",
            "NCDE001111-Q1": "neptune-id-003",
            "NDDD001888-B1": "neptune-id-004",
        }

        stats = process_nem12_file(nem12_multiple_meters_path, mappings, mock_s3_local, dry_run=False)

        # Should have processed multiple NMIs
        assert stats["nmis_total"] == 2
        assert stats["files_uploaded"] == 4  # E1, B1, Q1 for NCDE + B1 for NDDD


# ==================== Tests for main function ====================


class TestMain:
    """Tests for main function and CLI argument parsing."""

    def test_main_file_not_found(self) -> None:
        """Test main exits with error when file not found."""
        from process_nem12_locally import main

        with patch("sys.argv", ["process_nem12_locally.py", "/nonexistent/file.csv"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1

    def test_main_dry_run_flag(self, mock_s3_local: S3Client, nem12_sample_path: str) -> None:
        """Test main function with --dry-run flag."""
        from process_nem12_locally import main

        with (
            patch("sys.argv", ["process_nem12_locally.py", nem12_sample_path, "--dry-run"]),
            patch("process_nem12_locally.boto3.Session") as mock_session,
        ):
            mock_session.return_value.client.return_value = mock_s3_local

            # Should complete without error
            main()

    def test_main_argument_parsing(self) -> None:
        """Test CLI argument parsing."""
        from process_nem12_locally import main

        # Test that argparse is configured correctly
        with patch("sys.argv", ["process_nem12_locally.py", "--help"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            # --help exits with code 0
            assert exc_info.value.code == 0


# ==================== Tests for output format ====================


class TestOutputFormat:
    """Tests for output CSV format and structure."""

    def test_output_csv_columns(self, mock_s3_local: S3Client, nem12_sample_path: str) -> None:
        """Test that output CSV has correct columns."""
        from process_nem12_locally import process_nem12_file

        mappings = {"VABD000163-E1": "neptune-id-001"}

        process_nem12_file(nem12_sample_path, mappings, mock_s3_local, dry_run=False)

        # Get uploaded file
        response = mock_s3_local.list_objects_v2(Bucket="hudibucketsrc", Prefix="sensorDataFiles/")
        file_key = response["Contents"][0]["Key"]
        file_content = mock_s3_local.get_object(Bucket="hudibucketsrc", Key=file_key)["Body"].read().decode("utf-8")

        # Parse CSV and check columns
        import csv

        reader = csv.DictReader(StringIO(file_content))
        rows = list(reader)

        assert len(rows) > 0
        assert set(reader.fieldnames) == {"sensorId", "ts", "val", "unit", "its"}

        # Check data format
        first_row = rows[0]
        assert first_row["sensorId"] == "neptune-id-001"
        assert first_row["unit"] == "kwh"
        # Timestamp format: YYYY-MM-DD HH:MM:SS
        assert len(first_row["ts"]) == 19

    def test_output_file_naming(self, mock_s3_local: S3Client, nem12_sample_path: str) -> None:
        """Test that output files are named correctly."""
        from process_nem12_locally import process_nem12_file

        mappings = {"VABD000163-E1": "neptune-id-001"}

        process_nem12_file(nem12_sample_path, mappings, mock_s3_local, dry_run=False)

        # Get uploaded file
        response = mock_s3_local.list_objects_v2(Bucket="hudibucketsrc", Prefix="sensorDataFiles/")
        file_key = response["Contents"][0]["Key"]

        # Check file naming pattern: sensorDataFiles/{neptune_id}_{timestamp}.csv
        assert file_key.startswith("sensorDataFiles/neptune-id-001_")
        assert file_key.endswith(".csv")


# ==================== Tests for edge cases ====================


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_nem12_file(self, mock_s3_local: S3Client, tmp_path: Path) -> None:
        """Test handling of empty NEM12 file."""
        from process_nem12_locally import process_nem12_file

        # Create empty NEM12 file
        empty_file = tmp_path / "empty.csv"
        empty_file.write_text("100,NEM12,200405011135,MDA1,Ret1\n900\n")

        mappings = {"VABD000163-E1": "neptune-id-001"}

        stats = process_nem12_file(str(empty_file), mappings, mock_s3_local, dry_run=True)

        assert stats["nmis_total"] == 0
        assert stats["files_uploaded"] == 0

    def test_special_characters_in_neptune_id(self, mock_s3_local: S3Client, nem12_sample_path: str) -> None:
        """Test handling Neptune IDs with special characters."""
        from process_nem12_locally import process_nem12_file

        # Neptune IDs can have colons and other characters
        mappings = {"VABD000163-E1": "p:amp_sites:r:12345678-abcdef"}

        stats = process_nem12_file(nem12_sample_path, mappings, mock_s3_local, dry_run=False)

        assert stats["files_uploaded"] == 1

        # Verify the file was uploaded with correct sensorId
        response = mock_s3_local.list_objects_v2(Bucket="hudibucketsrc", Prefix="sensorDataFiles/")
        file_key = response["Contents"][0]["Key"]
        file_content = mock_s3_local.get_object(Bucket="hudibucketsrc", Key=file_key)["Body"].read().decode("utf-8")

        assert "p:amp_sites:r:12345678-abcdef" in file_content

    def test_large_number_of_readings(self, mock_s3_local: S3Client, tmp_path: Path) -> None:
        """Test processing file with many readings (performance check)."""
        from process_nem12_locally import process_nem12_file

        # Create NEM12 file with multiple days of data
        nem12_content = "100,NEM12,200405011135,MDA1,Ret1\n"
        nem12_content += "200,TESTMETER001,E1Q1,1,E1,N1,METSER123,kWh,30,\n"

        # Add 7 days of 48 readings each
        readings = ",".join(["1.0"] * 48)
        for day in range(1, 8):
            nem12_content += f"300,2024010{day},{readings},A,,,20240102120025,\n"

        nem12_content += "900\n"

        test_file = tmp_path / "large_nem12.csv"
        test_file.write_text(nem12_content)

        mappings = {"TESTMETER001-E1": "neptune-id-large"}

        stats = process_nem12_file(str(test_file), mappings, mock_s3_local, dry_run=False)

        # Should handle large number of readings
        assert stats["readings_total"] == 7 * 48  # 7 days * 48 readings
        assert stats["files_uploaded"] == 1
