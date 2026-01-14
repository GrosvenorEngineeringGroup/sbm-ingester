"""Unit tests for migrate_archives_to_weekly.py script."""

import os
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import boto3
from moto import mock_aws

# Add scripts to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))


class TestGetIsoWeek:
    """Tests for get_iso_week function."""

    def test_get_iso_week_basic(self) -> None:
        """Test ISO week format for a known date."""
        from migrate_archives_to_weekly import get_iso_week

        dt = datetime(2025, 7, 15)
        assert get_iso_week(dt) == "2025-W29"

    def test_get_iso_week_year_boundary(self) -> None:
        """Test ISO week at year boundary."""
        from migrate_archives_to_weekly import get_iso_week

        # Dec 31, 2025 is in ISO week 1 of 2026
        dt = datetime(2025, 12, 31)
        assert get_iso_week(dt) == "2026-W01"

    def test_get_iso_week_padding(self) -> None:
        """Test ISO week number is zero-padded."""
        from migrate_archives_to_weekly import get_iso_week

        dt = datetime(2025, 1, 6)  # Week 2
        result = get_iso_week(dt)
        assert result == "2025-W02"
        assert len(result.split("-W")[1]) == 2


class TestExtractDateFromFilename:
    """Tests for extract_date_from_filename function."""

    def test_extract_timestamp_suffix(self) -> None:
        """Test extraction from standard timestamp suffix."""
        from migrate_archives_to_weekly import extract_date_from_filename

        result = extract_date_from_filename("meter_data_2025073018315281.csv")
        assert result is not None
        assert result.year == 2025
        assert result.month == 7
        assert result.day == 30

    def test_extract_date_hyphenated(self) -> None:
        """Test extraction from hyphenated date format."""
        from migrate_archives_to_weekly import extract_date_from_filename

        result = extract_date_from_filename("report_2025-08-15_export.csv")
        assert result is not None
        assert result.year == 2025
        assert result.month == 8
        assert result.day == 15

    def test_extract_no_date(self) -> None:
        """Test returns None when no date found."""
        from migrate_archives_to_weekly import extract_date_from_filename

        result = extract_date_from_filename("random_file.csv")
        assert result is None

    def test_extract_invalid_date(self) -> None:
        """Test returns None for invalid dates."""
        from migrate_archives_to_weekly import extract_date_from_filename

        # Month 13 is invalid
        result = extract_date_from_filename("file_2025130115000000.csv")
        assert result is None

    def test_extract_multiple_date_patterns(self) -> None:
        """Test extraction prioritizes timestamp suffix."""
        from migrate_archives_to_weekly import extract_date_from_filename

        # Should extract from timestamp suffix (2025-07-30), not hyphenated (2025-06-01)
        result = extract_date_from_filename("2025-06-01_meter_2025073018315281.csv")
        assert result is not None
        assert result.month == 7
        assert result.day == 30

    def test_extract_short_timestamp(self) -> None:
        """Test extraction with shorter timestamp."""
        from migrate_archives_to_weekly import extract_date_from_filename

        # 14-digit timestamp
        result = extract_date_from_filename("data_20250730183152.csv")
        assert result is not None
        assert result.year == 2025
        assert result.month == 7
        assert result.day == 30


class TestGetWeekFromMonthDir:
    """Tests for get_week_from_month_dir function."""

    def test_valid_month_dir(self) -> None:
        """Test extraction from valid month directory."""
        from migrate_archives_to_weekly import get_week_from_month_dir

        result = get_week_from_month_dir("2025-07")
        assert result is not None
        # July 15, 2025 is in Week 29
        assert result == "2025-W29"

    def test_invalid_month_dir(self) -> None:
        """Test returns None for invalid format."""
        from migrate_archives_to_weekly import get_week_from_month_dir

        result = get_week_from_month_dir("2025-W32")
        assert result is None

    def test_year_end_month(self) -> None:
        """Test December directory."""
        from migrate_archives_to_weekly import get_week_from_month_dir

        result = get_week_from_month_dir("2025-12")
        assert result is not None
        # Dec 15, 2025 is in Week 51
        assert result == "2025-W51"


class TestMigrateMonthlyToWeekly:
    """Tests for migrate_monthly_to_weekly function."""

    @mock_aws
    def test_migrate_files_from_monthly_to_weekly(self) -> None:
        """Test migration of files from monthly to weekly directories."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        # Create a file in monthly directory with timestamp in filename
        s3.put_object(
            Bucket="sbm-file-ingester",
            Key="newP/archived/2025-07/meter_2025073018315281.csv",
            Body=b"test data",
        )

        with patch("migrate_archives_to_weekly.get_s3_client", return_value=s3):
            from migrate_archives_to_weekly import migrate_monthly_to_weekly

            stats = migrate_monthly_to_weekly(dry_run=False)

            assert stats["total"] == 1
            assert stats["migrated"] == 1
            assert stats["errors"] == 0
            assert stats["skipped"] == 0

            # Verify file was moved to weekly directory
            # July 30, 2025 is in Week 31
            weekly_objects = s3.list_objects_v2(Bucket="sbm-file-ingester", Prefix="newP/archived/2025-W31/")
            assert weekly_objects["KeyCount"] == 1

            # Verify original is gone
            monthly_objects = s3.list_objects_v2(Bucket="sbm-file-ingester", Prefix="newP/archived/2025-07/")
            assert monthly_objects.get("KeyCount", 0) == 0

    @mock_aws
    def test_dry_run_does_not_move_files(self) -> None:
        """Test dry run mode doesn't actually move files but counts them."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        s3.put_object(
            Bucket="sbm-file-ingester",
            Key="newP/archived/2025-08/meter_2025081518315281.csv",
            Body=b"test data",
        )

        with patch("migrate_archives_to_weekly.get_s3_client", return_value=s3):
            from migrate_archives_to_weekly import migrate_monthly_to_weekly

            stats = migrate_monthly_to_weekly(dry_run=True)

            assert stats["total"] == 1
            assert stats["migrated"] == 1  # Counted as migrated in dry run (would be migrated)

            # Verify file still in original location (not actually moved)
            monthly_objects = s3.list_objects_v2(Bucket="sbm-file-ingester", Prefix="newP/archived/2025-08/")
            assert monthly_objects["KeyCount"] == 1

    @mock_aws
    def test_skip_already_weekly_format(self) -> None:
        """Test files already in weekly format are skipped."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        # File already in weekly format
        s3.put_object(
            Bucket="sbm-file-ingester",
            Key="newP/archived/2025-W31/meter_data.csv",
            Body=b"test data",
        )

        with patch("migrate_archives_to_weekly.get_s3_client", return_value=s3):
            from migrate_archives_to_weekly import migrate_monthly_to_weekly

            stats = migrate_monthly_to_weekly(dry_run=False)

            assert stats["total"] == 0  # Not counted as it's already weekly

    @mock_aws
    def test_fallback_to_month_directory_date(self) -> None:
        """Test fallback to month directory when filename has no date."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        # File without date in filename
        s3.put_object(
            Bucket="sbm-file-ingester",
            Key="newP/archived/2025-07/random_file.csv",
            Body=b"test data",
        )

        with patch("migrate_archives_to_weekly.get_s3_client", return_value=s3):
            from migrate_archives_to_weekly import migrate_monthly_to_weekly

            stats = migrate_monthly_to_weekly(dry_run=False)

            assert stats["total"] == 1
            assert stats["migrated"] == 1

            # Should use July 15 (mid-month) -> Week 29
            weekly_objects = s3.list_objects_v2(Bucket="sbm-file-ingester", Prefix="newP/archived/2025-W29/")
            assert weekly_objects["KeyCount"] == 1

    @mock_aws
    def test_process_multiple_prefixes(self) -> None:
        """Test migration processes all three prefixes."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        # Files in each prefix
        s3.put_object(
            Bucket="sbm-file-ingester",
            Key="newP/archived/2025-07/file1_2025070118315281.csv",
            Body=b"test",
        )
        s3.put_object(
            Bucket="sbm-file-ingester",
            Key="newIrrevFiles/archived/2025-08/file2_2025081518315281.csv",
            Body=b"test",
        )
        s3.put_object(
            Bucket="sbm-file-ingester",
            Key="newParseErr/archived/2025-09/file3_2025091518315281.csv",
            Body=b"test",
        )

        with patch("migrate_archives_to_weekly.get_s3_client", return_value=s3):
            from migrate_archives_to_weekly import migrate_monthly_to_weekly

            stats = migrate_monthly_to_weekly(dry_run=False)

            assert stats["total"] == 3
            assert stats["migrated"] == 3

    @mock_aws
    def test_handle_s3_error(self) -> None:
        """Test error handling when S3 operation fails."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        s3.put_object(
            Bucket="sbm-file-ingester",
            Key="newP/archived/2025-07/file_2025070118315281.csv",
            Body=b"test",
        )

        # Create a mock S3 client that fails on copy
        mock_s3 = MagicMock()
        mock_s3.get_paginator.return_value.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "newP/archived/2025-07/file_2025070118315281.csv"},
                ]
            }
        ]
        mock_s3.copy_object.side_effect = Exception("Access Denied")

        with patch("migrate_archives_to_weekly.get_s3_client", return_value=mock_s3):
            from migrate_archives_to_weekly import migrate_monthly_to_weekly

            stats = migrate_monthly_to_weekly(dry_run=False)

            assert stats["total"] == 1
            assert stats["errors"] == 1
            assert stats["migrated"] == 0


class TestConstants:
    """Tests for module constants."""

    def test_bucket_name(self) -> None:
        """Test BUCKET_NAME constant."""
        from migrate_archives_to_weekly import BUCKET_NAME

        assert BUCKET_NAME == "sbm-file-ingester"

    def test_prefixes(self) -> None:
        """Test PREFIXES constant contains archived paths."""
        from migrate_archives_to_weekly import PREFIXES

        assert "newP/archived/" in PREFIXES
        assert "newIrrevFiles/archived/" in PREFIXES
        assert "newParseErr/archived/" in PREFIXES
        assert len(PREFIXES) == 3
