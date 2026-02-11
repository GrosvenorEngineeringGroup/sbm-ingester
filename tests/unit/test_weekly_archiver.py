"""Unit tests for weekly_archiver Lambda function."""

import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import boto3
from botocore.exceptions import ClientError
from moto import mock_aws

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))


class TestGetIsoWeek:
    """Tests for get_iso_week function."""

    def test_get_iso_week_basic(self) -> None:
        """Test ISO week format for a known date."""
        from functions.weekly_archiver.app import get_iso_week

        # 2026-01-05 is Monday of Week 2
        dt = datetime(2026, 1, 5)
        assert get_iso_week(dt) == "2026-W02"

    def test_get_iso_week_first_week(self) -> None:
        """Test ISO week format for first week of year."""
        from functions.weekly_archiver.app import get_iso_week

        # 2026-01-01 is Thursday, still Week 1
        dt = datetime(2026, 1, 1)
        assert get_iso_week(dt) == "2026-W01"

    def test_get_iso_week_last_week(self) -> None:
        """Test ISO week format for last week of year."""
        from functions.weekly_archiver.app import get_iso_week

        # 2025-12-31 is Wednesday of Week 1 in 2026
        dt = datetime(2025, 12, 31)
        assert get_iso_week(dt) == "2026-W01"

    def test_get_iso_week_mid_year(self) -> None:
        """Test ISO week format for mid-year date."""
        from functions.weekly_archiver.app import get_iso_week

        # 2025-07-15 is Tuesday of Week 29
        dt = datetime(2025, 7, 15)
        assert get_iso_week(dt) == "2025-W29"

    def test_get_iso_week_single_digit_week(self) -> None:
        """Test ISO week format pads single digit weeks."""
        from functions.weekly_archiver.app import get_iso_week

        dt = datetime(2026, 1, 5)  # Week 2
        result = get_iso_week(dt)
        assert result == "2026-W02"
        assert len(result.split("-W")[1]) == 2


class TestValidateTargetWeek:
    """Tests for validate_target_week function."""

    def test_valid_week_format(self) -> None:
        """Test valid week formats are accepted."""
        from functions.weekly_archiver.app import validate_target_week

        assert validate_target_week("2026-W01") is True
        assert validate_target_week("2026-W52") is True
        assert validate_target_week("2026-W53") is True
        assert validate_target_week("2025-W29") is True

    def test_invalid_week_format_missing_zero(self) -> None:
        """Test week without leading zero is rejected."""
        from functions.weekly_archiver.app import validate_target_week

        assert validate_target_week("2026-W1") is False
        assert validate_target_week("2026-W9") is False

    def test_invalid_week_format_wrong_pattern(self) -> None:
        """Test invalid patterns are rejected."""
        from functions.weekly_archiver.app import validate_target_week

        assert validate_target_week("invalid") is False
        assert validate_target_week("2026-01") is False
        assert validate_target_week("2026W01") is False
        assert validate_target_week("W01-2026") is False

    def test_invalid_week_number(self) -> None:
        """Test invalid week numbers are rejected."""
        from functions.weekly_archiver.app import validate_target_week

        assert validate_target_week("2026-W00") is False
        assert validate_target_week("2026-W54") is False
        assert validate_target_week("2026-W99") is False


class TestArchiveResult:
    """Tests for ArchiveResult enum."""

    def test_archive_result_values(self) -> None:
        """Test ArchiveResult enum has correct values."""
        from functions.weekly_archiver.app import ArchiveResult

        assert ArchiveResult.SUCCESS.value == "success"
        assert ArchiveResult.SKIPPED.value == "skipped"
        assert ArchiveResult.ERROR.value == "error"


class TestArchiveSingleFile:
    """Tests for archive_single_file function."""

    @mock_aws
    def test_archive_single_file_success(self) -> None:
        """Test successful file archiving."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )
        s3.put_object(Bucket="sbm-file-ingester", Key="newP/test.csv", Body=b"test")

        with patch("functions.weekly_archiver.app.s3", s3):
            from functions.weekly_archiver.app import ArchiveResult, archive_single_file

            result, msg = archive_single_file("newP/test.csv", "newP/", "2026-W01")

            assert result == ArchiveResult.SUCCESS
            assert msg == "newP/test.csv"

            # Verify file moved
            archived = s3.list_objects_v2(Bucket="sbm-file-ingester", Prefix="newP/archived/2026-W01/")
            assert archived["KeyCount"] == 1

    @mock_aws
    def test_archive_single_file_skipped_no_such_key(self) -> None:
        """Test file that no longer exists returns SKIPPED."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )
        # Note: file does NOT exist

        with patch("functions.weekly_archiver.app.s3", s3):
            from functions.weekly_archiver.app import ArchiveResult, archive_single_file

            result, msg = archive_single_file("newP/nonexistent.csv", "newP/", "2026-W01")

            assert result == ArchiveResult.SKIPPED
            assert msg == "newP/nonexistent.csv"

    def test_archive_single_file_error(self) -> None:
        """Test S3 error returns ERROR result."""
        mock_s3 = MagicMock()
        error_response = {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}
        mock_s3.copy_object.side_effect = ClientError(error_response, "CopyObject")

        with patch("functions.weekly_archiver.app.s3", mock_s3):
            from functions.weekly_archiver.app import ArchiveResult, archive_single_file

            result, msg = archive_single_file("newP/test.csv", "newP/", "2026-W01")

            assert result == ArchiveResult.ERROR
            assert "newP/test.csv" in msg
            assert "AccessDenied" in msg or "Access Denied" in msg


class TestArchiveFilesForPrefix:
    """Tests for archive_files_for_prefix function."""

    @mock_aws
    def test_archive_files_matching_week(self) -> None:
        """Test that files from target week are archived."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        s3.put_object(Bucket="sbm-file-ingester", Key="newP/test_file.csv", Body=b"test")

        with (
            patch("functions.weekly_archiver.app.s3", s3),
            patch("functions.weekly_archiver.app.tracer") as mock_tracer,
        ):
            mock_tracer.capture_method = lambda f: f

            from functions.weekly_archiver.app import archive_files_for_prefix, get_iso_week

            response = s3.head_object(Bucket="sbm-file-ingester", Key="newP/test_file.csv")
            target_week = get_iso_week(response["LastModified"])

            result = archive_files_for_prefix("newP/", target_week)

            assert result["archived"] == 1
            assert result["skipped"] == 0
            assert result["errors"] == 0

            # Verify file was moved
            archived_objects = s3.list_objects_v2(Bucket="sbm-file-ingester", Prefix=f"newP/archived/{target_week}/")
            assert archived_objects["KeyCount"] == 1

    @mock_aws
    def test_skip_files_from_different_week(self) -> None:
        """Test that files from different weeks are not archived."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        s3.put_object(Bucket="sbm-file-ingester", Key="newP/test_file.csv", Body=b"test")

        with (
            patch("functions.weekly_archiver.app.s3", s3),
            patch("functions.weekly_archiver.app.tracer") as mock_tracer,
        ):
            mock_tracer.capture_method = lambda f: f

            from functions.weekly_archiver.app import archive_files_for_prefix

            # Use a week from 2020, which won't match any recently uploaded file
            result = archive_files_for_prefix("newP/", "2020-W01")

            assert result["archived"] == 0
            assert result["skipped"] == 0
            assert result["errors"] == 0

    @mock_aws
    def test_skip_already_archived_files(self) -> None:
        """Test that files already in archived/ are skipped."""
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
            Key="newP/archived/2026-W01/already_archived.csv",
            Body=b"test",
        )

        with (
            patch("functions.weekly_archiver.app.s3", s3),
            patch("functions.weekly_archiver.app.tracer") as mock_tracer,
        ):
            mock_tracer.capture_method = lambda f: f

            from functions.weekly_archiver.app import archive_files_for_prefix

            result = archive_files_for_prefix("newP/", "2026-W01")

            assert result["archived"] == 0

    @mock_aws
    def test_archive_multiple_files(self) -> None:
        """Test archiving multiple files at once."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        for i in range(5):
            s3.put_object(Bucket="sbm-file-ingester", Key=f"newP/file_{i}.csv", Body=b"test")

        with (
            patch("functions.weekly_archiver.app.s3", s3),
            patch("functions.weekly_archiver.app.tracer") as mock_tracer,
        ):
            mock_tracer.capture_method = lambda f: f

            from functions.weekly_archiver.app import archive_files_for_prefix, get_iso_week

            response = s3.head_object(Bucket="sbm-file-ingester", Key="newP/file_0.csv")
            target_week = get_iso_week(response["LastModified"])

            result = archive_files_for_prefix("newP/", target_week)

            assert result["archived"] == 5
            assert result["skipped"] == 0
            assert result["errors"] == 0


class TestLambdaHandler:
    """Tests for lambda_handler function."""

    @mock_aws
    def test_lambda_handler_returns_success(self) -> None:
        """Test that lambda_handler returns success response."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"
        os.environ["POWERTOOLS_SERVICE_NAME"] = "weekly-archiver"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        event: dict = {}
        mock_context = MagicMock()
        mock_context.function_name = "sbm-weekly-archiver"
        mock_context.memory_limit_in_mb = 1024
        mock_context.invoked_function_arn = "arn:aws:lambda:ap-southeast-2:123456789012:function:sbm-weekly-archiver"
        mock_context.aws_request_id = "test-request-id"

        with (
            patch("functions.weekly_archiver.app.s3", s3),
            patch("functions.weekly_archiver.app.tracer") as mock_tracer,
            patch("functions.weekly_archiver.app.metrics") as mock_metrics,
            patch("functions.weekly_archiver.app.logger") as mock_logger,
        ):
            mock_tracer.capture_method = lambda f: f
            mock_tracer.capture_lambda_handler = lambda f: f
            mock_metrics.log_metrics = lambda **kwargs: lambda f: f
            mock_metrics.add_metric = MagicMock()
            mock_logger.inject_lambda_context = lambda f: f
            mock_logger.info = MagicMock()
            mock_logger.error = MagicMock()

            from functions.weekly_archiver.app import lambda_handler

            result = lambda_handler(event, mock_context)

            assert result["statusCode"] == 200
            assert "archived" in result
            assert "skipped" in result
            assert "errors" in result
            assert "week" in result

    @mock_aws
    def test_lambda_handler_manual_trigger_valid_week(self) -> None:
        """Test lambda_handler with valid manual target_week."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"
        os.environ["POWERTOOLS_SERVICE_NAME"] = "weekly-archiver"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        event = {"target_week": "2026-W01"}
        mock_context = MagicMock()
        mock_context.function_name = "sbm-weekly-archiver"

        with (
            patch("functions.weekly_archiver.app.s3", s3),
            patch("functions.weekly_archiver.app.tracer") as mock_tracer,
            patch("functions.weekly_archiver.app.metrics") as mock_metrics,
            patch("functions.weekly_archiver.app.logger") as mock_logger,
        ):
            mock_tracer.capture_method = lambda f: f
            mock_tracer.capture_lambda_handler = lambda f: f
            mock_metrics.log_metrics = lambda **kwargs: lambda f: f
            mock_metrics.add_metric = MagicMock()
            mock_logger.inject_lambda_context = lambda f: f
            mock_logger.info = MagicMock()
            mock_logger.error = MagicMock()

            from functions.weekly_archiver.app import lambda_handler

            result = lambda_handler(event, mock_context)

            assert result["statusCode"] == 200
            assert result["week"] == "2026-W01"

    @mock_aws
    def test_lambda_handler_manual_trigger_invalid_week(self) -> None:
        """Test lambda_handler with invalid manual target_week returns 400."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"
        os.environ["POWERTOOLS_SERVICE_NAME"] = "weekly-archiver"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        event = {"target_week": "invalid"}
        mock_context = MagicMock()
        mock_context.function_name = "sbm-weekly-archiver"

        with (
            patch("functions.weekly_archiver.app.s3", s3),
            patch("functions.weekly_archiver.app.tracer") as mock_tracer,
            patch("functions.weekly_archiver.app.metrics") as mock_metrics,
            patch("functions.weekly_archiver.app.logger") as mock_logger,
        ):
            mock_tracer.capture_method = lambda f: f
            mock_tracer.capture_lambda_handler = lambda f: f
            mock_metrics.log_metrics = lambda **kwargs: lambda f: f
            mock_metrics.add_metric = MagicMock()
            mock_logger.inject_lambda_context = lambda f: f
            mock_logger.info = MagicMock()
            mock_logger.error = MagicMock()

            from functions.weekly_archiver.app import lambda_handler

            result = lambda_handler(event, mock_context)

            assert result["statusCode"] == 400
            assert "error" in result

    @mock_aws
    def test_lambda_handler_processes_all_prefixes(self) -> None:
        """Test that lambda_handler processes all three prefixes."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"
        os.environ["POWERTOOLS_SERVICE_NAME"] = "weekly-archiver"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        s3.put_object(Bucket="sbm-file-ingester", Key="newP/file1.csv", Body=b"test")
        s3.put_object(Bucket="sbm-file-ingester", Key="newIrrevFiles/file2.csv", Body=b"test")
        s3.put_object(Bucket="sbm-file-ingester", Key="newParseErr/file3.csv", Body=b"test")

        mock_context = MagicMock()
        mock_context.function_name = "sbm-weekly-archiver"

        with (
            patch("functions.weekly_archiver.app.s3", s3),
            patch("functions.weekly_archiver.app.tracer") as mock_tracer,
            patch("functions.weekly_archiver.app.metrics") as mock_metrics,
            patch("functions.weekly_archiver.app.logger") as mock_logger,
        ):
            mock_tracer.capture_method = lambda f: f
            mock_tracer.capture_lambda_handler = lambda f: f
            mock_metrics.log_metrics = lambda **kwargs: lambda f: f
            mock_metrics.add_metric = MagicMock()
            mock_logger.inject_lambda_context = lambda f: f
            mock_logger.info = MagicMock()
            mock_logger.error = MagicMock()

            from functions.weekly_archiver.app import get_iso_week, lambda_handler

            response = s3.head_object(Bucket="sbm-file-ingester", Key="newP/file1.csv")
            target_week = get_iso_week(response["LastModified"])

            event = {"target_week": target_week}
            result = lambda_handler(event, mock_context)

            assert result["archived"] == 3
            assert result["statusCode"] == 200

    @mock_aws
    def test_lambda_handler_partial_success_returns_207(self) -> None:
        """Test lambda_handler returns 207 when there are errors."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"
        os.environ["POWERTOOLS_SERVICE_NAME"] = "weekly-archiver"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        s3.put_object(Bucket="sbm-file-ingester", Key="newP/file1.csv", Body=b"test")

        mock_context = MagicMock()
        mock_context.function_name = "sbm-weekly-archiver"

        # Create a mock S3 that fails on copy
        mock_s3 = MagicMock()
        mock_s3.get_paginator.return_value.paginate.return_value = [
            {"Contents": [{"Key": "newP/file1.csv", "LastModified": datetime(2026, 1, 5, tzinfo=UTC)}]}
        ]
        error_response = {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}
        mock_s3.copy_object.side_effect = ClientError(error_response, "CopyObject")

        with (
            patch("functions.weekly_archiver.app.s3", mock_s3),
            patch("functions.weekly_archiver.app.tracer") as mock_tracer,
            patch("functions.weekly_archiver.app.metrics") as mock_metrics,
            patch("functions.weekly_archiver.app.logger") as mock_logger,
        ):
            mock_tracer.capture_method = lambda f: f
            mock_tracer.capture_lambda_handler = lambda f: f
            mock_metrics.log_metrics = lambda **kwargs: lambda f: f
            mock_metrics.add_metric = MagicMock()
            mock_logger.inject_lambda_context = lambda f: f
            mock_logger.info = MagicMock()
            mock_logger.error = MagicMock()

            from functions.weekly_archiver.app import lambda_handler

            event = {"target_week": "2026-W02"}
            result = lambda_handler(event, mock_context)

            assert result["statusCode"] == 207
            assert result["errors"] > 0


class TestConstants:
    """Tests for module constants."""

    def test_bucket_name_constant(self) -> None:
        """Test BUCKET_NAME constant is correct."""
        from functions.weekly_archiver.app import BUCKET_NAME

        assert BUCKET_NAME == "sbm-file-ingester"

    def test_prefixes_constant(self) -> None:
        """Test PREFIXES constant contains all required prefixes."""
        from functions.weekly_archiver.app import PREFIXES

        assert PREFIXES == ["newP/", "newIrrevFiles/", "newParseErr/"]
        assert len(PREFIXES) == 3

    def test_max_workers_constant(self) -> None:
        """Test MAX_WORKERS constant is set."""
        from functions.weekly_archiver.app import MAX_WORKERS

        assert MAX_WORKERS == 50

    def test_target_week_pattern(self) -> None:
        """Test TARGET_WEEK_PATTERN regex is defined."""
        from functions.weekly_archiver.app import TARGET_WEEK_PATTERN

        assert TARGET_WEEK_PATTERN.match("2026-W01")
        assert not TARGET_WEEK_PATTERN.match("invalid")
