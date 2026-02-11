"""Unit tests for interval_exporter/uploader.py module.

Tests S3 client initialization and file upload functionality.
"""

from unittest.mock import patch

import boto3
from moto import mock_aws

from tests.unit.optima_exporter.conftest import reload_uploader_module


class TestGetS3Client:
    """Tests for get_s3_client function."""

    @mock_aws
    def test_lazy_initialization(self) -> None:
        """Test that S3 client is lazily initialized."""
        uploader_module = reload_uploader_module()

        # First call should create the client
        result1 = uploader_module.get_s3_client()
        assert result1 is not None

        # Second call should return the same client
        result2 = uploader_module.get_s3_client()
        assert result1 is result2

    @mock_aws
    def test_singleton_pattern(self) -> None:
        """Test that get_s3_client returns the same instance."""
        uploader_module = reload_uploader_module()

        result1 = uploader_module.get_s3_client()
        result2 = uploader_module.get_s3_client()
        result3 = uploader_module.get_s3_client()

        assert result1 is result2 is result3


class TestUploadToS3:
    """Tests for upload_to_s3 function."""

    @mock_aws
    def test_successful_upload(self) -> None:
        """Test successful CSV upload to S3."""
        # Create S3 bucket
        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        uploader_module = reload_uploader_module()
        csv_content = b"header1,header2\nvalue1,value2"
        filename = "optima_bunnings_NMI#TEST123_2026-01-20_2026-01-26.csv"

        result = uploader_module.upload_to_s3(csv_content, filename)

        assert result is True

        # Verify file exists in S3
        response = s3.get_object(Bucket="sbm-file-ingester", Key=f"newTBP/{filename}")
        assert response["Body"].read() == csv_content
        assert response["ContentType"] == "text/csv"

    @mock_aws
    def test_upload_with_custom_bucket_and_prefix(self) -> None:
        """Test upload with custom bucket and prefix."""
        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="custom-bucket",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        uploader_module = reload_uploader_module()
        csv_content = b"data"
        filename = "test.csv"

        result = uploader_module.upload_to_s3(csv_content, filename, bucket="custom-bucket", prefix="custom-prefix/")

        assert result is True
        response = s3.get_object(Bucket="custom-bucket", Key="custom-prefix/test.csv")
        assert response["Body"].read() == csv_content

    @mock_aws
    def test_upload_failure_returns_false(self) -> None:
        """Test that upload failure returns False."""
        # Don't create bucket to trigger error
        uploader_module = reload_uploader_module()
        csv_content = b"data"
        filename = "test.csv"

        result = uploader_module.upload_to_s3(csv_content, filename)

        assert result is False

    @mock_aws
    def test_upload_logs_success(self) -> None:
        """Test that successful upload logs correctly."""
        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        uploader_module = reload_uploader_module()

        with patch.object(uploader_module.logger, "info") as mock_info:
            uploader_module.upload_to_s3(b"data", "test.csv")

            # Verify logging happened
            assert mock_info.call_count >= 2  # Upload start and success

    @mock_aws
    def test_upload_logs_error_on_failure(self) -> None:
        """Test that errors are logged on upload failure."""
        uploader_module = reload_uploader_module()

        with patch.object(uploader_module.logger, "error") as mock_error:
            result = uploader_module.upload_to_s3(b"data", "test.csv")

            assert result is False
            mock_error.assert_called_once()
