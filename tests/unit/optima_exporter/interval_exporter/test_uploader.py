"""Unit tests for interval_exporter/uploader.py module.

Tests S3 client initialization and file upload functionality.
"""

import importlib
from typing import Any

import boto3
from moto import mock_aws


def reload_interval_uploader_module() -> Any:
    """Reload the interval_exporter uploader module with fresh environment."""
    import interval_exporter.uploader as uploader_module

    uploader_module._s3_client = None
    importlib.reload(uploader_module)
    return uploader_module


class TestGetS3Client:
    @mock_aws
    def test_lazy_initialization_reuses_s3_client(self) -> None:
        uploader_module = reload_interval_uploader_module()

        result1 = uploader_module.get_s3_client()
        result2 = uploader_module.get_s3_client()

        assert result1 is not None
        assert result1 is result2


class TestUploadToS3:
    @mock_aws
    def test_successful_upload_uses_default_bucket_prefix_and_csv_content_type(self) -> None:
        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        uploader_module = reload_interval_uploader_module()
        csv_content = b"Identifier,kWh\n3117512760,12.5"
        filename = "optima_bunnings_interval_NMI#OPTIMA_3117512760_2026-05-05.csv"

        result = uploader_module.upload_to_s3(csv_content, filename)

        assert result is True
        response = s3.get_object(Bucket="sbm-file-ingester", Key=f"newTBP/{filename}")
        assert response["Body"].read() == csv_content
        assert response["ContentType"] == "text/csv"

    @mock_aws
    def test_upload_failure_returns_false(self) -> None:
        uploader_module = reload_interval_uploader_module()

        result = uploader_module.upload_to_s3(b"data", "test.csv")

        assert result is False

    @mock_aws
    def test_upload_with_custom_bucket_and_prefix(self) -> None:
        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="custom-bucket",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        uploader_module = reload_interval_uploader_module()

        result = uploader_module.upload_to_s3(
            b"data",
            "test.csv",
            bucket="custom-bucket",
            prefix="custom-prefix/",
        )

        assert result is True
        response = s3.get_object(Bucket="custom-bucket", Key="custom-prefix/test.csv")
        assert response["Body"].read() == b"data"
        assert response["ContentType"] == "text/csv"

    @mock_aws
    def test_logger_service_name_is_interval_exporter(self) -> None:
        uploader_module = reload_interval_uploader_module()

        assert uploader_module.logger.service == "optima-interval-exporter"
