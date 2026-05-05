"""Unit tests for demand_exporter/uploader.py module.

Tests S3 client initialization and file upload functionality.
"""

from unittest.mock import patch

import boto3
from moto import mock_aws

from tests.unit.optima_exporter.conftest import reload_demand_uploader_module


class TestGetS3Client:
    @mock_aws
    def test_lazy_initialization(self) -> None:
        uploader_module = reload_demand_uploader_module()
        result1 = uploader_module.get_s3_client()
        assert result1 is not None
        result2 = uploader_module.get_s3_client()
        assert result1 is result2


class TestUploadToS3:
    @mock_aws
    def test_successful_upload(self) -> None:
        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        uploader_module = reload_demand_uploader_module()
        csv_content = b"Commodities:,Electricity\nIdentifier,kW\n3117512760,5.5"
        filename = "optima_racv_demand_profile_NMI#OPTIMA_3117512760_2026-04-29_2026-04-29_20260505000000.csv"

        result = uploader_module.upload_to_s3(csv_content, filename)

        assert result is True
        response = s3.get_object(Bucket="sbm-file-ingester", Key=f"newTBP/{filename}")
        assert response["Body"].read() == csv_content
        assert response["ContentType"] == "text/csv"

    @mock_aws
    def test_upload_failure_returns_false(self) -> None:
        # Don't create bucket → put_object raises NoSuchBucket
        uploader_module = reload_demand_uploader_module()
        result = uploader_module.upload_to_s3(b"data", "test.csv")
        assert result is False

    @mock_aws
    def test_upload_logs_error_on_failure(self) -> None:
        uploader_module = reload_demand_uploader_module()
        with patch.object(uploader_module.logger, "error") as mock_error:
            result = uploader_module.upload_to_s3(b"data", "test.csv")
            assert result is False
            mock_error.assert_called_once()

    @mock_aws
    def test_logger_service_name_is_demand_exporter(self) -> None:
        uploader_module = reload_demand_uploader_module()
        # aws_lambda_powertools.Logger stores the service in .service
        assert uploader_module.logger.service == "optima-demand-exporter"
