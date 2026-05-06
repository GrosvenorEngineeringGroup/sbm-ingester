"""Tests for shared.parsers.optima.racv_billing.racv_billing_parser."""

import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent / "src"))

from shared.parsers import NotRelevantParser
from shared.parsers.optima.racv_billing import racv_billing_parser


class TestRacvBillingParser:
    """Tests for racv_billing_parser function."""

    def test_rejects_optima_generation_file(self, temp_directory: str) -> None:
        """Test that OptimaGenerationData files are rejected."""
        with patch("shared.parsers.optima.racv_billing.logger"):
            from shared.parsers.optima.racv_billing import racv_billing_parser

            # File with OptimaGenerationData in name
            filepath = str(Path(temp_directory) / "OptimaGenerationData.csv")
            Path(filepath).write_text("dummy content")

            with pytest.raises(NotRelevantParser, match="Not Relevant Parser"):
                racv_billing_parser(filepath, "error_log")

    def test_rejects_non_racv_usage_file(self, temp_directory: str) -> None:
        """Test that non-RACV Usage and Spend files are rejected."""
        with patch("shared.parsers.optima.racv_billing.logger"):
            from shared.parsers.optima.racv_billing import racv_billing_parser

            # File without "RACV-Usage and Spend Report" in name
            filepath = str(Path(temp_directory) / "other_report.csv")
            Path(filepath).write_text("dummy content")

            with pytest.raises(NotRelevantParser, match="Not Valid Optima Usage And Spend File"):
                racv_billing_parser(filepath, "error_log")

    @pytest.fixture
    def aws_env(self) -> None:
        """Set up AWS environment variables."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

    def test_uploads_racv_usage_and_spend_file(self, temp_directory: str, aws_env: None) -> None:
        """Test that valid RACV Usage and Spend files are uploaded to S3."""
        import boto3
        from moto import mock_aws

        with mock_aws():
            # Create the target bucket
            s3 = boto3.client("s3", region_name="ap-southeast-2")
            s3.create_bucket(
                Bucket="gegoptimareports", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"}
            )

            with patch("shared.parsers.optima.racv_billing.logger"):
                from shared.parsers.optima.racv_billing import racv_billing_parser

                # Create file with correct name pattern
                filepath = str(Path(temp_directory) / "RACV-Usage and Spend Report.csv")
                Path(filepath).write_text("date,usage,spend\n2024-01-01,100,50.00")

                result = racv_billing_parser(filepath, "error_log")

                assert result.status == "processed_external"
                assert result.reason == "gegoptimareports"

                # Verify file was uploaded
                response = s3.get_object(Bucket="gegoptimareports", Key="usageAndSpendReports/racvUsageAndSpend.csv")
                body = response["Body"].read().decode("utf-8")
                assert "date,usage,spend" in body


def test_racv_billing_success_returns_processed_external(tmp_path) -> None:
    path = tmp_path / "20260414-RACV-Usage and Spend Report.csv"
    path.write_text("a,b\n1,2\n")

    with (
        patch("shared.parsers.optima.racv_billing.boto3.client") as mock_client,
        patch("shared.parsers.optima.racv_billing.logger") as mock_logger,
    ):
        mock_client.return_value.put_object.return_value = {"ETag": "etag"}
        result = racv_billing_parser(str(path), "error_log")

    assert result.status == "processed_external"
    assert result.reason == "gegoptimareports"
    mock_logger.info.assert_called_once_with(
        "racv_billing_uploaded",
        extra={
            "bucket": "gegoptimareports",
            "key": "usageAndSpendReports/racvUsageAndSpend.csv",
        },
    )


def test_racv_billing_upload_failure_raises_processing_error(tmp_path) -> None:
    from shared.parsers import ProcessingError

    path = tmp_path / "20260414-RACV-Usage and Spend Report.csv"
    path.write_text("a,b\n1,2\n")

    with patch("shared.parsers.optima.racv_billing.boto3.client") as mock_client:
        mock_client.return_value.put_object.side_effect = RuntimeError("boom")

        with pytest.raises(ProcessingError, match="Failed to upload RACV billing report"):
            racv_billing_parser(str(path), "error_log")
