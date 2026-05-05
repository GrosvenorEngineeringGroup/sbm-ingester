"""Tests for shared.parsers.optima.racv_billing.racv_billing_parser."""

import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent / "src"))


class TestRacvBillingParser:
    """Tests for racv_billing_parser function."""

    def test_rejects_optima_generation_file(self, temp_directory: str) -> None:
        """Test that OptimaGenerationData files are rejected."""
        with patch("shared.parsers.optima.racv_billing.logger"):
            from shared.parsers.optima.racv_billing import racv_billing_parser

            # File with OptimaGenerationData in name
            filepath = str(Path(temp_directory) / "OptimaGenerationData.csv")
            Path(filepath).write_text("dummy content")

            with pytest.raises(Exception, match="Not Relevant Parser"):
                racv_billing_parser(filepath, "error_log")

    def test_rejects_non_racv_usage_file(self, temp_directory: str) -> None:
        """Test that non-RACV Usage and Spend files are rejected."""
        with patch("shared.parsers.optima.racv_billing.logger"):
            from shared.parsers.optima.racv_billing import racv_billing_parser

            # File without "RACV-Usage and Spend Report" in name
            filepath = str(Path(temp_directory) / "other_report.csv")
            Path(filepath).write_text("dummy content")

            with pytest.raises(Exception, match="Not Valid Optima Usage And Spend File"):
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

                # Should return empty list
                assert result == []

                # Verify file was uploaded
                response = s3.get_object(Bucket="gegoptimareports", Key="usageAndSpendReports/racvUsageAndSpend.csv")
                body = response["Body"].read().decode("utf-8")
                assert "date,usage,spend" in body
