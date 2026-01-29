"""Unit tests for interval_exporter/processor.py module.

Tests date range calculation, site processing, and export orchestration.
"""

import os
from typing import Any
from unittest.mock import patch

import boto3
import responses
from freezegun import freeze_time
from moto import mock_aws

from tests.unit.optima_exporter.conftest import reload_processor_module


class TestGetDateRange:
    """Tests for get_date_range function."""

    @freeze_time("2026-01-23 10:00:00")
    def test_returns_correct_date_range(self) -> None:
        """Test that correct date range is calculated."""
        processor_module = reload_processor_module()

        start_date, end_date = processor_module.get_date_range()

        # End date should be yesterday (2026-01-22)
        # Start date should be 7 days back from end_date (2026-01-16)
        assert end_date == "2026-01-22"
        assert start_date == "2026-01-16"

    @freeze_time("2026-01-23 10:00:00")
    def test_respects_optima_days_back(self) -> None:
        """Test that OPTIMA_DAYS_BACK is respected."""
        os.environ["OPTIMA_DAYS_BACK"] = "14"
        processor_module = reload_processor_module()

        start_date, end_date = processor_module.get_date_range()

        assert end_date == "2026-01-22"
        assert start_date == "2026-01-09"

    @freeze_time("2026-01-01 10:00:00")
    def test_end_date_is_yesterday(self) -> None:
        """Test that end date is always yesterday."""
        processor_module = reload_processor_module()

        _start_date, end_date = processor_module.get_date_range()

        assert end_date == "2025-12-31"

    @freeze_time("2026-01-01 00:30:00")
    def test_at_midnight(self) -> None:
        """Test date range calculation at midnight."""
        processor_module = reload_processor_module()

        _start_date, end_date = processor_module.get_date_range()

        # Should still use yesterday
        assert end_date == "2025-12-31"


class TestProcessSite:
    """Tests for process_site function."""

    @mock_aws
    @responses.activate
    def test_successful_process_returns_success(self) -> None:
        """Test that successful processing returns success result."""
        # Create S3 bucket
        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        processor_module = reload_processor_module()

        csv_content = b"Date,Value\n2026-01-01,100"
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportActualIntervalUsageProfile",
            status=200,
            body=csv_content,
            content_type="text/csv",
        )

        result = processor_module.process_site(
            cookies=".ASPXAUTH=token123",
            nmi="NMI001",
            site_id_str="site-guid-001",
            start_date="2026-01-01",
            end_date="2026-01-07",
            project="bunnings",
        )

        assert result["success"] is True
        assert result["nmi"] == "NMI001"
        assert result["error"] is None
        assert "filename" in result
        assert "s3_key" in result
        assert result["s3_key"].startswith("newTBP/")

    @responses.activate
    def test_download_failure_returns_error(self) -> None:
        """Test that download failure returns error result."""
        processor_module = reload_processor_module()

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportActualIntervalUsageProfile",
            status=500,
        )

        result = processor_module.process_site(
            cookies=".ASPXAUTH=token123",
            nmi="NMI001",
            site_id_str="site-guid-001",
            start_date="2026-01-01",
            end_date="2026-01-07",
            project="bunnings",
        )

        assert result["success"] is False
        assert result["error"] == "Failed to download CSV"

    @mock_aws
    @responses.activate
    def test_s3_upload_failure_returns_error(self) -> None:
        """Test that S3 upload failure returns error result."""
        # Don't create bucket to trigger error
        processor_module = reload_processor_module()

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportActualIntervalUsageProfile",
            status=200,
            body=b"data",
            content_type="text/csv",
        )

        result = processor_module.process_site(
            cookies=".ASPXAUTH=token123",
            nmi="NMI001",
            site_id_str="site-guid-001",
            start_date="2026-01-01",
            end_date="2026-01-07",
            project="bunnings",
        )

        assert result["success"] is False
        assert result["error"] == "Failed to upload to S3"

    @mock_aws
    @responses.activate
    def test_uploads_csv_to_correct_s3_location(self) -> None:
        """Test that CSV is uploaded to the correct S3 location."""
        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        processor_module = reload_processor_module()

        csv_content = b"Date,Value\n2026-01-01,100"
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportActualIntervalUsageProfile",
            status=200,
            body=csv_content,
            content_type="text/csv",
        )

        result = processor_module.process_site(
            cookies=".ASPXAUTH=token123",
            nmi="NMI001",
            site_id_str="site-guid-001",
            start_date="2026-01-01",
            end_date="2026-01-07",
            project="bunnings",
        )

        assert result["success"] is True

        # Verify file in S3
        response = s3.get_object(Bucket="sbm-file-ingester", Key=result["s3_key"])
        assert response["Body"].read() == csv_content


class TestProcessExport:
    """Tests for process_export function (renamed from process_ondemand_export)."""

    def test_validates_project_required(self) -> None:
        """Test that project parameter is handled properly."""
        from interval_exporter.processor import process_export

        # project is required and has no default - this tests the function behavior
        result = process_export(project="unknown_project")
        assert result["statusCode"] == 400
        assert "No credentials configured" in result["body"]

    @mock_aws
    @freeze_time("2026-01-23 10:00:00")
    def test_process_with_project_only(self) -> None:
        """Test export with only project (exports all NMIs with default dates)."""
        # Set up DynamoDB
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
        table.put_item(Item={"project": "bunnings", "nmi": "NMI001", "siteIdStr": "site-guid-001"})
        table.put_item(Item={"project": "bunnings", "nmi": "NMI002", "siteIdStr": "site-guid-002"})

        processor_module = reload_processor_module()

        with (
            patch("interval_exporter.processor.login_bidenergy", return_value=".ASPXAUTH=token"),
            patch.object(processor_module, "process_site") as mock_process,
        ):
            mock_process.return_value = {"success": True, "nmi": "NMI001"}

            result = processor_module.process_export(project="bunnings")

            assert result["statusCode"] == 200
            assert mock_process.call_count == 2
            # Verify default dates are used (2026-01-16 to 2026-01-22)
            assert result["body"]["date_range"]["start"] == "2026-01-16"
            assert result["body"]["date_range"]["end"] == "2026-01-22"

    @mock_aws
    @freeze_time("2026-01-23 10:00:00")
    def test_process_with_project_and_nmi(self) -> None:
        """Test export with project and specific NMI."""
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
        table.put_item(Item={"project": "bunnings", "nmi": "NMI001", "siteIdStr": "site-guid-001"})
        table.put_item(Item={"project": "bunnings", "nmi": "NMI002", "siteIdStr": "site-guid-002"})

        processor_module = reload_processor_module()

        with (
            patch("interval_exporter.processor.login_bidenergy", return_value=".ASPXAUTH=token"),
            patch.object(processor_module, "process_site") as mock_process,
        ):
            mock_process.return_value = {"success": True, "nmi": "NMI001"}

            result = processor_module.process_export(project="bunnings", nmi="NMI001")

            assert result["statusCode"] == 200
            # Only 1 site should be processed
            assert mock_process.call_count == 1
            assert result["body"]["success_count"] == 1

    @mock_aws
    def test_process_with_date_range(self) -> None:
        """Test export with project and custom date range."""
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
        table.put_item(Item={"project": "bunnings", "nmi": "NMI001", "siteIdStr": "site-guid-001"})

        processor_module = reload_processor_module()

        with (
            patch("interval_exporter.processor.login_bidenergy", return_value=".ASPXAUTH=token"),
            patch.object(processor_module, "process_site") as mock_process,
        ):
            mock_process.return_value = {"success": True, "nmi": "NMI001"}

            result = processor_module.process_export(
                project="bunnings",
                start_date="2026-01-01",
                end_date="2026-01-07",
            )

            assert result["statusCode"] == 200
            assert result["body"]["date_range"]["start"] == "2026-01-01"
            assert result["body"]["date_range"]["end"] == "2026-01-07"

    @mock_aws
    def test_nmi_not_found(self) -> None:
        """Test that 404 is returned when NMI is not found."""
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
        # No sites added

        processor_module = reload_processor_module()

        result = processor_module.process_export(project="bunnings", nmi="NONEXISTENT")

        assert result["statusCode"] == 404
        assert "not found" in result["body"].lower()

    @mock_aws
    def test_no_sites_for_project(self) -> None:
        """Test that 404 is returned when project has no sites."""
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
        # No sites added

        processor_module = reload_processor_module()

        result = processor_module.process_export(project="bunnings")

        assert result["statusCode"] == 404
        assert "No sites found" in result["body"]

    @mock_aws
    def test_returns_401_on_auth_failure(self) -> None:
        """Test that 401 is returned when authentication fails."""
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
        table.put_item(Item={"project": "bunnings", "nmi": "NMI001", "siteIdStr": "site-guid-001"})

        processor_module = reload_processor_module()

        with patch("interval_exporter.processor.login_bidenergy", return_value=None):
            result = processor_module.process_export(
                project="bunnings",
                nmi="NMI001",
                start_date="2026-01-01",
                end_date="2026-01-07",
            )

            assert result["statusCode"] == 401
            assert "authenticate" in result["body"].lower()

    @mock_aws
    def test_returns_207_on_partial_failure(self) -> None:
        """Test that 207 is returned on partial failure."""
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
        table.put_item(Item={"project": "bunnings", "nmi": "NMI001", "siteIdStr": "site-guid-001"})
        table.put_item(Item={"project": "bunnings", "nmi": "NMI002", "siteIdStr": "site-guid-002"})

        processor_module = reload_processor_module()

        call_count = [0]

        def mock_process(*args: Any, **kwargs: Any) -> dict[str, Any]:
            call_count[0] += 1
            if call_count[0] == 1:
                return {"success": True, "nmi": "NMI001"}
            return {"success": False, "nmi": "NMI002", "error": "Failed"}

        with (
            patch("interval_exporter.processor.login_bidenergy", return_value=".ASPXAUTH=token"),
            patch.object(processor_module, "process_site", side_effect=mock_process),
        ):
            result = processor_module.process_export(
                project="bunnings",
                start_date="2026-01-01",
                end_date="2026-01-07",
            )

            assert result["statusCode"] == 207
            assert result["body"]["success_count"] == 1
            assert result["body"]["error_count"] == 1

    @mock_aws
    def test_project_name_lowercased(self) -> None:
        """Test that project name is lowercased."""
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
        # Note: DynamoDB key lookup is case-sensitive, so we use lowercase
        table.put_item(Item={"project": "bunnings", "nmi": "NMI001", "siteIdStr": "site-guid-001"})

        processor_module = reload_processor_module()

        with (
            patch("interval_exporter.processor.login_bidenergy", return_value=".ASPXAUTH=token"),
            patch.object(processor_module, "process_site") as mock_process,
        ):
            mock_process.return_value = {"success": True}

            processor_module.process_export(
                project="BUNNINGS",  # Uppercase
                nmi="NMI001",
                start_date="2026-01-01",
                end_date="2026-01-07",
            )

            # Verify process_site was called with lowercase project
            call_kwargs = mock_process.call_args.kwargs
            assert call_kwargs["project"] == "bunnings"


class TestParallelProcessing:
    """Tests for parallel processing with ThreadPoolExecutor."""

    @freeze_time("2026-01-23 10:00:00")
    @mock_aws
    def test_parallel_processes_all_sites(self) -> None:
        """Test that all sites are processed in parallel."""
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
        # Add multiple sites
        for i in range(5):
            table.put_item(Item={"project": "bunnings", "nmi": f"NMI00{i}", "siteIdStr": f"site-guid-00{i}"})

        processor_module = reload_processor_module()

        processed_nmis: list[str] = []

        def mock_process(*args: Any, **kwargs: Any) -> dict[str, Any]:
            nmi = kwargs.get("nmi", args[1] if len(args) > 1 else "unknown")
            processed_nmis.append(nmi)
            return {"success": True, "nmi": nmi}

        with (
            patch("interval_exporter.processor.login_bidenergy", return_value=".ASPXAUTH=token"),
            patch.object(processor_module, "process_site", side_effect=mock_process),
        ):
            result = processor_module.process_export(project="bunnings")

            # All 5 sites should be processed
            assert len(processed_nmis) == 5
            assert result["body"]["success_count"] == 5
            assert result["body"]["error_count"] == 0

    @freeze_time("2026-01-23 10:00:00")
    @mock_aws
    def test_parallel_handles_thread_exception(self) -> None:
        """Test that thread exception is caught and doesn't crash other threads."""
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
        table.put_item(Item={"project": "bunnings", "nmi": "NMI001", "siteIdStr": "site-guid-001"})
        table.put_item(Item={"project": "bunnings", "nmi": "NMI002", "siteIdStr": "site-guid-002"})
        table.put_item(Item={"project": "bunnings", "nmi": "NMI003", "siteIdStr": "site-guid-003"})

        processor_module = reload_processor_module()

        call_count = [0]

        def mock_process(*args: Any, **kwargs: Any) -> dict[str, Any]:
            call_count[0] += 1
            nmi = kwargs.get("nmi", args[1] if len(args) > 1 else "unknown")
            # Raise exception for NMI002
            if nmi == "NMI002":
                raise RuntimeError("Simulated thread failure")
            return {"success": True, "nmi": nmi}

        with (
            patch("interval_exporter.processor.login_bidenergy", return_value=".ASPXAUTH=token"),
            patch.object(processor_module, "process_site", side_effect=mock_process),
        ):
            result = processor_module.process_export(project="bunnings")

            # All 3 sites should be attempted
            assert call_count[0] == 3
            # 2 successes, 1 failure
            assert result["body"]["success_count"] == 2
            assert result["body"]["error_count"] == 1
            # Check that the error result contains thread exception info
            error_results = [r for r in result["body"]["results"] if not r.get("success")]
            assert len(error_results) == 1
            assert "Thread execution failed" in error_results[0]["error"]

    @freeze_time("2026-01-23 10:00:00")
    @mock_aws
    def test_parallel_respects_max_workers(self) -> None:
        """Test that parallel processing respects MAX_WORKERS setting."""
        os.environ["OPTIMA_MAX_WORKERS"] = "2"

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
        for i in range(4):
            table.put_item(Item={"project": "bunnings", "nmi": f"NMI00{i}", "siteIdStr": f"site-guid-00{i}"})

        processor_module = reload_processor_module()

        with (
            patch("interval_exporter.processor.login_bidenergy", return_value=".ASPXAUTH=token"),
            patch.object(processor_module, "process_site") as mock_process,
        ):
            mock_process.return_value = {"success": True, "nmi": "NMI"}

            result = processor_module.process_export(project="bunnings")

            # All 4 sites should still be processed
            assert mock_process.call_count == 4
            assert result["body"]["success_count"] == 4
