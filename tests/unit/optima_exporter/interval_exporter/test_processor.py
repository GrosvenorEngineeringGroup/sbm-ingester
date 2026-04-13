"""Unit tests for interval_exporter/processor.py module.

Tests date range calculation, site processing, and export orchestration.
"""

import os
from typing import Any
from unittest.mock import patch

import boto3
import pytest
import responses
from freezegun import freeze_time
from moto import mock_aws

from tests.unit.optima_exporter.conftest import reload_processor_module


class TestGetDateRange:
    """Tests for get_date_range function."""

    @freeze_time("2026-01-23 10:00:00")
    def test_returns_correct_date_range(self) -> None:
        """Default DAYS_BACK=1 returns yesterday only (single-day range)."""
        processor_module = reload_processor_module()

        start_date, end_date = processor_module.get_date_range()

        # Both dates equal yesterday (2026-01-22) - single-day window
        assert end_date == "2026-01-22"
        assert start_date == "2026-01-22"

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

        # Valid NEM12 content with NMI001 so _prefix_nmi_in_nem12 can rewrite it
        csv_content = (
            b"100,NEM12,202601080000,MDP1,Origin\n"
            b"200,NMI001,E1,E1,E1,E1,12345,kWh,30\n"
            b"300,20260101,1.0,A,,,20260102000000,\n"
            b"900\n"
        )
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
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
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
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

        # Valid NEM12 content so prefix rewrite succeeds, then S3 upload fails (no bucket)
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
            status=200,
            body=(
                b"100,NEM12,202601080000,MDP1,Origin\n"
                b"200,NMI001,E1,E1,E1,E1,12345,kWh,30\n"
                b"300,20260101,1.0,A,,,20260102000000,\n"
                b"900\n"
            ),
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

        # Valid NEM12 content; the processor rewrites NMI001 → Optima_NMI001 before upload
        raw_content = (
            b"100,NEM12,202601080000,MDP1,Origin\n"
            b"200,NMI001,E1,E1,E1,E1,12345,kWh,30\n"
            b"300,20260101,1.0,A,,,20260102000000,\n"
            b"900\n"
        )
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
            status=200,
            body=raw_content,
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

        # Verify file in S3 - NMI prefix has been applied by the processor
        s3_body = s3.get_object(Bucket="sbm-file-ingester", Key=result["s3_key"])["Body"].read()
        assert b"200,Optima_NMI001," in s3_body
        assert b"200,NMI001," not in s3_body


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
            # Default DAYS_BACK=1 - both dates equal yesterday (2026-01-22)
            assert result["body"]["date_range"]["start"] == "2026-01-22"
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


class TestPartialDateParameters:
    """Tests for partial date parameter handling."""

    @mock_aws
    @freeze_time("2026-02-04 10:00:00")
    def test_process_export_with_only_start_date_uses_yesterday_as_end_date(self) -> None:
        """When only startDate is provided, endDate should default to yesterday."""
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

            result = processor_module.process_export(project="bunnings", start_date="2024-01-01")

            assert result["statusCode"] == 200
            # start_date should be preserved as provided
            assert result["body"]["date_range"]["start"] == "2024-01-01"
            # end_date should be yesterday (2026-02-03)
            assert result["body"]["date_range"]["end"] == "2026-02-03"

    @mock_aws
    @freeze_time("2026-02-04 10:00:00")
    def test_process_export_with_only_end_date_anchors_start_to_end(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When only endDate is provided, start must be derived from end (not today)."""
        # Use DAYS_BACK=7 to make the anchoring observable (start = end - 6)
        monkeypatch.setenv("OPTIMA_DAYS_BACK", "7")

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

            # Backfill scenario - endDate far in the past
            result = processor_module.process_export(project="bunnings", end_date="2024-06-15")

            assert result["statusCode"] == 200
            assert result["body"]["date_range"]["end"] == "2024-06-15"
            # start = end - (DAYS_BACK - 1) = 2024-06-15 - 6 = 2024-06-09
            assert result["body"]["date_range"]["start"] == "2024-06-09"


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


class TestProductionDefaults:
    """Verify source-code defaults match the design (DAYS_BACK=1, MAX_WORKERS=20).

    Uses monkeypatch.delenv to remove the autouse env override and observe the raw
    `os.environ.get(...)` fallback in config.py.
    """

    def test_default_days_back_is_one(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import importlib

        monkeypatch.delenv("OPTIMA_DAYS_BACK", raising=False)
        from optima_shared import config

        importlib.reload(config)
        assert config.OPTIMA_DAYS_BACK == 1

    def test_default_max_workers_is_twenty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import importlib

        monkeypatch.delenv("OPTIMA_MAX_WORKERS", raising=False)
        from optima_shared import config

        importlib.reload(config)
        assert config.MAX_WORKERS == 20


class TestDateRangeValidation:
    """Validation that startDate <= endDate."""

    def test_rejects_start_after_end(self) -> None:
        from interval_exporter.processor import process_export

        result = process_export(
            project="bunnings",
            start_date="2026-04-15",
            end_date="2026-04-10",
        )

        assert result["statusCode"] == 400
        assert "startDate" in result["body"]
        assert "endDate" in result["body"]
        assert "2026-04-15" in result["body"]
        assert "2026-04-10" in result["body"]

    @mock_aws
    def test_accepts_equal_start_and_end(self) -> None:
        """Single-day range (start == end) must be accepted."""
        from unittest.mock import patch

        import boto3
        from interval_exporter import processor

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

        with (
            patch("interval_exporter.processor.login_bidenergy", return_value=".ASPXAUTH=token"),
            patch.object(processor, "process_site", return_value={"success": True, "nmi": "NMI001"}),
        ):
            result = processor.process_export(
                project="bunnings",
                start_date="2026-04-10",
                end_date="2026-04-10",
            )
            assert result["statusCode"] == 200

    @mock_aws
    @freeze_time("2026-04-13 10:00:00")
    def test_rejects_partial_date_that_resolves_to_inverted_range(self) -> None:
        """User supplies only future startDate; end resolves to yesterday => invalid."""
        import boto3
        from interval_exporter.processor import process_export

        # Set up DynamoDB so get_sites_for_project succeeds and we reach date resolution
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

        # end_date will default to yesterday (2026-04-12); start is future => inverted
        result = process_export(
            project="bunnings",
            start_date="2026-05-01",
        )

        assert result["statusCode"] == 400
        assert "resolution" in result["body"].lower() or "invalid range" in result["body"].lower()
        assert "2026-05-01" in result["body"]


class TestProcessSitePassesNmiPrefix:
    """Regression: process_site must pass nmi_prefix='Optima_' (and country) to download_csv."""

    @mock_aws
    def test_process_site_passes_optima_prefix_and_country(self) -> None:
        from unittest.mock import patch

        import boto3
        from interval_exporter import processor

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        with (
            patch.object(processor, "download_csv") as mock_dl,
            patch.object(processor, "upload_to_s3", return_value=True),
        ):
            mock_dl.return_value = (b"100,NEM12,...", "fakefile.csv")
            processor.process_site(
                cookies=".ASPXAUTH=token",
                nmi="Optima_4001348123",
                site_id_str="site-guid-001",
                start_date="2026-04-10",
                end_date="2026-04-12",
                project="bunnings",
                country="NZ",
            )

        assert mock_dl.call_count == 1
        kwargs = mock_dl.call_args.kwargs
        assert kwargs.get("nmi_prefix") == "Optima_"
        assert kwargs.get("country") == "NZ"

    def test_optima_nmi_prefix_constant_value(self) -> None:
        from interval_exporter.processor import OPTIMA_NMI_PREFIX

        assert OPTIMA_NMI_PREFIX == "Optima_"
