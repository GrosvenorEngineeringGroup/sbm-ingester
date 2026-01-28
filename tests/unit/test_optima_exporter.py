"""Unit tests for optima_exporter Lambda function.

Tests the Optima/BidEnergy data exporter that downloads CSV interval data
and sends it via email.
"""

import importlib
import os
from collections.abc import Generator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import boto3
import pytest
import responses
from botocore.exceptions import ClientError
from freezegun import freeze_time
from moto import mock_aws


# ================================
# Test Fixtures
# ================================
@pytest.fixture(autouse=True)
def reset_env() -> Generator[None]:
    """Reset environment variables before each test."""
    # Save original env
    original_env = os.environ.copy()

    # Set up test environment
    os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["POWERTOOLS_TRACE_DISABLED"] = "true"
    os.environ["POWERTOOLS_METRICS_NAMESPACE"] = "test"

    # SES config
    os.environ["SES_SENDER"] = "test@example.com"
    os.environ["SES_REGION"] = "ap-southeast-2"

    # Optima config
    os.environ["OPTIMA_PROJECTS"] = "bunnings,racv"
    os.environ["OPTIMA_DAYS_BACK"] = "7"
    os.environ["OPTIMA_CONFIG_TABLE"] = "sbm-optima-config"
    os.environ["OPTIMA_DEFAULT_RECIPIENTS"] = "test@example.com"
    os.environ["BIDENERGY_BASE_URL"] = "https://app.bidenergy.com"

    # Project credentials - bunnings
    os.environ["OPTIMA_BUNNINGS_USERNAME"] = "bunnings@test.com"
    os.environ["OPTIMA_BUNNINGS_PASSWORD"] = "bunnings_pass"
    os.environ["OPTIMA_BUNNINGS_CLIENT_ID"] = "bunnings_client"

    # Project credentials - racv
    os.environ["OPTIMA_RACV_USERNAME"] = "racv@test.com"
    os.environ["OPTIMA_RACV_PASSWORD"] = "racv_pass"
    os.environ["OPTIMA_RACV_CLIENT_ID"] = "racv_client"

    yield

    # Restore original env
    os.environ.clear()
    os.environ.update(original_env)


@pytest.fixture
def mock_lambda_context() -> MagicMock:
    """Create mock Lambda context."""
    context = MagicMock()
    context.function_name = "sbm-optima-exporter"
    context.memory_limit_in_mb = 256
    context.invoked_function_arn = "arn:aws:lambda:ap-southeast-2:123456789012:function:sbm-optima-exporter"
    context.aws_request_id = "test-request-id"
    return context


def reload_app_module() -> Any:
    """Reload the app module with fresh environment."""
    import src.functions.optima_exporter.app as app_module

    app_module._dynamodb = None
    importlib.reload(app_module)
    return app_module


# ================================
# TestGetDynamodb
# ================================
class TestGetDynamodb:
    """Tests for get_dynamodb function."""

    @mock_aws
    def test_lazy_initialization(self) -> None:
        """Test that DynamoDB resource is lazily initialized."""
        app_module = reload_app_module()

        # First call should create the resource
        result1 = app_module.get_dynamodb()
        assert result1 is not None

        # Second call should return the same resource
        result2 = app_module.get_dynamodb()
        assert result1 is result2

    @mock_aws
    def test_singleton_pattern(self) -> None:
        """Test that get_dynamodb returns the same instance."""
        app_module = reload_app_module()

        result1 = app_module.get_dynamodb()
        result2 = app_module.get_dynamodb()
        result3 = app_module.get_dynamodb()

        assert result1 is result2 is result3


# ================================
# TestGetProjectConfig
# ================================
class TestGetProjectConfig:
    """Tests for get_project_config function."""

    def test_returns_config_when_all_env_vars_present(self) -> None:
        """Test that config is returned when all credentials are present."""
        app_module = reload_app_module()

        config = app_module.get_project_config("bunnings")

        assert config is not None
        assert config["username"] == "bunnings@test.com"
        assert config["password"] == "bunnings_pass"
        assert config["client_id"] == "bunnings_client"
        assert "recipients" in config

    def test_returns_none_when_username_missing(self) -> None:
        """Test that None is returned when username is missing."""
        os.environ.pop("OPTIMA_BUNNINGS_USERNAME", None)
        app_module = reload_app_module()

        config = app_module.get_project_config("bunnings")
        assert config is None

    def test_returns_none_when_password_missing(self) -> None:
        """Test that None is returned when password is missing."""
        os.environ.pop("OPTIMA_BUNNINGS_PASSWORD", None)
        app_module = reload_app_module()

        config = app_module.get_project_config("bunnings")
        assert config is None

    def test_returns_none_when_client_id_missing(self) -> None:
        """Test that None is returned when client_id is missing."""
        os.environ.pop("OPTIMA_BUNNINGS_CLIENT_ID", None)
        app_module = reload_app_module()

        config = app_module.get_project_config("bunnings")
        assert config is None


# ================================
# TestGetSitesForProject
# ================================
class TestGetSitesForProject:
    """Tests for get_sites_for_project function."""

    @mock_aws
    def test_returns_sites_from_dynamodb(self) -> None:
        """Test that sites are fetched from DynamoDB."""
        # Create DynamoDB table and add data
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

        app_module = reload_app_module()
        sites = app_module.get_sites_for_project("bunnings")

        assert len(sites) == 2
        assert {"nmi": "NMI001", "siteIdStr": "site-guid-001"} in sites
        assert {"nmi": "NMI002", "siteIdStr": "site-guid-002"} in sites

    @mock_aws
    def test_handles_pagination(self) -> None:
        """Test that pagination is handled for large datasets."""
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

        # Add many items
        for i in range(50):
            table.put_item(Item={"project": "bunnings", "nmi": f"NMI{i:03d}", "siteIdStr": f"site-guid-{i:03d}"})

        app_module = reload_app_module()
        sites = app_module.get_sites_for_project("bunnings")
        assert len(sites) == 50

    @mock_aws
    def test_filters_invalid_sites(self) -> None:
        """Test that sites missing required fields are filtered out."""
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
        table.put_item(Item={"project": "bunnings", "nmi": "NMI002"})  # Missing siteIdStr

        app_module = reload_app_module()
        sites = app_module.get_sites_for_project("bunnings")

        assert len(sites) == 1
        assert sites[0]["nmi"] == "NMI001"

    @mock_aws
    def test_returns_empty_list_when_no_sites(self) -> None:
        """Test that empty list is returned when no sites exist."""
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

        app_module = reload_app_module()
        sites = app_module.get_sites_for_project("nonexistent")
        assert sites == []

    @mock_aws
    def test_handles_extra_fields(self) -> None:
        """Test handling of items with extra fields."""
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
        table.put_item(
            Item={"project": "bunnings", "nmi": "NMI001", "siteIdStr": "site-guid-001", "extra_field": "ignored"}
        )

        app_module = reload_app_module()
        sites = app_module.get_sites_for_project("bunnings")

        assert len(sites) == 1
        assert sites[0] == {"nmi": "NMI001", "siteIdStr": "site-guid-001"}


# ================================
# TestGetSiteByNmi
# ================================
class TestGetSiteByNmi:
    """Tests for get_site_by_nmi function."""

    @mock_aws
    def test_returns_site_when_found(self) -> None:
        """Test that site is returned when found."""
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

        app_module = reload_app_module()
        site = app_module.get_site_by_nmi("bunnings", "NMI001")

        assert site is not None
        assert site["nmi"] == "NMI001"
        assert site["siteIdStr"] == "site-guid-001"

    @mock_aws
    def test_returns_none_when_not_found(self) -> None:
        """Test that None is returned when site not found."""
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
        # No data inserted

        app_module = reload_app_module()
        site = app_module.get_site_by_nmi("bunnings", "NONEXISTENT")

        assert site is None

    @mock_aws
    def test_returns_none_when_missing_siteIdStr(self) -> None:
        """Test that None is returned when site exists but missing siteIdStr."""
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
        # Insert item without siteIdStr
        table.put_item(Item={"project": "bunnings", "nmi": "NMI001", "otherField": "value"})

        app_module = reload_app_module()
        site = app_module.get_site_by_nmi("bunnings", "NMI001")

        assert site is None

    @mock_aws
    def test_excludes_extra_fields(self) -> None:
        """Test that only nmi and siteIdStr are returned."""
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
        table.put_item(
            Item={
                "project": "bunnings",
                "nmi": "NMI001",
                "siteIdStr": "site-guid-001",
                "extraField": "ignored",
                "anotherField": 123,
            }
        )

        app_module = reload_app_module()
        site = app_module.get_site_by_nmi("bunnings", "NMI001")

        assert site is not None
        assert site == {"nmi": "NMI001", "siteIdStr": "site-guid-001"}
        assert "extraField" not in site

    @mock_aws
    def test_case_sensitive_lookup(self) -> None:
        """Test that lookup is case-sensitive."""
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

        app_module = reload_app_module()

        # Lowercase nmi should not match
        site = app_module.get_site_by_nmi("bunnings", "nmi001")
        assert site is None

        # Uppercase project should not match
        site = app_module.get_site_by_nmi("BUNNINGS", "NMI001")
        assert site is None


# ================================
# TestGetDateRange
# ================================
class TestGetDateRange:
    """Tests for get_date_range function."""

    @freeze_time("2026-01-23 10:00:00")
    def test_returns_correct_date_range(self) -> None:
        """Test that correct date range is calculated."""
        app_module = reload_app_module()

        start_date, end_date = app_module.get_date_range()

        # End date should be yesterday (2026-01-22)
        # Start date should be 7 days back from end_date (2026-01-16)
        assert end_date == "2026-01-22"
        assert start_date == "2026-01-16"

    @freeze_time("2026-01-23 10:00:00")
    def test_respects_optima_days_back(self) -> None:
        """Test that OPTIMA_DAYS_BACK is respected."""
        os.environ["OPTIMA_DAYS_BACK"] = "14"
        app_module = reload_app_module()

        start_date, end_date = app_module.get_date_range()

        assert end_date == "2026-01-22"
        assert start_date == "2026-01-09"

    @freeze_time("2026-01-01 10:00:00")
    def test_end_date_is_yesterday(self) -> None:
        """Test that end date is always yesterday."""
        app_module = reload_app_module()

        _start_date, end_date = app_module.get_date_range()

        assert end_date == "2025-12-31"


# ================================
# TestFormatDateForUrl
# ================================
class TestFormatDateForUrl:
    """Tests for format_date_for_url function."""

    def test_formats_date_correctly(self) -> None:
        """Test that date is formatted correctly for URL."""
        from src.functions.optima_exporter.app import format_date_for_url

        result = format_date_for_url("2026-01-15")
        assert result == "15 Jan 2026"

    def test_handles_different_months(self) -> None:
        """Test formatting for different months."""
        from src.functions.optima_exporter.app import format_date_for_url

        assert format_date_for_url("2026-12-01") == "01 Dec 2026"
        assert format_date_for_url("2026-06-15") == "15 Jun 2026"
        assert format_date_for_url("2026-09-30") == "30 Sep 2026"

    def test_handles_leap_year(self) -> None:
        """Test formatting for leap year date."""
        from src.functions.optima_exporter.app import format_date_for_url

        result = format_date_for_url("2024-02-29")
        assert result == "29 Feb 2024"


# ================================
# TestLoginBidenergy
# ================================
class TestLoginBidenergy:
    """Tests for login_bidenergy function."""

    @responses.activate
    def test_successful_login_returns_cookie(self) -> None:
        """Test that successful login returns cookie string."""
        from src.functions.optima_exporter.app import login_bidenergy

        responses.add(
            responses.POST,
            "https://app.bidenergy.com/Account/LogOn",
            status=302,
            headers={"Set-Cookie": ".ASPXAUTH=token123; path=/"},
        )

        result = login_bidenergy("user@test.com", "password", "ClientId")

        assert result is not None
        assert ".ASPXAUTH=token123" in result

    @responses.activate
    def test_failed_login_returns_none(self) -> None:
        """Test that failed login (200 response) returns None."""
        from src.functions.optima_exporter.app import login_bidenergy

        responses.add(
            responses.POST,
            "https://app.bidenergy.com/Account/LogOn",
            status=200,
            body="Login failed",
        )

        result = login_bidenergy("user@test.com", "wrong_password", "ClientId")
        assert result is None

    @responses.activate
    def test_missing_aspxauth_cookie_returns_none(self) -> None:
        """Test that missing .ASPXAUTH cookie returns None."""
        from src.functions.optima_exporter.app import login_bidenergy

        responses.add(
            responses.POST,
            "https://app.bidenergy.com/Account/LogOn",
            status=302,
            headers={"Set-Cookie": "other_cookie=value; path=/"},
        )

        result = login_bidenergy("user@test.com", "password", "ClientId")
        assert result is None

    @responses.activate
    def test_network_error_returns_none(self) -> None:
        """Test that network error returns None."""
        import requests as req_lib

        from src.functions.optima_exporter.app import login_bidenergy

        responses.add(
            responses.POST,
            "https://app.bidenergy.com/Account/LogOn",
            body=req_lib.exceptions.ConnectionError("Network error"),
        )

        result = login_bidenergy("user@test.com", "password", "ClientId")
        assert result is None

    @responses.activate
    def test_non_302_response_returns_none(self) -> None:
        """Test that non-302 response returns None."""
        from src.functions.optima_exporter.app import login_bidenergy

        responses.add(
            responses.POST,
            "https://app.bidenergy.com/Account/LogOn",
            status=500,
            body="Server error",
        )

        result = login_bidenergy("user@test.com", "password", "ClientId")
        assert result is None


# ================================
# TestDownloadCsv
# ================================
class TestDownloadCsv:
    """Tests for download_csv function."""

    @responses.activate
    def test_successful_download_returns_content(self) -> None:
        """Test that successful download returns CSV content and filename."""
        from src.functions.optima_exporter.app import download_csv

        csv_content = b"Date,Value\n2026-01-01,100\n2026-01-02,200"
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportActualIntervalUsageProfile",
            status=200,
            body=csv_content,
            content_type="text/csv",
        )

        result = download_csv(
            cookies=".ASPXAUTH=token123",
            site_id_str="site-guid-001",
            start_date="2026-01-01",
            end_date="2026-01-07",
            project="bunnings",
            nmi="NMI001",
        )

        assert result is not None
        content, filename = result
        assert content == csv_content
        assert filename == "optima_bunnings_NMI#NMI001_2026-01-01_2026-01-07.csv"

    @responses.activate
    def test_returns_none_on_http_error(self) -> None:
        """Test that HTTP error returns None."""
        from src.functions.optima_exporter.app import download_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportActualIntervalUsageProfile",
            status=500,
            body="Server error",
        )

        result = download_csv(
            cookies=".ASPXAUTH=token123",
            site_id_str="site-guid-001",
            start_date="2026-01-01",
            end_date="2026-01-07",
            project="bunnings",
            nmi="NMI001",
        )

        assert result is None

    @responses.activate
    def test_detects_html_error_page(self) -> None:
        """Test that HTML error page is detected and returns None."""
        from src.functions.optima_exporter.app import download_csv

        html_content = b"<!DOCTYPE html><html><body>Error</body></html>"
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportActualIntervalUsageProfile",
            status=200,
            body=html_content,
            content_type="text/html",
        )

        result = download_csv(
            cookies=".ASPXAUTH=token123",
            site_id_str="site-guid-001",
            start_date="2026-01-01",
            end_date="2026-01-07",
            project="bunnings",
            nmi="NMI001",
        )

        assert result is None

    @responses.activate
    def test_generates_correct_filename(self) -> None:
        """Test that filename is generated correctly with uppercase project and NMI."""
        from src.functions.optima_exporter.app import download_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportActualIntervalUsageProfile",
            status=200,
            body=b"data",
            content_type="text/csv",
        )

        result = download_csv(
            cookies=".ASPXAUTH=token123",
            site_id_str="site-guid-001",
            start_date="2026-01-01",
            end_date="2026-01-07",
            project="RACV",  # Uppercase project
            nmi="nmi123",  # Lowercase NMI
        )

        assert result is not None
        _, filename = result
        assert filename == "optima_racv_NMI#NMI123_2026-01-01_2026-01-07.csv"

    @responses.activate
    def test_handles_timeout(self) -> None:
        """Test that timeout is handled gracefully."""
        import requests as req_lib

        from src.functions.optima_exporter.app import download_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportActualIntervalUsageProfile",
            body=req_lib.exceptions.Timeout("Timeout"),
        )

        result = download_csv(
            cookies=".ASPXAUTH=token123",
            site_id_str="site-guid-001",
            start_date="2026-01-01",
            end_date="2026-01-07",
            project="bunnings",
            nmi="NMI001",
        )

        assert result is None

    @responses.activate
    def test_handles_large_file(self) -> None:
        """Test that large CSV files are handled."""
        from src.functions.optima_exporter.app import download_csv

        # Generate large CSV content
        large_content = b"Date,Value\n" + b"2026-01-01,100\n" * 100000
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportActualIntervalUsageProfile",
            status=200,
            body=large_content,
            content_type="text/csv",
        )

        result = download_csv(
            cookies=".ASPXAUTH=token123",
            site_id_str="site-guid-001",
            start_date="2026-01-01",
            end_date="2026-01-07",
            project="bunnings",
            nmi="NMI001",
        )

        assert result is not None
        content, _ = result
        assert len(content) == len(large_content)


# ================================
# TestSendEmailWithAttachment
# ================================
class TestSendEmailWithAttachment:
    """Tests for send_email_with_attachment function."""

    @mock_aws
    def test_sends_email_successfully(self, tmp_path: Path) -> None:
        """Test that email is sent successfully via SES."""
        # Verify and setup SES identity
        ses_client = boto3.client("ses", region_name="ap-southeast-2")
        ses_client.verify_email_identity(EmailAddress="test@example.com")
        ses_client.verify_email_identity(EmailAddress="recipient@test.com")

        app_module = reload_app_module()

        # Create test file
        test_file = tmp_path / "test.csv"
        test_file.write_text("Date,Value\n2026-01-01,100")

        result = app_module.send_email_with_attachment(
            file_path=str(test_file),
            recipients=["recipient@test.com"],
            subject="Test Subject",
            body="Test Body",
        )

        assert result is True

    def test_returns_false_on_ses_error(self, tmp_path: Path) -> None:
        """Test that False is returned on SES error."""
        app_module = reload_app_module()

        test_file = tmp_path / "test.csv"
        test_file.write_text("data")

        # Mock SES client to raise an error
        with patch("boto3.client") as mock_client:
            mock_ses = MagicMock()
            mock_client.return_value = mock_ses
            mock_ses.send_raw_email.side_effect = ClientError(
                {"Error": {"Code": "MessageRejected", "Message": "Email rejected"}},
                "SendRawEmail",
            )

            result = app_module.send_email_with_attachment(
                file_path=str(test_file),
                recipients=["recipient@test.com"],
                subject="Test Subject",
                body="Test Body",
            )

            assert result is False

    @mock_aws
    def test_attaches_file_correctly(self, tmp_path: Path) -> None:
        """Test that file is attached correctly."""
        # Verify SES identities
        ses_client = boto3.client("ses", region_name="ap-southeast-2")
        ses_client.verify_email_identity(EmailAddress="test@example.com")
        ses_client.verify_email_identity(EmailAddress="recipient@test.com")

        app_module = reload_app_module()

        test_file = tmp_path / "test_file.csv"
        test_file.write_text("Date,Value\n2026-01-01,100")

        # Mock SES client to capture the raw message
        with patch("boto3.client") as mock_client:
            mock_ses = MagicMock()
            mock_client.return_value = mock_ses
            mock_ses.send_raw_email.return_value = {"MessageId": "test-message-id"}

            app_module.send_email_with_attachment(
                file_path=str(test_file),
                recipients=["recipient@test.com"],
                subject="Test Subject",
                body="Test Body",
            )

            # Verify send_raw_email was called
            mock_ses.send_raw_email.assert_called_once()
            call_kwargs = mock_ses.send_raw_email.call_args[1]
            raw_message = call_kwargs["RawMessage"]["Data"]

            assert "test_file.csv" in raw_message
            assert "Test Subject" in raw_message

    @mock_aws
    def test_handles_multiple_recipients(self, tmp_path: Path) -> None:
        """Test that multiple recipients are handled."""
        # Verify SES identities
        ses_client = boto3.client("ses", region_name="ap-southeast-2")
        ses_client.verify_email_identity(EmailAddress="test@example.com")
        for email in ["user1@test.com", "user2@test.com", "user3@test.com"]:
            ses_client.verify_email_identity(EmailAddress=email)

        app_module = reload_app_module()

        test_file = tmp_path / "test.csv"
        test_file.write_text("data")

        # Mock SES client to capture the recipients
        with patch("boto3.client") as mock_client:
            mock_ses = MagicMock()
            mock_client.return_value = mock_ses
            mock_ses.send_raw_email.return_value = {"MessageId": "test-message-id"}

            recipients = ["user1@test.com", "user2@test.com", "user3@test.com"]
            app_module.send_email_with_attachment(
                file_path=str(test_file),
                recipients=recipients,
                subject="Test Subject",
                body="Test Body",
            )

            # Verify send_raw_email was called with all recipients
            call_kwargs = mock_ses.send_raw_email.call_args[1]
            assert call_kwargs["Destinations"] == recipients


# ================================
# TestProcessSite
# ================================
class TestProcessSite:
    """Tests for process_site function."""

    @responses.activate
    def test_successful_process_returns_success(self) -> None:
        """Test that successful processing returns success result."""
        from src.functions.optima_exporter.app import process_site

        csv_content = b"Date,Value\n2026-01-01,100"
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportActualIntervalUsageProfile",
            status=200,
            body=csv_content,
            content_type="text/csv",
        )

        with patch("src.functions.optima_exporter.app.send_email_with_attachment") as mock_send:
            mock_send.return_value = True

            result = process_site(
                cookies=".ASPXAUTH=token123",
                nmi="NMI001",
                site_id_str="site-guid-001",
                start_date="2026-01-01",
                end_date="2026-01-07",
                recipients=["test@example.com"],
                project="bunnings",
            )

            assert result["success"] is True
            assert result["nmi"] == "NMI001"
            assert result["error"] is None
            assert "filename" in result

    @responses.activate
    def test_download_failure_returns_error(self) -> None:
        """Test that download failure returns error result."""
        from src.functions.optima_exporter.app import process_site

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportActualIntervalUsageProfile",
            status=500,
        )

        result = process_site(
            cookies=".ASPXAUTH=token123",
            nmi="NMI001",
            site_id_str="site-guid-001",
            start_date="2026-01-01",
            end_date="2026-01-07",
            recipients=["test@example.com"],
            project="bunnings",
        )

        assert result["success"] is False
        assert result["error"] == "Failed to download CSV"

    @responses.activate
    def test_email_failure_returns_error(self) -> None:
        """Test that email failure returns error result."""
        from src.functions.optima_exporter.app import process_site

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportActualIntervalUsageProfile",
            status=200,
            body=b"data",
            content_type="text/csv",
        )

        with patch("src.functions.optima_exporter.app.send_email_with_attachment") as mock_send:
            mock_send.return_value = False

            result = process_site(
                cookies=".ASPXAUTH=token123",
                nmi="NMI001",
                site_id_str="site-guid-001",
                start_date="2026-01-01",
                end_date="2026-01-07",
                recipients=["test@example.com"],
                project="bunnings",
            )

            assert result["success"] is False
            assert result["error"] == "Failed to send email"

    @responses.activate
    def test_cleans_up_temp_file(self) -> None:
        """Test that temporary file is cleaned up after processing."""
        from src.functions.optima_exporter.app import process_site

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportActualIntervalUsageProfile",
            status=200,
            body=b"data",
            content_type="text/csv",
        )

        created_file_path = None

        def capture_file_path(file_path: str, *args: Any, **kwargs: Any) -> bool:
            nonlocal created_file_path
            created_file_path = file_path
            return True

        with patch("src.functions.optima_exporter.app.send_email_with_attachment", side_effect=capture_file_path):
            process_site(
                cookies=".ASPXAUTH=token123",
                nmi="NMI001",
                site_id_str="site-guid-001",
                start_date="2026-01-01",
                end_date="2026-01-07",
                recipients=["test@example.com"],
                project="bunnings",
            )

        # Verify file was cleaned up
        assert created_file_path is not None
        assert not Path(created_file_path).exists()

    @responses.activate
    def test_handles_file_write_error(self) -> None:
        """Test that file write error is handled gracefully."""
        from src.functions.optima_exporter.app import process_site

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportActualIntervalUsageProfile",
            status=200,
            body=b"data",
            content_type="text/csv",
        )

        with patch("pathlib.Path.write_bytes", side_effect=OSError("Disk full")):
            result = process_site(
                cookies=".ASPXAUTH=token123",
                nmi="NMI001",
                site_id_str="site-guid-001",
                start_date="2026-01-01",
                end_date="2026-01-07",
                recipients=["test@example.com"],
                project="bunnings",
            )

            assert result["success"] is False
            assert "File operation failed" in result["error"]


# ================================
# TestProcessScheduledExport
# ================================
class TestProcessScheduledExport:
    """Tests for process_scheduled_export function."""

    @freeze_time("2026-01-23 10:00:00")
    @mock_aws
    def test_processes_all_configured_projects(self) -> None:
        """Test that all configured projects are processed."""
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
        table.put_item(Item={"project": "racv", "nmi": "NMI002", "siteIdStr": "site-guid-002"})

        app_module = reload_app_module()

        with (
            patch.object(app_module, "login_bidenergy", return_value=".ASPXAUTH=token"),
            patch.object(app_module, "process_site") as mock_process,
        ):
            mock_process.return_value = {"success": True, "nmi": "NMI001"}

            result = app_module.process_scheduled_export()

            # Should have processed both projects
            assert mock_process.call_count == 2
            assert result["statusCode"] == 200

    @freeze_time("2026-01-23 10:00:00")
    @mock_aws
    def test_skips_projects_without_credentials(self) -> None:
        """Test that projects without credentials are skipped."""
        os.environ.pop("OPTIMA_RACV_USERNAME", None)

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
        table.put_item(Item={"project": "racv", "nmi": "NMI002", "siteIdStr": "site-guid-002"})

        app_module = reload_app_module()

        with (
            patch.object(app_module, "login_bidenergy", return_value=".ASPXAUTH=token"),
            patch.object(app_module, "process_site") as mock_process,
        ):
            mock_process.return_value = {"success": True}

            result = app_module.process_scheduled_export()

            # Should only process bunnings (racv has no credentials)
            assert mock_process.call_count == 1
            # Should have error for racv
            errors = [r for r in result["body"]["results"] if r.get("error") == "Missing credentials"]
            assert len(errors) == 1

    @freeze_time("2026-01-23 10:00:00")
    @mock_aws
    def test_skips_projects_without_sites(self) -> None:
        """Test that projects without configured sites are skipped."""
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
        # Only bunnings has sites, racv has none
        table.put_item(Item={"project": "bunnings", "nmi": "NMI001", "siteIdStr": "site-guid-001"})

        app_module = reload_app_module()

        with (
            patch.object(app_module, "login_bidenergy", return_value=".ASPXAUTH=token"),
            patch.object(app_module, "process_site") as mock_process,
        ):
            mock_process.return_value = {"success": True}

            result = app_module.process_scheduled_export()

            # Should only process bunnings
            assert mock_process.call_count == 1
            # Should have error for racv (no sites)
            errors = [r for r in result["body"]["results"] if r.get("error") == "No sites configured"]
            assert len(errors) == 1

    @freeze_time("2026-01-23 10:00:00")
    @mock_aws
    def test_handles_login_failure(self) -> None:
        """Test that login failure raises RuntimeError when all projects fail."""
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

        app_module = reload_app_module()

        with (
            patch.object(app_module, "login_bidenergy", return_value=None),
            patch.object(app_module, "process_site") as mock_process,
            pytest.raises(RuntimeError, match="All projects failed to process"),
        ):
            app_module.process_scheduled_export()

            # Should not process any sites
            mock_process.assert_not_called()

    @freeze_time("2026-01-23 10:00:00")
    @mock_aws
    def test_returns_207_on_partial_failure(self) -> None:
        """Test that 207 status is returned on partial failure."""
        # Use only one project to avoid counting "no sites" as an error
        os.environ["OPTIMA_PROJECTS"] = "bunnings"

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

        app_module = reload_app_module()

        # Return success for first site, failure for second
        call_count = [0]

        def mock_process(*args: Any, **kwargs: Any) -> dict[str, Any]:
            call_count[0] += 1
            if call_count[0] == 1:
                return {"success": True, "nmi": "NMI001"}
            return {"success": False, "nmi": "NMI002", "error": "Download failed"}

        with (
            patch.object(app_module, "login_bidenergy", return_value=".ASPXAUTH=token"),
            patch.object(app_module, "process_site", side_effect=mock_process),
        ):
            result = app_module.process_scheduled_export()

            assert result["statusCode"] == 207
            assert result["body"]["success_count"] == 1
            assert result["body"]["error_count"] == 1


# ================================
# TestProcessOndemandExport
# ================================
class TestProcessOndemandExport:
    """Tests for process_ondemand_export function."""

    def test_validates_project_required(self) -> None:
        """Test that project is required."""
        from src.functions.optima_exporter.app import process_ondemand_export

        result = process_ondemand_export({})
        assert result["statusCode"] == 400
        assert "project" in result["body"]

    def test_validates_project_credentials(self) -> None:
        """Test that project credentials must exist."""
        from src.functions.optima_exporter.app import process_ondemand_export

        result = process_ondemand_export({"project": "unknown_project"})
        assert result["statusCode"] == 400
        assert "No credentials configured" in result["body"]

    @mock_aws
    @freeze_time("2026-01-23 10:00:00")
    def test_ondemand_with_project_only(self) -> None:
        """Test on-demand export with only project (exports all NMIs with default dates)."""
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

        app_module = reload_app_module()

        with (
            patch.object(app_module, "login_bidenergy", return_value=".ASPXAUTH=token"),
            patch.object(app_module, "process_site") as mock_process,
        ):
            mock_process.return_value = {"success": True, "nmi": "NMI001"}

            result = app_module.process_ondemand_export({"project": "bunnings"})

            assert result["statusCode"] == 200
            assert mock_process.call_count == 2
            # Verify default dates are used (2026-01-16 to 2026-01-22)
            assert result["body"]["date_range"]["start"] == "2026-01-16"
            assert result["body"]["date_range"]["end"] == "2026-01-22"

    @mock_aws
    @freeze_time("2026-01-23 10:00:00")
    def test_ondemand_with_project_and_nmi(self) -> None:
        """Test on-demand export with project and specific NMI."""
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

        app_module = reload_app_module()

        with (
            patch.object(app_module, "login_bidenergy", return_value=".ASPXAUTH=token"),
            patch.object(app_module, "process_site") as mock_process,
        ):
            mock_process.return_value = {"success": True, "nmi": "NMI001"}

            result = app_module.process_ondemand_export({"project": "bunnings", "nmi": "NMI001"})

            assert result["statusCode"] == 200
            # Only 1 site should be processed
            assert mock_process.call_count == 1
            assert result["body"]["success_count"] == 1

    @mock_aws
    def test_ondemand_with_date_range(self) -> None:
        """Test on-demand export with project and custom date range."""
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

        app_module = reload_app_module()

        with (
            patch.object(app_module, "login_bidenergy", return_value=".ASPXAUTH=token"),
            patch.object(app_module, "process_site") as mock_process,
        ):
            mock_process.return_value = {"success": True, "nmi": "NMI001"}

            result = app_module.process_ondemand_export(
                {
                    "project": "bunnings",
                    "startDate": "2026-01-01",
                    "endDate": "2026-01-07",
                }
            )

            assert result["statusCode"] == 200
            assert result["body"]["date_range"]["start"] == "2026-01-01"
            assert result["body"]["date_range"]["end"] == "2026-01-07"

    @mock_aws
    def test_ondemand_with_all_params(self) -> None:
        """Test on-demand export with project, NMI, and date range."""
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

        app_module = reload_app_module()

        with (
            patch.object(app_module, "login_bidenergy", return_value=".ASPXAUTH=token"),
            patch.object(app_module, "process_site") as mock_process,
        ):
            mock_process.return_value = {"success": True, "nmi": "NMI001"}

            result = app_module.process_ondemand_export(
                {
                    "project": "bunnings",
                    "nmi": "NMI001",
                    "startDate": "2026-01-15",
                    "endDate": "2026-01-22",
                }
            )

            assert result["statusCode"] == 200
            assert mock_process.call_count == 1
            # Verify correct NMI and dates passed to process_site
            call_args = mock_process.call_args
            assert call_args[0][1] == "NMI001"  # nmi
            assert call_args[0][3] == "2026-01-15"  # start_date
            assert call_args[0][4] == "2026-01-22"  # end_date

    @mock_aws
    def test_ondemand_nmi_not_found(self) -> None:
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

        app_module = reload_app_module()

        result = app_module.process_ondemand_export({"project": "bunnings", "nmi": "NONEXISTENT"})

        assert result["statusCode"] == 404
        assert "not found" in result["body"].lower()

    @mock_aws
    def test_ondemand_no_sites_for_project(self) -> None:
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

        app_module = reload_app_module()

        result = app_module.process_ondemand_export({"project": "bunnings"})

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

        app_module = reload_app_module()

        with patch.object(app_module, "login_bidenergy", return_value=None):
            result = app_module.process_ondemand_export(
                {
                    "project": "bunnings",
                    "nmi": "NMI001",
                    "startDate": "2026-01-01",
                    "endDate": "2026-01-07",
                }
            )

            assert result["statusCode"] == 401
            assert "authenticate" in result["body"].lower()

    @mock_aws
    def test_processes_sites_successfully(self) -> None:
        """Test that sites are processed successfully."""
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

        app_module = reload_app_module()

        with (
            patch.object(app_module, "login_bidenergy", return_value=".ASPXAUTH=token"),
            patch.object(app_module, "process_site") as mock_process,
        ):
            mock_process.return_value = {"success": True, "nmi": "NMI001"}

            result = app_module.process_ondemand_export(
                {
                    "project": "bunnings",
                    "nmi": "NMI001",
                    "startDate": "2026-01-01",
                    "endDate": "2026-01-07",
                }
            )

            assert result["statusCode"] == 200
            assert result["body"]["success_count"] == 1
            mock_process.assert_called_once()

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

        app_module = reload_app_module()

        call_count = [0]

        def mock_process(*args: Any, **kwargs: Any) -> dict[str, Any]:
            call_count[0] += 1
            if call_count[0] == 1:
                return {"success": True, "nmi": "NMI001"}
            return {"success": False, "nmi": "NMI002", "error": "Failed"}

        with (
            patch.object(app_module, "login_bidenergy", return_value=".ASPXAUTH=token"),
            patch.object(app_module, "process_site", side_effect=mock_process),
        ):
            result = app_module.process_ondemand_export(
                {
                    "project": "bunnings",
                    "startDate": "2026-01-01",
                    "endDate": "2026-01-07",
                }
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

        app_module = reload_app_module()

        with (
            patch.object(app_module, "login_bidenergy", return_value=".ASPXAUTH=token"),
            patch.object(app_module, "process_site") as mock_process,
        ):
            mock_process.return_value = {"success": True}

            app_module.process_ondemand_export(
                {
                    "project": "BUNNINGS",  # Uppercase
                    "nmi": "NMI001",
                    "startDate": "2026-01-01",
                    "endDate": "2026-01-07",
                }
            )

            # Verify process_site was called with lowercase project
            call_args = mock_process.call_args
            assert call_args[0][6] == "bunnings"  # project is 7th argument


# ================================
# TestLambdaHandler
# ================================
class TestLambdaHandler:
    """Tests for lambda_handler function."""

    def test_empty_event_triggers_scheduled_export(self, mock_lambda_context: MagicMock) -> None:
        """Test that empty event triggers scheduled export."""
        from src.functions.optima_exporter.app import lambda_handler

        with patch("src.functions.optima_exporter.app.process_scheduled_export") as mock_scheduled:
            mock_scheduled.return_value = {"statusCode": 200, "body": {}}

            lambda_handler({}, mock_lambda_context)

            mock_scheduled.assert_called_once()

    def test_eventbridge_event_triggers_scheduled_export(self, mock_lambda_context: MagicMock) -> None:
        """Test that EventBridge event triggers scheduled export."""
        from src.functions.optima_exporter.app import lambda_handler

        with patch("src.functions.optima_exporter.app.process_scheduled_export") as mock_scheduled:
            mock_scheduled.return_value = {"statusCode": 200, "body": {}}

            # EventBridge event without project/sites
            event = {"version": "0", "source": "aws.events", "detail-type": "Scheduled Event"}

            lambda_handler(event, mock_lambda_context)

            mock_scheduled.assert_called_once()

    def test_event_with_project_triggers_ondemand_export(self, mock_lambda_context: MagicMock) -> None:
        """Test that event with project triggers on-demand export."""
        from src.functions.optima_exporter.app import lambda_handler

        with patch("src.functions.optima_exporter.app.process_ondemand_export") as mock_ondemand:
            mock_ondemand.return_value = {"statusCode": 200, "body": {}}

            event = {
                "project": "bunnings",
                "nmi": "NMI001",
                "startDate": "2026-01-01",
                "endDate": "2026-01-07",
            }

            lambda_handler(event, mock_lambda_context)

            mock_ondemand.assert_called_once_with(event)

    def test_event_with_any_param_triggers_ondemand_export(self, mock_lambda_context: MagicMock) -> None:
        """Test that any non-empty event triggers on-demand export."""
        from src.functions.optima_exporter.app import lambda_handler

        with patch("src.functions.optima_exporter.app.process_ondemand_export") as mock_ondemand:
            mock_ondemand.return_value = {"statusCode": 400, "body": "Missing project"}

            # Event with any parameter should trigger on-demand export
            event = {"project": "bunnings"}

            lambda_handler(event, mock_lambda_context)

            mock_ondemand.assert_called_once_with(event)


# ================================
# TestConfiguration
# ================================
class TestConfiguration:
    """Tests for configuration and environment variable handling."""

    def test_default_ses_sender(self) -> None:
        """Test default SES sender value."""
        os.environ.pop("SES_SENDER", None)
        app_module = reload_app_module()

        assert app_module.SES_SENDER == "client_ec_data@gegroup.com.au"

    def test_default_ses_region(self) -> None:
        """Test default SES region value."""
        os.environ.pop("SES_REGION", None)
        app_module = reload_app_module()

        assert app_module.SES_REGION == "ap-southeast-2"

    def test_default_days_back(self) -> None:
        """Test default OPTIMA_DAYS_BACK value."""
        os.environ.pop("OPTIMA_DAYS_BACK", None)
        app_module = reload_app_module()

        assert app_module.OPTIMA_DAYS_BACK == 7

    def test_default_config_table(self) -> None:
        """Test default OPTIMA_CONFIG_TABLE value."""
        os.environ.pop("OPTIMA_CONFIG_TABLE", None)
        app_module = reload_app_module()

        assert app_module.OPTIMA_CONFIG_TABLE == "sbm-optima-config"

    def test_optima_projects_parsing(self) -> None:
        """Test OPTIMA_PROJECTS is parsed correctly."""
        os.environ["OPTIMA_PROJECTS"] = "  project1 , project2 , project3  "
        app_module = reload_app_module()

        assert app_module.OPTIMA_PROJECTS == ["project1", "project2", "project3"]

    def test_default_recipients_parsing(self) -> None:
        """Test DEFAULT_RECIPIENTS is parsed correctly."""
        os.environ["OPTIMA_DEFAULT_RECIPIENTS"] = "user1@test.com, user2@test.com"
        app_module = reload_app_module()

        assert app_module.DEFAULT_RECIPIENTS == ["user1@test.com", "user2@test.com"]


# ================================
# TestEdgeCases
# ================================
class TestEdgeCases:
    """Tests for edge cases and error handling."""

    @responses.activate
    def test_download_csv_with_bom(self) -> None:
        """Test that CSV with BOM prefix is handled."""
        from src.functions.optima_exporter.app import download_csv

        # CSV with BOM
        csv_content = b"\xef\xbb\xbfDate,Value\n2026-01-01,100"
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportActualIntervalUsageProfile",
            status=200,
            body=csv_content,
            content_type="text/csv",
        )

        result = download_csv(
            cookies=".ASPXAUTH=token123",
            site_id_str="site-guid-001",
            start_date="2026-01-01",
            end_date="2026-01-07",
            project="bunnings",
            nmi="NMI001",
        )

        assert result is not None

    @responses.activate
    def test_download_csv_application_csv_content_type(self) -> None:
        """Test that application/csv content type is accepted."""
        from src.functions.optima_exporter.app import download_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportActualIntervalUsageProfile",
            status=200,
            body=b"data",
            content_type="application/csv",
        )

        result = download_csv(
            cookies=".ASPXAUTH=token123",
            site_id_str="site-guid-001",
            start_date="2026-01-01",
            end_date="2026-01-07",
            project="bunnings",
            nmi="NMI001",
        )

        assert result is not None

    @freeze_time("2026-01-01 00:30:00")
    def test_get_date_range_at_midnight(self) -> None:
        """Test date range calculation at midnight."""
        app_module = reload_app_module()

        _start_date, end_date = app_module.get_date_range()

        # Should still use yesterday
        assert end_date == "2025-12-31"

    @responses.activate
    def test_login_bidenergy_multiple_cookies(self) -> None:
        """Test that multiple cookies from login are combined."""
        from src.functions.optima_exporter.app import login_bidenergy

        responses.add(
            responses.POST,
            "https://app.bidenergy.com/Account/LogOn",
            status=302,
            headers={
                "Set-Cookie": ".ASPXAUTH=token123; path=/, session=abc; path=/, other=xyz; path=/",
            },
        )

        result = login_bidenergy("user@test.com", "password", "ClientId")

        assert result is not None
        assert ".ASPXAUTH=token123" in result

    @mock_aws
    def test_get_sites_for_project_with_extra_fields(self) -> None:
        """Test that sites with extra fields are processed correctly."""
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
        table.put_item(
            Item={
                "project": "bunnings",
                "nmi": "NMI001",
                "siteIdStr": "site-guid-001",
                "extra1": "value1",
                "extra2": 123,
                "extra3": {"nested": "data"},
            }
        )

        app_module = reload_app_module()
        sites = app_module.get_sites_for_project("bunnings")

        assert len(sites) == 1
        assert sites[0] == {"nmi": "NMI001", "siteIdStr": "site-guid-001"}
        assert "extra1" not in sites[0]
