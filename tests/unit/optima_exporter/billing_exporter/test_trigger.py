"""Unit tests for billing_exporter/trigger.py module.

Tests billing date range calculation, date validation, and report triggering.
"""

import responses


class TestBillingExporter:
    """Tests for billing exporter functionality."""

    def test_get_default_billing_date_range(self) -> None:
        """Test default billing date range calculation."""
        from billing_exporter.trigger import get_default_billing_date_range

        start_date, end_date = get_default_billing_date_range()

        # Should return dates in "Mmm YYYY" format
        assert " " in start_date
        assert " " in end_date

    def test_validate_billing_date_format_valid(self) -> None:
        """Test validation of valid billing date format."""
        from billing_exporter.trigger import validate_billing_date_format

        assert validate_billing_date_format("Jan 2026") is True
        assert validate_billing_date_format("Dec 2025") is True
        assert validate_billing_date_format("Feb 2024") is True

    def test_validate_billing_date_format_invalid(self) -> None:
        """Test validation of invalid billing date format."""
        from billing_exporter.trigger import validate_billing_date_format

        assert validate_billing_date_format("2026-01-01") is False
        assert validate_billing_date_format("January 2026") is False
        assert validate_billing_date_format("01/2026") is False

    @responses.activate
    def test_trigger_monthly_usage_report_success(self) -> None:
        """Test successful billing report trigger."""
        from billing_exporter.trigger import trigger_monthly_usage_report

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReportRead/Usage",
            status=200,
            json={"status": "queued"},
        )

        result = trigger_monthly_usage_report(
            cookies=".ASPXAUTH=token123",
            start_date="Jan 2026",
            end_date="Feb 2026",
            country="AU",
        )

        assert result["success"] is True
        assert result["country"] == "AU"

    @responses.activate
    def test_trigger_monthly_usage_report_auth_failure(self) -> None:
        """Test billing report trigger with auth failure."""
        from billing_exporter.trigger import trigger_monthly_usage_report

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReportRead/Usage",
            status=401,
        )

        result = trigger_monthly_usage_report(
            cookies=".ASPXAUTH=token123",
            start_date="Jan 2026",
            end_date="Feb 2026",
            country="AU",
        )

        assert result["success"] is False
        assert "Authentication" in result["message"]

    def test_process_billing_export_validates_project(self) -> None:
        """Test that billing export validates project."""
        from billing_exporter.trigger import process_billing_export

        result = process_billing_export(project="unknown_project")

        assert result["statusCode"] == 400
        assert "No credentials configured" in result["body"]

    def test_process_billing_export_validates_country(self) -> None:
        """Test that billing export validates country."""
        from billing_exporter.trigger import process_billing_export

        result = process_billing_export(project="racv", country="NZ")

        assert result["statusCode"] == 400
        assert "not supported" in result["body"]

    @responses.activate
    def test_process_billing_export_success(self) -> None:
        """Test successful billing export."""
        from billing_exporter.trigger import process_billing_export

        # Mock login
        responses.add(
            responses.POST,
            "https://app.bidenergy.com/Account/LogOn",
            status=302,
            headers={"Set-Cookie": ".ASPXAUTH=token123; path=/"},
        )

        # Mock report trigger
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReportRead/Usage",
            status=200,
            json={"status": "queued"},
        )

        result = process_billing_export(project="racv")

        assert result["statusCode"] == 200
        assert result["body"]["success_count"] == 1
