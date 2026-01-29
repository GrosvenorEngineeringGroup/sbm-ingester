"""Unit tests for interval_exporter/downloader.py module.

Tests date formatting and CSV download from BidEnergy API.
"""

import requests as req_lib
import responses


class TestFormatDateForUrl:
    """Tests for format_date_for_url function."""

    def test_formats_date_correctly(self) -> None:
        """Test that date is formatted correctly for URL."""
        from interval_exporter.downloader import format_date_for_url

        result = format_date_for_url("2026-01-15")
        assert result == "15 Jan 2026"

    def test_handles_different_months(self) -> None:
        """Test formatting for different months."""
        from interval_exporter.downloader import format_date_for_url

        assert format_date_for_url("2026-12-01") == "01 Dec 2026"
        assert format_date_for_url("2026-06-15") == "15 Jun 2026"
        assert format_date_for_url("2026-09-30") == "30 Sep 2026"

    def test_handles_leap_year(self) -> None:
        """Test formatting for leap year date."""
        from interval_exporter.downloader import format_date_for_url

        result = format_date_for_url("2024-02-29")
        assert result == "29 Feb 2024"


class TestDownloadCsv:
    """Tests for download_csv function."""

    @responses.activate
    def test_successful_download_returns_content(self) -> None:
        """Test that successful download returns CSV content and filename."""
        from interval_exporter.downloader import download_csv

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
        from interval_exporter.downloader import download_csv

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
        from interval_exporter.downloader import download_csv

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
        from interval_exporter.downloader import download_csv

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
        from interval_exporter.downloader import download_csv

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
        from interval_exporter.downloader import download_csv

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

    @responses.activate
    def test_with_bom(self) -> None:
        """Test that CSV with BOM prefix is handled."""
        from interval_exporter.downloader import download_csv

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
    def test_application_csv_content_type(self) -> None:
        """Test that application/csv content type is accepted."""
        from interval_exporter.downloader import download_csv

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
