"""Unit tests for demand_exporter/downloader.py module.

Tests date formatting and CSV download from BidEnergy DemandProfilePartial endpoint.
"""

import re
from urllib.parse import parse_qs, urlparse

import responses


class TestFormatDateForUrl:
    def test_formats_date_correctly(self) -> None:
        from demand_exporter.downloader import format_date_for_url

        assert format_date_for_url("2026-04-29") == "29 Apr 2026"

    def test_handles_different_months(self) -> None:
        from demand_exporter.downloader import format_date_for_url

        assert format_date_for_url("2026-12-01") == "01 Dec 2026"
        assert format_date_for_url("2026-09-30") == "30 Sep 2026"

    def test_handles_leap_year(self) -> None:
        from demand_exporter.downloader import format_date_for_url

        assert format_date_for_url("2024-02-29") == "29 Feb 2024"


SAMPLE_CSV_BODY = (
    b'Commodities:,"Electricity"\r\n'
    b'Sites (NMIs):,"3117512760"\r\n'
    b'Status:,"Active"\r\n'
    b"Country:, Australia\r\n"
    b"Start:,01-Apr-2026\r\n"
    b"End:,30-Apr-2026\r\n"
    b"\r\n"
    b"\r\n"
    b"Business Unit,Identifier,Identifier Type,ReadingDateTime,E,kW,kVa,Power Factor,Site Name\r\n"
    b",3117512760,NMI,01-Apr-2026 00:00:00,59.1000,118.2000,120.3100,0.9825,RACV NOOSA RESORT\r\n"
)


class TestDownloadDemandCsvHappyPath:
    @responses.activate
    def test_successful_download_returns_content_and_filename(self) -> None:
        from demand_exporter.downloader import download_demand_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=200,
            body=SAMPLE_CSV_BODY,
            content_type="application/vnd.csv",
        )

        result = download_demand_csv(
            cookies=".ASPXAUTH=token123",
            site_id_str="4f5855e0-0563-4bdc-b2d9-aa8d0041a2ca",
            start_date="2026-04-29",
            end_date="2026-04-29",
            project="racv",
            nmi="Optima_3117512760",
            country="AU",
        )

        assert result is not None
        content, filename = result
        assert content == SAMPLE_CSV_BODY
        assert re.match(
            r"^optima_racv_demand_profile_NMI#OPTIMA_3117512760_2026-04-29_2026-04-29_\d{14}\.csv$",
            filename,
        )

    @responses.activate
    def test_request_uses_correct_url_and_params(self) -> None:
        from demand_exporter.downloader import download_demand_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=200,
            body=SAMPLE_CSV_BODY,
            content_type="application/vnd.csv",
        )

        download_demand_csv(
            cookies=".ASPXAUTH=tok",
            site_id_str="site-abc",
            start_date="2026-04-29",
            end_date="2026-04-30",
            project="racv",
            nmi="Optima_X",
            country="NZ",
        )

        assert len(responses.calls) == 1
        url = responses.calls[0].request.url
        parsed = urlparse(url)
        assert parsed.path == "/BuyerReport/DemandProfilePartial"
        params = parse_qs(parsed.query)
        assert params["isCsv"] == ["true"]
        assert params["start"] == ["29 Apr 2026"]
        assert params["end"] == ["30 Apr 2026"]
        assert params["filter.SiteIdStr"] == ["site-abc"]
        assert params["filter.SiteStatus"] == ["Active"]
        assert params["filter.commodities"] == ["Electricity"]
        assert params["filter.countrystr"] == ["NZ"]
        # Confirm there is NO `nmi` URL parameter (kept as Python arg only)
        assert "nmi" not in params

    @responses.activate
    def test_country_defaults_to_au(self) -> None:
        from demand_exporter.downloader import download_demand_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=200,
            body=SAMPLE_CSV_BODY,
            content_type="application/vnd.csv",
        )

        download_demand_csv(
            cookies=".ASPXAUTH=tok",
            site_id_str="site-abc",
            start_date="2026-04-29",
            end_date="2026-04-29",
            project="racv",
            nmi="Optima_X",
        )

        params = parse_qs(urlparse(responses.calls[0].request.url).query)
        assert params["filter.countrystr"] == ["AU"]

    @responses.activate
    def test_request_sends_cookie_header(self) -> None:
        from demand_exporter.downloader import download_demand_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=200,
            body=SAMPLE_CSV_BODY,
            content_type="application/vnd.csv",
        )

        download_demand_csv(
            cookies=".ASPXAUTH=token123",
            site_id_str="site-abc",
            start_date="2026-04-29",
            end_date="2026-04-29",
            project="racv",
            nmi="Optima_X",
        )

        assert responses.calls[0].request.headers["Cookie"] == ".ASPXAUTH=token123"

    @responses.activate
    def test_accepts_body_without_csv_content_type_when_starts_with_commodities(self) -> None:
        """Sniff trumps content-type — if body starts with `Commodities:` it is treated as CSV."""
        from demand_exporter.downloader import download_demand_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=200,
            body=SAMPLE_CSV_BODY,
            content_type="text/plain",  # wrong content-type, but body sniffs as CSV
        )

        result = download_demand_csv(
            cookies=".ASPXAUTH=tok",
            site_id_str="site-abc",
            start_date="2026-04-29",
            end_date="2026-04-29",
            project="racv",
            nmi="Optima_X",
        )

        assert result is not None


NO_DATA_BODY = (
    b'Commodities:,"Electricity"\r\n'
    b'Sites (NMIs):,"0000005438UN02B"\r\n'
    b'Status:,"Active"\r\n'
    b"Country:, New Zealand\r\n"
    b"Start:,01-May-2026\r\n"
    b"End:,03-May-2026\r\n"
    b"\r\n"
    b"\r\n"
    b"No data found"
)


class TestDownloadDemandCsvNoDataSentinel:
    @responses.activate
    def test_no_data_response_returns_bytes_for_audit(self) -> None:
        """BidEnergy returns a CSV containing 'No data found' for sites with no demand
        meter. The downloader must STILL return the bytes so the caller uploads the
        sentinel CSV to S3 for audit retention."""
        from demand_exporter.downloader import download_demand_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=200,
            body=NO_DATA_BODY,
            content_type="application/vnd.csv",
        )

        result = download_demand_csv(
            cookies=".ASPXAUTH=tok",
            site_id_str="site-empty",
            start_date="2026-05-01",
            end_date="2026-05-03",
            project="bunnings",
            nmi="Optima_NODATA",
        )

        assert result is not None
        content, filename = result
        assert content == NO_DATA_BODY
        assert b"No data found" in content
        # Filename pattern still applies to sentinel CSVs
        assert filename.startswith("optima_bunnings_demand_profile_NMI#OPTIMA_NODATA_2026-05-01_2026-05-03_")

    @responses.activate
    def test_no_data_response_logs_at_info_level(self) -> None:
        """Sentinel responses log at INFO with a distinct message — not as an error."""
        from unittest.mock import patch

        from demand_exporter import downloader as dl_module

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=200,
            body=NO_DATA_BODY,
            content_type="application/vnd.csv",
        )

        with patch.object(dl_module.logger, "info") as mock_info, patch.object(dl_module.logger, "error") as mock_error:
            dl_module.download_demand_csv(
                cookies=".ASPXAUTH=tok",
                site_id_str="site-empty",
                start_date="2026-05-01",
                end_date="2026-05-03",
                project="bunnings",
                nmi="Optima_NODATA",
            )

        # No errors logged
        mock_error.assert_not_called()
        # An info log mentioning the no-data outcome was emitted
        info_messages = [call.args[0] for call in mock_info.call_args_list]
        assert any("no data" in m.lower() for m in info_messages)


class TestDownloadDemandCsvErrors:
    @responses.activate
    def test_html_error_page_returns_none(self) -> None:
        from demand_exporter.downloader import download_demand_csv

        html = b"<!DOCTYPE html><html><body>Server error</body></html>"
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=200,
            body=html,
            content_type="text/html",
        )

        result = download_demand_csv(
            cookies=".ASPXAUTH=tok",
            site_id_str="site-abc",
            start_date="2026-04-29",
            end_date="2026-04-29",
            project="racv",
            nmi="Optima_X",
        )
        assert result is None

    @responses.activate
    def test_401_returns_none(self) -> None:
        from demand_exporter.downloader import download_demand_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=401,
            body=b"Unauthorized",
        )

        assert (
            download_demand_csv(
                cookies=".ASPXAUTH=expired",
                site_id_str="site",
                start_date="2026-04-29",
                end_date="2026-04-29",
                project="racv",
                nmi="Optima_X",
            )
            is None
        )

    @responses.activate
    def test_403_returns_none(self) -> None:
        from demand_exporter.downloader import download_demand_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=403,
            body=b"Forbidden",
        )

        assert (
            download_demand_csv(
                cookies=".ASPXAUTH=tok",
                site_id_str="site",
                start_date="2026-04-29",
                end_date="2026-04-29",
                project="racv",
                nmi="Optima_X",
            )
            is None
        )

    @responses.activate
    def test_404_returns_none(self) -> None:
        from demand_exporter.downloader import download_demand_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=404,
            body=b"Not found",
        )

        assert (
            download_demand_csv(
                cookies=".ASPXAUTH=tok",
                site_id_str="bad-site",
                start_date="2026-04-29",
                end_date="2026-04-29",
                project="racv",
                nmi="Optima_X",
            )
            is None
        )

    @responses.activate
    def test_500_returns_none(self) -> None:
        from demand_exporter.downloader import download_demand_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=500,
            body=b"Server error",
        )

        assert (
            download_demand_csv(
                cookies=".ASPXAUTH=tok",
                site_id_str="site",
                start_date="2026-04-29",
                end_date="2026-04-29",
                project="racv",
                nmi="Optima_X",
            )
            is None
        )

    @responses.activate
    def test_timeout_returns_none(self) -> None:
        import requests as req_lib
        from demand_exporter.downloader import download_demand_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            body=req_lib.Timeout("request timed out"),
        )

        assert (
            download_demand_csv(
                cookies=".ASPXAUTH=tok",
                site_id_str="site",
                start_date="2026-04-29",
                end_date="2026-04-29",
                project="racv",
                nmi="Optima_X",
            )
            is None
        )

    @responses.activate
    def test_connection_error_returns_none(self) -> None:
        import requests as req_lib
        from demand_exporter.downloader import download_demand_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            body=req_lib.ConnectionError("connection refused"),
        )

        assert (
            download_demand_csv(
                cookies=".ASPXAUTH=tok",
                site_id_str="site",
                start_date="2026-04-29",
                end_date="2026-04-29",
                project="racv",
                nmi="Optima_X",
            )
            is None
        )
