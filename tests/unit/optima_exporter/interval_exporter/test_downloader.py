"""Unit tests for interval_exporter/downloader.py date formatting helpers."""

import io
import re
import zipfile
from unittest.mock import patch
from urllib.parse import parse_qs

import pytest
import requests as _req
import responses


def _make_zip_with_csv(csv_bytes: bytes, filename: str = "report.csv") -> bytes:
    """Helper: produce in-memory ZIP wrapping a single CSV."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(filename, csv_bytes)
    return buf.getvalue()


SAMPLE_CSV = b'BuyerShortName,Country\r\n"Bunnings","AU"\r\n'
SAMPLE_SENTINEL = (
    b"BuyerShortName,Country,Commodity,Identifier,IdentifierType,DistributorId,"
    b"Date,Start Time,Usage,Generation,DemandKva,Reactive\r\n"
    b"No data is available\r\n"
)
ZIP_HAPPY_BODY = _make_zip_with_csv(SAMPLE_CSV)


class TestFormatDateForUrl:
    def test_formats_date_correctly(self) -> None:
        from interval_exporter.downloader import format_date_for_url

        assert format_date_for_url("2026-04-29") == "29 Apr 2026"

    def test_handles_different_months(self) -> None:
        from interval_exporter.downloader import format_date_for_url

        assert format_date_for_url("2026-12-01") == "01 Dec 2026"
        assert format_date_for_url("2026-09-30") == "30 Sep 2026"

    def test_handles_leap_year(self) -> None:
        from interval_exporter.downloader import format_date_for_url

        assert format_date_for_url("2024-02-29") == "29 Feb 2024"


class TestExtractFirstCsv:
    def test_returns_inner_csv_bytes_verbatim(self) -> None:
        from interval_exporter.downloader import extract_first_csv

        zip_bytes = _make_zip_with_csv(SAMPLE_CSV)
        assert extract_first_csv(zip_bytes) == SAMPLE_CSV

    def test_returns_no_data_sentinel_unchanged(self) -> None:
        """The 148-byte sentinel CSV is returned verbatim; no synthesis."""
        from interval_exporter.downloader import extract_first_csv

        zip_bytes = _make_zip_with_csv(SAMPLE_SENTINEL)
        assert extract_first_csv(zip_bytes) == SAMPLE_SENTINEL

    def test_raises_on_invalid_zip(self) -> None:
        from interval_exporter.downloader import extract_first_csv

        with pytest.raises(zipfile.BadZipFile):
            extract_first_csv(b"not a zip")

    def test_raises_on_empty_zip(self) -> None:
        """Defensive; never observed in production samples."""
        from interval_exporter.downloader import extract_first_csv

        empty_zip = io.BytesIO()
        with zipfile.ZipFile(empty_zip, "w", zipfile.ZIP_DEFLATED):
            pass

        with pytest.raises(ValueError, match="empty"):
            extract_first_csv(empty_zip.getvalue())


class TestDownloadIntervalZipHappyPath:
    @responses.activate
    def test_returns_zip_bytes_and_filename_on_success(self) -> None:
        from interval_exporter.downloader import download_interval_zip

        responses.add(
            responses.POST,
            "https://app.bidenergy.com/BuyerReport/exportdailyusagecsv",
            status=200,
            body=ZIP_HAPPY_BODY,
            content_type="application/zip",
        )

        result = download_interval_zip(
            cookies=".ASPXAUTH=tok",
            site_id_str="abc-uuid",
            start_date="2026-04-29",
            end_date="2026-04-29",
            project="bunnings",
            nmi="Optima_2002105104",
        )

        assert result is not None
        zip_bytes, filename = result
        assert zip_bytes == ZIP_HAPPY_BODY
        assert re.match(
            r"^optima_bunnings_interval_NMI#OPTIMA_2002105104_2026-04-29_2026-04-29_\d{14}\.csv$",
            filename,
        )

    @responses.activate
    def test_request_uses_correct_url_method_and_form_body(self) -> None:
        from interval_exporter.downloader import download_interval_zip

        responses.add(
            responses.POST,
            "https://app.bidenergy.com/BuyerReport/exportdailyusagecsv",
            status=200,
            body=ZIP_HAPPY_BODY,
            content_type="application/zip",
        )

        download_interval_zip(
            cookies=".ASPXAUTH=tok",
            site_id_str="abc-uuid",
            start_date="2026-04-29",
            end_date="2026-04-30",
            project="bunnings",
            nmi="Optima_X",
        )

        assert len(responses.calls) == 1
        request = responses.calls[0].request
        assert request.method == "POST"
        assert request.url == "https://app.bidenergy.com/BuyerReport/exportdailyusagecsv"
        assert "application/x-www-form-urlencoded" in request.headers["Content-Type"]
        assert request.headers["Cookie"] == ".ASPXAUTH=tok"

        form = parse_qs(request.body)
        assert form["siteId"] == ["abc-uuid"]
        assert form["start"] == ["29 Apr 2026"]
        assert form["end"] == ["30 Apr 2026"]
        assert "nmi" not in form

    @responses.activate
    def test_sentinel_zip_is_returned_unchanged(self) -> None:
        from interval_exporter.downloader import download_interval_zip

        sentinel_zip = _make_zip_with_csv(SAMPLE_SENTINEL)
        responses.add(
            responses.POST,
            "https://app.bidenergy.com/BuyerReport/exportdailyusagecsv",
            status=200,
            body=sentinel_zip,
            content_type="application/zip",
        )

        result = download_interval_zip(
            cookies=".ASPXAUTH=tok",
            site_id_str="abc-uuid",
            start_date="2026-04-29",
            end_date="2026-04-29",
            project="bunnings",
            nmi="Optima_2002105104",
        )

        assert result is not None
        zip_bytes, _filename = result
        assert zip_bytes == sentinel_zip


class TestDownloadIntervalZipErrorPaths:
    @responses.activate
    def test_returns_none_on_html_response(self) -> None:
        from interval_exporter.downloader import download_interval_zip

        responses.add(
            responses.POST,
            "https://app.bidenergy.com/BuyerReport/exportdailyusagecsv",
            status=200,
            body=b"<!doctype html><html>error page</html>",
            content_type="text/html",
        )

        assert (
            download_interval_zip(
                cookies=".ASPXAUTH=tok",
                site_id_str="abc-uuid",
                start_date="2026-04-29",
                end_date="2026-04-29",
                project="bunnings",
                nmi="Optima_X",
            )
            is None
        )

    @responses.activate
    def test_returns_none_on_non_zip_body(self) -> None:
        from interval_exporter.downloader import download_interval_zip

        responses.add(
            responses.POST,
            "https://app.bidenergy.com/BuyerReport/exportdailyusagecsv",
            status=200,
            body=b"not a zip body",
            content_type="application/zip",
        )

        assert (
            download_interval_zip(
                cookies=".ASPXAUTH=tok",
                site_id_str="abc-uuid",
                start_date="2026-04-29",
                end_date="2026-04-29",
                project="bunnings",
                nmi="Optima_X",
            )
            is None
        )

    @pytest.mark.parametrize("status_code", [401, 403, 404, 500])
    @responses.activate
    def test_returns_none_on_non_200_status(self, status_code: int) -> None:
        from interval_exporter.downloader import download_interval_zip

        responses.add(
            responses.POST,
            "https://app.bidenergy.com/BuyerReport/exportdailyusagecsv",
            status=status_code,
            body=b"error",
        )

        assert (
            download_interval_zip(
                cookies=".ASPXAUTH=tok",
                site_id_str="abc-uuid",
                start_date="2026-04-29",
                end_date="2026-04-29",
                project="bunnings",
                nmi="Optima_X",
            )
            is None
        )

    def test_returns_none_on_timeout(self) -> None:
        from interval_exporter import downloader as dl_module

        with patch.object(dl_module.requests, "post", side_effect=_req.Timeout()):
            assert (
                dl_module.download_interval_zip(
                    cookies=".ASPXAUTH=tok",
                    site_id_str="abc-uuid",
                    start_date="2026-04-29",
                    end_date="2026-04-29",
                    project="bunnings",
                    nmi="Optima_X",
                )
                is None
            )

    def test_returns_none_on_connection_error(self) -> None:
        from interval_exporter import downloader as dl_module

        with patch.object(dl_module.requests, "post", side_effect=_req.ConnectionError("boom")):
            assert (
                dl_module.download_interval_zip(
                    cookies=".ASPXAUTH=tok",
                    site_id_str="abc-uuid",
                    start_date="2026-04-29",
                    end_date="2026-04-29",
                    project="bunnings",
                    nmi="Optima_X",
                )
                is None
            )

    def test_returns_none_on_request_exception(self) -> None:
        from interval_exporter import downloader as dl_module

        with patch.object(dl_module.requests, "post", side_effect=_req.RequestException("boom")):
            assert (
                dl_module.download_interval_zip(
                    cookies=".ASPXAUTH=tok",
                    site_id_str="abc-uuid",
                    start_date="2026-04-29",
                    end_date="2026-04-29",
                    project="bunnings",
                    nmi="Optima_X",
                )
                is None
            )
