"""Unit tests for interval_exporter/downloader.py date formatting helpers."""

import io
import zipfile

import pytest


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
