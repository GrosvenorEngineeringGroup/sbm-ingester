"""Unit tests for interval_exporter/downloader.py date formatting helpers."""


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
