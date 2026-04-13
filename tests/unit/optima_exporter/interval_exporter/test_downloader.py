"""Unit tests for interval_exporter/downloader.py module.

Tests date formatting and CSV download from BidEnergy API.
"""

import re

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
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
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
            nmi_prefix="",
        )

        assert result is not None
        content, filename = result
        assert content == csv_content
        # Filename includes timestamp suffix
        assert re.match(r"optima_bunnings_NMI#NMI001_2026-01-01_2026-01-07_\d{14}\.csv$", filename)

    @responses.activate
    def test_returns_none_on_http_error(self) -> None:
        """Test that HTTP error returns None."""
        from interval_exporter.downloader import download_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
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
            nmi_prefix="",
        )

        assert result is None

    @responses.activate
    def test_detects_html_error_page(self) -> None:
        """Test that HTML error page is detected and returns None."""
        from interval_exporter.downloader import download_csv

        html_content = b"<!DOCTYPE html><html><body>Error</body></html>"
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
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
            nmi_prefix="",
        )

        assert result is None

    @responses.activate
    def test_generates_correct_filename(self) -> None:
        """Test that filename is generated correctly with uppercase project and NMI."""
        from interval_exporter.downloader import download_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
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
            nmi_prefix="",
        )

        assert result is not None
        _, filename = result
        # Filename should have lowercase project, uppercase NMI, and timestamp suffix
        assert re.match(r"optima_racv_NMI#NMI123_2026-01-01_2026-01-07_\d{14}\.csv$", filename)

    @responses.activate
    def test_handles_timeout(self) -> None:
        """Test that timeout is handled gracefully."""
        from interval_exporter.downloader import download_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
            body=req_lib.exceptions.Timeout("Timeout"),
        )

        result = download_csv(
            cookies=".ASPXAUTH=token123",
            site_id_str="site-guid-001",
            start_date="2026-01-01",
            end_date="2026-01-07",
            project="bunnings",
            nmi="NMI001",
            nmi_prefix="",
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
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
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
            nmi_prefix="",
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
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
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
            nmi_prefix="",
        )

        assert result is not None

    @responses.activate
    def test_application_csv_content_type(self) -> None:
        """Test that application/csv content type is accepted."""
        from interval_exporter.downloader import download_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
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
            nmi_prefix="",
        )

        assert result is not None

    @responses.activate
    def test_passes_country_parameter_to_api(self) -> None:
        """Test that country parameter is passed as filter.countrystr."""
        from interval_exporter.downloader import download_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
            status=200,
            body=b"data",
            content_type="text/csv",
        )

        download_csv(
            cookies=".ASPXAUTH=token123",
            site_id_str="site-guid-001",
            start_date="2026-01-01",
            end_date="2026-01-07",
            project="bunnings",
            nmi="NMI001",
            country="NZ",
            nmi_prefix="",
        )

        # Verify the request was made with correct country parameter
        assert len(responses.calls) == 1
        request_url = responses.calls[0].request.url
        assert "filter.countrystr=NZ" in request_url

    @responses.activate
    def test_defaults_country_to_au(self) -> None:
        """Test that country defaults to AU when not specified."""
        from interval_exporter.downloader import download_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
            status=200,
            body=b"data",
            content_type="text/csv",
        )

        download_csv(
            cookies=".ASPXAUTH=token123",
            site_id_str="site-guid-001",
            start_date="2026-01-01",
            end_date="2026-01-07",
            project="bunnings",
            nmi="NMI001",
            nmi_prefix="",
        )

        assert len(responses.calls) == 1
        request_url = responses.calls[0].request.url
        assert "filter.countrystr=AU" in request_url


class TestPrefixNmiInNem12:
    """Comprehensive tests for the byte-level NMI prefix rewriter."""

    def test_prefixes_single_200_record(self) -> None:
        from interval_exporter.downloader import _prefix_nmi_in_nem12

        content = (
            b"100,NEM12,202604120100,MDP1,Origin\n"
            b"200,4001348123,B1E1K1Q1,E1,E1,E1,250920091,Kwh,5\n"
            b"300,20260410,1.0,A,,,20260411011219,\n"
            b"900\n"
        )
        out = _prefix_nmi_in_nem12(content, prefix="Optima_")
        assert b"200,Optima_4001348123,B1E1K1Q1,E1,E1,E1," in out
        assert b"200,4001348123," not in out

    def test_prefixes_all_four_channels_consistently(self) -> None:
        """Real BidEnergy responses have one 200 record per channel; all must be rewritten."""
        from pathlib import Path

        from interval_exporter.downloader import _prefix_nmi_in_nem12

        content = Path("tests/unit/fixtures/optima_bidenergy_nem12_sample.csv").read_bytes()
        out = _prefix_nmi_in_nem12(content, prefix="Optima_")

        # 4 channels x 1 NMI = 4 prefixed records
        assert out.count(b"200,Optima_4001348123,") == 4
        assert b"200,4001348123," not in out

        # Channel suffixes preserved
        for ch in (b"B1", b"E1", b"K1", b"Q1"):
            assert b"200,Optima_4001348123,B1E1K1Q1," + ch + b"," in out

    def test_handles_crlf_line_endings(self) -> None:
        """BidEnergy is ASP.NET; CRLF responses must rewrite identically."""
        from pathlib import Path

        from interval_exporter.downloader import _prefix_nmi_in_nem12

        content = Path("tests/unit/fixtures/optima_bidenergy_nem12_crlf.csv").read_bytes()
        out = _prefix_nmi_in_nem12(content, prefix="Optima_")

        assert out.count(b"200,Optima_4001348123,") == 4
        # CRLF preserved (we never touched line endings)
        assert b"\r\n" in out
        assert b"\r\n200,Optima_4001348123," in out

    def test_handles_bom_prefixed_response(self) -> None:
        """ASP.NET may emit UTF-8 BOM; helper must accept it."""
        from pathlib import Path

        from interval_exporter.downloader import _prefix_nmi_in_nem12

        content = Path("tests/unit/fixtures/optima_bidenergy_nem12_bom.csv").read_bytes()
        out = _prefix_nmi_in_nem12(content, prefix="Optima_")

        assert out.startswith(b"\xef\xbb\xbf100,")  # BOM preserved at file head
        assert out.count(b"200,Optima_4001348123,") == 4

    def test_does_not_touch_300_records_with_numeric_dates(self) -> None:
        from interval_exporter.downloader import _prefix_nmi_in_nem12

        content = (
            b"100,NEM12,202604120100,MDP1,Origin\n"
            b"200,4001348123,B1E1K1Q1,E1,E1,E1,250920091,Kwh,5\n"
            b"300,20260410,1.0,A,,,20260411011219,\n"
            b"300,20260411,2.0,A,,,20260411011219,\n"
            b"900\n"
        )
        out = _prefix_nmi_in_nem12(content, prefix="Optima_")
        # 300 rows untouched (dates not prefixed)
        assert b"300,20260410,1.0," in out
        assert b"300,20260411,2.0," in out
        assert b"Optima_20260410" not in out

    def test_anchor_resists_embedded_200_bytes_in_data(self) -> None:
        """Defensive: a 300 row whose interval payload happens to contain the bytes '200,' is not rewritten."""
        from interval_exporter.downloader import _prefix_nmi_in_nem12

        content = (
            b"100,NEM12,202604120100,MDP1,Origin\n"
            b"200,4001348123,B1E1K1Q1,E1,E1,E1,250920091,Kwh,5\n"
            b"300,20260410,200,300,A,,,20260411011219,\n"  # '200,' appears mid-line
            b"900\n"
        )
        out = _prefix_nmi_in_nem12(content, prefix="Optima_")
        # Only the real 200 record rewritten
        assert out.count(b"200,Optima_") == 1
        # The mid-line '200,' inside the 300 row is untouched
        assert b"300,20260410,200,300," in out

    def test_idempotent_on_already_prefixed(self) -> None:
        from interval_exporter.downloader import _prefix_nmi_in_nem12

        content = (
            b"100,NEM12,202604120100,MDP1,Origin\n"
            b"200,Optima_4001348123,B1E1K1Q1,E1,E1,E1,250920091,Kwh,5\n"
            b"300,20260410,1.0,A,,,20260411011219,\n"
            b"900\n"
        )
        out = _prefix_nmi_in_nem12(content, prefix="Optima_")
        assert out == content
        assert b"Optima_Optima_" not in out

    def test_raises_on_non_nem12_inputs(self) -> None:
        import pytest
        from interval_exporter.downloader import _prefix_nmi_in_nem12

        for invalid in [
            b"<!DOCTYPE html><html>session expired</html>",
            b"<html><body>error</body></html>",
            b"",
            b'{"error":"unauthorized"}',
            b"PK\x03\x04random_zip_bytes",
        ]:
            with pytest.raises(ValueError, match="missing 100 header"):
                _prefix_nmi_in_nem12(invalid, prefix="Optima_")

    def test_uses_supplied_prefix_value(self) -> None:
        """Prefix string is parameterised - confirm it's not hard-coded to 'Optima_'."""
        from interval_exporter.downloader import _prefix_nmi_in_nem12

        content = b"100,NEM12,202604120100,MDP1,Origin\n200,4001348123,B1E1K1Q1,E1,E1,E1,250920091,Kwh,5\n900\n"
        out = _prefix_nmi_in_nem12(content, prefix="TestNS_")
        assert b"200,TestNS_4001348123," in out
        assert b"200,Optima_" not in out


class TestDownloadCsvNmiPrefix:
    """Tests for the required nmi_prefix keyword-only argument and rewrite wiring."""

    def test_nmi_prefix_is_required(self) -> None:
        """Omitting nmi_prefix must raise TypeError."""
        import pytest
        from interval_exporter.downloader import download_csv

        with pytest.raises(TypeError, match="nmi_prefix"):
            download_csv(
                cookies=".ASPXAUTH=token",
                site_id_str="site-guid-001",
                start_date="2026-04-10",
                end_date="2026-04-10",
                project="bunnings",
                nmi="Optima_4001348123",
            )

    def test_country_is_keyword_only(self) -> None:
        """country must be passed by keyword (not positional)."""
        import pytest
        from interval_exporter.downloader import download_csv

        with pytest.raises(TypeError):
            # 7th positional arg should be rejected (country is now keyword-only)
            download_csv(
                ".ASPXAUTH=token",
                "site-guid-001",
                "2026-04-10",
                "2026-04-10",
                "bunnings",
                "Optima_4001348123",
                "AU",
            )

    @responses.activate
    def test_empty_nmi_prefix_does_not_rewrite(self) -> None:
        from interval_exporter.downloader import download_csv

        body = (
            b"100,NEM12,202604120100,MDP1,Origin\n"
            b"200,4001348123,B1E1K1Q1,E1,E1,E1,250920091,Kwh,5\n"
            b"300,20260410,1.0,A,,,20260411011219,\n"
            b"900\n"
        )
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
            status=200,
            body=body,
            content_type="application/vnd.csv",
        )

        result = download_csv(
            cookies=".ASPXAUTH=token",
            site_id_str="site-guid-001",
            start_date="2026-04-10",
            end_date="2026-04-10",
            project="bunnings",
            nmi="Optima_4001348123",
            nmi_prefix="",
        )
        assert result is not None
        content, _ = result
        assert content == body

    @responses.activate
    def test_optima_nmi_prefix_rewrites_all_200_records(self) -> None:
        from pathlib import Path

        from interval_exporter.downloader import download_csv

        sample = Path("tests/unit/fixtures/optima_bidenergy_nem12_sample.csv").read_bytes()
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
            status=200,
            body=sample,
            content_type="application/vnd.csv",
        )

        result = download_csv(
            cookies=".ASPXAUTH=token",
            site_id_str="site-guid-001",
            start_date="2026-04-10",
            end_date="2026-04-10",
            project="bunnings",
            nmi="Optima_4001348123",
            nmi_prefix="Optima_",
        )
        assert result is not None
        content, _ = result
        assert content.count(b"200,Optima_4001348123,") == 4
        assert b"200,4001348123," not in content


class TestDownloadCsvContentTypes:
    """Verify content-type variants BidEnergy may emit are all accepted."""

    @responses.activate
    def test_accepts_application_vnd_csv(self) -> None:
        from pathlib import Path

        from interval_exporter.downloader import download_csv

        sample = Path("tests/unit/fixtures/optima_bidenergy_nem12_sample.csv").read_bytes()
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
            status=200,
            body=sample,
            content_type="application/vnd.csv",
        )
        result = download_csv(
            cookies=".ASPXAUTH=token",
            site_id_str="site-guid-001",
            start_date="2026-04-10",
            end_date="2026-04-10",
            project="bunnings",
            nmi="Optima_4001348123",
            nmi_prefix="Optima_",
        )
        assert result is not None

    @responses.activate
    def test_accepts_nem12_with_text_html_content_type_via_body_sniff(self) -> None:
        """If BidEnergy mis-labels the response, body sniff (starts with 100,) saves us."""
        from interval_exporter.downloader import download_csv

        body = b"100,NEM12,202604120100,MDP1,Origin\n200,4001348123,B1E1K1Q1,E1,E1,E1,250920091,Kwh,5\n900\n"
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
            status=200,
            body=body,
            content_type="text/html",
        )
        result = download_csv(
            cookies=".ASPXAUTH=token",
            site_id_str="site-guid-001",
            start_date="2026-04-10",
            end_date="2026-04-10",
            project="bunnings",
            nmi="Optima_4001348123",
            nmi_prefix="Optima_",
        )
        assert result is not None

    @responses.activate
    def test_rejects_real_html_error_page(self) -> None:
        from interval_exporter.downloader import download_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
            status=200,
            body=b"<!DOCTYPE html><html><body>Session expired</body></html>",
            content_type="text/html",
        )
        result = download_csv(
            cookies=".ASPXAUTH=token",
            site_id_str="site-guid-001",
            start_date="2026-04-10",
            end_date="2026-04-10",
            project="bunnings",
            nmi="Optima_4001348123",
            nmi_prefix="Optima_",
        )
        assert result is None

    @responses.activate
    def test_rejects_html_with_bom(self) -> None:
        from interval_exporter.downloader import download_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
            status=200,
            body=b"\xef\xbb\xbf<!DOCTYPE html><html>error</html>",
            content_type="text/html",
        )
        result = download_csv(
            cookies=".ASPXAUTH=token",
            site_id_str="site-guid-001",
            start_date="2026-04-10",
            end_date="2026-04-10",
            project="bunnings",
            nmi="Optima_4001348123",
            nmi_prefix="Optima_",
        )
        assert result is None


class TestDownloadCsvEndToEnd:
    """End-to-end: downloaded NEM12 must parse cleanly via nem_adapter and yield Optima-prefixed NMI."""

    @responses.activate
    def test_real_fixture_yields_optima_prefixed_nmi(self) -> None:
        import sys
        import tempfile
        from pathlib import Path

        sys.path.insert(0, "src")
        from interval_exporter.downloader import download_csv

        from shared.nem_adapter import output_as_data_frames

        sample = Path("tests/unit/fixtures/optima_bidenergy_nem12_sample.csv").read_bytes()
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
            status=200,
            body=sample,
            content_type="application/vnd.csv",
        )

        result = download_csv(
            cookies=".ASPXAUTH=token",
            site_id_str="site-guid-001",
            start_date="2026-04-10",
            end_date="2026-04-10",
            project="bunnings",
            nmi="Optima_4001348123",
            nmi_prefix="Optima_",
        )
        assert result is not None
        content, _ = result

        tmp_file: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
                tmp.write(content)
                tmp_file = Path(tmp.name)
            frames = output_as_data_frames(str(tmp_file))
            assert len(frames) == 1
            nmi, df = frames[0]
            assert nmi == "Optima_4001348123"
            assert {"B1_Kwh", "E1_Kwh", "K1_Kvarh", "Q1_Kvarh"}.issubset(df.columns)
            assert len(df) == 288
        finally:
            if tmp_file:
                tmp_file.unlink()

    @responses.activate
    def test_quality_flag_preserved_through_pipeline(self) -> None:
        import sys
        import tempfile
        from pathlib import Path

        sys.path.insert(0, "src")
        from interval_exporter.downloader import download_csv

        from shared.nem_adapter import output_as_data_frames

        sample = Path("tests/unit/fixtures/optima_bidenergy_nem12_sample.csv").read_bytes()
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
            status=200,
            body=sample,
            content_type="application/vnd.csv",
        )

        result = download_csv(
            cookies=".ASPXAUTH=token",
            site_id_str="site-guid-001",
            start_date="2026-04-10",
            end_date="2026-04-10",
            project="bunnings",
            nmi="Optima_4001348123",
            nmi_prefix="Optima_",
        )
        assert result is not None
        content, _ = result

        tmp_file: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
                tmp.write(content)
                tmp_file = Path(tmp.name)
            frames = output_as_data_frames(str(tmp_file))
            _, df = frames[0]
            quality_cols = [c for c in df.columns if c.startswith("quality_")]
            assert quality_cols, f"no quality columns: {list(df.columns)}"
            for qc in quality_cols:
                non_null = df[qc].dropna().unique().tolist()
                assert "A" in non_null, f"{qc} did not contain 'A': {non_null}"
        finally:
            if tmp_file:
                tmp_file.unlink()

    @responses.activate
    def test_500_response_returns_none(self) -> None:
        from interval_exporter.downloader import download_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
            status=500,
            body=b"Internal Server Error",
        )
        result = download_csv(
            cookies=".ASPXAUTH=token",
            site_id_str="site-guid-001",
            start_date="2026-04-10",
            end_date="2026-04-10",
            project="bunnings",
            nmi="Optima_4001348123",
            nmi_prefix="Optima_",
        )
        assert result is None


class TestDownloadCsvErrorPaths:
    """Cover remaining HTTP error and exception branches."""

    @responses.activate
    def test_returns_none_on_401(self) -> None:
        from interval_exporter.downloader import download_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
            status=401,
            body=b"Unauthorized",
        )
        result = download_csv(
            cookies=".ASPXAUTH=expired",
            site_id_str="site-guid-001",
            start_date="2026-04-10",
            end_date="2026-04-10",
            project="bunnings",
            nmi="Optima_4001348123",
            nmi_prefix="Optima_",
        )
        assert result is None

    @responses.activate
    def test_returns_none_on_404(self) -> None:
        from interval_exporter.downloader import download_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
            status=404,
            body=b"Not Found",
        )
        result = download_csv(
            cookies=".ASPXAUTH=token",
            site_id_str="invalid-site",
            start_date="2026-04-10",
            end_date="2026-04-10",
            project="bunnings",
            nmi="Optima_4001348123",
            nmi_prefix="Optima_",
        )
        assert result is None

    @responses.activate
    def test_returns_none_on_connection_error(self) -> None:
        import requests as req_lib
        from interval_exporter.downloader import download_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
            body=req_lib.exceptions.ConnectionError("boom"),
        )
        result = download_csv(
            cookies=".ASPXAUTH=token",
            site_id_str="site-guid-001",
            start_date="2026-04-10",
            end_date="2026-04-10",
            project="bunnings",
            nmi="Optima_4001348123",
            nmi_prefix="Optima_",
        )
        assert result is None

    @responses.activate
    def test_returns_none_on_generic_request_exception(self) -> None:
        import requests as req_lib
        from interval_exporter.downloader import download_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
            body=req_lib.exceptions.RequestException("generic"),
        )
        result = download_csv(
            cookies=".ASPXAUTH=token",
            site_id_str="site-guid-001",
            start_date="2026-04-10",
            end_date="2026-04-10",
            project="bunnings",
            nmi="Optima_4001348123",
            nmi_prefix="Optima_",
        )
        assert result is None

    @responses.activate
    def test_returns_none_when_prefix_rewrite_fails_on_malformed_nem12(self) -> None:
        """If response body passes content-type check but is not a valid NEM12, rewrite raises and returns None.

        Triggered when content_type contains 'csv' (so the check passes via first clause),
        but body does not start with '100,' after BOM+whitespace strip — _prefix_nmi_in_nem12 raises.
        """
        from interval_exporter.downloader import download_csv

        # Content-type contains 'csv' so the body-sniff fallback isn't needed,
        # but the body is missing the 100 header, so prefix rewrite must raise ValueError.
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
            status=200,
            body=b"corrupted body without 100 header",
            content_type="text/csv",
        )
        result = download_csv(
            cookies=".ASPXAUTH=token",
            site_id_str="site-guid-001",
            start_date="2026-04-10",
            end_date="2026-04-10",
            project="bunnings",
            nmi="Optima_4001348123",
            nmi_prefix="Optima_",
        )
        assert result is None
