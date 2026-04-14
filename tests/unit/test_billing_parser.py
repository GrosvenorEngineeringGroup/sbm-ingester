"""Unit tests for Bunnings billing parser."""

from __future__ import annotations

from pathlib import Path

import pytest

from shared.billing_parser import bunnings_usage_and_spend_parser

FIXTURE_DIR = Path(__file__).parent / "fixtures"


def test_filename_mismatch_raises(tmp_path) -> None:
    """Parser must reject files that are not Bunnings billing reports."""
    f = tmp_path / "20260414-RACV-Usage and Spend Report.csv"
    f.write_bytes(b"irrelevant content")
    with pytest.raises(Exception, match="Not Bunnings Usage and Spend File"):
        bunnings_usage_and_spend_parser(str(f), "dummy-error-log")


def test_utf16_decoding_and_row_parsing(tmp_path, monkeypatch) -> None:
    """Parser decodes UTF-16 LE, skips 7 metadata rows, and parses data rows."""
    import shared.billing_parser as bp

    # Copy fixture to tmp_path with the correct Bunnings filename
    src = FIXTURE_DIR / "bunnings_billing_sample.csv"
    dst = tmp_path / "20260414.155519-Bunnings-Usage and Spend Report.csv"
    dst.write_bytes(src.read_bytes())

    # Intercept _parse_rows to inspect what the parser extracted
    captured: list[dict] = []

    def fake_process(rows, mappings):
        captured.extend(rows)
        return 0

    monkeypatch.setattr(bp, "_process_rows_and_write", fake_process)
    monkeypatch.setattr(bp, "_get_nem12_mappings", lambda: {})

    result = bp.bunnings_usage_and_spend_parser(str(dst), "dummy")
    assert result == []
    # 3 data rows in fixture (VCCCLG0019 Mar, VCCCLG0019 Feb, VAAA000266 Mar)
    assert len(captured) == 3
    assert captured[0]["Identifier"] == "VCCCLG0019"
    assert captured[0]["Date"] == "Mar 2026"
    assert captured[0]["Retailer"] == "ZenEnergy"
    assert captured[0]["Peak"] == "31105.09"
    assert captured[2]["Identifier"] == "VAAA000266"
    assert captured[2]["Estimated Peak"] == "5000.00"
