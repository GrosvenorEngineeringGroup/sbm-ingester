"""Unit tests for Bunnings billing parser."""

from __future__ import annotations

from pathlib import Path

import pytest

from shared.billing_parser import (
    CSV_FIELD_MAPPING,
    _billing_date_to_ts,
    _pick_unit,
    bunnings_usage_and_spend_parser,
)

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


def test_date_conversion_valid() -> None:
    assert _billing_date_to_ts("Mar 2026") == "2026-03-01 00:00:00"
    assert _billing_date_to_ts("Jan 2025") == "2025-01-01 00:00:00"
    assert _billing_date_to_ts("Dec 2024") == "2024-12-01 00:00:00"


def test_date_conversion_invalid_returns_none() -> None:
    assert _billing_date_to_ts("bogus") is None
    assert _billing_date_to_ts("") is None
    assert _billing_date_to_ts("2026-03") is None


def test_unit_selection() -> None:
    # usage suffix → usage unit
    assert _pick_unit("billing-peak-usage", "kWh", "AUD") == "kwh"
    assert _pick_unit("billing-total-greenpower-usage", "kWh", "AUD") == "kwh"
    assert _pick_unit("billing-estimated-peak-usage", "kWh", "AUD") == "kwh"
    # spend / charge suffix → spend unit
    assert _pick_unit("billing-energy-charge", "kWh", "AUD") == "aud"
    assert _pick_unit("billing-total-spend", "kWh", "AUD") == "aud"
    assert _pick_unit("billing-greenpower-spend", "kWh", "AUD") == "aud"
    assert _pick_unit("billing-estimated-metering-charge", "kWh", "AUD") == "aud"


def test_csv_field_mapping_has_23_entries() -> None:
    """Lock the mapping size; new fields require a deliberate change."""
    assert len(CSV_FIELD_MAPPING) == 23
    # Spot-check a few well-known entries
    csv_cols = {entry[0] for entry in CSV_FIELD_MAPPING}
    assert "Peak" in csv_cols
    assert "Estimated Peak" in csv_cols
    assert "Total Spend" in csv_cols
    assert "Total Estimated Spend" in csv_cols
