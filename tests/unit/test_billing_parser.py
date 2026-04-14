"""Unit tests for Bunnings billing parser."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

import shared.billing_parser as bp_mod
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


@pytest.fixture
def _reset_mappings_cache():
    """Ensure module-level cache starts empty for each test."""
    bp_mod._nem12_mappings_cache = None
    yield
    bp_mod._nem12_mappings_cache = None


@mock_aws
def test_get_nem12_mappings_loads_and_caches(_reset_mappings_cache) -> None:
    """First call loads from S3; subsequent calls reuse the cache."""
    s3 = boto3.client("s3", region_name="ap-southeast-2")
    s3.create_bucket(
        Bucket="sbm-file-ingester",
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
    )
    mappings_payload = {
        "VCCCLG0019-billing-peak-usage": "p:bunnings:19c88bf11c8-76959f",
        "VCCCLG0019-billing-off-peak-usage": "p:bunnings:19c88bf11ca-38fd75",
    }
    s3.put_object(
        Bucket="sbm-file-ingester",
        Key="nem12_mappings.json",
        Body=json.dumps(mappings_payload).encode(),
    )

    with patch("shared.billing_parser.boto3.client", wraps=boto3.client) as spy:
        first = bp_mod._get_nem12_mappings()
        second = bp_mod._get_nem12_mappings()

    assert first == mappings_payload
    assert second is first  # same dict object — cached
    # boto3.client("s3") should have been called exactly once
    s3_calls = [c for c in spy.call_args_list if c.args and c.args[0] == "s3"]
    assert len(s3_calls) == 1, f"expected 1 s3 client call, got {len(s3_calls)}"


@mock_aws
def test_happy_path_writes_expected_hudi_rows(_reset_mappings_cache, tmp_path) -> None:
    """Single Mar 2026 row for VCCCLG0019 produces the expected actual
    (non-zero) and estimated (zero) Hudi sensor rows. VAAA000266 Mar row
    only has estimated values; must also appear in the output."""
    s3 = boto3.client("s3", region_name="ap-southeast-2")
    s3.create_bucket(
        Bucket="sbm-file-ingester",
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
    )
    s3.create_bucket(
        Bucket="hudibucketsrc",
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
    )
    # Mappings must cover every NMI present in the fixture.
    mappings: dict[str, str] = {}
    for nmi in ("VCCCLG0019", "VAAA000266"):
        for _, suffix, _unit_source in bp_mod.CSV_FIELD_MAPPING:
            mappings[f"{nmi}-{suffix}"] = f"p:bunnings:mock-{nmi}-{suffix}"
    s3.put_object(
        Bucket="sbm-file-ingester",
        Key="nem12_mappings.json",
        Body=json.dumps(mappings).encode(),
    )

    src = FIXTURE_DIR / "bunnings_billing_sample.csv"
    dst = tmp_path / "20260414.155519-Bunnings-Usage and Spend Report.csv"
    dst.write_bytes(src.read_bytes())

    result = bp_mod.bunnings_usage_and_spend_parser(str(dst), "dummy")
    assert result == []

    # Find the exported Hudi CSV
    listed = s3.list_objects_v2(Bucket="hudibucketsrc", Prefix="sensorDataFiles/")
    keys = [o["Key"] for o in listed.get("Contents", [])]
    assert len(keys) == 1, f"expected one exported file, got: {keys}"
    key = keys[0]
    assert key.startswith("sensorDataFiles/billing_export_")
    assert key.endswith(".csv")

    body = s3.get_object(Bucket="hudibucketsrc", Key=key)["Body"].read().decode()
    lines = body.strip().split("\n")
    # Header + 3 source rows x up to 23 fields. Fixture values for VCCCLG0019
    # Mar 2026 actual Peak = 31105.09 — must appear with usage unit kwh.
    assert lines[0] == "sensorId,ts,val,unit,its,quality"
    assert any(
        "p:bunnings:mock-VCCCLG0019-billing-peak-usage,2026-03-01 00:00:00,31105.09,kwh" in line for line in lines[1:]
    ), "expected VCCCLG0019 Mar actual Peak row not found"
    # Mar 2026 estimated Peak = 0.00 (actual bill landed) — still emitted.
    assert any(
        "p:bunnings:mock-VCCCLG0019-billing-estimated-peak-usage,2026-03-01 00:00:00,0.00,kwh" in line
        for line in lines[1:]
    ), "expected VCCCLG0019 Mar estimated Peak (0) row not found"
    # Total Spend in AUD
    assert any(
        "p:bunnings:mock-VCCCLG0019-billing-total-spend,2026-03-01 00:00:00,14566.59,aud" in line for line in lines[1:]
    ), "expected VCCCLG0019 Mar Total Spend row not found"
    # VAAA000266 Mar 2026 has Estimated Peak = 5000.00 (no actual yet)
    assert any(
        "p:bunnings:mock-VAAA000266-billing-estimated-peak-usage,2026-03-01 00:00:00,5000.00,kwh" in line
        for line in lines[1:]
    ), "expected VAAA000266 Mar estimated Peak row not found"


@mock_aws
def test_whitespace_only_unit_falls_back_to_default(_reset_mappings_cache, tmp_path) -> None:
    """Whitespace-only Usage Measurement Unit / Spend Currency must fall
    back to defaults (kwh/aud), not pass through as a garbage unit."""
    s3 = boto3.client("s3", region_name="ap-southeast-2")
    s3.create_bucket(
        Bucket="sbm-file-ingester",
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
    )
    s3.create_bucket(
        Bucket="hudibucketsrc",
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
    )
    mappings = {
        "VCCCLG0019-billing-peak-usage": "p:bunnings:peak",
        "VCCCLG0019-billing-total-spend": "p:bunnings:spend",
    }
    s3.put_object(
        Bucket="sbm-file-ingester",
        Key="nem12_mappings.json",
        Body=json.dumps(mappings).encode(),
    )

    # Build a minimal UTF-16 LE fixture with whitespace-only unit columns
    header_cols = [
        "BuyerShortName",
        "Country",
        "Commodity",
        "Identifier",
        "IdentifierType",
        "DistributorId",
        "Site Name",
        "Site Reference",
        "Site Reference 2",
        "Site Reference 3",
        "Site Reference 4",
        "Site Reference 5",
        "Business Unit",
        "Address",
        "City",
        "State",
        "Postcode",
        "Cost Code",
        "GL Code",
        "Tags",
        "Site Move in Date",
        "Status",
        "Closed Date",
        "Closed Reason",
        "Date",
        "Retailer",
        "Peak",
        "OffPeak",
        "Shoulder",
        "Total Usage",
        "Total GreenPower",
        "Estimated Peak",
        "Estimated OffPeak",
        "Estimated Shoulder",
        "Total Estimated Usage",
        "Total Estimated GreenPower",
        "Usage Measurement Unit",
        "Energy Charge",
        "Total Network Charge",
        "Environmental Charge",
        "Metering Charge",
        "Other Charge",
        "Total Spend",
        "GreenPower Spend",
        "Estimated Energy Charge",
        "Estimated Network Charge",
        "Estimated Environmental Charge",
        "Estimated Metering Charge",
        "Estimated Other Charge",
        "Total Estimated Spend",
        "Spend Currency",
    ]
    cells = dict.fromkeys(header_cols, "")
    cells["Identifier"] = "VCCCLG0019"
    cells["Date"] = "Mar 2026"
    cells["Peak"] = "100.00"
    cells["Total Spend"] = "50.00"
    cells["Usage Measurement Unit"] = "   "  # whitespace only
    cells["Spend Currency"] = "   "  # whitespace only
    row = ",".join(cells[c] for c in header_cols)
    lines_txt = (
        "\n".join(
            [
                'Commodities:,"Electricity"',
                'Status:,"Active"',
                "Country:, Australia",
                "Start:,01 Jan 2026",
                "End:,31 Dec 2026",
                "",
                "",
                ",".join(header_cols),
                row,
            ]
        )
        + "\n"
    )
    data = b"\xff\xfe" + lines_txt.encode("utf-16-le")
    dst = tmp_path / "20260414.000000-Bunnings-Usage and Spend Report.csv"
    dst.write_bytes(data)

    bp_mod.bunnings_usage_and_spend_parser(str(dst), "dummy")

    key = s3.list_objects_v2(Bucket="hudibucketsrc", Prefix="sensorDataFiles/")["Contents"][0]["Key"]
    body = s3.get_object(Bucket="hudibucketsrc", Key=key)["Body"].read().decode()
    # Usage row must have unit "kwh" (default) not whitespace
    assert "p:bunnings:peak,2026-03-01 00:00:00,100.00,kwh," in body
    # Spend row must have unit "aud" (default)
    assert "p:bunnings:spend,2026-03-01 00:00:00,50.00,aud," in body


def _make_fixture(tmp_path, nmi: str, date: str, cells: dict[str, str]) -> Path:
    """Build a minimal UTF-16 LE billing CSV with one data row."""
    header_cols = [
        "BuyerShortName",
        "Country",
        "Commodity",
        "Identifier",
        "IdentifierType",
        "DistributorId",
        "Site Name",
        "Site Reference",
        "Site Reference 2",
        "Site Reference 3",
        "Site Reference 4",
        "Site Reference 5",
        "Business Unit",
        "Address",
        "City",
        "State",
        "Postcode",
        "Cost Code",
        "GL Code",
        "Tags",
        "Site Move in Date",
        "Status",
        "Closed Date",
        "Closed Reason",
        "Date",
        "Retailer",
        "Peak",
        "OffPeak",
        "Shoulder",
        "Total Usage",
        "Total GreenPower",
        "Estimated Peak",
        "Estimated OffPeak",
        "Estimated Shoulder",
        "Total Estimated Usage",
        "Total Estimated GreenPower",
        "Usage Measurement Unit",
        "Energy Charge",
        "Total Network Charge",
        "Environmental Charge",
        "Metering Charge",
        "Other Charge",
        "Total Spend",
        "GreenPower Spend",
        "Estimated Energy Charge",
        "Estimated Network Charge",
        "Estimated Environmental Charge",
        "Estimated Metering Charge",
        "Estimated Other Charge",
        "Total Estimated Spend",
        "Spend Currency",
    ]
    defaults = dict.fromkeys(header_cols, "")
    defaults.update(
        {
            "BuyerShortName": "Bunnings",
            "Country": "AU",
            "Commodity": "Electricity",
            "Identifier": nmi,
            "IdentifierType": "NMI",
            "Date": date,
            "Usage Measurement Unit": "kWh",
            "Spend Currency": "AUD",
        }
    )
    defaults.update(cells)
    row = ",".join(defaults[c] for c in header_cols)
    lines_txt = (
        "\n".join(
            [
                'Commodities:,"Electricity"',
                'Status:,"Active"',
                "Country:, Australia",
                "Start:,01 Jan 2026",
                "End:,31 Dec 2026",
                "",
                "",
                ",".join(header_cols),
                row,
            ]
        )
        + "\n"
    )
    data = b"\xff\xfe" + lines_txt.encode("utf-16-le")
    dst = tmp_path / "20260414.000000-Bunnings-Usage and Spend Report.csv"
    dst.write_bytes(data)
    return dst


def _setup_s3_with_mappings(mappings: dict) -> boto3.client:
    s3 = boto3.client("s3", region_name="ap-southeast-2")
    s3.create_bucket(
        Bucket="sbm-file-ingester",
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
    )
    s3.create_bucket(
        Bucket="hudibucketsrc",
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
    )
    s3.put_object(
        Bucket="sbm-file-ingester",
        Key="nem12_mappings.json",
        Body=json.dumps(mappings).encode(),
    )
    return s3


@mock_aws
def test_missing_mapping_skipped(_reset_mappings_cache, tmp_path) -> None:
    """Rows for NMIs not in nem12_mappings produce no output but do not error."""
    s3 = _setup_s3_with_mappings({})  # empty mappings
    src = _make_fixture(tmp_path, "UNKNOWN_NMI", "Mar 2026", {"Peak": "100.00"})
    result = bp_mod.bunnings_usage_and_spend_parser(str(src), "dummy")
    assert result == []
    # No Hudi CSV should be written when zero rows map
    listed = s3.list_objects_v2(Bucket="hudibucketsrc", Prefix="sensorDataFiles/")
    assert listed.get("KeyCount", 0) == 0


@mock_aws
def test_blank_value_skipped(_reset_mappings_cache, tmp_path) -> None:
    """Cells with empty string produce no Hudi row for that field."""
    mappings = {
        "VCCCLG0019-billing-peak-usage": "p:bunnings:peak",
        "VCCCLG0019-billing-off-peak-usage": "p:bunnings:offpeak",
    }
    s3 = _setup_s3_with_mappings(mappings)
    src = _make_fixture(tmp_path, "VCCCLG0019", "Mar 2026", {"Peak": "100.00", "OffPeak": ""})
    bp_mod.bunnings_usage_and_spend_parser(str(src), "dummy")
    key = s3.list_objects_v2(Bucket="hudibucketsrc", Prefix="sensorDataFiles/")["Contents"][0]["Key"]
    body = s3.get_object(Bucket="hudibucketsrc", Key=key)["Body"].read().decode()
    assert "p:bunnings:peak,2026-03-01 00:00:00,100.00,kwh" in body
    # The empty OffPeak cell must not produce a row:
    assert "p:bunnings:offpeak" not in body


@mock_aws
def test_invalid_date_skipped(_reset_mappings_cache, tmp_path) -> None:
    """Bogus Date string skips the row entirely (no partial writes)."""
    mappings = {"VCCCLG0019-billing-peak-usage": "p:bunnings:peak"}
    s3 = _setup_s3_with_mappings(mappings)
    src = _make_fixture(tmp_path, "VCCCLG0019", "not-a-month", {"Peak": "100.00"})
    bp_mod.bunnings_usage_and_spend_parser(str(src), "dummy")
    listed = s3.list_objects_v2(Bucket="hudibucketsrc", Prefix="sensorDataFiles/")
    assert listed.get("KeyCount", 0) == 0


@mock_aws
def test_unit_selection_in_output(_reset_mappings_cache, tmp_path) -> None:
    """Usage fields use kWh; spend fields use AUD."""
    mappings = {
        "VCCCLG0019-billing-peak-usage": "p:bunnings:peak",
        "VCCCLG0019-billing-total-spend": "p:bunnings:spend",
    }
    s3 = _setup_s3_with_mappings(mappings)
    src = _make_fixture(
        tmp_path,
        "VCCCLG0019",
        "Mar 2026",
        {"Peak": "100.00", "Total Spend": "1234.56"},
    )
    bp_mod.bunnings_usage_and_spend_parser(str(src), "dummy")
    key = s3.list_objects_v2(Bucket="hudibucketsrc", Prefix="sensorDataFiles/")["Contents"][0]["Key"]
    body = s3.get_object(Bucket="hudibucketsrc", Key=key)["Body"].read().decode()
    assert "p:bunnings:peak,2026-03-01 00:00:00,100.00,kwh" in body
    assert "p:bunnings:spend,2026-03-01 00:00:00,1234.56,aud" in body


@mock_aws
def test_zero_rows_skips_s3_put(_reset_mappings_cache, tmp_path) -> None:
    """If every row is unmapped or blank, no Hudi CSV is uploaded."""
    mappings: dict[str, str] = {}  # none match
    s3 = _setup_s3_with_mappings(mappings)
    src = _make_fixture(tmp_path, "VCCCLG0019", "Mar 2026", {"Peak": "100.00"})
    result = bp_mod.bunnings_usage_and_spend_parser(str(src), "dummy")
    assert result == []
    assert s3.list_objects_v2(Bucket="hudibucketsrc", Prefix="sensorDataFiles/").get("KeyCount", 0) == 0


@mock_aws
def test_s3_write_target_is_hudibucketsrc(_reset_mappings_cache, tmp_path) -> None:
    """Explicit guard: we must write to hudibucketsrc/sensorDataFiles/, never elsewhere."""
    mappings = {"VCCCLG0019-billing-peak-usage": "p:bunnings:peak"}
    s3 = _setup_s3_with_mappings(mappings)
    src = _make_fixture(tmp_path, "VCCCLG0019", "Mar 2026", {"Peak": "100.00"})
    bp_mod.bunnings_usage_and_spend_parser(str(src), "dummy")
    listed = s3.list_objects_v2(Bucket="hudibucketsrc", Prefix="sensorDataFiles/")
    keys = [o["Key"] for o in listed.get("Contents", [])]
    assert len(keys) == 1
    assert keys[0].startswith("sensorDataFiles/billing_export_")
    assert keys[0].endswith(".csv")
    # Length check: microsecond timestamp has 20 chars between the prefix and '.csv'
    prefix = "sensorDataFiles/billing_export_"
    assert len(keys[0]) == len(prefix) + 20 + len(".csv")


@mock_aws
def test_dispatcher_routes_bunnings_file(_reset_mappings_cache, tmp_path) -> None:
    """End-to-end: get_non_nem_df should route a Bunnings billing file to
    bunnings_usage_and_spend_parser and return []."""
    from shared.non_nem_parsers import get_non_nem_df

    mappings = {"VCCCLG0019-billing-peak-usage": "p:bunnings:peak"}
    _setup_s3_with_mappings(mappings)
    src = _make_fixture(tmp_path, "VCCCLG0019", "Mar 2026", {"Peak": "100.00"})

    result = get_non_nem_df(str(src), "dummy")
    assert result == []


@mock_aws
def test_dispatcher_still_routes_racv_file_to_racv_parser(_reset_mappings_cache, tmp_path) -> None:
    """Regression guard: RACV files must still hit optima_usage_and_spend_to_s3,
    not the new Bunnings parser."""
    from shared.non_nem_parsers import get_non_nem_df

    s3 = boto3.client("s3", region_name="ap-southeast-2")
    s3.create_bucket(
        Bucket="gegoptimareports",
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
    )
    dst = tmp_path / "20260414.024550-RACV-Usage and Spend Report.csv"
    dst.write_bytes(b"dummy content")

    result = get_non_nem_df(str(dst), "dummy")
    assert result == []  # RACV parser also returns []

    # RACV parser copies to gegoptimareports — verify we hit it, not ours
    obj = s3.get_object(Bucket="gegoptimareports", Key="usageAndSpendReports/racvUsageAndSpend.csv")
    assert obj["Body"].read() == b"dummy content"
