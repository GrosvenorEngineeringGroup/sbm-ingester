"""Unit tests for Bunnings billing parser."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

import shared.parsers.optima.bunnings_billing as bp_mod
from shared.parsers import NotRelevantParser
from shared.parsers import _mappings as mappings_mod
from shared.parsers.optima.bunnings_billing import (
    CSV_FIELD_MAPPING,
    _billing_date_to_ts,
    _pick_unit,
    bunnings_billing_parser,
)

FIXTURE_DIR = Path(__file__).parent.parent.parent / "fixtures"


def test_filename_mismatch_raises(tmp_path) -> None:
    """Parser must reject files that are not Bunnings billing reports."""
    f = tmp_path / "20260414-RACV-Usage and Spend Report.csv"
    f.write_bytes(b"irrelevant content")
    with pytest.raises(NotRelevantParser, match="Not Bunnings Usage and Spend File"):
        bunnings_billing_parser(str(f))


def test_utf16_decoding_and_row_parsing(tmp_path) -> None:
    """Parser decodes UTF-16 LE, skips 7 metadata rows, and parses data rows."""
    src = FIXTURE_DIR / "bunnings_billing_sample.csv"
    dst = tmp_path / "20260414.155519-Bunnings-Usage and Spend Report.csv"
    dst.write_bytes(src.read_bytes())

    captured = bp_mod._parse_billing_rows(str(dst))

    # 3 data rows in fixture (VCCCLG0019 Mar, VCCCLG0019 Feb, VAAA000266 Mar)
    assert len(captured.rows) == 3
    assert captured.rows_skipped == 0
    assert not captured.skip_reasons
    assert captured.rows[0]["Identifier"] == "VCCCLG0019"
    assert captured.rows[0]["Date"] == "Mar 2026"
    assert captured.rows[0]["Retailer"] == "ZenEnergy"
    assert captured.rows[0]["Peak"] == "31105.09"
    assert captured.rows[2]["Identifier"] == "VAAA000266"
    assert captured.rows[2]["Estimated Peak"] == "5000.00"


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
    mappings_mod._cache = None
    yield
    mappings_mod._cache = None


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

    result = bp_mod.bunnings_billing_parser(str(dst))
    assert result.status == "processed"
    assert result.source_row_count == 3
    assert result.candidate_row_count == 69
    assert result.rows_written == 69
    assert result.unmapped_count == 0

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
def test_quality_cell_is_empty_for_billing_rows(_reset_mappings_cache, tmp_path) -> None:
    """Bunnings billing exports do not carry vendor quality codes — quality
    cell must be empty (zero-length) so Athena reads it as NULL. Spec line 570.
    """
    s3 = boto3.client("s3", region_name="ap-southeast-2")
    s3.create_bucket(
        Bucket="sbm-file-ingester",
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
    )
    s3.create_bucket(
        Bucket="hudibucketsrc",
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
    )
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

    result = bp_mod.bunnings_billing_parser(str(dst))
    assert result.status == "processed"

    listed = s3.list_objects_v2(Bucket="hudibucketsrc", Prefix="sensorDataFiles/")
    keys = [o["Key"] for o in listed.get("Contents", [])]
    assert len(keys) == 1
    body = s3.get_object(Bucket="hudibucketsrc", Key=keys[0])["Body"].read().decode()
    data_lines = [L for L in body.strip().split("\n")[1:] if L]

    # Every data line must end with ``,`` (zero characters between the final
    # comma and the line terminator) — never the literal ``""``.
    for line in data_lines:
        fields = line.split(",")
        assert len(fields) == 6
        assert fields[5] == "", f"quality must be empty cell, got {fields[5]!r}"
        assert line.endswith(",")  # raw bytes: trailing empty cell

    # Sanity: csv.reader yields zero-length string for the quality column.
    import csv
    import io

    rows = list(csv.reader(io.StringIO(body)))
    for row in rows[1:]:
        assert row[-1] == ""


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

    result = bp_mod.bunnings_billing_parser(str(dst))
    assert result.status == "processed"
    assert result.source_row_count == 1
    assert result.candidate_row_count == 2
    assert result.rows_written == 2
    assert result.unmapped_count == 0

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
    result = bp_mod.bunnings_billing_parser(str(src))
    assert result.status == "unmapped"
    assert result.source_row_count == 1
    assert result.candidate_row_count == 1
    assert result.rows_written == 0
    assert result.unmapped_count == 1
    # No Hudi CSV should be written when zero rows map
    listed = s3.list_objects_v2(Bucket="hudibucketsrc", Prefix="sensorDataFiles/")
    assert listed.get("KeyCount", 0) == 0


def test_all_valid_billing_candidates_unmapped_returns_unmapped(_reset_mappings_cache, tmp_path, monkeypatch) -> None:
    src = FIXTURE_DIR / "bunnings_billing_sample.csv"
    dst = tmp_path / "20260414.155519-Bunnings-Usage and Spend Report.csv"
    dst.write_bytes(src.read_bytes())
    monkeypatch.setattr(bp_mod, "get_nem12_mappings", lambda: {})

    result = bp_mod.bunnings_billing_parser(str(dst))

    assert result.status == "unmapped"
    assert result.source_row_count == 3
    assert result.candidate_row_count > 0
    assert result.rows_written == 0
    assert result.unmapped_count == result.candidate_row_count


def test_truncated_billing_row_is_skipped_without_upload(_reset_mappings_cache, tmp_path, monkeypatch) -> None:
    # Truncated row (missing trailing required cells) is skipped, not raised.
    # With no other valid rows the parser returns processed_empty(all_skipped).
    src = _make_fixture(tmp_path, "VCCCLG0019", "Mar 2026", {"Peak": "100.00"})
    text = src.read_bytes().decode("utf-16-le").lstrip("\ufeff")
    lines = text.rstrip("\n").split("\n")
    header = lines[7].split(",")
    peak_index = header.index("Peak")
    truncated_row = ",".join(lines[8].split(",")[: peak_index + 1])
    lines[8] = truncated_row
    src.write_bytes(b"\xff\xfe" + ("\n".join(lines) + "\n").encode("utf-16-le"))
    monkeypatch.setattr(
        bp_mod,
        "get_nem12_mappings",
        lambda: {"VCCCLG0019-billing-peak-usage": "p:bunnings:peak"},
    )

    with patch("shared.parsers.optima.bunnings_billing.boto3.client") as mock_client:
        result = bp_mod.bunnings_billing_parser(str(src))

    assert result.status == "processed_empty"
    assert result.reason == "all_skipped"
    assert result.rows_written == 0
    assert result.rows_skipped == 1
    # Missing trailing required value columns counts as row_shape_mismatch
    # (Identifier is still present, so it's not row_anchor_failure).
    assert result.skip_reasons["row_shape_mismatch"] == 1
    mock_client.return_value.put_object.assert_not_called()


@mock_aws
def test_blank_value_skipped(_reset_mappings_cache, tmp_path) -> None:
    """Cells with empty string produce no Hudi row for that field."""
    mappings = {
        "VCCCLG0019-billing-peak-usage": "p:bunnings:peak",
        "VCCCLG0019-billing-off-peak-usage": "p:bunnings:offpeak",
    }
    s3 = _setup_s3_with_mappings(mappings)
    src = _make_fixture(tmp_path, "VCCCLG0019", "Mar 2026", {"Peak": "100.00", "OffPeak": ""})
    result = bp_mod.bunnings_billing_parser(str(src))
    assert result.status == "processed"
    assert result.source_row_count == 1
    assert result.candidate_row_count == 1
    assert result.rows_written == 1
    assert result.unmapped_count == 0
    key = s3.list_objects_v2(Bucket="hudibucketsrc", Prefix="sensorDataFiles/")["Contents"][0]["Key"]
    body = s3.get_object(Bucket="hudibucketsrc", Key=key)["Body"].read().decode()
    assert "p:bunnings:peak,2026-03-01 00:00:00,100.00,kwh" in body
    # The empty OffPeak cell must not produce a row:
    assert "p:bunnings:offpeak" not in body


@mock_aws
def test_blank_billing_values_return_processed_empty(_reset_mappings_cache, tmp_path) -> None:
    """A valid source row with no billing values is processed but writes no Hudi CSV."""
    s3 = _setup_s3_with_mappings({})
    src = _make_fixture(tmp_path, "VCCCLG0019", "Mar 2026", cells={})

    result = bp_mod.bunnings_billing_parser(str(src))

    assert result.status == "processed_empty"
    assert result.reason == "all_blank"
    assert result.source_row_count == 1
    assert result.rows_written == 0
    listed = s3.list_objects_v2(Bucket="hudibucketsrc", Prefix="sensorDataFiles/")
    assert listed.get("KeyCount", 0) == 0


@mock_aws
def test_invalid_date_returns_processed_empty_all_skipped(_reset_mappings_cache, tmp_path) -> None:
    """Bogus Date string skips the row; with no other rows the file maps to
    processed_empty(all_skipped) instead of raising. No Hudi CSV is written."""
    mappings = {"VCCCLG0019-billing-peak-usage": "p:bunnings:peak"}
    s3 = _setup_s3_with_mappings(mappings)
    src = _make_fixture(tmp_path, "VCCCLG0019", "not-a-month", {"Peak": "100.00"})
    result = bp_mod.bunnings_billing_parser(str(src))

    assert result.status == "processed_empty"
    assert result.reason == "all_skipped"
    assert result.rows_written == 0
    assert result.rows_skipped == 1
    assert result.skip_reasons["unparseable_timestamp"] == 1

    listed = s3.list_objects_v2(Bucket="hudibucketsrc", Prefix="sensorDataFiles/")
    assert listed.get("KeyCount", 0) == 0


@mock_aws
def test_mixed_mapped_row_and_invalid_billing_value_writes_good_row(_reset_mappings_cache, tmp_path) -> None:
    """One well-formed row + one row with a non-numeric Peak: parser writes
    the good row to Hudi and reports the bad row via skip_reasons."""
    mappings = {"VCCCLG0019-billing-peak-usage": "p:bunnings:peak"}
    s3 = _setup_s3_with_mappings(mappings)
    src = _make_fixture(tmp_path, "VCCCLG0019", "Mar 2026", {"Peak": "100.00"})
    text = src.read_bytes().decode("utf-16-le").lstrip("\ufeff")
    lines = text.rstrip("\n").split("\n")
    header = lines[7].split(",")
    invalid_row = lines[8].split(",")
    invalid_row[header.index("Peak")] = "not-a-number"
    lines.append(",".join(invalid_row))
    src.write_bytes(b"\xff\xfe" + ("\n".join(lines) + "\n").encode("utf-16-le"))

    result = bp_mod.bunnings_billing_parser(str(src))

    assert result.status == "processed"
    assert result.rows_written == 1
    assert result.rows_skipped == 1
    assert result.skip_reasons["unparseable_value"] == 1

    listed = s3.list_objects_v2(Bucket="hudibucketsrc", Prefix="sensorDataFiles/")
    assert listed.get("KeyCount", 0) == 1


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
    result = bp_mod.bunnings_billing_parser(str(src))
    assert result.status == "processed"
    assert result.source_row_count == 1
    assert result.candidate_row_count == 2
    assert result.rows_written == 2
    assert result.unmapped_count == 0
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
    result = bp_mod.bunnings_billing_parser(str(src))
    assert result.status == "unmapped"
    assert result.source_row_count == 1
    assert result.candidate_row_count == 1
    assert result.rows_written == 0
    assert result.unmapped_count == 1
    assert s3.list_objects_v2(Bucket="hudibucketsrc", Prefix="sensorDataFiles/").get("KeyCount", 0) == 0


def test_bunnings_hudi_write_failure_raises_processing_error(_reset_mappings_cache, tmp_path, monkeypatch) -> None:
    from shared.parsers import ProcessingError

    src = FIXTURE_DIR / "bunnings_billing_sample.csv"
    dst = tmp_path / "20260414.155519-Bunnings-Usage and Spend Report.csv"
    dst.write_bytes(src.read_bytes())
    mappings = {}
    for nmi in ("VCCCLG0019", "VAAA000266"):
        for _, suffix, _unit_source in bp_mod.CSV_FIELD_MAPPING:
            mappings[f"{nmi}-{suffix}"] = f"p:test:{nmi}:{suffix}"
    monkeypatch.setattr(bp_mod, "get_nem12_mappings", lambda: mappings)

    with patch("shared.parsers.optima.bunnings_billing.boto3.client") as mock_client:
        mock_client.return_value.put_object.side_effect = RuntimeError("boom")

        with pytest.raises(ProcessingError, match="Failed to write Bunnings billing Hudi CSV"):
            bp_mod.bunnings_billing_parser(str(dst))


@mock_aws
def test_s3_write_target_is_hudibucketsrc(_reset_mappings_cache, tmp_path) -> None:
    """Explicit guard: we must write to hudibucketsrc/sensorDataFiles/, never elsewhere."""
    mappings = {"VCCCLG0019-billing-peak-usage": "p:bunnings:peak"}
    s3 = _setup_s3_with_mappings(mappings)
    src = _make_fixture(tmp_path, "VCCCLG0019", "Mar 2026", {"Peak": "100.00"})
    result = bp_mod.bunnings_billing_parser(str(src))
    assert result.status == "processed"
    listed = s3.list_objects_v2(Bucket="hudibucketsrc", Prefix="sensorDataFiles/")
    keys = [o["Key"] for o in listed.get("Contents", [])]
    assert len(keys) == 1
    assert keys[0].startswith("sensorDataFiles/billing_export_")
    assert keys[0].endswith(".csv")
    # Length check: microsecond timestamp has 20 chars between the prefix and '.csv'
    prefix = "sensorDataFiles/billing_export_"
    assert len(keys[0]) == len(prefix) + 20 + len(".csv")


@mock_aws
def test_partial_failure_one_bad_date_writes_other_rows(_reset_mappings_cache, tmp_path) -> None:
    """Two source rows: one with valid Date, one with bogus Date.
    The valid row writes; the bad row is reported via rows_skipped."""
    mappings = {"VCCCLG0019-billing-peak-usage": "p:bunnings:peak"}
    s3 = _setup_s3_with_mappings(mappings)
    src = _make_fixture(tmp_path, "VCCCLG0019", "Mar 2026", {"Peak": "100.00"})
    text = src.read_bytes().decode("utf-16-le").lstrip("﻿")
    lines = text.rstrip("\n").split("\n")
    header = lines[7].split(",")
    bad_row = lines[8].split(",")
    bad_row[header.index("Date")] = "not-a-month"
    lines.append(",".join(bad_row))
    src.write_bytes(b"\xff\xfe" + ("\n".join(lines) + "\n").encode("utf-16-le"))

    result = bp_mod.bunnings_billing_parser(str(src))

    assert result.status == "processed"
    assert result.source_row_count == 2
    assert result.rows_written == 1
    assert result.rows_skipped == 1
    assert result.skip_reasons["unparseable_timestamp"] == 1

    listed = s3.list_objects_v2(Bucket="hudibucketsrc", Prefix="sensorDataFiles/")
    assert listed.get("KeyCount", 0) == 1


@mock_aws
def test_partial_failure_one_unparseable_value_other_value_writes(_reset_mappings_cache, tmp_path) -> None:
    """A single row with one bad value column and one good value column
    still writes the good column to Hudi; the bad cell is reported in
    skip_reasons but the row is not counted as fully skipped."""
    mappings = {
        "VCCCLG0019-billing-peak-usage": "p:bunnings:peak",
        "VCCCLG0019-billing-total-spend": "p:bunnings:spend",
    }
    s3 = _setup_s3_with_mappings(mappings)
    src = _make_fixture(
        tmp_path,
        "VCCCLG0019",
        "Mar 2026",
        {"Peak": "not-a-number", "Total Spend": "1234.56"},
    )

    result = bp_mod.bunnings_billing_parser(str(src))

    assert result.status == "processed"
    assert result.rows_written == 1
    assert result.rows_skipped == 0
    assert result.skip_reasons["unparseable_value"] == 1

    listed = s3.list_objects_v2(Bucket="hudibucketsrc", Prefix="sensorDataFiles/")
    assert listed.get("KeyCount", 0) == 1


@mock_aws
def test_partial_failure_extra_trailing_cells_row_skipped(_reset_mappings_cache, tmp_path) -> None:
    """A row with extra trailing comma cells is skipped as row_shape_mismatch.
    A second well-formed row still writes to Hudi."""
    mappings = {"VCCCLG0019-billing-peak-usage": "p:bunnings:peak"}
    s3 = _setup_s3_with_mappings(mappings)
    src = _make_fixture(tmp_path, "VCCCLG0019", "Mar 2026", {"Peak": "100.00"})
    text = src.read_bytes().decode("utf-16-le").lstrip("﻿")
    lines = text.rstrip("\n").split("\n")
    # Append a row with extra trailing cells
    extra_row = lines[8] + ",extra,extra,extra"
    lines.append(extra_row)
    src.write_bytes(b"\xff\xfe" + ("\n".join(lines) + "\n").encode("utf-16-le"))

    result = bp_mod.bunnings_billing_parser(str(src))

    assert result.status == "processed"
    assert result.rows_written == 1
    assert result.rows_skipped == 1
    assert result.skip_reasons["row_shape_mismatch"] == 1

    listed = s3.list_objects_v2(Bucket="hudibucketsrc", Prefix="sensorDataFiles/")
    assert listed.get("KeyCount", 0) == 1


@mock_aws
def test_dispatcher_routes_bunnings_file(_reset_mappings_cache, tmp_path) -> None:
    """End-to-end: get_non_nem_outcome should route a Bunnings billing file."""
    from shared.non_nem_parsers import get_non_nem_outcome

    mappings = {"VCCCLG0019-billing-peak-usage": "p:bunnings:peak"}
    s3 = _setup_s3_with_mappings(mappings)
    src = _make_fixture(tmp_path, "VCCCLG0019", "Mar 2026", {"Peak": "100.00"})

    result = get_non_nem_outcome(str(src))

    assert result.status == "processed"
    assert result.source_row_count == 1
    assert result.candidate_row_count == 1
    assert result.rows_written == 1
    assert result.unmapped_count == 0
    listed = s3.list_objects_v2(Bucket="hudibucketsrc", Prefix="sensorDataFiles/")
    keys = [o["Key"] for o in listed.get("Contents", [])]
    assert len(keys) == 1


@mock_aws
def test_dispatcher_still_routes_racv_file_to_racv_parser(_reset_mappings_cache, tmp_path) -> None:
    """Regression guard: RACV files must still hit optima_usage_and_spend_to_s3,
    not the new Bunnings parser."""
    from shared.non_nem_parsers import get_non_nem_outcome

    s3 = boto3.client("s3", region_name="ap-southeast-2")
    s3.create_bucket(
        Bucket="gegoptimareports",
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
    )
    dst = tmp_path / "20260414.024550-RACV-Usage and Spend Report.csv"
    dst.write_bytes(b"dummy content")

    result = get_non_nem_outcome(str(dst))
    assert result.status == "processed_external"
    assert result.reason == "external_gegoptimareports"

    # RACV parser copies to gegoptimareports — verify we hit it, not ours
    obj = s3.get_object(Bucket="gegoptimareports", Key="usageAndSpendReports/racvUsageAndSpend.csv")
    assert obj["Body"].read() == b"dummy content"


@mock_aws
def test_partial_mapping_populates_unmapped_identifiers(_reset_mappings_cache, tmp_path) -> None:
    """Partial-mapped Bunnings billing file must surface unmapped lookup keys.

    The outcome's ``unmapped_identifiers`` records (kind="nmi", value=lookup_key)
    so dashboards can debug "why didn't this NMI map" without reconstructing
    the suffix separately. Spec: parser-outcome-semantics-design,
    identifier-kind table.
    """
    mappings = {
        "VCCCLG0019-billing-peak-usage": "p:bunnings:peak",
    }
    _setup_s3_with_mappings(mappings)
    src = _make_fixture(
        tmp_path,
        "VCCCLG0019",
        "Mar 2026",
        {"Peak": "100.00", "OffPeak": "50.00"},
    )

    result = bp_mod.bunnings_billing_parser(str(src))

    assert result.status == "processed"
    assert result.unmapped_count >= 1
    values = {value for _kind, value in result.unmapped_identifiers}
    assert "VCCCLG0019-billing-off-peak-usage" in values
    assert all(kind == "nmi" for kind, _ in result.unmapped_identifiers)


@mock_aws
def test_all_unmapped_outcome_carries_identifiers(_reset_mappings_cache, tmp_path) -> None:
    _setup_s3_with_mappings({})
    src = _make_fixture(tmp_path, "UNKNOWN_NMI", "Mar 2026", {"Peak": "100.00"})

    result = bp_mod.bunnings_billing_parser(str(src))

    assert result.status == "unmapped"
    assert len(result.unmapped_identifiers) >= 1
    assert all(kind == "nmi" for kind, _ in result.unmapped_identifiers)
    values = {value for _kind, value in result.unmapped_identifiers}
    assert "UNKNOWN_NMI-billing-peak-usage" in values
