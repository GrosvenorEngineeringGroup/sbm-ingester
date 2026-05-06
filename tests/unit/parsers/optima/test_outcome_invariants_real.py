"""Sanity-check existing optima parser outcomes against spec invariants.

These tests run real side-effect parsers (demand, bunnings_billing) with
real fixtures and confirm the resulting ``ParserOutcome`` satisfies the
test-only ``assert_parser_outcome_invariants`` helper. If any of these
fail, that's a real, pre-existing bug in the parser — not a flaw in the
helper.

Note: only side-effect parsers (demand, bunnings_billing) populate
``rows_written`` directly. DataFrame-returning parsers (interval, envizi
vertical_*, racv, comx) leave ``rows_written=0`` because the
file_processor enriches that field downstream. Therefore the helper is
not yet meaningful against those parsers' raw outcomes — see the
implementation plan / Task 21 report for follow-up.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

import shared.parsers.optima.bunnings_billing as bp_mod
from shared.parsers import _mappings as mappings_mod
from shared.parsers.optima.demand import demand_parser
from tests._outcome_invariants import assert_parser_outcome_invariants

FIXTURE_DIR = Path(__file__).parent.parent.parent / "fixtures"


@pytest.fixture
def _reset_mappings_cache():
    """Clear the shared mappings cache before and after each test."""
    mappings_mod._cache = None
    yield
    mappings_mod._cache = None


# ---------------------------------------------------------------------------
# demand_parser: side-effect parser, populates rows_written.
# ---------------------------------------------------------------------------


def test_demand_parser_partial_skip_passes_invariants(write_demand_csv, monkeypatch, _reset_mappings_cache) -> None:
    """One good row + one truncated row → processed with rows_written>=1."""
    fake_mappings = {
        "Optima_4001260599-demand-kw": "p:bunnings:kw",
        "Optima_4001260599-demand-kva": "p:bunnings:kva",
        "Optima_4001260599-demand-pf": "p:bunnings:pf",
    }
    monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: fake_mappings)

    body = "\n".join(
        [
            'Commodities:,"Electricity"',
            'Sites (NMIs):,"4001260599"',
            'Status:,"Active"',
            "Country:, Australia",
            "Start:,01-Feb-2026",
            "End:,30-Apr-2026",
            "",
            "",
            "Business Unit,Identifier,Identifier Type,ReadingDateTime,E,kW,kVa,Power Factor,Site Name",
            "Bunnings Australia,4001260599,NMI,01-Feb-2026 00:00:00,5.2400,10.4800,10.4800,1.0000,BUN AUS Forbes",
            # truncated row -> row_shape_mismatch skip
            "Bunnings Australia,4001260599,NMI,01-Feb-2026 00:30:00,5.2400,10.4800",
        ]
    )
    path = write_demand_csv(body_override=body)

    with patch("shared.parsers.optima.demand.boto3.client") as mock_client:
        mock_client.return_value.put_object.return_value = {"ETag": "fake"}
        result = demand_parser(str(path), "/tmp/err.log")

    assert_parser_outcome_invariants(result)


def test_demand_parser_all_skipped_passes_invariants(write_demand_csv, monkeypatch, _reset_mappings_cache) -> None:
    """Single truncated row → processed_empty with all_skipped reason."""
    fake_mappings = {"Optima_4001260599-demand-kw": "p:bunnings:kw"}
    monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: fake_mappings)

    body = "\n".join(
        [
            'Commodities:,"Electricity"',
            'Sites (NMIs):,"4001260599"',
            'Status:,"Active"',
            "Country:, Australia",
            "Start:,01-Feb-2026",
            "End:,30-Apr-2026",
            "",
            "",
            "Business Unit,Identifier,Identifier Type,ReadingDateTime,E,kW,kVa,Power Factor,Site Name",
            "Bunnings Australia,4001260599,NMI,01-Feb-2026 00:00:00,5.2400,10.4800",
        ]
    )
    path = write_demand_csv(body_override=body)

    with patch("shared.parsers.optima.demand.boto3.client"):
        result = demand_parser(str(path), "/tmp/err.log")

    assert_parser_outcome_invariants(result)


# ---------------------------------------------------------------------------
# bunnings_billing_parser: side-effect parser, populates rows_written.
# ---------------------------------------------------------------------------


def _setup_s3_with_mappings(mappings: dict[str, str]):
    """Create the in-memory S3 buckets and seed the mappings JSON."""
    s3 = boto3.client("s3", region_name="ap-southeast-2")
    s3.create_bucket(
        Bucket="hudibucketsrc",
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
    )
    s3.create_bucket(
        Bucket="sbm-file-ingester",
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
    )
    s3.put_object(
        Bucket="sbm-file-ingester",
        Key="nem12_mappings.json",
        Body=json.dumps(mappings).encode(),
    )
    return s3


def _make_billing_fixture(tmp_path: Path, identifier: str, date: str, fields: dict[str, str]) -> Path:
    """Build a minimal Bunnings billing CSV (UTF-16 LE w/ BOM)."""
    src = FIXTURE_DIR / "bunnings_billing_sample.csv"
    text = src.read_bytes().decode("utf-16-le").lstrip("﻿")
    lines = text.rstrip("\n").split("\n")
    header = lines[7].split(",")
    template_row = lines[8].split(",")
    template_row[header.index("Identifier")] = identifier
    template_row[header.index("Date")] = date
    for col, val in fields.items():
        template_row[header.index(col)] = val
    new_lines = [*lines[:8], ",".join(template_row)]
    dst = tmp_path / "20260414.155519-Bunnings-Usage and Spend Report.csv"
    dst.write_bytes(b"\xff\xfe" + ("\n".join(new_lines) + "\n").encode("utf-16-le"))
    return dst


@mock_aws
def test_bunnings_billing_partial_skip_passes_invariants(_reset_mappings_cache, tmp_path: Path) -> None:
    """Single bad cell + good cell → processed with rows_written>=1."""
    mappings = {
        "VCCCLG0019-billing-peak-usage": "p:bunnings:peak",
        "VCCCLG0019-billing-total-spend": "p:bunnings:spend",
    }
    _setup_s3_with_mappings(mappings)
    src = _make_billing_fixture(
        tmp_path,
        "VCCCLG0019",
        "Mar 2026",
        {"Peak": "not-a-number", "Total Spend": "1234.56"},
    )

    result = bp_mod.bunnings_billing_parser(str(src), "dummy")
    assert_parser_outcome_invariants(result)


@mock_aws
def test_bunnings_billing_happy_path_passes_invariants(_reset_mappings_cache, tmp_path: Path) -> None:
    """All-good row → processed with rows_written>=1."""
    mappings = {"VCCCLG0019-billing-peak-usage": "p:bunnings:peak"}
    _setup_s3_with_mappings(mappings)
    src = _make_billing_fixture(tmp_path, "VCCCLG0019", "Mar 2026", {"Peak": "100.00"})

    result = bp_mod.bunnings_billing_parser(str(src), "dummy")
    assert_parser_outcome_invariants(result)
