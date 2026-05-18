"""Tests for billing_snapshot.pivot pure functions."""

import json
from pathlib import Path

from pivot import COLUMNS, METRIC_COLUMNS, build_reverse_map, normalise_field


def test_columns_count_and_order():
    assert COLUMNS == [
        "nmi",
        "month",
        "currency",
        # USAGE
        "total_usage",
        "total_estimated_usage",
        "peak_usage",
        "estimated_peak_usage",
        "off_peak_usage",
        "estimated_off_peak_usage",
        "shoulder_usage",
        "estimated_shoulder_usage",
        # GREENPOWER
        "total_greenpower_usage",
        "total_estimated_greenpower_usage",
        "greenpower_spend",
        # CHARGES
        "energy_charge",
        "estimated_energy_charge",
        "network_charge",
        "estimated_network_charge",
        "metering_charge",
        "estimated_metering_charge",
        "environmental_charge",
        "estimated_environmental_charge",
        "other_charge",
        "estimated_other_charge",
        # SPEND
        "total_spend",
        "total_estimated_spend",
    ]
    assert len(COLUMNS) == 26
    assert len(METRIC_COLUMNS) == 23
    assert COLUMNS[3:] == METRIC_COLUMNS


def test_normalise_field_dash_to_underscore():
    assert normalise_field("billing-peak-usage") == "peak_usage"
    assert normalise_field("billing-total-spend") == "total_spend"
    assert normalise_field("billing-estimated-energy-charge") == "estimated_energy_charge"
    assert normalise_field("billing-total-estimated-greenpower-usage") == "total_estimated_greenpower_usage"


def test_normalise_field_strips_billing_prefix():
    assert normalise_field("billing-other-charge") == "other_charge"


FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load_mappings():
    return json.loads((FIXTURES_DIR / "mappings_truncated.json").read_text())


def test_build_reverse_map_returns_only_billing_keys():
    data = _load_mappings()
    rmap = build_reverse_map(data)
    # 7 billing keys in fixture; non-billing entries (QB05747155-EK, JDZH0007) excluded
    assert len(rmap) == 7
    assert "p:bunnings:not-billing" not in rmap
    assert "p:amp_sites:r:269ff25a-543a0702" not in rmap


def test_build_reverse_map_decodes_nmi_and_field():
    rmap = build_reverse_map(_load_mappings())
    assert rmap["p:bunnings:s1-peak"] == ("2002105104", "peak_usage")
    assert rmap["p:bunnings:s1-offpeak"] == ("2002105104", "off_peak_usage")
    assert rmap["p:bunnings:s1-echarge"] == ("2002105104", "energy_charge")
    assert rmap["p:bunnings:s2-tspend"] == ("0000005438UN02B", "total_spend")
