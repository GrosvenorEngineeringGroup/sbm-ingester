"""Tests for billing_snapshot.pivot pure functions."""

import json
from pathlib import Path

from pivot import (
    COLUMNS,
    METRIC_COLUMNS,
    CurrencyStats,
    build_pivot,
    build_reverse_map,
    derive_currencies,
    normalise_field,
)


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


def test_build_pivot_long_to_wide():
    rmap = {
        "p:bunnings:s1-peak": ("2002105104", "peak_usage"),
        "p:bunnings:s1-tspend": ("2002105104", "total_spend"),
        "p:bunnings:s2-peak": ("0000005438UN02B", "peak_usage"),
    }
    rows = [
        # (sensorid, ts, val, unit)
        ("p:bunnings:s1-peak", "2025-01-01 00:00:00.000", "100.5", "kwh"),
        ("p:bunnings:s1-tspend", "2025-01-01 00:00:00.000", "50.25", "aud"),
        ("p:bunnings:s1-peak", "2025-02-01 00:00:00.000", "110.0", "kwh"),
        ("p:bunnings:s2-peak", "2025-01-01 00:00:00.000", "9.99", "nzd"),
    ]
    pivot = build_pivot(rows, rmap)
    assert pivot[("2002105104", "2025-01-01")]["peak_usage"] == (100.5, "kwh")
    assert pivot[("2002105104", "2025-01-01")]["total_spend"] == (50.25, "aud")
    assert pivot[("2002105104", "2025-02-01")]["peak_usage"] == (110.0, "kwh")
    assert pivot[("0000005438UN02B", "2025-01-01")]["peak_usage"] == (9.99, "nzd")


def test_build_pivot_silently_ignores_unmapped_sensor():
    rmap = {"p:bunnings:s1": ("NMI1", "peak_usage")}
    rows = [
        ("p:bunnings:s1", "2025-01-01 00:00:00.000", "1.0", "kwh"),
        ("p:bunnings:unknown", "2025-01-01 00:00:00.000", "999.0", "kwh"),
    ]
    pivot = build_pivot(rows, rmap)
    assert len(pivot) == 1
    assert ("NMI1", "2025-01-01") in pivot


def test_build_pivot_preserves_negative_values():
    rmap = {"p:bunnings:s1": ("NMI1", "energy_charge")}
    rows = [("p:bunnings:s1", "2025-03-01 00:00:00.000", "-42.50", "aud")]
    pivot = build_pivot(rows, rmap)
    assert pivot[("NMI1", "2025-03-01")]["energy_charge"] == (-42.5, "aud")


def test_build_pivot_handles_timestamp_without_fractional_seconds():
    rmap = {"p:bunnings:s1": ("NMI1", "peak_usage")}
    rows = [("p:bunnings:s1", "2025-04-01 00:00:00", "1.0", "kwh")]
    pivot = build_pivot(rows, rmap)
    assert ("NMI1", "2025-04-01") in pivot


def test_derive_currencies_unique_aud():
    pivot = {
        ("NMI1", "2025-01-01"): {
            "energy_charge": (10.0, "aud"),
            "total_spend": (12.0, "aud"),
            "peak_usage": (100.0, "kwh"),  # usage unit — ignored for currency
        }
    }
    currencies, stats = derive_currencies(pivot)
    assert currencies["NMI1"] == "AUD"
    assert stats == CurrencyStats(conflict=0, unknown=0, suspect=0)


def test_derive_currencies_unique_nzd():
    pivot = {
        ("0000005438UN02B", "2025-01-01"): {
            "energy_charge": (5.0, "nzd"),
            "total_spend": (5.0, "nzd"),
        }
    }
    currencies, stats = derive_currencies(pivot)
    assert currencies["0000005438UN02B"] == "NZD"
    assert stats == CurrencyStats(conflict=0, unknown=0, suspect=0)


def test_derive_currencies_charge_less_nmi_defaults_aud_and_emits_unknown():
    pivot = {
        ("NMI1", "2025-01-01"): {
            "peak_usage": (100.0, "kwh"),
            "total_greenpower_usage": (0.0, "kwh"),
        }
    }
    currencies, stats = derive_currencies(pivot)
    assert currencies["NMI1"] == "AUD"
    assert stats.unknown == 1


def test_derive_currencies_conflicting_units_uses_sorted_pick():
    # In practice impossible but spec requires deterministic handling.
    pivot = {
        ("NMI1", "2025-01-01"): {
            "energy_charge": (10.0, "aud"),
            "total_spend": (12.0, "nzd"),
        }
    }
    currencies, stats = derive_currencies(pivot)
    # sorted(["aud", "nzd"])[0] == "aud" → "AUD"
    assert currencies["NMI1"] == "AUD"
    assert stats.conflict == 1


def test_derive_currencies_nz_format_with_aud_emits_suspect():
    # 15-char ICP format labelled AUD — likely upstream blank Spend Currency
    pivot = {
        ("0000005438UN02B", "2025-01-01"): {
            "energy_charge": (5.0, "aud"),
        }
    }
    currencies, stats = derive_currencies(pivot)
    assert currencies["0000005438UN02B"] == "AUD"
    assert stats.suspect == 1


def test_derive_currencies_au_nmi_with_aud_is_not_suspect():
    pivot = {("2002105104", "2025-01-01"): {"energy_charge": (5.0, "aud")}}
    _, stats = derive_currencies(pivot)
    assert stats.suspect == 0
