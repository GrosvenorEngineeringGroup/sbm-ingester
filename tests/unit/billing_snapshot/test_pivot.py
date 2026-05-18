"""Tests for billing_snapshot.pivot pure functions."""

from pivot import COLUMNS, METRIC_COLUMNS, normalise_field


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
