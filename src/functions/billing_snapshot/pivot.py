"""Pure-Python data transforms for the billing snapshot Lambda.

No AWS dependencies; all functions deterministic and unit-testable.
"""

from __future__ import annotations

# Column order per spec — paired actual/estimated within four category blocks.
# Must stay in lockstep with `tests/unit/billing_snapshot/test_pivot.py::test_columns_count_and_order`.
COLUMNS: list[str] = [
    "nmi",
    "month",
    "currency",
    # USAGE block (8)
    "total_usage",
    "total_estimated_usage",
    "peak_usage",
    "estimated_peak_usage",
    "off_peak_usage",
    "estimated_off_peak_usage",
    "shoulder_usage",
    "estimated_shoulder_usage",
    # GREENPOWER block (3)
    "total_greenpower_usage",
    "total_estimated_greenpower_usage",
    "greenpower_spend",
    # CHARGES block (10)
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
    # SPEND block (2)
    "total_spend",
    "total_estimated_spend",
]

METRIC_COLUMNS: list[str] = COLUMNS[3:]


def normalise_field(billing_suffix: str) -> str:
    """Convert a `billing-foo-bar-baz` suffix to a `foo_bar_baz` column name.

    The leading `billing-` prefix is stripped; remaining dashes become underscores.
    """
    stripped = billing_suffix.removeprefix("billing-")
    return stripped.replace("-", "_")


BILLING_DELIMITER = "-billing-"


def build_reverse_map(mappings: dict[str, str]) -> dict[str, tuple[str, str]]:
    """Return `{sensor_id: (nmi, field)}` for every `*-billing-*` key in the input.

    Non-billing keys are silently skipped. Field names are dash-normalised.
    """
    result: dict[str, tuple[str, str]] = {}
    for key, sensor_id in mappings.items():
        if BILLING_DELIMITER not in key:
            continue
        nmi, _, suffix = key.partition(BILLING_DELIMITER)
        field = normalise_field(f"billing-{suffix}")
        result[sensor_id] = (nmi, field)
    return result
