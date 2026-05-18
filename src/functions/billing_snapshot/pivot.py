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


PivotKey = tuple[str, str]  # (nmi, month_iso_date)
PivotValue = tuple[float, str]  # (val, unit)
Pivot = dict[PivotKey, dict[str, PivotValue]]


def build_pivot(
    rows: list[tuple[str, str, str, str]],
    reverse_map: dict[str, tuple[str, str]],
) -> Pivot:
    """Long → wide pivot.

    Each input row is (sensorid, ts, val, unit) where ts is an Athena timestamp
    string like ``2025-01-01 00:00:00.000``. Rows whose sensorid is not in
    reverse_map are silently ignored (defensive — should never happen for an
    explicit IN-list query).
    """
    pivot: Pivot = {}
    for sensorid, ts, val_str, unit in rows:
        target = reverse_map.get(sensorid)
        if target is None:
            continue
        nmi, field = target
        month_iso = ts[:10]  # YYYY-MM-DD prefix
        val = float(val_str)
        pivot.setdefault((nmi, month_iso), {})[field] = (val, unit)
    return pivot
