"""Pure-Python data transforms for the billing snapshot Lambda.

No AWS dependencies; all functions deterministic and unit-testable.
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from typing import IO

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


NZ_ICP_PATTERN = re.compile(r"^\d{10}[A-Z0-9]{5}$")

# Field name fragments that indicate a money column. Currency derivation
# inspects only these (usage fields carry "kwh" units and must be ignored).
_MONEY_FRAGMENTS = ("charge", "spend")


@dataclass(frozen=True)
class CurrencyStats:
    conflict: int  # NMIs with multiple distinct money units
    unknown: int  # NMIs with no money columns (defaulted to AUD)
    suspect: int  # NZ-ICP-formatted NMIs labelled AUD


def derive_currencies(pivot: Pivot) -> tuple[dict[str, str], CurrencyStats]:
    """Derive an upper-case currency per NMI from money-column unit values.

    Returns ``(nmi_to_currency, stats)``. See spec section "Derive currency per
    NMI" for the four-case decision table.
    """
    by_nmi: dict[str, set[str]] = {}
    for (nmi, _month), fields in pivot.items():
        for field, (_val, unit) in fields.items():
            if not unit:
                continue
            if not any(frag in field for frag in _MONEY_FRAGMENTS):
                continue
            by_nmi.setdefault(nmi, set()).add(unit.lower())

    all_nmis = {nmi for (nmi, _) in pivot}

    currencies: dict[str, str] = {}
    conflict = 0
    unknown = 0
    suspect = 0
    for nmi in all_nmis:
        units = by_nmi.get(nmi, set())
        if not units:
            currencies[nmi] = "AUD"
            unknown += 1
            continue
        if len(units) > 1:
            conflict += 1
            picked = sorted(units)[0].upper()
        else:
            picked = next(iter(units)).upper()
        currencies[nmi] = picked
        if picked == "AUD" and NZ_ICP_PATTERN.match(nmi):
            suspect += 1
    return currencies, CurrencyStats(conflict=conflict, unknown=unknown, suspect=suspect)


class EmptyPivotError(RuntimeError):
    """Raised by write_csv when the pivot dict is empty.

    Prevents overwriting `billing-latest.csv` with a header-only file when
    every chunk legitimately returned zero rows (which would itself be a
    pipeline failure worth alerting on).
    """


def write_csv(
    out: IO[str],
    pivot: Pivot,
    currencies: dict[str, str],
) -> None:
    """Write the wide CSV (header + rows) into ``out``.

    Rows are sorted by ``(nmi, month)``. Missing metric cells are emitted as
    empty strings (NOT zero) — distinguishes "no bill" from "$0 bill".
    Numeric values are formatted with `f"{val:.2f}"`.

    Raises ``EmptyPivotError`` if pivot is empty.
    """
    if not pivot:
        raise EmptyPivotError("pivot is empty; refusing to overwrite billing-latest.csv")

    writer = csv.writer(out)
    writer.writerow(COLUMNS)
    for nmi, month in sorted(pivot.keys()):
        fields = pivot[(nmi, month)]
        row = [nmi, month, currencies.get(nmi, "AUD")]
        for col in METRIC_COLUMNS:
            entry = fields.get(col)
            if entry is None:
                row.append("")
            else:
                val, _unit = entry
                row.append(f"{val:.2f}")
        writer.writerow(row)
