"""Optima/BidEnergy "Demand Profile" CSV parser.

Persists three columns per interval per NMI:
  - kW           → sensor Optima_<NMI>-demand-kw,  unit "kw"
  - kVa          → sensor Optima_<NMI>-demand-kva, unit "kva"
  - Power Factor → sensor Optima_<NMI>-demand-pf,  unit ""  (dimensionless)

Like bunnings_billing_parser, this writes Hudi rows directly to
s3://hudibucketsrc/sensorDataFiles/ and returns [] to the dispatcher;
file_processor's channel-suffix gate would otherwise drop non-NEM12
column names like "kw"/"kva"/"pf".
"""

from __future__ import annotations

import csv
import io
from pathlib import Path
from typing import TYPE_CHECKING

from aws_lambda_powertools import Logger

if TYPE_CHECKING:
    from shared.parsers import ParserResult

logger = Logger(service="optima-demand-parser", child=True)

# (CSV column name, demand suffix in nem12_id, Hudi unit string)
CSV_FIELD_MAPPING: list[tuple[str, str, str]] = [
    ("kW", "kw", "kw"),
    ("kVa", "kva", "kva"),  # BidEnergy's actual capitalisation, not standard kVA
    ("Power Factor", "pf", ""),  # Dimensionless ratio
]


def _parse_demand_rows(file_path: str) -> list[dict[str, str]]:
    """Skip metadata rows, return data rows as DictReader dicts.

    Layout:
      Row 1-6: metadata key:value pairs (Commodities/Sites/Status/Country/Start/End)
      Row 7-8: blank
      Row 9: column header
      Row 10+: data    OR a single "No data found" sentinel line

    Returns [] if the file is the empty-data sentinel form.
    """
    with Path(file_path).open(encoding="utf-8") as f:
        lines = f.read().splitlines()

    # Empty-data sentinel: BidEnergy returns "No data found" instead of
    # column header + data rows when a site has no demand profile data.
    if any("No data found" in line for line in lines):
        return []

    data_section = "\n".join(lines[8:])  # row 9 onward (0-indexed 8)
    reader = csv.DictReader(io.StringIO(data_section))
    return [row for row in reader if row.get("Identifier")]


def demand_parser(file_name: str, error_file_path: str) -> ParserResult:
    # 1. Fast filename reject (no I/O) — case-insensitive, treat _ as space
    if "demand profile" not in Path(file_name).name.lower().replace("_", " "):
        raise Exception("Not a Demand Profile file (filename mismatch)")

    # 2. Content sniff (read first line only)
    with Path(file_name).open(encoding="utf-8") as f:
        first_line = f.readline()
    if not first_line.startswith("Commodities:"):
        raise Exception("Not a Demand Profile file (missing metadata header)")

    # 3. Parse data rows; short-circuit on no-data sentinel or empty
    rows = _parse_demand_rows(file_name)
    if not rows:
        logger.info("demand_no_rows_to_process", extra={"file": file_name})
        return []

    # TODO: Task 4 implements mapping lookup + Hudi write
    return []
