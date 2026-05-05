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

from pathlib import Path
from typing import TYPE_CHECKING

from aws_lambda_powertools import Logger

if TYPE_CHECKING:
    from shared.parsers import ParserResult

logger = Logger(service="optima-demand-parser", child=True)


def demand_parser(file_name: str, error_file_path: str) -> ParserResult:
    # 1. Fast filename reject (no I/O) — case-insensitive
    # Normalise underscores → spaces so both "Demand_Profile" and
    # "demand profile" (manual download) match the same substring check.
    normalised_name = Path(file_name).name.lower().replace("_", " ")
    if "demand profile" not in normalised_name:
        raise Exception("Not a Demand Profile file (filename mismatch)")

    # 2. Content sniff (read first line only)
    with Path(file_name).open(encoding="utf-8") as f:
        first_line = f.readline()
    if not first_line.startswith("Commodities:"):
        raise Exception("Not a Demand Profile file (missing metadata header)")

    # TODO: Tasks 3-4 fill in the body
    return []
