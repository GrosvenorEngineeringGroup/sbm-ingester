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
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import boto3
from aws_lambda_powertools import Logger

from shared.parsers import _mappings as _mappings_mod

if TYPE_CHECKING:
    from shared.parsers import ParserResult

logger = Logger(service="optima-demand-parser", child=True)

HUDI_BUCKET = "hudibucketsrc"
HUDI_PREFIX = "sensorDataFiles"

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

    if any("No data found" in line for line in lines):
        return []

    data_section = "\n".join(lines[8:])  # row 9 onward (0-indexed 8)
    reader = csv.DictReader(io.StringIO(data_section))
    return [row for row in reader if row.get("Identifier")]


def _build_hudi_csv(rows: list[dict[str, str]], mappings: dict) -> tuple[str, int, int]:
    """Build the Hudi CSV body and return (body, rows_written, unmapped_count).

    locale note: %b (abbreviated month name) is locale-dependent. AWS Lambda
    Python runtime defaults to en_US.UTF-8 / C.UTF-8, where %b matches "Feb",
    "Mar", etc. Local dev environments using non-English locales would fail
    parsing — if this becomes a problem, switch to an explicit dict mapping.
    """
    buf = io.StringIO()
    buf.write("sensorId,ts,val,unit,its,quality\n")
    rows_written = 0
    unmapped_count = 0

    for row in rows:
        nmi = (row.get("Identifier") or "").strip()
        raw_ts = (row.get("ReadingDateTime") or "").strip()
        if not nmi or not raw_ts:
            continue
        try:
            ts = datetime.strptime(raw_ts, "%d-%b-%Y %H:%M:%S")
        except ValueError:
            logger.warning("demand_bad_timestamp", extra={"nmi": nmi, "raw_ts": raw_ts})
            continue
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")

        for csv_col, suffix, unit in CSV_FIELD_MAPPING:
            raw_val = (row.get(csv_col) or "").strip()
            if not raw_val:
                continue
            try:
                val = float(raw_val)
            except ValueError:
                continue

            sensor_id = mappings.get(f"Optima_{nmi}-demand-{suffix}")
            if not sensor_id:
                unmapped_count += 1
                continue

            # Hudi format: sensorId,ts,val,unit,its,quality
            buf.write(f"{sensor_id},{ts_str},{val},{unit},{ts_str},\n")
            rows_written += 1

    return buf.getvalue(), rows_written, unmapped_count


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

    # 4. Build Hudi CSV using cached nem12 mappings
    mappings = _mappings_mod.get_nem12_mappings()
    body, rows_written, unmapped_count = _build_hudi_csv(rows, mappings)

    if rows_written == 0:
        logger.info(
            "demand_no_rows_written",
            extra={"file": file_name, "unmapped": unmapped_count},
        )
        return []

    # 5. Upload Hudi CSV directly to S3 (bypasses file_processor channel gate)
    ts_key = datetime.now(UTC).strftime("%Y%m%d%H%M%S%f")
    key = f"{HUDI_PREFIX}/demand_export_{ts_key}.csv"
    boto3.client("s3").put_object(
        Bucket=HUDI_BUCKET,
        Key=key,
        Body=body.encode(),
    )
    logger.info(
        "demand_written",
        extra={"key": key, "rows": rows_written, "unmapped": unmapped_count},
    )
    return []
