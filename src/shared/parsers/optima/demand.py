"""Optima/BidEnergy "Demand Profile" CSV parser.

Persists three columns per interval per NMI:
  - kW           → sensor Optima_<NMI>-demand-kw,  unit "kw"
  - kVa          → sensor Optima_<NMI>-demand-kva, unit "kva"
  - Power Factor → sensor Optima_<NMI>-demand-pf,  unit ""  (dimensionless)

Like bunnings_billing_parser, this writes Hudi rows directly to
s3://hudibucketsrc/sensorDataFiles/ and returns an explicit parser outcome.
The legacy dispatcher compatibility wrapper still unwraps the empty dfs list;
file_processor's channel-suffix gate would otherwise drop non-NEM12 column
names like "kw"/"kva"/"pf".
"""

from __future__ import annotations

import csv
import io
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3
from aws_lambda_powertools import Logger

from shared.parsers import (
    NotRelevantParser,
    ParserError,
    ParserOutcome,
    ProcessingError,
    SkipReason,
)
from shared.parsers import _mappings as _mappings_mod

logger = Logger(service="optima-demand-parser", child=True)

HUDI_BUCKET = "hudibucketsrc"
HUDI_PREFIX = "sensorDataFiles"

# (CSV column name, demand suffix in nem12_id, Hudi unit string)
CSV_FIELD_MAPPING: list[tuple[str, str, str]] = [
    ("kW", "kw", "kw"),
    ("kVa", "kva", "kva"),  # BidEnergy's actual capitalisation, not standard kVA
    ("Power Factor", "pf", ""),  # Dimensionless ratio
]

REQUIRED_DEMAND_COLUMNS: tuple[str, ...] = (
    "Identifier",
    "ReadingDateTime",
    *(csv_col for csv_col, _suffix, _unit in CSV_FIELD_MAPPING),
)


@dataclass(frozen=True)
class DemandParseResult:
    rows: list[dict[str, str]]
    no_data_sentinel: bool
    rows_skipped: int = 0
    skip_reasons: Counter[SkipReason] = field(default_factory=Counter)


@dataclass(frozen=True)
class DemandBuildResult:
    body: str
    source_row_count: int
    candidate_row_count: int
    rows_written: int
    unmapped_count: int
    rows_skipped: int
    skip_reasons: Counter[SkipReason] = field(default_factory=Counter)


def _validate_required_headers(fieldnames: list[str] | None) -> None:
    present_headers = set(fieldnames or [])
    missing_headers = [header for header in REQUIRED_DEMAND_COLUMNS if header not in present_headers]
    if missing_headers:
        raise ParserError(f"Missing demand columns: {', '.join(missing_headers)}")


def _row_has_content(row: dict[str | None, Any]) -> bool:
    for value in row.values():
        if isinstance(value, list):
            if any(str(item).strip() for item in value if item is not None):
                return True
            continue
        if value is not None and str(value).strip():
            return True
    return False


def _classify_row_shape(row: dict[str | None, Any], fieldnames: list[str] | None) -> SkipReason | None:
    """Inspect row shape, returning a SkipReason if the row should be skipped.

    Skip-and-count semantics (replaces previous raise-on-mismatch behaviour):
      - Extra trailing cells (None key in row dict) → ``row_shape_mismatch``.
      - Missing required cells (Identifier / ReadingDateTime / value columns)
        → ``row_anchor_failure`` if the identifier is missing, otherwise
        ``row_shape_mismatch``.
      - Missing optional cells (e.g. Site Name) → row proceeds (returns None).
    """
    if None in row:
        return "row_shape_mismatch"

    missing_required = [
        fieldname
        for fieldname in REQUIRED_DEMAND_COLUMNS
        if fieldname in (fieldnames or []) and row.get(fieldname) is None
    ]
    if not missing_required:
        return None
    if "Identifier" in missing_required:
        return "row_anchor_failure"
    return "row_shape_mismatch"


def _parse_demand_rows(file_path: str) -> DemandParseResult:
    """Skip metadata rows and return data rows plus empty-data sentinel state.

    Layout:
      Row 1-6: metadata key:value pairs (Commodities/Sites/Status/Country/Start/End)
      Row 7-8: blank
      Row 9: column header
      Row 10+: data    OR a single "No data found" sentinel line
    """
    with Path(file_path).open(encoding="utf-8") as f:
        lines = f.read().splitlines()

    if any("No data found" in line for line in lines):
        return DemandParseResult(rows=[], no_data_sentinel=True)

    data_section = "\n".join(lines[8:])  # row 9 onward (0-indexed 8)
    reader = csv.DictReader(io.StringIO(data_section))
    _validate_required_headers(reader.fieldnames)
    rows: list[dict[str, str]] = []
    rows_skipped = 0
    skip_reasons: Counter[SkipReason] = Counter()
    for row_number, row in enumerate(reader, start=10):
        if not _row_has_content(row):
            continue
        shape_skip = _classify_row_shape(row, reader.fieldnames)
        if shape_skip is not None:
            rows_skipped += 1
            skip_reasons[shape_skip] += 1
            logger.warning(
                "demand_row_shape_skip",
                extra={"row": row_number, "reason": shape_skip},
            )
            continue
        rows.append(row)
    return DemandParseResult(
        rows=rows,
        no_data_sentinel=False,
        rows_skipped=rows_skipped,
        skip_reasons=skip_reasons,
    )


def _build_hudi_csv(
    rows: list[dict[str, str]],
    mappings: dict[str, str],
    rows_skipped: int = 0,
    skip_reasons: Counter[SkipReason] | None = None,
) -> DemandBuildResult:
    """Build the Hudi CSV body and collect candidate disposition statistics.

    locale note: %b (abbreviated month name) is locale-dependent. AWS Lambda
    Python runtime defaults to en_US.UTF-8 / C.UTF-8, where %b matches "Feb",
    "Mar", etc. Local dev environments using non-English locales would fail
    parsing — if this becomes a problem, switch to an explicit dict mapping.
    """
    skip_reasons = Counter(skip_reasons or {})
    buf = io.StringIO()
    buf.write("sensorId,ts,val,unit,its,quality\n")
    # source_row_count counts every non-blank source row, including those
    # already skipped at the shape-validation stage. This mirrors the
    # envizi DataFrame-parser convention where rows_skipped is reported
    # alongside the original total.
    source_row_count = len(rows) + rows_skipped
    candidate_row_count = 0
    rows_written = 0
    unmapped_count = 0

    for row in rows:
        nmi = (row.get("Identifier") or "").strip()
        raw_ts = (row.get("ReadingDateTime") or "").strip()
        if not nmi:
            rows_skipped += 1
            skip_reasons["row_anchor_failure"] += 1
            continue
        if not raw_ts:
            rows_skipped += 1
            skip_reasons["unparseable_timestamp"] += 1
            continue
        try:
            ts = datetime.strptime(raw_ts, "%d-%b-%Y %H:%M:%S")
        except ValueError:
            rows_skipped += 1
            skip_reasons["unparseable_timestamp"] += 1
            logger.warning("demand_bad_timestamp", extra={"nmi": nmi, "raw_ts": raw_ts})
            continue
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")

        row_had_any_value = False
        row_produced_rows_or_unmapped = False
        for csv_col, suffix, unit in CSV_FIELD_MAPPING:
            raw_val = (row.get(csv_col) or "").strip()
            if not raw_val:
                continue
            row_had_any_value = True
            # Validate as numeric but persist the raw string — preserves
            # source precision (e.g. "0.8800" stays "0.8800" instead of
            # collapsing to "0.88"). Matches bunnings_billing_parser's
            # raw-pass-through behaviour. Hudi stores as double either way.
            try:
                float(raw_val)
            except ValueError:
                skip_reasons["unparseable_value"] += 1
                continue

            row_produced_rows_or_unmapped = True
            candidate_row_count += 1
            sensor_id = mappings.get(f"Optima_{nmi}-demand-{suffix}")
            if not sensor_id:
                unmapped_count += 1
                continue

            # Hudi format: sensorId,ts,val,unit,its,quality
            # unit/quality fields are constants from CSV_FIELD_MAPPING
            # (kw/kva/"") so no comma-injection sanitisation needed.
            buf.write(f"{sensor_id},{ts_str},{raw_val},{unit},{ts_str},\n")
            rows_written += 1
        # Only count the source row as "skipped" if every populated value
        # cell failed to parse (so the row contributed nothing to the
        # output). Rows where one value column was bad but another was
        # valid still produce Hudi rows and are not considered skipped.
        if row_had_any_value and not row_produced_rows_or_unmapped:
            rows_skipped += 1

    return DemandBuildResult(
        body=buf.getvalue(),
        source_row_count=source_row_count,
        candidate_row_count=candidate_row_count,
        rows_written=rows_written,
        unmapped_count=unmapped_count,
        rows_skipped=rows_skipped,
        skip_reasons=skip_reasons,
    )


def demand_parser(file_name: str, error_file_path: str) -> ParserOutcome:
    # 1. Fast filename reject (no I/O) — case-insensitive, treat _ as space
    if "demand profile" not in Path(file_name).name.lower().replace("_", " "):
        raise NotRelevantParser("Not a Demand Profile file (filename mismatch)")

    # 2. Content sniff (read first line only)
    with Path(file_name).open(encoding="utf-8") as f:
        first_line = f.readline()
    if not first_line.startswith("Commodities:"):
        raise NotRelevantParser("Not a Demand Profile file (missing metadata header)")

    # 3. Parse data rows; short-circuit on no-data sentinel or empty
    parsed = _parse_demand_rows(file_name)
    if parsed.no_data_sentinel:
        logger.info("demand_no_rows_to_process", extra={"file": file_name})
        return ParserOutcome(status="processed_empty", reason="no_data_sentinel")
    if not parsed.rows:
        logger.info("demand_no_rows_to_process", extra={"file": file_name})
        if parsed.rows_skipped > 0:
            return ParserOutcome(
                status="processed_empty",
                source_row_count=parsed.rows_skipped,
                rows_written=0,
                rows_skipped=parsed.rows_skipped,
                skip_reasons=parsed.skip_reasons,
                reason="all_skipped",
            )
        return ParserOutcome(
            status="processed_empty",
            source_row_count=0,
            rows_written=0,
            reason="blank_values",
        )

    # 4. Build Hudi CSV using cached nem12 mappings
    mappings = _mappings_mod.get_nem12_mappings()
    build = _build_hudi_csv(
        parsed.rows,
        mappings,
        rows_skipped=parsed.rows_skipped,
        skip_reasons=parsed.skip_reasons,
    )

    if build.rows_written == 0:
        if build.candidate_row_count > 0 and build.unmapped_count == build.candidate_row_count:
            return ParserOutcome(
                status="unmapped",
                source_row_count=build.source_row_count,
                candidate_row_count=build.candidate_row_count,
                rows_written=0,
                unmapped_count=build.unmapped_count,
                rows_skipped=build.rows_skipped,
                skip_reasons=build.skip_reasons,
                reason="all_candidates_unmapped",
            )
        if build.candidate_row_count == 0 and build.rows_skipped == 0:
            return ParserOutcome(
                status="processed_empty",
                source_row_count=build.source_row_count,
                reason="blank_values",
            )
        logger.info(
            "demand_no_rows_written",
            extra={
                "file": file_name,
                "candidates": build.candidate_row_count,
                "skipped": build.rows_skipped,
                "unmapped": build.unmapped_count,
            },
        )
        return ParserOutcome(
            status="processed_empty",
            source_row_count=build.source_row_count,
            candidate_row_count=build.candidate_row_count,
            rows_written=0,
            unmapped_count=build.unmapped_count,
            rows_skipped=build.rows_skipped,
            skip_reasons=build.skip_reasons,
            reason="all_skipped",
        )

    # 5. Upload Hudi CSV directly to S3 (bypasses file_processor channel gate)
    ts_key = datetime.now(UTC).strftime("%Y%m%d%H%M%S%f")
    key = f"{HUDI_PREFIX}/demand_export_{ts_key}.csv"
    try:
        boto3.client("s3").put_object(
            Bucket=HUDI_BUCKET,
            Key=key,
            Body=build.body.encode(),
        )
    except Exception as e:
        raise ProcessingError(f"Failed to write demand Hudi CSV: {e}") from e
    logger.info(
        "demand_written",
        extra={
            "key": key,
            "rows": build.rows_written,
            "unmapped": build.unmapped_count,
            "skipped": build.rows_skipped,
        },
    )
    return ParserOutcome(
        status="processed",
        source_row_count=build.source_row_count,
        candidate_row_count=build.candidate_row_count,
        rows_written=build.rows_written,
        unmapped_count=build.unmapped_count,
        rows_skipped=build.rows_skipped,
        skip_reasons=build.skip_reasons,
    )
