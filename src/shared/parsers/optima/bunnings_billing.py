"""Bunnings BidEnergy "Usage and Spend Report" parser.

Reads UTF-16 LE encoded monthly billing CSVs, looks up Neptune point IDs from
the shared nem12_mappings.json, and writes Hudi-format sensor rows directly
to the Hudi source bucket. Designed to slot into the existing dispatcher
parser chain: matches by filename, side-effects the Hudi CSV, and returns an
explicit parser outcome.
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

from shared.common import HUDI_BUCKET, HUDI_FINAL_PREFIX
from shared.parsers import (
    NotRelevantParser,
    ParserError,
    ParserOutcome,
    ProcessingError,
    SkipReason,
)
from shared.parsers._mappings import get_nem12_mappings

logger = Logger(service="bunnings-billing-parser", child=True)

HUDI_PREFIX = HUDI_FINAL_PREFIX


# (CSV column name, billing suffix used in nem12_mappings key, unit source)
# unit_source: "usage" → Usage Measurement Unit, "spend" → Spend Currency
CSV_FIELD_MAPPING: list[tuple[str, str, str]] = [
    ("Peak", "billing-peak-usage", "usage"),
    ("OffPeak", "billing-off-peak-usage", "usage"),
    ("Shoulder", "billing-shoulder-usage", "usage"),
    ("Total Usage", "billing-total-usage", "usage"),
    ("Total GreenPower", "billing-total-greenpower-usage", "usage"),
    ("Estimated Peak", "billing-estimated-peak-usage", "usage"),
    ("Estimated OffPeak", "billing-estimated-off-peak-usage", "usage"),
    ("Estimated Shoulder", "billing-estimated-shoulder-usage", "usage"),
    ("Total Estimated Usage", "billing-total-estimated-usage", "usage"),
    ("Total Estimated GreenPower", "billing-total-estimated-greenpower-usage", "usage"),
    ("Energy Charge", "billing-energy-charge", "spend"),
    ("Total Network Charge", "billing-network-charge", "spend"),
    ("Environmental Charge", "billing-environmental-charge", "spend"),
    ("Metering Charge", "billing-metering-charge", "spend"),
    ("Other Charge", "billing-other-charge", "spend"),
    ("Total Spend", "billing-total-spend", "spend"),
    ("GreenPower Spend", "billing-greenpower-spend", "spend"),
    ("Estimated Energy Charge", "billing-estimated-energy-charge", "spend"),
    ("Estimated Network Charge", "billing-estimated-network-charge", "spend"),
    ("Estimated Environmental Charge", "billing-estimated-environmental-charge", "spend"),
    ("Estimated Metering Charge", "billing-estimated-metering-charge", "spend"),
    ("Estimated Other Charge", "billing-estimated-other-charge", "spend"),
    ("Total Estimated Spend", "billing-total-estimated-spend", "spend"),
]

REQUIRED_BILLING_COLUMNS: tuple[str, ...] = (
    "Identifier",
    "Date",
    *(csv_col for csv_col, _suffix, _unit_source in CSV_FIELD_MAPPING),
)


@dataclass(frozen=True)
class BillingParseResult:
    rows: list[dict[str, str]]
    rows_skipped: int = 0
    skip_reasons: Counter[SkipReason] = field(default_factory=Counter)


@dataclass(frozen=True)
class BillingBuildResult:
    body: str
    source_row_count: int
    candidate_row_count: int
    rows_written: int
    unmapped_count: int
    rows_skipped: int
    skip_reasons: Counter[SkipReason] = field(default_factory=Counter)
    # (kind, value) pairs of identifiers that missed mapping. Capped at 100
    # per file (spec parser-outcome-semantics-design, signal-fields table)
    # and deduplicated. Kind is "nmi" per spec identifier-kind table; value
    # is the full mapping lookup key so dashboards can debug unmapped
    # identifiers without reconstructing the suffix separately.
    unmapped_identifiers: tuple[tuple[str, str], ...] = ()


def _billing_date_to_ts(date_str: str) -> str | None:
    """Convert 'Mmm YYYY' (e.g. 'Mar 2026') to 'YYYY-MM-01 00:00:00'.

    Returns None if the string does not parse; callers skip such rows.
    """
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str.strip(), "%b %Y")
    except ValueError:
        return None
    return dt.strftime("%Y-%m-01 00:00:00")


def _pick_unit(billing_suffix: str, usage_unit: str, spend_currency: str) -> str:
    """Choose the Hudi unit string based on the billing-field suffix.

    Spend/charge fields use the Spend Currency column (e.g. AUD); everything
    else uses Usage Measurement Unit (e.g. kWh). Returned lowercased to match
    existing Hudi rows (e.g. 'kwh').
    """
    if "charge" in billing_suffix or "spend" in billing_suffix:
        return (spend_currency or "aud").lower()
    return (usage_unit or "kwh").lower()


def _decode_utf16_csv(file_path: str) -> list[str]:
    """Decode a UTF-16 LE (with BOM) CSV and normalise line endings.

    Returns a list of logical lines with trailing newline stripped.
    """
    raw = Path(file_path).read_bytes()
    text = raw.decode("utf-16-le")
    if text.startswith("﻿"):
        text = text[1:]
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    return text.split("\n")


def _validate_required_headers(fieldnames: list[str] | None) -> None:
    present_headers = set(fieldnames or [])
    missing_headers = [header for header in REQUIRED_BILLING_COLUMNS if header not in present_headers]
    if missing_headers:
        raise ParserError(f"Missing Bunnings billing columns: {', '.join(missing_headers)}")


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
      - Missing required cells (Identifier / Date / value columns) →
        ``row_anchor_failure`` if the identifier is missing, otherwise
        ``row_shape_mismatch``.
      - Missing optional cells (e.g. Site Name) → row proceeds (returns None).
    """
    if None in row:
        return "row_shape_mismatch"

    missing_required = [
        fieldname
        for fieldname in REQUIRED_BILLING_COLUMNS
        if fieldname in (fieldnames or []) and row.get(fieldname) is None
    ]
    if not missing_required:
        return None
    if "Identifier" in missing_required:
        return "row_anchor_failure"
    return "row_shape_mismatch"


def _parse_billing_rows(file_path: str) -> BillingParseResult:
    """Skip 7 metadata rows and return data rows as DictReader dicts."""
    lines = _decode_utf16_csv(file_path)
    # Row 1-5: metadata key:value; Row 6-7: blank; Row 8: header; Row 9+: data
    # Feed from row 8 onward (index 7) so DictReader treats row 8 as header.
    data_section = "\n".join(lines[7:])
    reader = csv.DictReader(io.StringIO(data_section))
    _validate_required_headers(reader.fieldnames)
    rows: list[dict[str, str]] = []
    rows_skipped = 0
    skip_reasons: Counter[SkipReason] = Counter()
    for row_number, row in enumerate(reader, start=9):
        if not _row_has_content(row):
            continue
        shape_skip = _classify_row_shape(row, reader.fieldnames)
        if shape_skip is not None:
            rows_skipped += 1
            skip_reasons[shape_skip] += 1
            logger.warning(
                "bunnings_billing_row_shape_skip",
                extra={"row": row_number, "reason": shape_skip},
            )
            continue
        rows.append(row)
    return BillingParseResult(
        rows=rows,
        rows_skipped=rows_skipped,
        skip_reasons=skip_reasons,
    )


def _build_hudi_csv(
    rows: list[dict[str, str]],
    mappings: dict[str, str],
    rows_skipped: int = 0,
    skip_reasons: Counter[SkipReason] | None = None,
) -> BillingBuildResult:
    """Expand billing rows into Hudi sensor rows and collect outcome stats."""
    skip_reasons = Counter(skip_reasons or {})
    buf = io.StringIO()
    buf.write("sensorId,ts,val,unit,its,quality\n")
    # source_row_count counts every non-blank source row, including those
    # already skipped at the shape-validation stage (rows_skipped).
    source_row_count = len(rows) + rows_skipped
    candidate_row_count = 0
    rows_written = 0
    unmapped_count = 0
    # Track distinct mapping lookup keys that missed the mapping, capped
    # at 100 per spec.
    unmapped_identifiers_set: set[tuple[str, str]] = set()

    for row in rows:
        nmi = (row.get("Identifier") or "").strip()
        raw_date = row.get("Date") or ""
        ts = _billing_date_to_ts(raw_date)
        if not nmi:
            rows_skipped += 1
            skip_reasons["row_anchor_failure"] += 1
            continue
        if ts is None:
            rows_skipped += 1
            skip_reasons["unparseable_timestamp"] += 1
            if raw_date.strip():
                logger.warning(
                    "bunnings_billing_skip_bad_date",
                    extra={"nmi": nmi, "date": raw_date},
                )
            continue

        usage_unit = (row.get("Usage Measurement Unit") or "").strip() or "kWh"
        spend_currency = (row.get("Spend Currency") or "").strip() or "AUD"

        row_had_any_value = False
        row_produced_rows_or_unmapped = False
        for csv_col, billing_suffix, _unit_source in CSV_FIELD_MAPPING:
            raw_val = (row.get(csv_col) or "").strip()
            if not raw_val:
                continue
            row_had_any_value = True
            try:
                float(raw_val)
            except ValueError:
                skip_reasons["unparseable_value"] += 1
                continue

            row_produced_rows_or_unmapped = True
            candidate_row_count += 1
            lookup_key = f"{nmi}-{billing_suffix}"
            sensor_id = mappings.get(lookup_key)
            if not sensor_id:
                unmapped_count += 1
                if len(unmapped_identifiers_set) < 100:
                    unmapped_identifiers_set.add(("nmi", lookup_key))
                continue
            unit = _pick_unit(billing_suffix, usage_unit, spend_currency)
            buf.write(f"{sensor_id},{ts},{raw_val},{unit},{ts},\n")
            rows_written += 1
        # Only count the source row as "skipped" if every populated value
        # cell failed to parse (the row produced no Hudi rows at all).
        if row_had_any_value and not row_produced_rows_or_unmapped:
            rows_skipped += 1

    return BillingBuildResult(
        body=buf.getvalue(),
        source_row_count=source_row_count,
        candidate_row_count=candidate_row_count,
        rows_written=rows_written,
        unmapped_count=unmapped_count,
        rows_skipped=rows_skipped,
        skip_reasons=skip_reasons,
        unmapped_identifiers=tuple(sorted(unmapped_identifiers_set)),
    )


def bunnings_billing_parser(file_name: str) -> ParserOutcome:
    """Parse Bunnings billing CSV and write Hudi sensor rows to S3.

    Args:
        file_name: Local path to the downloaded CSV.

    Returns:
        ParserOutcome describing side-effect write status. The legacy
        dispatcher unwraps the empty dfs list until file_processor migrates.

    Raises:
        NotRelevantParser: If file_name does not look like a Bunnings billing CSV.
        ParserError: If a matching report cannot form valid billing candidates.
        ProcessingError: If the Hudi CSV cannot be written.
    """
    if "Bunnings-Usage and Spend Report" not in file_name:
        raise NotRelevantParser("Not Bunnings Usage and Spend File")

    parsed = _parse_billing_rows(file_name)
    if not parsed.rows:
        logger.info("bunnings_billing_no_rows_to_process", extra={"file": file_name})
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
            reason="all_blank",
        )

    mappings = get_nem12_mappings()
    build = _build_hudi_csv(
        parsed.rows,
        mappings,
        rows_skipped=parsed.rows_skipped,
        skip_reasons=parsed.skip_reasons,
    )

    if build.rows_written == 0:
        if build.candidate_row_count == 0 and build.rows_skipped == 0:
            return ParserOutcome(
                status="processed_empty",
                source_row_count=build.source_row_count,
                rows_written=0,
                reason="all_blank",
            )
        if build.candidate_row_count > 0 and build.unmapped_count == build.candidate_row_count:
            # Per spec edge case matrix: when all candidates fail to map,
            # the outcome is unmapped with reason=None.
            return ParserOutcome(
                status="unmapped",
                source_row_count=build.source_row_count,
                candidate_row_count=build.candidate_row_count,
                rows_written=0,
                unmapped_count=build.unmapped_count,
                rows_skipped=build.rows_skipped,
                skip_reasons=build.skip_reasons,
                unmapped_identifiers=build.unmapped_identifiers,
            )
        logger.info(
            "bunnings_billing_no_rows_written",
            extra={
                "file": file_name,
                "source_rows": build.source_row_count,
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
            unmapped_identifiers=build.unmapped_identifiers,
            reason="all_skipped",
        )

    ts_key = datetime.now(UTC).strftime("%Y%m%d%H%M%S%f")
    key = f"{HUDI_PREFIX}/billing_export_{ts_key}.csv"
    try:
        boto3.client("s3").put_object(
            Bucket=HUDI_BUCKET,
            Key=key,
            Body=build.body.encode(),
        )
    except Exception as e:
        raise ProcessingError(f"Failed to write Bunnings billing Hudi CSV: {e}") from e
    logger.info(
        "bunnings_billing_written",
        extra={
            "key": key,
            "rows_written": build.rows_written,
            "unmapped_count": build.unmapped_count,
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
        unmapped_identifiers=build.unmapped_identifiers,
    )
