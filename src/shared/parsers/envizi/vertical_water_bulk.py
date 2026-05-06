"""Envizi vertical-format bulk water CSV parser."""

from __future__ import annotations

from collections import Counter
from pathlib import Path

import pandas as pd
from aws_lambda_powertools import Logger

from shared.parsers import (
    NotRelevantParser,
    ParserError,
    ParserOutcome,
    ParserResult,
    SkipReason,
)

logger = Logger(service="envizi-vertical-water-bulk-parser", child=True)

ENVIZI_BULK_WATER_REQUIRED = {"Serial_No", "Date_Time", "kL"}


def _coerce_numeric_column(raw_df: pd.DataFrame, column: str) -> tuple[int, int]:
    """Permissively coerce ``column`` to numeric in place.

    Returns ``(unparseable_count, blank_count)``.
    """
    series = raw_df[column]
    parsed = pd.to_numeric(series, errors="coerce")
    blank_mask = series.isna() | series.astype(str).str.strip().eq("")
    unparseable_mask = (~blank_mask) & parsed.isna()
    raw_df[column] = parsed
    return int(unparseable_mask.sum()), int(blank_mask.sum())


def envizi_vertical_parser_water_bulk(file_name: str, error_file_path: str) -> ParserOutcome:
    if "OptimaGenerationData" in file_name:
        raise NotRelevantParser("Not Relevant Parser For File")

    # Cheap relevance gate: skip files with embedded null bytes and require
    # all expected column markers in the first header line. ``utf-8-sig``
    # strips a BOM transparently.
    try:
        with Path(file_name).open("rb") as file:
            if b"\x00" in file.read(4096):
                raise ValueError("embedded null byte")
        with Path(file_name).open(encoding="utf-8-sig") as f:
            first_line = f.readline()
    except (OSError, UnicodeDecodeError, ValueError) as e:
        raise NotRelevantParser(f"Not readable as an Envizi CSV: {e}") from e

    if not all(token in first_line for token in ENVIZI_BULK_WATER_REQUIRED):
        raise NotRelevantParser("Not an Envizi bulk water CSV")

    # Gate passed — full parse. Failures here are ParserError.
    try:
        raw_df = pd.read_csv(file_name, encoding="utf-8-sig")
    except Exception as e:
        raise ParserError(f"Failed to read Envizi bulk water CSV: {e}") from e

    source_row_count = len(raw_df)
    skip_reasons: Counter[SkipReason] = Counter()

    raw_df["Date_Time"] = pd.to_datetime(raw_df["Date_Time"], errors="coerce")
    bad_ts_mask = raw_df["Date_Time"].isna()
    bad_ts_count = int(bad_ts_mask.sum())
    if bad_ts_count:
        skip_reasons["unparseable_timestamp"] += bad_ts_count
        raw_df = raw_df.loc[~bad_ts_mask].copy()

    unparseable, blank = _coerce_numeric_column(raw_df, "kL")
    if unparseable:
        skip_reasons["unparseable_value"] += unparseable
    if blank:
        skip_reasons["blank_value"] += blank

    valid_value_mask = raw_df["kL"].notna()
    raw_df = raw_df.loc[valid_value_mask].copy()

    candidate_row_count = len(raw_df)
    rows_skipped = source_row_count - candidate_row_count

    if candidate_row_count == 0:
        if source_row_count == 0:
            return ParserOutcome(
                status="processed_empty",
                source_row_count=0,
                reason="blank_values",
            )
        if bad_ts_count == source_row_count:
            return ParserOutcome(
                status="processed_empty",
                source_row_count=source_row_count,
                rows_skipped=rows_skipped,
                skip_reasons=skip_reasons,
                reason="all_skipped",
            )
        return ParserOutcome(
            status="processed_empty",
            source_row_count=source_row_count,
            rows_skipped=rows_skipped,
            skip_reasons=skip_reasons,
            reason="blank_values",
        )

    raw_df["Serial_No"] = raw_df["Serial_No"].astype(str)

    dfs: ParserResult = []
    for name in sorted(raw_df["Serial_No"].unique()):
        buf_df = raw_df.loc[raw_df["Serial_No"] == name, ["Date_Time", "kL"]]
        buf_df = buf_df.rename(columns={"Date_Time": "t_start", "kL": "E1_kL"})
        buf_df = buf_df.set_index("t_start")
        dfs.append((f"Envizi_{name}", buf_df))

    if not dfs:
        return ParserOutcome(
            status="processed_empty",
            source_row_count=source_row_count,
            candidate_row_count=candidate_row_count,
            rows_skipped=rows_skipped,
            skip_reasons=skip_reasons,
            reason="no_rows",
        )

    return ParserOutcome(
        status="processed",
        dfs=dfs,
        source_row_count=source_row_count,
        candidate_row_count=candidate_row_count,
        rows_skipped=rows_skipped,
        skip_reasons=skip_reasons,
    )
