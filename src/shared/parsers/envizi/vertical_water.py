"""Envizi vertical-format water CSV parser."""

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
from shared.parsers._coerce import coerce_numeric_column

logger = Logger(service="envizi-vertical-water-parser", child=True)

ENVIZI_WATER_REQUIRED = {"Serial_No", "Interval_Start", "Interval_End", "Consumption", "Consumption Unit"}


def envizi_vertical_parser_water(file_name: str) -> ParserOutcome:
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

    if not all(token in first_line for token in ENVIZI_WATER_REQUIRED):
        raise NotRelevantParser("Not an Envizi water CSV")

    # Gate passed — full parse. Failures here are ParserError.
    try:
        raw_df = pd.read_csv(file_name, encoding="utf-8-sig")
    except Exception as e:
        raise ParserError(f"Failed to read Envizi water CSV: {e}") from e

    source_row_count = len(raw_df)
    skip_reasons: Counter[SkipReason] = Counter()

    raw_df["Interval_Start"] = pd.to_datetime(raw_df["Interval_Start"], errors="coerce")
    bad_ts_mask = raw_df["Interval_Start"].isna()
    bad_ts_count = int(bad_ts_mask.sum())
    if bad_ts_count:
        skip_reasons["unparseable_timestamp"] += bad_ts_count
        raw_df = raw_df.loc[~bad_ts_mask].copy()

    coerced, unparseable, blank = coerce_numeric_column(raw_df["Consumption"])
    raw_df["Consumption"] = coerced
    if unparseable:
        skip_reasons["unparseable_value"] += unparseable
    if blank:
        skip_reasons["blank_value"] += blank

    valid_value_mask = raw_df["Consumption"].notna()
    raw_df = raw_df.loc[valid_value_mask].copy()

    candidate_row_count = len(raw_df)
    rows_skipped = source_row_count - candidate_row_count

    if candidate_row_count == 0:
        if source_row_count == 0:
            return ParserOutcome(
                status="processed_empty",
                source_row_count=0,
                reason="all_blank",
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
            reason="all_blank",
        )

    raw_df["Serial_No"] = raw_df["Serial_No"].astype(str)

    dfs: ParserResult = []
    for name in sorted(raw_df["Serial_No"].unique()):
        buf_df = raw_df.loc[
            raw_df["Serial_No"] == name, ["Interval_Start", "Interval_End", "Consumption", "Consumption Unit"]
        ]

        unit_count = buf_df["Consumption Unit"].nunique()
        if unit_count != 1:
            logger.error(
                "envizi_vertical_parser_water: Multiple units", extra={"file": file_name, "unit_count": unit_count}
            )

        unit = buf_df["Consumption Unit"].iloc[0]
        buf_df = buf_df[["Interval_Start", "Consumption"]].rename(
            columns={"Interval_Start": "t_start", "Consumption": f"E1_{unit}"}
        )
        buf_df = buf_df.set_index("t_start")
        dfs.append((f"Envizi_{name}", buf_df))

    if not dfs:
        return ParserOutcome(
            status="processed_empty",
            source_row_count=source_row_count,
            candidate_row_count=candidate_row_count,
            rows_skipped=rows_skipped,
            skip_reasons=skip_reasons,
            reason="zero_rows",
        )

    return ParserOutcome(
        status="processed",
        dataframes=dfs,
        source_row_count=source_row_count,
        candidate_row_count=candidate_row_count,
        rows_skipped=rows_skipped,
        skip_reasons=skip_reasons,
    )
