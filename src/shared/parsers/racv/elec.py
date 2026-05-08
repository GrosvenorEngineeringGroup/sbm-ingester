"""RACV electricity multi-meter wide-format CSV parser.

Reads the RACV-internal electricity export (skiprows=2 to drop two header
rows; column names contain 'kWh'). One row per interval; columns named
"<meter-name> kWh" are each emitted as a separate (NMI, DataFrame) pair
keyed by Optima_<meter-name-prefix>. Days where the meter sums to zero
across all intervals are filtered out as invalid.
"""

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

logger = Logger(service="racv-elec-parser", child=True)


def racv_elec_parser(file_name: str, error_file_path: str) -> ParserOutcome:
    if "OptimaGenerationData" in file_name:
        raise NotRelevantParser("Not Relevant Parser For File")

    # Cheap relevance gate: skiprows=[0,1] means the column header sits on
    # line 3. Read up to the first 3 lines and require the header line to
    # contain Date, Start Time, and a kWh marker. ``utf-8-sig`` strips a
    # BOM transparently.
    try:
        with Path(file_name).open(encoding="utf-8-sig") as f:
            header_lines = [f.readline() for _ in range(3)]
    except (OSError, UnicodeDecodeError) as e:
        raise NotRelevantParser(f"Not readable as a RACV electricity CSV: {e}") from e

    header_line = header_lines[2] if len(header_lines) >= 3 else ""
    if not all(token in header_line for token in ("Date", "Start Time")) or "kWh" not in header_line:
        raise NotRelevantParser("Not a RACV electricity CSV")

    # Gate passed — full parse. Failures here are ParserError.
    try:
        raw_df = pd.read_csv(file_name, skiprows=[0, 1], encoding="utf-8-sig")
    except Exception as e:
        raise ParserError(f"Failed to read RACV electricity CSV: {e}") from e

    cols = [x for x in raw_df.columns if "kWh" in x or x in ["Date", "Start Time"]]
    meter_cols = [x for x in cols if "kWh" in x]
    if not meter_cols:
        raise ParserError("Missing kWh meter columns in RACV electricity CSV")

    source_row_count = len(raw_df)
    skip_reasons: Counter[SkipReason] = Counter()

    # Timestamp coercion: rows whose timestamp does not parse are dropped
    # entirely (one bad timestamp invalidates the row across all meter
    # columns in this wide format).
    combined_ts = raw_df["Date"].astype(str) + " " + raw_df["Start Time"].astype(str)
    raw_df["Interval_Start"] = pd.to_datetime(combined_ts, errors="coerce")
    bad_ts_mask = raw_df["Interval_Start"].isna()
    bad_ts_count = int(bad_ts_mask.sum())
    if bad_ts_count:
        skip_reasons["unparseable_timestamp"] += bad_ts_count
        raw_df = raw_df.loc[~bad_ts_mask].copy()

    if raw_df.empty:
        rows_skipped = source_row_count
        if bad_ts_count == source_row_count and source_row_count > 0:
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
            reason="all_zero_valid",
        )

    dfs: ParserResult = []
    for mn in meter_cols:
        buf_df = raw_df[["Interval_Start", mn]].rename(columns={"Interval_Start": "t_start", mn: "E1_kWh"})
        # Permissive coerce: unparseable non-blank cells become NaN and are
        # counted as ``unparseable_value``. Blank cells (whitespace/NA) are
        # part of the wide-format contract (sparse meters) and are not
        # counted as skipped — we discard the blank_count return value.
        coerced, unparseable, _blank = coerce_numeric_column(buf_df["E1_kWh"])
        buf_df["E1_kWh"] = coerced
        if unparseable:
            skip_reasons["unparseable_value"] += unparseable
        buf_df = buf_df.set_index("t_start")

        # Daily aggregation to filter out invalid days
        daily_sum = buf_df.resample("D").sum(numeric_only=True)
        non_zero_dates = daily_sum[daily_sum["E1_kWh"] != 0].index
        buf_df = buf_df[buf_df.index.normalize().isin(non_zero_dates)]

        if not non_zero_dates.empty:
            dfs.append((f"Optima_{mn.split(' ')[0]}", buf_df))

    candidate_row_count = len(raw_df)
    rows_skipped = source_row_count - candidate_row_count

    if dfs:
        return ParserOutcome(
            status="processed",
            dataframes=dfs,
            source_row_count=source_row_count,
            candidate_row_count=candidate_row_count,
            rows_skipped=rows_skipped,
            skip_reasons=skip_reasons,
        )
    return ParserOutcome(
        status="processed_empty",
        source_row_count=source_row_count,
        rows_skipped=rows_skipped,
        skip_reasons=skip_reasons,
        reason="all_zero_valid",
    )
