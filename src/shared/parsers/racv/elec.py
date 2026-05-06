"""RACV electricity multi-meter wide-format CSV parser.

Reads the RACV-internal electricity export (skiprows=2 to drop two header
rows; column names contain 'kWh'). One row per interval; columns named
"<meter-name> kWh" are each emitted as a separate (NMI, DataFrame) pair
keyed by Optima_<meter-name-prefix>. Days where the meter sums to zero
across all intervals are filtered out as invalid.
"""

from __future__ import annotations

import pandas as pd
from aws_lambda_powertools import Logger

from shared.parsers import NotRelevantParser, ParserError, ParserOutcome, ParserResult

logger = Logger(service="racv-elec-parser", child=True)


def _coerce_numeric_column(raw_df: pd.DataFrame, column: str) -> None:
    series = raw_df[column]
    parsed = pd.to_numeric(series, errors="coerce")
    non_blank = series.notna() & series.astype(str).str.strip().ne("")
    invalid = non_blank & parsed.isna()
    if invalid.any():
        bad_value = series.loc[invalid].iloc[0]
        raise ParserError(f"Failed to parse RACV electricity values: {bad_value!r}")
    raw_df[column] = parsed


def racv_elec_parser(file_name: str, error_file_path: str) -> ParserOutcome:
    if "OptimaGenerationData" in file_name:
        raise NotRelevantParser("Not Relevant Parser For File")

    try:
        raw_df = pd.read_csv(file_name, skiprows=[0, 1])
    except Exception as e:
        raise NotRelevantParser(f"Not readable as a RACV electricity CSV: {e}") from e

    required_columns = {"Date", "Start Time"}
    if not required_columns.issubset(raw_df.columns):
        raise NotRelevantParser("Not a RACV electricity CSV")

    cols = [x for x in raw_df.columns if "kWh" in x or x in ["Date", "Start Time"]]
    meter_cols = [x for x in cols if "kWh" in x]
    if not meter_cols:
        raise NotRelevantParser("Not a RACV electricity CSV")

    try:
        raw_df["Interval_Start"] = pd.to_datetime(raw_df["Date"] + " " + raw_df["Start Time"])
    except Exception as e:
        raise ParserError(f"Failed to parse RACV electricity timestamps: {e}") from e

    dfs: ParserResult = []
    for mn in meter_cols:
        buf_df = raw_df[["Interval_Start", mn]].rename(columns={"Interval_Start": "t_start", mn: "E1_kWh"})
        _coerce_numeric_column(buf_df, "E1_kWh")
        buf_df = buf_df.set_index("t_start")

        # Daily aggregation to filter out invalid days
        daily_sum = buf_df.resample("D").sum(numeric_only=True)
        non_zero_dates = daily_sum[daily_sum["E1_kWh"] != 0].index
        buf_df = buf_df[buf_df.index.normalize().isin(non_zero_dates)]

        if not non_zero_dates.empty:
            dfs.append((f"Optima_{mn.split(' ')[0]}", buf_df))

    if dfs:
        return ParserOutcome(status="processed", dfs=dfs, source_row_count=len(raw_df))
    return ParserOutcome(status="processed_empty", source_row_count=len(raw_df), reason="all_zero_valid")
