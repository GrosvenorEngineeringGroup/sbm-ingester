"""Optima/BidEnergy "Export Interval Usage Csv" parser.

Handles the 12-column long-format CSV produced by the BidEnergy
"Export Interval Usage Csv" download (POST /BuyerReport/exportdailyusagecsv).
File contains both Usage and Generation columns per interval; both are
persisted as separate channels (E1_kWh and B1_kWh respectively) keyed by NMI.
"""

from __future__ import annotations

import pandas as pd
from aws_lambda_powertools import Logger

from shared.parsers import NotRelevantParser, ParserError, ParserOutcome, ParserResult

logger = Logger(service="optima-interval-parser", child=True)


def _is_no_data_sentinel(raw_df: pd.DataFrame) -> bool:
    if len(raw_df) != 1 or "BuyerShortName" not in raw_df.columns:
        return False

    buyer_short_name = raw_df["BuyerShortName"].iloc[0]
    if pd.isna(buyer_short_name):
        return False

    if str(buyer_short_name).strip() != "No data is available":
        return False

    other_values = raw_df.drop(columns=["BuyerShortName"]).iloc[0]
    non_blank_values = other_values.notna() & other_values.astype(str).str.strip().ne("")
    return not non_blank_values.any()


def _coerce_numeric_column(raw_df: pd.DataFrame, column: str) -> None:
    series = raw_df[column]
    parsed = pd.to_numeric(series, errors="coerce")
    non_blank = series.notna() & series.astype(str).str.strip().ne("")
    invalid = non_blank & parsed.isna()
    if invalid.any():
        bad_value = series.loc[invalid].iloc[0]
        raise ParserError(f"Failed to parse interval {column} values: {bad_value!r}")
    raw_df[column] = parsed


def interval_parser(file_name: str, error_file_path: str) -> ParserOutcome:
    try:
        raw_df = pd.read_csv(file_name)
    except Exception as e:
        raise NotRelevantParser(f"Not readable as an Optima interval CSV: {e}") from e

    required_columns = {"Date", "Start Time", "Identifier"}
    if not required_columns.issubset(raw_df.columns):
        raise NotRelevantParser("Not an Optima interval CSV")

    # BidEnergy returns a 148-byte sentinel CSV when a site has no data for the
    # requested range. Match the marker row explicitly so malformed interval
    # rows with blank dates still go through validation.
    if _is_no_data_sentinel(raw_df):
        logger.info("interval_no_data_sentinel", extra={"file": file_name})
        return ParserOutcome(
            status="processed_empty",
            source_row_count=len(raw_df),
            reason="no_data_sentinel",
        )

    value_columns = [column for column in ("Usage", "Generation") if column in raw_df.columns]
    if not value_columns:
        raise ParserError("Missing interval value column: expected Usage or Generation")

    for column in value_columns:
        _coerce_numeric_column(raw_df, column)

    try:
        raw_df["Interval_Start"] = pd.to_datetime(raw_df["Date"] + " " + raw_df["Start Time"])
    except Exception as e:
        raise ParserError(f"Failed to parse interval timestamps: {e}") from e

    if not any(raw_df[column].notna().any() for column in value_columns):
        return ParserOutcome(
            status="processed_empty",
            source_row_count=len(raw_df),
            reason="blank_values",
        )

    raw_df["Identifier"] = raw_df["Identifier"].astype(str)

    dfs: ParserResult = []
    for name in sorted(raw_df["Identifier"].unique()):
        base_df = raw_df.loc[raw_df["Identifier"] == name].copy()

        # Build output DataFrame with t_start as index
        output_df = base_df[["Interval_Start"]].copy()
        output_df = output_df.rename(columns={"Interval_Start": "t_start"})

        # Add Usage column as E1_kWh if present
        if "Usage" in raw_df.columns:
            output_df["E1_kWh"] = base_df["Usage"].values

        # Add Generation column as B1_kWh if present
        if "Generation" in raw_df.columns:
            output_df["B1_kWh"] = base_df["Generation"].values

        output_df = output_df.set_index("t_start")
        dfs.append((f"Optima_{name}", output_df))

    return ParserOutcome(status="processed", dfs=dfs, source_row_count=len(raw_df))
