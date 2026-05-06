"""Envizi vertical-format electricity CSV parser."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from aws_lambda_powertools import Logger

from shared.parsers import NotRelevantParser, ParserError, ParserOutcome, ParserResult

logger = Logger(service="envizi-vertical-electricity-parser", child=True)

ENVIZI_ELECTRICITY_REQUIRED = {"Serial_No", "Interval_Start", "Interval_End", "kWh"}


def _coerce_numeric_column(raw_df: pd.DataFrame, column: str) -> None:
    series = raw_df[column]
    parsed = pd.to_numeric(series, errors="coerce")
    non_blank = series.notna() & series.astype(str).str.strip().ne("")
    invalid = non_blank & parsed.isna()
    if invalid.any():
        bad_value = series.loc[invalid].iloc[0]
        raise ParserError(f"Failed to parse Envizi electricity {column} values: {bad_value!r}")
    raw_df[column] = parsed


def envizi_vertical_parser_electricity(file_name: str, error_file_path: str) -> ParserOutcome:
    if "OptimaGenerationData" in file_name:
        raise NotRelevantParser("Not Relevant Parser For File")

    try:
        with Path(file_name).open("rb") as file:
            if b"\x00" in file.read(4096):
                raise ValueError("embedded null byte")
        raw_df = pd.read_csv(file_name)
    except Exception as e:
        raise NotRelevantParser(f"Not readable as an Envizi CSV: {e}") from e

    if not ENVIZI_ELECTRICITY_REQUIRED.issubset(raw_df.columns):
        raise NotRelevantParser("Not an Envizi electricity CSV")

    try:
        raw_df["Interval_Start"] = pd.to_datetime(raw_df["Interval_Start"])
    except Exception as e:
        raise ParserError(f"Failed to parse Envizi electricity timestamps: {e}") from e
    _coerce_numeric_column(raw_df, "kWh")
    if not raw_df["kWh"].notna().any():
        return ParserOutcome(status="processed_empty", source_row_count=len(raw_df), reason="blank_values")
    raw_df["Serial_No"] = raw_df["Serial_No"].astype(str)

    dfs: ParserResult = []
    for name in sorted(raw_df["Serial_No"].unique()):
        buf_df = raw_df.loc[raw_df["Serial_No"] == name, ["Interval_Start", "Interval_End", "kWh"]]
        buf_df = buf_df.rename(columns={"Interval_Start": "t_start", "kWh": "E1_kWh"})
        buf_df = buf_df.set_index("t_start")
        dfs.append((f"Envizi_{name}", buf_df))

    if not dfs:
        return ParserOutcome(status="processed_empty", source_row_count=len(raw_df), reason="no_rows")

    return ParserOutcome(status="processed", dfs=dfs, source_row_count=len(raw_df))
