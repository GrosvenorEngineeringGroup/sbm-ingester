"""Envizi vertical-format water CSV parser."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from aws_lambda_powertools import Logger

from shared.parsers import NotRelevantParser, ParserError, ParserOutcome, ParserResult

logger = Logger(service="envizi-vertical-water-parser", child=True)

ENVIZI_WATER_REQUIRED = {"Serial_No", "Interval_Start", "Interval_End", "Consumption", "Consumption Unit"}


def _coerce_numeric_column(raw_df: pd.DataFrame, column: str) -> None:
    series = raw_df[column]
    parsed = pd.to_numeric(series, errors="coerce")
    non_blank = series.notna() & series.astype(str).str.strip().ne("")
    invalid = non_blank & parsed.isna()
    if invalid.any():
        bad_value = series.loc[invalid].iloc[0]
        raise ParserError(f"Failed to parse Envizi water {column} values: {bad_value!r}")
    raw_df[column] = parsed


def envizi_vertical_parser_water(file_name: str, error_file_path: str) -> ParserOutcome:
    if "OptimaGenerationData" in file_name:
        raise NotRelevantParser("Not Relevant Parser For File")

    try:
        with Path(file_name).open("rb") as file:
            if b"\x00" in file.read(4096):
                raise ValueError("embedded null byte")
        raw_df = pd.read_csv(file_name)
    except Exception as e:
        raise NotRelevantParser(f"Not readable as an Envizi CSV: {e}") from e

    if not ENVIZI_WATER_REQUIRED.issubset(raw_df.columns):
        raise NotRelevantParser("Not an Envizi water CSV")

    try:
        raw_df["Interval_Start"] = pd.to_datetime(raw_df["Interval_Start"])
    except Exception as e:
        raise ParserError(f"Failed to parse Envizi water timestamps: {e}") from e
    _coerce_numeric_column(raw_df, "Consumption")
    if not raw_df["Consumption"].notna().any():
        return ParserOutcome(status="processed_empty", source_row_count=len(raw_df), reason="blank_values")
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
        return ParserOutcome(status="processed_empty", source_row_count=len(raw_df), reason="no_rows")

    return ParserOutcome(status="processed", dfs=dfs, source_row_count=len(raw_df))
