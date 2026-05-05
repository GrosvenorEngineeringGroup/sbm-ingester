"""Envizi vertical-format bulk water CSV parser."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
from aws_lambda_powertools import Logger

if TYPE_CHECKING:
    from shared.parsers import ParserResult

logger = Logger(service="envizi-vertical-water-bulk-parser", child=True)


def envizi_vertical_parser_water_bulk(file_name: str, error_file_path: str) -> ParserResult:
    if "OptimaGenerationData" in file_name:
        raise Exception("Not Relevant Parser For File")

    raw_df = pd.read_csv(file_name)
    raw_df["Date_Time"] = pd.to_datetime(raw_df["Date_Time"])
    raw_df["Serial_No"] = raw_df["Serial_No"].astype(str)

    dfs: ParserResult = []
    for name in sorted(raw_df["Serial_No"].unique()):
        buf_df = raw_df.loc[raw_df["Serial_No"] == name, ["Date_Time", "kL"]]
        buf_df = buf_df.rename(columns={"Date_Time": "t_start", "kL": "E1_kL"})
        buf_df = buf_df.set_index("t_start")
        dfs.append((f"Envizi_{name}", buf_df))

    return dfs
