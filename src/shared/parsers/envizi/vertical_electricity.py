"""Envizi vertical-format electricity CSV parser."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
from aws_lambda_powertools import Logger

if TYPE_CHECKING:
    from shared.parsers import ParserResult

logger = Logger(service="envizi-vertical-electricity-parser", child=True)


def envizi_vertical_parser_electricity(file_name: str, error_file_path: str) -> ParserResult:
    if "OptimaGenerationData" in file_name:
        raise Exception("Not Relevant Parser For File")

    raw_df = pd.read_csv(file_name)
    raw_df["Interval_Start"] = pd.to_datetime(raw_df["Interval_Start"])
    raw_df["Serial_No"] = raw_df["Serial_No"].astype(str)

    dfs: ParserResult = []
    for name in sorted(raw_df["Serial_No"].unique()):
        buf_df = raw_df.loc[raw_df["Serial_No"] == name, ["Interval_Start", "Interval_End", "kWh"]]
        buf_df = buf_df.rename(columns={"Interval_Start": "t_start", "kWh": "E1_kWh"})
        buf_df = buf_df.set_index("t_start")
        dfs.append((f"Envizi_{name}", buf_df))

    return dfs
