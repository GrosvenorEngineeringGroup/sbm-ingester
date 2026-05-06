"""Optima/BidEnergy "Export Interval Usage Csv" parser.

Handles the 12-column long-format CSV produced by the BidEnergy
"Export Interval Usage Csv" download (POST /BuyerReport/exportdailyusagecsv).
File contains both Usage and Generation columns per interval; both are
persisted as separate channels (E1_kWh and B1_kWh respectively) keyed by NMI.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
from aws_lambda_powertools import Logger

if TYPE_CHECKING:
    from shared.parsers import ParserResult

logger = Logger(service="optima-interval-parser", child=True)


def interval_parser(file_name: str, error_file_path: str) -> ParserResult:
    raw_df = pd.read_csv(file_name)
    # BidEnergy returns a 148-byte sentinel CSV when a site has no data for the
    # requested range. Pandas reads "No data is available" as a single row with
    # NaN-typed Date/Start Time columns, which would crash the str+str datetime
    # concat below with UFuncTypeError. Detect and short-circuit to [].
    if len(raw_df) == 1 and raw_df["Date"].isna().all():
        logger.info("interval_no_data_sentinel", extra={"file": file_name})
        return []

    raw_df["Interval_Start"] = pd.to_datetime(raw_df["Date"] + " " + raw_df["Start Time"])
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

    return dfs
