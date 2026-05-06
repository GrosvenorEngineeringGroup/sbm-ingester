"""Green Square Schneider ComX 510 private wire CSV parser."""

from __future__ import annotations

from collections import Counter
from pathlib import Path

import pandas as pd
from aws_lambda_powertools import Logger

from shared.parsers import (
    NotRelevantParser,
    ParserError,
    ParserOutcome,
    SkipReason,
)

logger = Logger(service="green-square-comx-parser", child=True)


def green_square_private_wire_schneider_comx_parser(file_name: str, error_file_path: str) -> ParserOutcome:
    # Cheap relevance gate: read at most the first 2 lines. The ComX header
    # is on line 2; column 0 is the marker ``ComX510_Green_Square`` and
    # column 4 is the site name. ``utf-8-sig`` strips a BOM transparently.
    try:
        with Path(file_name).open(encoding="utf-8-sig", newline="") as f:
            first_two = [f.readline() for _ in range(2)]
    except (OSError, UnicodeDecodeError) as e:
        raise NotRelevantParser(f"Not readable as a Green Square ComX CSV: {e}") from e

    if len(first_two) < 2 or not first_two[1].strip():
        raise NotRelevantParser("Not Relevant Parser For File")

    # Marker is the first comma-separated field on line 2; substring match
    # would falsely accept e.g. ``NotComX510_Green_Square,...``.
    second_line_first_field = first_two[1].split(",", 1)[0].strip().strip('"')
    if second_line_first_field != "ComX510_Green_Square":
        raise NotRelevantParser("Not Relevant Parser For File")

    # Gate passed — re-parse the first two lines into a DataFrame to
    # extract the site name (column 4 of row 2). Body parse failures from
    # here on are ParserError.
    try:
        first_rows = pd.read_csv(file_name, header=None, nrows=2, encoding="utf-8-sig")
    except Exception as e:
        raise ParserError(f"Failed to read Green Square ComX header: {e}") from e

    try:
        raw_site_name = first_rows.iloc[1, 4]
    except IndexError as e:
        raise ParserError("Missing site name in ComX header") from e
    if not isinstance(raw_site_name, str) or not raw_site_name.strip():
        raise ParserError("Missing site name in ComX header")
    site_name = raw_site_name.replace(" ", "")

    try:
        raw_df = pd.read_csv(file_name, header=6, skip_blank_lines=False, encoding="utf-8-sig")
    except Exception as e:
        raise ParserError(f"Failed to read Green Square ComX data rows: {e}") from e

    source_row_count = len(raw_df)
    skip_reasons: Counter[SkipReason] = Counter()
    if "Active energy (Wh)" in raw_df.columns:
        energy_col = "Active energy (Wh)"
        divisor = 1000
    elif "Active energy (kWh)" in raw_df.columns:
        energy_col = "Active energy (kWh)"
        divisor = 1
    else:
        raise ParserError("Missing Active energy column in file.")

    if "Local Time Stamp" not in raw_df.columns:
        raise ParserError("Missing Local Time Stamp column in file.")

    energy_series = raw_df[energy_col]
    parsed = pd.to_numeric(energy_series, errors="coerce")
    blank_mask = energy_series.isna() | energy_series.astype(str).str.strip().eq("")
    unparseable_mask = (~blank_mask) & parsed.isna()
    unparseable_count = int(unparseable_mask.sum())
    blank_count = int(blank_mask.sum())
    if unparseable_count:
        skip_reasons["unparseable_value"] += unparseable_count
    if blank_count:
        skip_reasons["blank_value"] += blank_count

    valid_energy = parsed.notna()
    raw_df = raw_df.loc[valid_energy].copy()
    raw_df[energy_col] = parsed.loc[valid_energy] / divisor

    if raw_df.empty:
        rows_skipped = source_row_count
        return ParserOutcome(
            status="processed_empty",
            source_row_count=source_row_count,
            rows_skipped=rows_skipped,
            skip_reasons=skip_reasons,
            reason="no_valid_energy_rows",
        )

    raw_df["Local Time Stamp"] = pd.to_datetime(raw_df["Local Time Stamp"], dayfirst=True, errors="coerce")
    bad_ts_mask = raw_df["Local Time Stamp"].isna()
    bad_ts_count = int(bad_ts_mask.sum())
    if bad_ts_count:
        skip_reasons["unparseable_timestamp"] += bad_ts_count
        raw_df = raw_df.loc[~bad_ts_mask].copy()

    candidate_row_count = len(raw_df)
    rows_skipped = source_row_count - candidate_row_count

    if raw_df.empty:
        return ParserOutcome(
            status="processed_empty",
            source_row_count=source_row_count,
            rows_skipped=rows_skipped,
            skip_reasons=skip_reasons,
            reason="all_skipped",
        )

    buf_df = raw_df[["Local Time Stamp", energy_col]].rename(
        columns={"Local Time Stamp": "t_start", energy_col: "E1_kWh"}
    )
    buf_df = buf_df.set_index("t_start")

    return ParserOutcome(
        status="processed",
        dfs=[(f"GPWComX_{site_name}", buf_df)],
        source_row_count=source_row_count,
        candidate_row_count=candidate_row_count,
        rows_skipped=rows_skipped,
        skip_reasons=skip_reasons,
    )
