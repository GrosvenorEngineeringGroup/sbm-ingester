"""Optima/BidEnergy "Export Interval Usage Csv" parser.

Handles the 12-column long-format CSV produced by the BidEnergy
"Export Interval Usage Csv" download (POST /BuyerReport/exportdailyusagecsv).
File contains both Usage and Generation columns per interval; both are
persisted as separate channels (E1_kWh and B1_kWh respectively) keyed by NMI.
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

logger = Logger(service="optima-interval-parser", child=True)


def _is_no_data_sentinel(raw_df: pd.DataFrame) -> bool:
    """Detect AU/NZ ('No data is available') and WA ('No data found') sentinels."""
    # AU/NZ endpoint sentinel: single row with BuyerShortName == "No data is available"
    # and every other column blank.
    if len(raw_df) == 1 and "BuyerShortName" in raw_df.columns:
        buyer_short_name = raw_df["BuyerShortName"].iloc[0]
        if pd.notna(buyer_short_name) and str(buyer_short_name).strip() == "No data is available":
            other_values = raw_df.drop(columns=["BuyerShortName"]).iloc[0]
            non_blank_values = other_values.notna() & other_values.astype(str).str.strip().ne("")
            if not non_blank_values.any():
                return True

    # WA endpoint sentinel: pandas inferred "Unnamed:" columns around an "NMI" column
    # because the source CSV header was malformed-but-recognisable; at most two data
    # rows; one cell contains the literal "No data found". Bounded row count rules
    # out legitimate interval files that might happen to contain the string.
    if "NMI" in raw_df.columns and len(raw_df) <= 2:
        for col in raw_df.columns:
            if raw_df[col].astype(str).str.strip().eq("No data found").any():
                return True

    return False


def interval_parser(file_name: str) -> ParserOutcome:
    # Cheap relevance gate: read first line only. ``utf-8-sig`` strips a BOM
    # transparently so BOM-prefixed files (R1746-style exports) still match.
    try:
        with Path(file_name).open(encoding="utf-8-sig") as f:
            first_line = f.readline()
    except (OSError, UnicodeDecodeError) as e:
        raise NotRelevantParser(f"Not readable as an Optima interval CSV: {e}") from e

    # All three column markers must appear in the header row, OR the file matches
    # the WA endpoint's "No data found" sentinel header (different shape, no Date /
    # Start Time / Identifier columns — pandas inferred "Unnamed:" placeholders).
    is_wa_sentinel_header = all(token in first_line for token in ("Unnamed: 0", "NMI", "Unnamed: 2"))
    if not (all(token in first_line for token in ("Date", "Start Time", "Identifier")) or is_wa_sentinel_header):
        raise NotRelevantParser("Not an Optima interval CSV")

    # Gate passed — full parse. Failures here indicate a corrupt body of a
    # file that already self-identified as ours, so they are ParserError.
    try:
        raw_df = pd.read_csv(file_name, encoding="utf-8-sig")
    except Exception as e:
        raise ParserError(f"Failed to read Optima interval CSV: {e}") from e

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

    source_row_count = len(raw_df)
    skip_reasons: Counter[SkipReason] = Counter()

    # Numeric coercion: non-blank malformed → unparseable_value;
    # we do NOT count blank_value here because a row with a blank value in one
    # column may still be valid via another value column.
    for column in value_columns:
        coerced, unparseable, _blank = coerce_numeric_column(raw_df[column])
        raw_df[column] = coerced
        if unparseable:
            skip_reasons["unparseable_value"] += unparseable

    # Timestamp coercion: drop rows whose timestamp does not parse.
    combined_ts = raw_df["Date"].astype(str) + " " + raw_df["Start Time"].astype(str)
    raw_df["Interval_Start"] = pd.to_datetime(combined_ts, errors="coerce")
    bad_ts_mask = raw_df["Interval_Start"].isna()
    bad_ts_count = int(bad_ts_mask.sum())
    if bad_ts_count:
        skip_reasons["unparseable_timestamp"] += bad_ts_count
        raw_df = raw_df.loc[~bad_ts_mask].copy()

    # A row is a value-bearing candidate only if at least one of the value
    # columns has a usable numeric. Rows where all value columns are blank
    # (or all non-blank but unparseable) get filtered here; they do not
    # contribute to candidate_row_count.
    if value_columns:
        any_value_mask = pd.Series(False, index=raw_df.index)
        for column in value_columns:
            any_value_mask = any_value_mask | raw_df[column].notna()
        # Rows with neither timestamp parse failure nor any usable value
        # are effectively skipped; they were originally blank-value rows
        # (the unparseable were already counted above).
        no_value_mask = ~any_value_mask
        no_value_count = int(no_value_mask.sum())
        if no_value_count:
            skip_reasons["blank_value"] += no_value_count
        raw_df = raw_df.loc[any_value_mask].copy()

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

    return ParserOutcome(
        status="processed",
        dataframes=dfs,
        source_row_count=source_row_count,
        candidate_row_count=candidate_row_count,
        rows_skipped=rows_skipped,
        skip_reasons=skip_reasons,
    )
