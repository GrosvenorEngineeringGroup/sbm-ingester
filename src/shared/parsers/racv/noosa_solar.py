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

logger = Logger(service="noosa-solar-parser", child=True)

# Fronius inverter operating mode → numeric code
FRONIUS_MODE_MAP: dict[str, int] = {
    "Off": 1,
    "In Operation, No Feed In": 2,
    "Run Up Phase": 3,
    "Normal Operation": 4,
    "Power Reduction": 5,
    "Switch Off Phase": 6,
    "Error Exists": 7,
    "Standby": 8,
    "No Fronius Solar Net Comm": 9,
    "No Comm with Inverter": 10,
    "Overcurrent detected in Fronius Solar Net": 11,
    "Inverter Update being Performed": 12,
    "AFCI Event": 13,
}


def noosa_solar_parser(file_name: str, error_file_path: str) -> ParserOutcome:
    """Parse RACV Noosa Solar CSV with SkySpark point IDs as column headers."""
    if "RACV_Noosa_Solar" not in Path(file_name).name:
        raise NotRelevantParser("Not a Noosa Solar file")

    try:
        df = pd.read_csv(file_name, encoding="utf-8-sig")
    except Exception as e:
        raise ParserError(f"Failed to read Noosa Solar file: {e}") from e

    # Validate expected format: first column must be 'timestamp'
    if df.columns.empty or df.columns[0] != "timestamp":
        raise ParserError("Missing timestamp column in Noosa Solar file")
    if not df.empty and not df["timestamp"].notna().any():
        raise ParserError("Missing timestamp values in Noosa Solar file")

    source_row_count = len(df)
    skip_reasons: Counter[SkipReason] = Counter()

    # Strip timezone suffix (AEST/AEDT) and parse timestamps permissively.
    timestamp_text = df["timestamp"].astype(str)
    tz_values = timestamp_text.dropna().str.extract(r"\s+([A-Z]{3,4})$")[0].dropna().unique()
    unexpected_tz = [tz for tz in tz_values if tz != "AEST"]
    if len(unexpected_tz) > 0:
        logger.warning(
            "Unexpected timezone in Noosa Solar file",
            extra={"timezones": unexpected_tz},
        )

    timestamp_text = timestamp_text.str.replace(r"\s+[A-Z]{3,4}$", "", regex=True)
    df["timestamp"] = pd.to_datetime(timestamp_text, format="%d-%b-%y %I:%M %p", errors="coerce")
    bad_ts_mask = df["timestamp"].isna()
    bad_ts_count = int(bad_ts_mask.sum())
    if bad_ts_count:
        skip_reasons["unparseable_timestamp"] += bad_ts_count
        df = df.loc[~bad_ts_mask].copy()

    sensor_columns = [col for col in df.columns if col.startswith("p:")]

    results: ParserResult = []
    for raw_col in sensor_columns:
        series = df[raw_col]
        # Strip parenthesized suffix e.g. "p:racv:r:xxx (kW-hr)" -> "p:racv:r:xxx"
        sensor_id = raw_col.split(" (")[0]

        # Dynamic type detection: try numeric conversion
        numeric_series = pd.to_numeric(series, errors="coerce")
        non_null_count = series.dropna().shape[0]
        numeric_count = numeric_series.dropna().shape[0]

        if non_null_count == 0:
            continue  # Skip all-NaN columns

        if numeric_count >= non_null_count * 0.5:
            # Numeric column (kWh energy readings) — permissive coerce via
            # the shared helper. We only count unparseable_value here;
            # blank cells are tracked separately in tall-format parsers
            # but here we already gate the column at non_null_count == 0
            # above, so blank counts are the dropped/NA tail and not
            # informative as skip reasons.
            coerced, unparseable, _blank = coerce_numeric_column(series)
            if unparseable:
                skip_reasons["unparseable_value"] += unparseable

            col_name = "E1_kWh"
            out_df = pd.DataFrame(
                {
                    "t_start": df["timestamp"],
                    col_name: coerced,
                }
            )
        else:
            # Status column — map strings to Fronius mode codes
            col_name = "E1_mode"
            mapped = series.map(FRONIUS_MODE_MAP)
            unmapped = series.dropna()[~series.dropna().isin(FRONIUS_MODE_MAP)].unique()
            if len(unmapped) > 0:
                logger.warning(
                    "Unknown Fronius mode values",
                    extra={"sensor_id": sensor_id, "values": unmapped.tolist()},
                )
            out_df = pd.DataFrame(
                {
                    "t_start": df["timestamp"],
                    col_name: mapped.astype(float),
                }
            )

        out_df = out_df.dropna(subset=[col_name])
        out_df = out_df.set_index("t_start")

        if not out_df.empty:
            results.append((sensor_id, out_df))

    candidate_row_count = len(df)
    rows_skipped = source_row_count - candidate_row_count

    if not results:
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
            rows_skipped=rows_skipped,
            skip_reasons=skip_reasons,
            reason="all_blank",
        )

    return ParserOutcome(
        status="processed",
        dataframes=results,
        source_row_count=source_row_count,
        candidate_row_count=candidate_row_count,
        rows_skipped=rows_skipped,
        skip_reasons=skip_reasons,
    )
