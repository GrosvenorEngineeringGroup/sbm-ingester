from pathlib import Path

import pandas as pd
from aws_lambda_powertools import Logger

logger = Logger(service="noosa-solar-parser", child=True)

# Type alias — defined locally to avoid circular import with non_nem_parsers.py
ParserResult = list[tuple[str, pd.DataFrame]]

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
    "Inverter Update being Processed": 12,
    "AFCI Event": 13,
}


def noosa_solar_parser(file_name: str, error_file_path: str) -> ParserResult:
    """Parse RACV Noosa Solar CSV with SkySpark point IDs as column headers."""
    if "RACV_Noosa_Solar" not in Path(file_name).name:
        raise Exception("Not a Noosa Solar file")

    df = pd.read_csv(file_name, encoding="utf-8-sig")

    # Validate expected format: first column must be 'timestamp'
    if df.columns[0] != "timestamp":
        raise Exception("Missing timestamp column in Noosa Solar file")

    # Strip timezone suffix (AEST/AEDT) and parse timestamps
    tz_values = df["timestamp"].dropna().str.extract(r"\s+([A-Z]{3,4})$")[0].dropna().unique()
    unexpected_tz = [tz for tz in tz_values if tz != "AEST"]
    if len(unexpected_tz) > 0:
        logger.warning(
            "Unexpected timezone in Noosa Solar file",
            extra={"timezones": unexpected_tz.tolist()},
        )

    df["timestamp"] = df["timestamp"].str.replace(r"\s+[A-Z]{3,4}$", "", regex=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="%d-%b-%y %I:%M %p")

    sensor_columns = [col for col in df.columns if col.startswith("p:")]

    results: ParserResult = []
    for sensor_id in sensor_columns:
        series = df[sensor_id]

        # Dynamic type detection: try numeric conversion
        numeric_series = pd.to_numeric(series, errors="coerce")
        non_null_count = series.dropna().shape[0]
        numeric_count = numeric_series.dropna().shape[0]

        if non_null_count == 0:
            continue  # Skip all-NaN columns

        if numeric_count >= non_null_count * 0.5:
            # Numeric column (kWh energy readings)
            col_name = "E1_kWh"
            out_df = pd.DataFrame(
                {
                    "t_start": df["timestamp"],
                    col_name: numeric_series,
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

    if not results:
        raise Exception(f"No valid data in Noosa Solar file: {file_name}")

    return results
