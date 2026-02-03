"""Core detection logic for data gaps in Hudi data lake."""

from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd

# Configuration
BATCH_SIZE = 50
MAX_WORKERS = 5
DATABASE = "default"
TABLE = "sensordata_default"
ATHENA_OUTPUT = "s3://hudibucketsrc/queryresult/"


def chunk_list(items: list, chunk_size: int) -> list[list]:
    """Split a list into chunks of specified size."""
    if not items:
        return []
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def build_query(sensor_ids: list[str], start_date: str, end_date: str) -> str:
    """
    Build Athena SQL query for batch of sensors.

    Args:
        sensor_ids: List of sensorId values to query
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        SQL query string
    """
    ids_str = ",".join(f"'{sid}'" for sid in sensor_ids)

    return f"""
        SELECT
            sensorId,
            DATE(ts) as data_date,
            COUNT(*) as record_count
        FROM {DATABASE}.{TABLE}
        WHERE sensorId IN ({ids_str})
          AND ts >= TIMESTAMP '{start_date} 00:00:00'
          AND ts <= TIMESTAMP '{end_date} 23:59:59'
        GROUP BY sensorId, DATE(ts)
        ORDER BY sensorId, data_date
    """


def analyze_sensor_gaps(
    sensor_id: str,
    nmi_channel: str,
    df: pd.DataFrame,
    start_date: str | None,
    end_date: str | None,
) -> dict[str, Any] | None:
    """
    Analyze a single sensor's data for gaps.

    Args:
        sensor_id: Neptune point ID
        nmi_channel: NMI-channel identifier
        df: DataFrame with data_date column for this sensor
        start_date: Optional user-specified start date
        end_date: Optional user-specified end date

    Returns:
        Dict with gap info or None if no gaps
    """
    # Normalize column names to lowercase (Athena returns lowercase)
    df_normalized = df.rename(columns={col: col.lower() for col in df.columns}) if not df.empty else df

    # Filter to this sensor
    sensor_df = df_normalized[df_normalized["sensorid"] == sensor_id] if not df_normalized.empty else df_normalized

    # No data case
    if sensor_df.empty:
        return {
            "nmi_channel": nmi_channel,
            "point_id": sensor_id,
            "issue_type": "no_data",
            "missing_dates": "",
            "missing_count": 0,
            "data_start": "",
            "data_end": "",
            "total_expected_days": 0,
        }

    # Get actual dates
    actual_dates = set(sensor_df["data_date"].tolist())

    # Determine date range
    if start_date and end_date:
        range_start = datetime.strptime(start_date, "%Y-%m-%d").date()
        range_end = datetime.strptime(end_date, "%Y-%m-%d").date()
    else:
        range_start = min(actual_dates)
        range_end = max(actual_dates)

    # Generate expected dates
    expected_dates: set[date] = set()
    current = range_start
    while current <= range_end:
        expected_dates.add(current)
        current += timedelta(days=1)

    # Find missing dates
    missing_dates = sorted(expected_dates - actual_dates)
    total_expected = len(expected_dates)

    # No gaps - data is complete
    if not missing_dates:
        return None

    return {
        "nmi_channel": nmi_channel,
        "point_id": sensor_id,
        "issue_type": "missing_dates",
        "missing_dates": ",".join(d.strftime("%Y-%m-%d") for d in missing_dates),
        "missing_count": len(missing_dates),
        "data_start": range_start.strftime("%Y-%m-%d"),
        "data_end": range_end.strftime("%Y-%m-%d"),
        "total_expected_days": total_expected,
    }
