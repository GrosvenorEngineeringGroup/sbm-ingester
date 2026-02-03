"""Athena query execution for data gap detection."""

from concurrent.futures import ThreadPoolExecutor, as_completed

import awswrangler as wr
import pandas as pd
from aws_lambda_powertools import Logger

from src.functions.data_gap_detector.detector import (
    ATHENA_OUTPUT,
    DATABASE,
    build_query,
    chunk_list,
)

logger = Logger(service="data-gap-detector")

# Configuration
BATCH_SIZE = 50
MAX_WORKERS = 5


def query_batch(
    sensor_ids: list[str],
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    Query Athena for a batch of sensors.

    Args:
        sensor_ids: List of sensorId values to query
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        DataFrame with sensorId, data_date, record_count columns
    """
    query = build_query(sensor_ids, start_date, end_date)

    return wr.athena.read_sql_query(
        query,
        database=DATABASE,
        s3_output=ATHENA_OUTPUT,
    )


def query_all_sensors(
    sensor_ids: list[str],
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    Query all sensors in batches with concurrent execution.

    Args:
        sensor_ids: List of all sensorId values to query
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        Combined DataFrame with all results
    """
    if not sensor_ids:
        return pd.DataFrame(columns=["sensorId", "data_date", "record_count"])

    batches = chunk_list(sensor_ids, BATCH_SIZE)
    results: list[pd.DataFrame] = []

    logger.info(
        "Querying sensors",
        extra={
            "total_sensors": len(sensor_ids),
            "batch_count": len(batches),
            "batch_size": BATCH_SIZE,
            "max_workers": MAX_WORKERS,
        },
    )

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(query_batch, batch, start_date, end_date): i for i, batch in enumerate(batches)}

        for future in as_completed(futures):
            batch_num = futures[future]
            try:
                df = future.result()
                results.append(df)
                logger.info(f"Batch {batch_num + 1}/{len(batches)} complete: {len(df)} rows")
            except Exception as e:
                logger.error(f"Batch {batch_num + 1} failed: {e}")
                # Continue with other batches

    if not results:
        return pd.DataFrame(columns=["sensorId", "data_date", "record_count"])

    return pd.concat(results, ignore_index=True)
