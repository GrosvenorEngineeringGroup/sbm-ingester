"""Athena query execution for data gap detection."""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import awswrangler as wr
import pandas as pd
from aws_lambda_powertools import Logger

from src.functions.data_gap_detector.detector import (
    ATHENA_OUTPUT,
    BATCH_SIZE,
    DATABASE,
    MAX_WORKERS,
    build_query,
    chunk_list,
)

logger = Logger(service="data-gap-detector")

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 2


def query_batch(
    sensor_ids: list[str],
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    Query Athena for a batch of sensors with retry logic.

    Args:
        sensor_ids: List of sensorId values to query
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        DataFrame with sensorId, data_date, record_count columns

    Raises:
        Exception: If all retries fail
    """
    query = build_query(sensor_ids, start_date, end_date)

    last_exception = None
    for attempt in range(MAX_RETRIES):
        try:
            return wr.athena.read_sql_query(
                query,
                database=DATABASE,
                s3_output=ATHENA_OUTPUT,
            )
        except Exception as e:
            last_exception = e
            if attempt < MAX_RETRIES - 1:
                logger.warning(
                    f"Query attempt {attempt + 1}/{MAX_RETRIES} failed, retrying...",
                    extra={"error": str(e)},
                )
                time.sleep(RETRY_DELAY_SECONDS * (attempt + 1))  # Exponential backoff

    raise last_exception  # type: ignore[misc]


def query_all_sensors(
    sensor_ids: list[str],
    start_date: str,
    end_date: str,
) -> tuple[pd.DataFrame, list[str]]:
    """
    Query all sensors in batches with concurrent execution.

    Args:
        sensor_ids: List of all sensorId values to query
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        Tuple of (Combined DataFrame with all results, List of failed sensor IDs)
    """
    if not sensor_ids:
        return pd.DataFrame(columns=["sensorId", "data_date", "record_count"]), []

    batches = chunk_list(sensor_ids, BATCH_SIZE)
    results: list[pd.DataFrame] = []
    failed_sensors: list[str] = []

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
        futures = {
            executor.submit(query_batch, batch, start_date, end_date): (i, batch) for i, batch in enumerate(batches)
        }

        for future in as_completed(futures):
            batch_num, batch = futures[future]
            try:
                df = future.result()
                results.append(df)
                logger.info(f"Batch {batch_num + 1}/{len(batches)} complete: {len(df)} rows")
            except Exception as e:
                logger.error(f"Batch {batch_num + 1} failed after {MAX_RETRIES} retries: {e}")
                failed_sensors.extend(batch)

    if not results:
        return pd.DataFrame(columns=["sensorId", "data_date", "record_count"]), failed_sensors

    return pd.concat(results, ignore_index=True), failed_sensors
