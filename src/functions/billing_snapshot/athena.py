"""Athena query orchestration for the billing snapshot Lambda."""

from __future__ import annotations

import csv
import io
import time
from typing import Any
from urllib.parse import urlparse


def chunk_sensor_ids(ids: list[str], chunk_count: int) -> list[list[str]]:
    """Split ``ids`` into roughly equal ``chunk_count`` chunks.

    The last chunk absorbs any remainder. Chunk sizes are kept below the
    Athena 256 KB SQL string limit — for ~11K Bunnings sensor IDs the
    spec's default ``chunk_count=8`` yields ~85 KB per chunk SQL.
    """
    base_size = len(ids) // chunk_count
    chunks: list[list[str]] = []
    for i in range(chunk_count - 1):
        chunks.append(ids[i * base_size : (i + 1) * base_size])
    chunks.append(ids[(chunk_count - 1) * base_size :])
    return chunks


def build_chunk_sql(ids: list[str], table: str, start_date: str) -> str:
    """Return the per-chunk Athena SQL with an IN-list of sensor IDs.

    ``start_date`` is an ISO date string used in ``ts >= timestamp '...'``.
    """
    in_list = ", ".join(f"'{sid}'" for sid in ids)
    return (
        f"SELECT sensorid, ts, val, unit FROM {table} WHERE sensorid IN ({in_list}) AND ts >= timestamp '{start_date}'"
    )


class AthenaQueryFailed(RuntimeError):
    """Raised when an Athena query ends in a non-SUCCEEDED state."""


class AthenaQueryTimeout(RuntimeError):
    """Raised when an Athena query does not complete within the poll timeout."""


def submit_query(client: Any, sql: str, workgroup: str, database: str) -> str:
    response = client.start_query_execution(
        QueryString=sql,
        WorkGroup=workgroup,
        QueryExecutionContext={"Database": database},
    )
    return response["QueryExecutionId"]


def poll_until_complete(
    client: Any,
    query_execution_id: str,
    interval: float,
    timeout: float,
) -> str:
    """Poll Athena until SUCCEEDED, then return the results S3 URI.

    Raises ``AthenaQueryFailed`` on FAILED/CANCELLED, ``AthenaQueryTimeout``
    after ``timeout`` seconds with no terminal state.
    """
    start = time.monotonic()
    while True:
        response = client.get_query_execution(QueryExecutionId=query_execution_id)
        execution = response["QueryExecution"]
        state = execution["Status"]["State"]
        if state == "SUCCEEDED":
            return execution["ResultConfiguration"]["OutputLocation"]
        if state in ("FAILED", "CANCELLED"):
            reason = execution["Status"].get("StateChangeReason", "unknown")
            raise AthenaQueryFailed(f"Athena query {query_execution_id} {state}: {reason}")
        if time.monotonic() - start >= timeout:
            raise AthenaQueryTimeout(f"Athena query {query_execution_id} did not finish within {timeout}s")
        time.sleep(interval)


def read_results_csv(s3_client: Any, s3_uri: str) -> list[tuple[str, str, str, str]]:
    """Download the Athena results CSV and return rows as 4-tuples.

    Header row is skipped. Field order matches the SELECT in build_chunk_sql:
    (sensorid, ts, val, unit).
    """
    parsed = urlparse(s3_uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read().decode("utf-8")
    reader = csv.reader(io.StringIO(body))
    next(reader)  # discard header
    return [(row[0], row[1], row[2], row[3]) for row in reader]
