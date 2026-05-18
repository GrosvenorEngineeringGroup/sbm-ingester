"""Athena query orchestration for the billing snapshot Lambda."""

from __future__ import annotations


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
