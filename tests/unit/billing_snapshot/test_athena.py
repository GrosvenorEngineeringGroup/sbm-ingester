"""Tests for billing_snapshot.athena helpers."""

from unittest.mock import MagicMock

import pytest
from athena import (
    AthenaQueryFailed,
    AthenaQueryTimeout,
    build_chunk_sql,
    chunk_sensor_ids,
    poll_until_complete,
    submit_query,
)


def test_chunk_sensor_ids_even_split():
    ids = [f"s{i}" for i in range(16)]
    chunks = chunk_sensor_ids(ids, chunk_count=4)
    assert [len(c) for c in chunks] == [4, 4, 4, 4]
    # Every ID appears exactly once
    assert {sid for c in chunks for sid in c} == set(ids)


def test_chunk_sensor_ids_uneven_remainder_into_last():
    ids = [f"s{i}" for i in range(10)]
    chunks = chunk_sensor_ids(ids, chunk_count=3)
    # 10 / 3 = 3 with remainder 1; last chunk absorbs the remainder
    assert [len(c) for c in chunks] == [3, 3, 4]
    assert sum(len(c) for c in chunks) == 10


def test_chunk_sensor_ids_chunk_count_one_returns_single_list():
    ids = ["a", "b", "c"]
    chunks = chunk_sensor_ids(ids, chunk_count=1)
    assert chunks == [["a", "b", "c"]]


def test_build_chunk_sql_contains_all_ids_and_filter():
    ids = ["p:bunnings:x1", "p:bunnings:x2"]
    sql = build_chunk_sql(ids, table="sensordata_default", start_date="2025-01-01")
    assert "SELECT sensorid, ts, val, unit" in sql
    assert "FROM sensordata_default" in sql
    assert "'p:bunnings:x1'" in sql
    assert "'p:bunnings:x2'" in sql
    assert "AND ts >= timestamp '2025-01-01'" in sql


def test_build_chunk_sql_quotes_ids_safely():
    sql = build_chunk_sql(["p:bunnings:a"], table="t", start_date="2025-01-01")
    assert "IN ('p:bunnings:a')" in sql


def test_submit_query_returns_query_execution_id():
    fake_athena = MagicMock()
    fake_athena.start_query_execution.return_value = {"QueryExecutionId": "qid-123"}
    qid = submit_query(fake_athena, sql="SELECT 1", workgroup="wg", database="default")
    assert qid == "qid-123"
    fake_athena.start_query_execution.assert_called_once_with(
        QueryString="SELECT 1",
        WorkGroup="wg",
        QueryExecutionContext={"Database": "default"},
    )


def test_poll_until_complete_succeeds_on_succeeded_state():
    fake_athena = MagicMock()
    fake_athena.get_query_execution.side_effect = [
        {"QueryExecution": {"Status": {"State": "RUNNING"}}},
        {
            "QueryExecution": {
                "Status": {"State": "SUCCEEDED"},
                "ResultConfiguration": {"OutputLocation": "s3://b/qid.csv"},
            }
        },
    ]
    location = poll_until_complete(fake_athena, "qid-123", interval=0, timeout=10)
    assert location == "s3://b/qid.csv"


def test_poll_until_complete_raises_on_failed():
    fake_athena = MagicMock()
    fake_athena.get_query_execution.return_value = {
        "QueryExecution": {
            "Status": {"State": "FAILED", "StateChangeReason": "boom"},
        }
    }
    with pytest.raises(AthenaQueryFailed) as exc:
        poll_until_complete(fake_athena, "qid-123", interval=0, timeout=10)
    assert "boom" in str(exc.value)


def test_poll_until_complete_raises_on_timeout():
    fake_athena = MagicMock()
    fake_athena.get_query_execution.return_value = {"QueryExecution": {"Status": {"State": "RUNNING"}}}
    with pytest.raises(AthenaQueryTimeout):
        poll_until_complete(fake_athena, "qid-123", interval=0, timeout=0)
