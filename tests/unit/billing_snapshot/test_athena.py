"""Tests for billing_snapshot.athena helpers."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from athena import (
    AthenaQueryFailed,
    AthenaQueryTimeout,
    build_chunk_sql,
    chunk_sensor_ids,
    poll_until_complete,
    read_results_csv,
    submit_query,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


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


def test_read_results_csv_strips_header_and_yields_tuples(s3_client):
    s3_client.create_bucket(
        Bucket="test-results",
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
    )
    s3_client.put_object(
        Bucket="test-results",
        Key="qid.csv",
        Body=(FIXTURES_DIR / "athena_results_sample.csv").read_bytes(),
    )
    rows = read_results_csv(s3_client, "s3://test-results/qid.csv")
    assert rows == [
        ("p:bunnings:s1", "2025-01-01 00:00:00.000", "100.5", "kwh"),
        ("p:bunnings:s1", "2025-02-01 00:00:00.000", "110.0", "kwh"),
        ("p:bunnings:s2", "2025-01-01 00:00:00.000", "-42.50", "aud"),
    ]


def test_run_chunks_parallel_aggregates_all_rows():
    """All 3 chunks succeed → merged rows returned."""
    fake_athena = MagicMock()
    fake_athena.start_query_execution.side_effect = [
        {"QueryExecutionId": "q1"},
        {"QueryExecutionId": "q2"},
        {"QueryExecutionId": "q3"},
    ]
    fake_athena.get_query_execution.return_value = {
        "QueryExecution": {
            "Status": {"State": "SUCCEEDED"},
            "ResultConfiguration": {"OutputLocation": "s3://b/k.csv"},
        }
    }

    def fake_reader(_s3, uri):
        return [("sensor-x", "2025-01-01 00:00:00.000", "1.0", "kwh")]

    from athena import run_chunks_parallel

    rows = run_chunks_parallel(
        athena_client=fake_athena,
        s3_client=MagicMock(),
        chunks=[["a"], ["b"], ["c"]],
        workgroup="wg",
        database="default",
        table="t",
        start_date="2025-01-01",
        max_workers=3,
        poll_interval=0,
        poll_timeout=10,
        results_reader=fake_reader,
    )
    assert len(rows) == 3
    assert fake_athena.start_query_execution.call_count == 3


def test_run_chunks_parallel_raises_on_first_chunk_failure():
    fake_athena = MagicMock()
    fake_athena.start_query_execution.side_effect = [
        {"QueryExecutionId": "q1"},
        {"QueryExecutionId": "q2"},
    ]
    fake_athena.get_query_execution.side_effect = [
        {"QueryExecution": {"Status": {"State": "FAILED", "StateChangeReason": "boom"}}},
        {
            "QueryExecution": {
                "Status": {"State": "SUCCEEDED"},
                "ResultConfiguration": {"OutputLocation": "s3://b/k.csv"},
            }
        },
    ]

    from athena import run_chunks_parallel

    with pytest.raises(AthenaQueryFailed):
        run_chunks_parallel(
            athena_client=fake_athena,
            s3_client=MagicMock(),
            chunks=[["a"], ["b"]],
            workgroup="wg",
            database="default",
            table="t",
            start_date="2025-01-01",
            max_workers=2,
            poll_interval=0,
            poll_timeout=10,
            results_reader=lambda _s3, _uri: [],
        )
