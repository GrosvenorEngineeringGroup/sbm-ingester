"""Tests for InstrumentedDynamoDBPersistenceLayer cache-hit log emission.

This suite captures the persistence module logger's handler stream and parses
JSON lines, rather than reading raw ``LogRecord`` attributes via ``caplog``.

Why not ``caplog``: ``extra=`` kwargs land on the ``LogRecord`` regardless of
which formatter is attached, so a test that reads ``record.source_bucket``
silently passes even when the *serialized* output (what CloudWatch sees) has
dropped those fields. The actual production failure mode is exactly that —
a mismatched parent service name caused Powertools to fall back to stdlib's
plain formatter, which serialized only the message and discarded the JSON
fields.

To assert the *serialized* output, the test re-points the persistence
logger's handler stream at an in-test ``StringIO`` (Powertools snapshots
``sys.stdout`` at Logger init time, so neither ``capsys`` nor ``capfd``
intercepts the handler in this test process). We then parse the buffer for
JSON lines, which is exactly the shape CloudWatch ingests.
"""

from __future__ import annotations

import io
import json
import logging
from unittest.mock import patch

import boto3
import pytest
from aws_lambda_powertools.utilities.idempotency import DynamoDBPersistenceLayer
from aws_lambda_powertools.utilities.idempotency.exceptions import (
    IdempotencyItemAlreadyExistsError,
)
from moto import mock_aws

# Importing the file_processor app triggers instantiation of the parent
# Powertools Logger (service="file-processor"), which is the prerequisite
# for the persistence module's child logger to inherit the JSON formatter.
from functions.file_processor import app as _app  # noqa: F401
from functions.file_processor.persistence import InstrumentedDynamoDBPersistenceLayer


@pytest.fixture
def idempotency_table():
    with mock_aws():
        ddb = boto3.client("dynamodb", region_name="ap-southeast-2")
        ddb.create_table(
            TableName="sbm-ingester-idempotency",
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        yield ddb


@pytest.fixture
def captured_log_stream():
    """Re-point the file-processor logger's handler stream to a StringIO.

    Powertools attaches its JSON-formatting ``StreamHandler`` to the parent
    Python logger named ``file-processor``. The handler holds a reference to
    ``sys.stdout`` taken at Logger init time, which is invisible to pytest's
    runtime stdout capture. Swapping the handler stream lets the test read
    exactly the bytes that would otherwise hit stdout.
    """
    target_logger = logging.getLogger("file-processor")
    buffer = io.StringIO()
    original_streams: list[tuple[logging.Handler, object]] = []
    for handler in target_logger.handlers:
        if isinstance(handler, logging.StreamHandler):
            original_streams.append((handler, handler.stream))
            handler.stream = buffer
    try:
        yield buffer
    finally:
        for handler, original in original_streams:
            handler.stream = original


def _extract_cache_hit_logs(captured: str) -> list[dict]:
    """Parse a captured log buffer for JSON lines whose ``message`` is the cache-hit event."""
    hits: list[dict] = []
    for line in captured.splitlines():
        stripped = line.strip()
        if not stripped.startswith("{"):
            continue
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        if parsed.get("message") == "idempotent_cache_hit":
            hits.append(parsed)
    return hits


class TestIdempotentCacheHitLog:
    def test_emits_structured_json_with_source_fields(self, captured_log_stream, idempotency_table) -> None:
        layer = InstrumentedDynamoDBPersistenceLayer(
            table_name="sbm-ingester-idempotency",
        )
        data = {"bucket": "sbm-file-ingester", "key": "newTBP/foo.csv"}

        with (
            patch.object(
                DynamoDBPersistenceLayer,
                "save_inprogress",
                side_effect=IdempotencyItemAlreadyExistsError(),
            ),
            pytest.raises(IdempotencyItemAlreadyExistsError),
        ):
            layer.save_inprogress(data=data)

        out = captured_log_stream.getvalue()
        cache_hits = _extract_cache_hit_logs(out)
        assert len(cache_hits) == 1, f"Expected 1 cache-hit JSON line; got {len(cache_hits)}.\nbuffer:\n{out}"
        log = cache_hits[0]
        assert log["source_bucket"] == "sbm-file-ingester"
        assert log["source_key"] == "newTBP/foo.csv"

    def test_no_log_on_successful_save(self, captured_log_stream, idempotency_table) -> None:
        layer = InstrumentedDynamoDBPersistenceLayer(
            table_name="sbm-ingester-idempotency",
        )
        with patch.object(DynamoDBPersistenceLayer, "save_inprogress", return_value=None):
            layer.save_inprogress(data={"bucket": "b", "key": "k"})

        out = captured_log_stream.getvalue()
        cache_hits = _extract_cache_hit_logs(out)
        assert cache_hits == []
