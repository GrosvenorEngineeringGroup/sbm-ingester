"""Tests for InstrumentedDynamoDBPersistenceLayer cache-hit log emission."""

from __future__ import annotations

import logging
from unittest.mock import patch

import boto3
import pytest
from aws_lambda_powertools.utilities.idempotency.exceptions import (
    IdempotencyItemAlreadyExistsError,
)
from moto import mock_aws

from functions.file_processor.persistence import InstrumentedDynamoDBPersistenceLayer


@pytest.fixture
def idempotency_table():
    with mock_aws():
        ddb = boto3.client("dynamodb")
        ddb.create_table(
            TableName="sbm-ingester-idempotency",
            KeySchema=[{"AttributeName": "file_key", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "file_key", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        yield ddb


class TestInstrumentedPersistenceLayer:
    def test_logs_idempotent_cache_hit_on_existing_record(self, idempotency_table, caplog) -> None:
        layer = InstrumentedDynamoDBPersistenceLayer(
            table_name="sbm-ingester-idempotency",
            key_attr="file_key",
        )

        # Simulate a parent-class save_inprogress that detects an existing record.
        with patch.object(
            type(layer).__mro__[1],  # parent class (DynamoDBPersistenceLayer)
            "save_inprogress",
            side_effect=IdempotencyItemAlreadyExistsError(),
        ):
            with caplog.at_level(logging.INFO):
                with pytest.raises(IdempotencyItemAlreadyExistsError):
                    layer.save_inprogress(data={"bucket": "sbm-file-ingester", "key": "newTBP/foo.csv"})

        # Find the structured log record by message.
        cache_hit_records = [r for r in caplog.records if r.message == "idempotent_cache_hit"]
        assert len(cache_hit_records) == 1
        # Logger may emit as JSON or as extra kwargs; the test asserts that
        # source_bucket and source_key fields are reachable on the record.
        record = cache_hit_records[0]
        # Powertools structures via 'extra'; the attributes land on the LogRecord.
        assert getattr(record, "source_bucket", None) == "sbm-file-ingester"
        assert getattr(record, "source_key", None) == "newTBP/foo.csv"

    def test_no_log_on_first_call(self, idempotency_table, caplog) -> None:
        layer = InstrumentedDynamoDBPersistenceLayer(
            table_name="sbm-ingester-idempotency",
            key_attr="file_key",
        )

        with patch.object(
            type(layer).__mro__[1],
            "save_inprogress",
            return_value=None,
        ):
            with caplog.at_level(logging.INFO):
                layer.save_inprogress(data={"bucket": "b", "key": "k"})

        cache_hit_records = [r for r in caplog.records if r.message == "idempotent_cache_hit"]
        assert len(cache_hit_records) == 0
