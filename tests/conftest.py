"""Pytest configuration for sbm-ingester tests."""

import json
import os
import sys
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

# Add function directories to sys.path for Lambda-style imports.
optima_exporter_path = Path(__file__).parent.parent / "src" / "functions" / "optima_exporter"
if str(optima_exporter_path) not in sys.path:
    sys.path.insert(0, str(optima_exporter_path))

# Add src to path so `from shared...` imports resolve at module-import time.
src_path = Path(__file__).parent.parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

# Required env vars for module-import-time reads. Without this, importing
# functions.file_processor.app raises KeyError because production code reads
# os.environ["SQS_QUEUE_URL"] at import time (no fallback).
os.environ.setdefault("SQS_QUEUE_URL", "https://sqs.test.local/queue")

from shared.common import HUDI_BUCKET, INPUT_BUCKET  # noqa: E402


@pytest.fixture
def mock_s3_buckets():
    """Yield a moto-mocked S3 client with the input + Hudi buckets created.

    Seeds ``nem12_mappings.json`` with a single ``NMI001-E1`` mapping for
    happy-path NEM12 fixtures.
    """
    with mock_aws():
        s3 = boto3.client("s3")
        for bucket in [INPUT_BUCKET, HUDI_BUCKET]:
            s3.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
            )
        s3.put_object(
            Bucket=INPUT_BUCKET,
            Key="nem12_mappings.json",
            Body=json.dumps({"NMI001-E1": "p:bunnings:abc"}),
        )
        yield s3


@pytest.fixture
def mock_dynamodb_idempotency():
    """Yield a moto-mocked DynamoDB client with the idempotency table created."""
    with mock_aws():
        ddb = boto3.client("dynamodb")
        ddb.create_table(
            TableName="sbm-ingester-idempotency",
            KeySchema=[{"AttributeName": "file_key", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "file_key", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        yield ddb


@pytest.fixture
def file_in_newtbp(mock_s3_buckets):
    """Factory: place a CSV body at ``newTBP/<key>`` and return a ``SourceFile``."""
    from shared.source_file import SourceFile

    def _factory(body: bytes, key: str = "newTBP/sample.csv"):
        mock_s3_buckets.put_object(Bucket=INPUT_BUCKET, Key=key, Body=body)
        return SourceFile(bucket=INPUT_BUCKET, key=key)

    return _factory
