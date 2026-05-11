"""Tests for the cache-hit / cache-miss / raise-vs-return contract."""

from __future__ import annotations

import json

import boto3
import pytest

# tests/unit/conftest.py globally patches aws_lambda_powertools idempotent_function
# to a passthrough. Bypass that here by re-decorating with the real implementation
# from the underlying module (which the patch did NOT touch).
from aws_lambda_powertools.utilities.idempotency.idempotency import (
    idempotent_function as _real_idempotent_function,
)
from moto import mock_aws

from functions.file_processor import pipeline as _pipeline_mod
from functions.file_processor.pipeline import (
    _parser_outcome_serializer,
)
from functions.file_processor.pipeline import (
    idempotency_config as _idempotency_config,
)
from functions.file_processor.pipeline import (
    persistence_layer as _persistence_layer,
)
from functions.file_processor.pipeline import (
    tracer as _tracer,
)
from shared.common import HUDI_BUCKET, INPUT_BUCKET
from shared.parsers import _mappings as _mappings_mod
from shared.source_file import SourceFile

_bare_ingest_file = (
    _pipeline_mod.ingest_file.__wrapped__
    if hasattr(_pipeline_mod.ingest_file, "__wrapped__")
    else _pipeline_mod.ingest_file
)
ingest_file = _tracer.capture_method(
    _real_idempotent_function(
        data_keyword_argument="source_file",
        persistence_store=_persistence_layer,
        config=_idempotency_config,
        output_serializer=_parser_outcome_serializer,
    )(_bare_ingest_file)
)


NEM12_BODY = b"""\
100,NEM12,202605060200,MDP1,Origin
200,NMI001,E1,1,E1,N1,METER1,kWh,30,
300,20260506,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,A,,,
900
"""


@pytest.fixture
def aws_environment(monkeypatch):
    monkeypatch.setenv("SQS_QUEUE_URL", "https://sqs.test.local/queue")
    monkeypatch.setattr(_mappings_mod, "_cache", None)
    with mock_aws():
        s3 = boto3.client("s3")
        ddb = boto3.client("dynamodb")
        for bucket in [INPUT_BUCKET, HUDI_BUCKET]:
            s3.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
            )
        ddb.create_table(
            TableName="sbm-ingester-idempotency",
            KeySchema=[{"AttributeName": "file_key", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "file_key", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        s3.put_object(
            Bucket=INPUT_BUCKET,
            Key="nem12_mappings.json",
            Body=json.dumps({"NMI001-E1": "p:bunnings:abc"}),
        )
        yield s3, ddb


class TestIdempotencyBoundary:
    def test_duplicate_call_returns_cached_outcome_without_reprocessing(self, aws_environment) -> None:
        s3, _ = aws_environment
        s3.put_object(Bucket=INPUT_BUCKET, Key="newTBP/sample.csv", Body=NEM12_BODY)

        src = SourceFile(bucket=INPUT_BUCKET, key="newTBP/sample.csv")
        outcome1 = ingest_file(source_file=src)

        # Manually re-place the source file at newTBP/ so the second call
        # would re-process if the cache were missed.
        s3.put_object(Bucket=INPUT_BUCKET, Key="newTBP/sample.csv", Body=NEM12_BODY)

        outcome2 = ingest_file(source_file=src)

        assert outcome2.status == outcome1.status
        assert outcome2.rows_written == outcome1.rows_written

        # Crucially: the second call did NOT move the re-placed file to newP/
        # again — it returned the cached outcome.
        newtbp_listing = s3.list_objects_v2(Bucket=INPUT_BUCKET, Prefix="newTBP/").get("Contents", [])
        # The re-placed file is still in newTBP/ because cache-hit short-circuits.
        assert any(o["Key"] == "newTBP/sample.csv" for o in newtbp_listing)

    def test_runtime_error_in_nem_path_propagates_does_not_reach_dispatcher(self, aws_environment, monkeypatch) -> None:
        from functions.file_processor import pipeline

        s3, _ = aws_environment
        s3.put_object(Bucket=INPUT_BUCKET, Key="newTBP/sample.csv", Body=NEM12_BODY)

        # Streaming parser raises a RuntimeError (not in _NEM_FALLTHROUGH_ERRORS).
        # Per spec: must propagate and must NOT be silently routed to non-NEM dispatcher.
        def boom(*_a, **_kw):
            raise RuntimeError("simulated nemreader internal bug")

        monkeypatch.setattr(pipeline, "stream_as_data_frames", boom)

        # dispatch_non_nem must NOT be reached.
        called = {"non_nem": False}

        def must_not_be_called(*_a, **_kw):
            called["non_nem"] = True
            raise AssertionError("dispatcher must not be reached on RuntimeError")

        monkeypatch.setattr(pipeline, "dispatch_non_nem", must_not_be_called)

        with pytest.raises(RuntimeError):
            ingest_file(source_file=SourceFile(bucket=INPUT_BUCKET, key="newTBP/sample.csv"))

        assert called["non_nem"] is False
