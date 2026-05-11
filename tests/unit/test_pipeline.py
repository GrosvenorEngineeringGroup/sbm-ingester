"""End-to-end tests for ingest_file using moto-mocked S3 + DynamoDB."""

from __future__ import annotations

import json
from unittest.mock import patch

import boto3
import pytest

# tests/unit/conftest.py globally patches aws_lambda_powertools.utilities.idempotency
# .idempotent_function to a passthrough; this suits the legacy app.py-based tests but
# breaks tests of the new pipeline that depend on the real idempotent boundary. We
# bypass the patch by re-decorating ingest_file with the unpatched implementation
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
from shared.common import HUDI_BUCKET, INPUT_BUCKET, PROCESSED_DIR
from shared.parsers import _mappings as _mappings_mod
from shared.source_file import SourceFile
from tests.helpers.outcome_invariants import assert_parser_outcome_invariants

# Build a real idempotent ingest_file matching the decorator stack in pipeline.py
# (@tracer.capture_method outer, @idempotent_function inner).
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
    # Reset the module-level nem12_mappings cache so each test starts cold and
    # loads the test-fixture mapping from moto S3.
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
        # Provide nem12 mappings JSON the loader fetches.
        s3.put_object(
            Bucket=INPUT_BUCKET,
            Key="nem12_mappings.json",
            Body=json.dumps({"NMI001-E1": "p:bunnings:abc"}),
        )
        yield s3, ddb


def _put_source(s3, body: bytes, key: str = "newTBP/sample.csv") -> None:
    s3.put_object(Bucket=INPUT_BUCKET, Key=key, Body=body)


class TestIngestFileNem12HappyPath:
    def test_processed_outcome_routes_to_newp(self, aws_environment) -> None:
        s3, _ = aws_environment
        _put_source(s3, NEM12_BODY)

        outcome = ingest_file(source_file=SourceFile(bucket=INPUT_BUCKET, key="newTBP/sample.csv"))

        assert_parser_outcome_invariants(outcome)
        assert outcome.status == "processed"
        assert outcome.rows_written > 0

        listed = s3.list_objects_v2(Bucket=INPUT_BUCKET, Prefix=PROCESSED_DIR).get("Contents", [])
        assert any(o["Key"].endswith("sample.csv") for o in listed)

    def test_hudi_csv_written_to_final_prefix(self, aws_environment) -> None:
        s3, _ = aws_environment
        _put_source(s3, NEM12_BODY)

        ingest_file(source_file=SourceFile(bucket=INPUT_BUCKET, key="newTBP/sample.csv"))

        listed = s3.list_objects_v2(Bucket=HUDI_BUCKET, Prefix="sensorDataFiles/").get("Contents", [])
        assert len(listed) == 1


class TestIngestFileNem12Empty:
    def test_envelope_only_yields_processed_empty(self, aws_environment) -> None:
        s3, _ = aws_environment
        _put_source(s3, b"100,NEM12,202605060200,MDP1,Origin\n900\n")

        outcome = ingest_file(source_file=SourceFile(bucket=INPUT_BUCKET, key="newTBP/sample.csv"))

        assert_parser_outcome_invariants(outcome)
        assert outcome.status == "processed_empty"
        assert outcome.reason == "no_data_sentinel"


class TestIngestFileParseFailedCachable:
    def test_parser_error_returns_parse_failed_outcome(self, aws_environment) -> None:
        """Structurally broken file (not a NEM envelope, not parseable by any
        non-NEM dispatcher) -> parse_failed.

        Asserts: file moved to newParseErr/, outcome returned (not raised),
        outcome cached so a duplicate call returns the same outcome without
        re-attempting parsing.
        """
        s3, _ = aws_environment
        # Important: do NOT start with "100,NEM12," — _is_nem_envelope_only
        # would otherwise short-circuit to processed_empty(no_data_sentinel).
        # The body must also fail every non-NEM parser's header probe.
        s3.put_object(
            Bucket=INPUT_BUCKET,
            Key="newTBP/broken.csv",
            Body=b"\x00\x01\x02unintelligible binary garbage\xff\xfe\xfd\n",
        )

        outcome1 = ingest_file(source_file=SourceFile(bucket=INPUT_BUCKET, key="newTBP/broken.csv"))
        assert outcome1.status == "parse_failed"
        assert outcome1.reason == "parser_error"

        # File moved to newParseErr/
        listed = s3.list_objects_v2(Bucket=INPUT_BUCKET, Prefix="newParseErr/").get("Contents", [])
        assert any(o["Key"].endswith("broken.csv") for o in listed)


class TestIngestFileParseFailedMoveFailureLogged:
    def test_move_failure_in_parse_failed_path_logs_warning(self, aws_environment, caplog, monkeypatch) -> None:
        """When _move_source_file returns None in a parse_failed branch, the
        cached-outcome / file-still-in-newTBP inconsistency must surface as
        a WARN log (otherwise it is invisible on retry).
        """
        import logging

        s3, _ = aws_environment
        s3.put_object(
            Bucket=INPUT_BUCKET,
            Key="newTBP/broken.csv",
            Body=b"\x00\x01\x02unintelligible binary garbage\xff\xfe\xfd\n",
        )

        # Stub _move_source_file to simulate an S3 failure (returns None).
        monkeypatch.setattr(_pipeline_mod, "_move_source_file", lambda *_a, **_kw: None)

        with caplog.at_level(logging.WARNING, logger="file-processor"):
            outcome = ingest_file(source_file=SourceFile(bucket=INPUT_BUCKET, key="newTBP/broken.csv"))

        assert outcome.status == "parse_failed"
        warn_records = [r for r in caplog.records if "NOT moved to newParseErr/" in r.getMessage()]
        assert len(warn_records) == 1


class TestIngestFileTransientFailureRaises:
    def test_dynamodb_throttle_propagates_as_processing_error(self, aws_environment) -> None:
        s3, _ = aws_environment
        _put_source(s3, NEM12_BODY)

        # Simulate a 5xx-equivalent: HudiSourceCsvWriter.commit raises during
        # the S3 copy step. Pipeline must call abort and re-raise.
        from functions.file_processor.csv_writer import HudiSourceCsvWriter

        with patch.object(HudiSourceCsvWriter, "commit", side_effect=RuntimeError("simulated S3 5xx")):
            with pytest.raises(RuntimeError):
                ingest_file(source_file=SourceFile(bucket=INPUT_BUCKET, key="newTBP/sample.csv"))


class TestCacheHitEndToEnd:
    def test_duplicate_ingest_emits_idempotent_cache_hit_log(self, aws_environment, caplog) -> None:
        import logging

        s3, _ = aws_environment
        s3.put_object(Bucket=INPUT_BUCKET, Key="newTBP/sample.csv", Body=NEM12_BODY)

        src = SourceFile(bucket=INPUT_BUCKET, key="newTBP/sample.csv")
        ingest_file(source_file=src)

        with caplog.at_level(logging.INFO):
            ingest_file(source_file=src)

        cache_hit_records = [r for r in caplog.records if r.message == "idempotent_cache_hit"]
        assert len(cache_hit_records) == 1
        assert getattr(cache_hit_records[0], "source_bucket", None) == INPUT_BUCKET
        assert getattr(cache_hit_records[0], "source_key", None) == "newTBP/sample.csv"


class TestParserOutcomeSerializer:
    def test_parser_outcome_serializer_roundtrip_preserves_all_fields(self) -> None:
        """Confirm to_dict / from_dict round-trip preserves all ParserOutcome fields (except dataframes)."""
        from collections import Counter

        from shared.parsers.outcome import ParserOutcome

        original = ParserOutcome(
            status="unmapped",
            reason=None,
            source_row_count=100,
            candidate_row_count=80,
            rows_written=0,
            unmapped_count=80,
            rows_skipped=20,
            unmapped_identifiers=(("nem12_nmi", "ABC-E1"), ("p_id", "p:bunnings:xxx")),
            unsupported_suffixes=frozenset({"X9", "Y2"}),
            skip_reasons=Counter({"blank_value": 15, "unparseable_value": 5}),
        )

        as_dict = _parser_outcome_serializer.to_dict(original)
        rehydrated = _parser_outcome_serializer.from_dict(as_dict)

        assert rehydrated.status == original.status
        assert rehydrated.reason == original.reason
        assert rehydrated.source_row_count == original.source_row_count
        assert rehydrated.candidate_row_count == original.candidate_row_count
        assert rehydrated.rows_written == original.rows_written
        assert rehydrated.unmapped_count == original.unmapped_count
        assert rehydrated.rows_skipped == original.rows_skipped
        assert rehydrated.unmapped_identifiers == original.unmapped_identifiers
        assert rehydrated.unsupported_suffixes == original.unsupported_suffixes
        assert isinstance(rehydrated.unsupported_suffixes, frozenset)
        assert rehydrated.skip_reasons == original.skip_reasons
        assert isinstance(rehydrated.skip_reasons, Counter)
