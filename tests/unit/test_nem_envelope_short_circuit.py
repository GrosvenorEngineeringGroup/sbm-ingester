"""Tests for the empty NEM envelope short-circuit in ``ingest_file``.

An empty NEM12/NEM13 envelope (only ``100`` and ``900`` records, no data
``300`` rows) is a legitimate "no data this period" sentinel and must yield
``ParserOutcome(status="processed_empty", reason="no_data_sentinel")``
without ever consulting the non-NEM dispatcher.

A genuinely malformed NEM-shaped file (e.g. nemreader internal bug) must
NOT be short-circuited — it should surface via the normal parse_failed path.
"""

from __future__ import annotations

import pytest
from aws_lambda_powertools.utilities.idempotency.idempotency import (
    idempotent_function as _real_idempotent_function,
)

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
from tests.helpers.outcome_invariants import assert_parser_outcome_invariants

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


EMPTY_NEM12_BODY = b"100,NEM12,202605060200,MDP1,Origin\n900\n"
EMPTY_NEM13_BODY = b"100,NEM13,202605060200,MDP1,Origin\n900\n"


@pytest.fixture(autouse=True)
def _reset_mappings_cache(monkeypatch, mock_dynamodb_idempotency):
    monkeypatch.setattr(_mappings_mod, "_cache", None)
    yield


class TestEmptyNemEnvelopeShortCircuit:
    def test_empty_nem12_envelope_emits_processed_empty(self, file_in_newtbp, mock_s3_buckets) -> None:
        source = file_in_newtbp(EMPTY_NEM12_BODY, key="newTBP/empty_nem12.csv")

        outcome = ingest_file(source_file=source)

        assert_parser_outcome_invariants(outcome)
        assert outcome.status == "processed_empty"
        assert outcome.reason == "no_data_sentinel"

        # Source moved to newP/, nothing written to Hudi.
        newp = mock_s3_buckets.list_objects_v2(Bucket=INPUT_BUCKET, Prefix=PROCESSED_DIR).get("Contents", [])
        assert any(o["Key"].endswith("empty_nem12.csv") for o in newp)

        hudi = mock_s3_buckets.list_objects_v2(Bucket=HUDI_BUCKET, Prefix="sensorDataFiles/").get("Contents", [])
        assert hudi == []

    def test_empty_nem13_envelope_emits_processed_empty(self, file_in_newtbp, mock_s3_buckets) -> None:
        source = file_in_newtbp(EMPTY_NEM13_BODY, key="newTBP/empty_nem13.csv")

        outcome = ingest_file(source_file=source)

        assert_parser_outcome_invariants(outcome)
        assert outcome.status == "processed_empty"
        assert outcome.reason == "no_data_sentinel"

    def test_short_circuit_does_not_consult_non_nem_dispatcher(self, file_in_newtbp, monkeypatch) -> None:
        """Empty NEM envelope must NOT fall through to ``dispatch_non_nem``."""
        called = {"dispatched": False}

        def must_not_be_called(*_a, **_kw):
            called["dispatched"] = True
            raise AssertionError("dispatch_non_nem must not be called for empty NEM envelopes")

        monkeypatch.setattr(_pipeline_mod, "dispatch_non_nem", must_not_be_called)

        source = file_in_newtbp(EMPTY_NEM12_BODY, key="newTBP/empty_nem12_b.csv")
        outcome = ingest_file(source_file=source)

        assert outcome.status == "processed_empty"
        assert called["dispatched"] is False
