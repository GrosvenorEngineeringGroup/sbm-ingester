"""Tests for routing files to ``newIrrevFiles/`` (UNMAPPED_DIR).

When every candidate row in a file is unmapped — i.e. no Neptune ID is
found for any (NMI, suffix) pair — the file's final disposition must be
``unmapped`` and the source must be moved to ``newIrrevFiles/``, NOT
``newP/``. No rows must be written to Hudi.
"""

from __future__ import annotations

import json

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
from shared.common import HUDI_BUCKET, INPUT_BUCKET, PROCESSED_DIR, UNMAPPED_DIR
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


# NEM12 body where NMI "NMI_UNMAPPED" has no mapping in the seeded
# nem12_mappings.json — every row in this file must end up unmapped.
NEM12_UNMAPPED_BODY = b"""\
100,NEM12,202605060200,MDP1,Origin
200,NMI_UNMAPPED,E1,1,E1,N1,METER1,kWh,30,
300,20260506,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,A,,,
900
"""

# Partially mapped: NMI001-E1 IS in seeded mappings; UNMAPPED_NMI-E1 is not.
NEM12_PARTIAL_MAPPED_BODY = b"""\
100,NEM12,202605060200,MDP1,Origin
200,NMI001,E1,1,E1,N1,METER1,kWh,30,
300,20260506,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,A,,,
200,UNMAPPED_NMI,E1,1,E1,N1,METER2,kWh,30,
300,20260506,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,A,,,
900
"""


@pytest.fixture(autouse=True)
def _reset_mappings_cache(monkeypatch, mock_dynamodb_idempotency):
    monkeypatch.setattr(_mappings_mod, "_cache", None)
    yield


class TestUnmappedRoutesToNewIrrevFiles:
    def test_all_unmapped_routes_to_unmapped_dir(self, file_in_newtbp, mock_s3_buckets) -> None:
        source = file_in_newtbp(NEM12_UNMAPPED_BODY, key="newTBP/unmapped.csv")

        outcome = ingest_file(source_file=source)

        assert_parser_outcome_invariants(outcome)
        assert outcome.status == "unmapped"
        assert outcome.rows_written == 0

        unmapped_listing = mock_s3_buckets.list_objects_v2(Bucket=INPUT_BUCKET, Prefix=UNMAPPED_DIR).get("Contents", [])
        assert any(o["Key"].endswith("unmapped.csv") for o in unmapped_listing)

        processed_listing = mock_s3_buckets.list_objects_v2(Bucket=INPUT_BUCKET, Prefix=PROCESSED_DIR).get(
            "Contents", []
        )
        assert not any(o["Key"].endswith("unmapped.csv") for o in processed_listing)

        # No Hudi rows written.
        hudi_listing = mock_s3_buckets.list_objects_v2(Bucket=HUDI_BUCKET, Prefix="sensorDataFiles/").get(
            "Contents", []
        )
        assert hudi_listing == []

    def test_partial_mapping_routes_to_processed_dir(self, file_in_newtbp, mock_s3_buckets) -> None:
        """When at least one channel is mapped, the file is ``processed`` (newP/).

        The unmapped channels are recorded in the audit accumulator but do
        NOT downgrade the file's disposition.
        """
        source = file_in_newtbp(NEM12_PARTIAL_MAPPED_BODY, key="newTBP/partial.csv")

        outcome = ingest_file(source_file=source)

        assert_parser_outcome_invariants(outcome)
        assert outcome.status == "processed"
        assert outcome.rows_written > 0

        processed_listing = mock_s3_buckets.list_objects_v2(Bucket=INPUT_BUCKET, Prefix=PROCESSED_DIR).get(
            "Contents", []
        )
        assert any(o["Key"].endswith("partial.csv") for o in processed_listing)

        unmapped_listing = mock_s3_buckets.list_objects_v2(Bucket=INPUT_BUCKET, Prefix=UNMAPPED_DIR).get("Contents", [])
        assert not any(o["Key"].endswith("partial.csv") for o in unmapped_listing)

    def test_empty_mappings_results_in_unmapped(self, file_in_newtbp, mock_s3_buckets) -> None:
        """If the mappings JSON is empty, even mapped-friendly NMIs end up unmapped."""
        # Overwrite the seeded mappings with an empty dict.
        mock_s3_buckets.put_object(
            Bucket=INPUT_BUCKET,
            Key="nem12_mappings.json",
            Body=json.dumps({}),
        )

        # Use the body that would otherwise be happy-path mapped.
        body = b"""\
100,NEM12,202605060200,MDP1,Origin
200,NMI001,E1,1,E1,N1,METER1,kWh,30,
300,20260506,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,A,,,
900
"""
        source = file_in_newtbp(body, key="newTBP/no_mappings.csv")

        outcome = ingest_file(source_file=source)

        assert outcome.status == "unmapped"
        unmapped_listing = mock_s3_buckets.list_objects_v2(Bucket=INPUT_BUCKET, Prefix=UNMAPPED_DIR).get("Contents", [])
        assert any(o["Key"].endswith("no_mappings.csv") for o in unmapped_listing)
