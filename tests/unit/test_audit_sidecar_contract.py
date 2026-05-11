"""End-to-end contract tests for the audit sidecar wiring in ``ingest_file``.

The sidecar must be:

* Written when at least one row was skipped, or any candidates were unmapped,
  or any unsupported suffixes were observed.
* Absent for clean files (no skips, no unmapped, no unsupported suffixes).
* Subject to a 100-sample cap with a trailing ``{"truncated": true, ...}`` marker.
* Best-effort — a failure inside ``write_audit_sidecar`` must NOT fail the
  file's primary disposition.
"""

from __future__ import annotations

import json
from typing import Any

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


# Clean NEM12 — single happy-path row, full mapping coverage, no skips.
NEM12_CLEAN_BODY = b"""\
100,NEM12,202605060200,MDP1,Origin
200,NMI001,E1,1,E1,N1,METER1,kWh,30,
300,20260506,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,A,,,
900
"""

# Unmapped NEM12 — guarantees audit sidecar via unmapped_count > 0.
NEM12_UNMAPPED_BODY = b"""\
100,NEM12,202605060200,MDP1,Origin
200,NMI_UNMAPPED,E1,1,E1,N1,METER1,kWh,30,
300,20260506,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,A,,,
900
"""


@pytest.fixture(autouse=True)
def _reset_mappings_cache(monkeypatch, mock_dynamodb_idempotency):
    monkeypatch.setattr(_mappings_mod, "_cache", None)
    yield


def _audit_keys(s3_client) -> list[str]:
    return [obj["Key"] for obj in s3_client.list_objects_v2(Bucket=HUDI_BUCKET, Prefix="audit/").get("Contents", [])]


class TestAuditSidecarWiring:
    def test_clean_file_writes_no_audit_sidecar(self, file_in_newtbp, mock_s3_buckets) -> None:
        file_in_newtbp(NEM12_CLEAN_BODY, key="newTBP/clean.csv")
        outcome = ingest_file(source_file=SourceFile(bucket=INPUT_BUCKET, key="newTBP/clean.csv"))

        assert outcome.status == "processed"
        assert _audit_keys(mock_s3_buckets) == []

    def test_unmapped_file_writes_audit_sidecar(self, file_in_newtbp, mock_s3_buckets) -> None:
        file_in_newtbp(NEM12_UNMAPPED_BODY, key="newTBP/unmapped.csv")
        outcome = ingest_file(source_file=SourceFile(bucket=INPUT_BUCKET, key="newTBP/unmapped.csv"))

        assert outcome.status == "unmapped"

        keys = _audit_keys(mock_s3_buckets)
        assert len(keys) == 1
        assert keys[0].endswith("/unmapped.csv.skipped.json")

        payload = json.loads(mock_s3_buckets.get_object(Bucket=HUDI_BUCKET, Key=keys[0])["Body"].read().decode())
        # Schema sanity — required top-level keys per spec lines 605-621.
        for required in (
            "source_file",
            "outcome",
            "skip_reasons",
            "unmapped_identifiers",
            "unsupported_suffixes",
            "skipped_samples",
        ):
            assert required in payload, f"audit sidecar missing required field: {required}"

        assert payload["source_file"] == "unmapped.csv"
        assert payload["outcome"]["status"] == "unmapped"
        assert payload["outcome"]["unmapped_count"] >= 1
        # Unmapped identifiers must use the canonical ``nem12_nmi`` kind.
        kinds = {pair[0] for pair in payload["unmapped_identifiers"]}
        assert kinds == {"nem12_nmi"}

    def test_audit_failure_does_not_fail_pipeline(self, file_in_newtbp, mock_s3_buckets, monkeypatch) -> None:
        """If ``write_audit_sidecar`` raises, the file's primary disposition is unaffected."""

        def boom(*_a, **_kw):
            raise RuntimeError("simulated audit-write failure")

        monkeypatch.setattr(_pipeline_mod, "write_audit_sidecar", boom)

        file_in_newtbp(NEM12_UNMAPPED_BODY, key="newTBP/unmapped_audit_fail.csv")
        outcome = ingest_file(source_file=SourceFile(bucket=INPUT_BUCKET, key="newTBP/unmapped_audit_fail.csv"))

        # Primary disposition still applied.
        assert outcome.status == "unmapped"
        # No audit sidecar (write raised before put_object succeeded).
        assert _audit_keys(mock_s3_buckets) == []


class TestAuditSampleCap:
    """``write_audit_sidecar`` enforces a 100-sample cap with truncation marker."""

    def test_sample_cap_appends_truncation_marker(self, mock_s3_buckets) -> None:
        """Direct unit test of the cap — exercises the writer without going
        through the full pipeline so we can control sample count exactly.
        """
        from shared.audit import SAMPLE_CAP, write_audit_sidecar

        oversupplied: list[dict[str, Any]] = [
            {"row": i, "column": "E1_kWh", "value": "bad", "reason": "unparseable_value"}
            for i in range(SAMPLE_CAP + 50)  # 150 samples
        ]

        key = write_audit_sidecar(
            batch_ts="2026_May_07T00_00_00_000000",
            source_filename="oversupplied.csv",
            outcome_summary={"status": "processed", "rows_skipped": 150},
            skip_reasons={"unparseable_value": 150},
            unmapped_identifiers=[],
            unsupported_suffixes=[],
            skipped_samples=oversupplied,
            s3_client=mock_s3_buckets,
        )

        payload = json.loads(mock_s3_buckets.get_object(Bucket=HUDI_BUCKET, Key=key)["Body"].read())
        # First SAMPLE_CAP entries + 1 truncation marker.
        assert len(payload["skipped_samples"]) == SAMPLE_CAP + 1
        marker = payload["skipped_samples"][-1]
        assert marker.get("truncated") is True
        assert marker.get("total_skipped") == 150

    def test_under_cap_writes_no_truncation_marker(self, mock_s3_buckets) -> None:
        from shared.audit import write_audit_sidecar

        small: list[dict[str, Any]] = [
            {"row": i, "column": "E1_kWh", "value": "bad", "reason": "unparseable_value"} for i in range(5)
        ]

        key = write_audit_sidecar(
            batch_ts="2026_May_07T00_00_00_000000",
            source_filename="small.csv",
            outcome_summary={"status": "processed", "rows_skipped": 5},
            skip_reasons={"unparseable_value": 5},
            unmapped_identifiers=[],
            unsupported_suffixes=[],
            skipped_samples=small,
            s3_client=mock_s3_buckets,
            total_skipped=5,
        )

        payload = json.loads(mock_s3_buckets.get_object(Bucket=HUDI_BUCKET, Key=key)["Body"].read())
        assert len(payload["skipped_samples"]) == 5
        # No truncation marker.
        assert not any(s.get("truncated") for s in payload["skipped_samples"])

    def test_total_skipped_exceeds_supplied_samples_emits_marker(self, mock_s3_buckets) -> None:
        """Caller pre-caps samples but reports a larger ``total_skipped``."""
        from shared.audit import write_audit_sidecar

        pre_capped: list[dict[str, Any]] = [
            {"row": i, "column": "E1_kWh", "value": "bad", "reason": "unparseable_value"} for i in range(50)
        ]

        key = write_audit_sidecar(
            batch_ts="2026_May_07T00_00_00_000000",
            source_filename="precapped.csv",
            outcome_summary={"status": "processed", "rows_skipped": 250},
            skip_reasons={"unparseable_value": 250},
            unmapped_identifiers=[],
            unsupported_suffixes=[],
            skipped_samples=pre_capped,
            s3_client=mock_s3_buckets,
            total_skipped=250,
        )

        payload = json.loads(mock_s3_buckets.get_object(Bucket=HUDI_BUCKET, Key=key)["Body"].read())
        marker = payload["skipped_samples"][-1]
        assert marker.get("truncated") is True
        assert marker.get("total_skipped") == 250


class TestSourceFilenameSafety:
    def test_path_separators_replaced_in_key(self, mock_s3_buckets) -> None:
        from shared.audit import write_audit_sidecar

        # Path separators must not bleed into the S3 key as additional segments.
        key = write_audit_sidecar(
            batch_ts="2026_May_07T00_00_00_000000",
            source_filename="newTBP/sub/dirfile.csv",
            outcome_summary={"status": "processed"},
            skip_reasons={},
            unmapped_identifiers=[],
            unsupported_suffixes=[],
            skipped_samples=[],
            s3_client=mock_s3_buckets,
        )

        # Single audit segment in the key past ``audit/<batch_ts>/``.
        suffix = key.split("/", 2)[-1]
        assert "/" not in suffix
        # File still moves into newP regardless of audit; this test only checks
        # the audit key shape. Smoke-check the resulting key landed in S3.
        listed = _audit_keys_for(mock_s3_buckets)
        assert key in listed


def _audit_keys_for(s3_client) -> list[str]:
    return [obj["Key"] for obj in s3_client.list_objects_v2(Bucket=HUDI_BUCKET, Prefix="audit/").get("Contents", [])]


class TestProcessedDirOnAuditPath:
    """Sanity: the audit sidecar path doesn't perturb the disposition prefix."""

    def test_unmapped_file_still_routes_source_correctly(self, file_in_newtbp, mock_s3_buckets) -> None:
        file_in_newtbp(NEM12_UNMAPPED_BODY, key="newTBP/unmapped_audit_routing.csv")
        ingest_file(source_file=SourceFile(bucket=INPUT_BUCKET, key="newTBP/unmapped_audit_routing.csv"))

        # Source file must NOT remain in newTBP/ nor land in newP/.
        newtbp = mock_s3_buckets.list_objects_v2(Bucket=INPUT_BUCKET, Prefix="newTBP/").get("Contents", [])
        assert not any(o["Key"].endswith("unmapped_audit_routing.csv") for o in newtbp)
        newp = mock_s3_buckets.list_objects_v2(Bucket=INPUT_BUCKET, Prefix=PROCESSED_DIR).get("Contents", [])
        assert not any(o["Key"].endswith("unmapped_audit_routing.csv") for o in newp)
