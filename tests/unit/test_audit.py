"""Direct unit tests for shared.audit.write_audit_sidecar."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

# Ensure src/ is importable when running standalone.
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from shared.audit import AUDIT_BUCKET, AUDIT_PREFIX, SAMPLE_CAP, write_audit_sidecar


def _set_aws_env() -> None:
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"


def _make_audit_bucket():
    s3_resource = boto3.resource("s3", region_name="ap-southeast-2")
    s3_resource.create_bucket(
        Bucket=AUDIT_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
    )
    return s3_resource


class TestWriteAuditSidecar:
    @mock_aws
    def test_writes_sidecar_with_correct_payload(self) -> None:
        _set_aws_env()
        s3_resource = _make_audit_bucket()
        s3_client = boto3.client("s3", region_name="ap-southeast-2")

        key = write_audit_sidecar(
            batch_ts="2026_Jan_01T00_00_00_000000",
            source_filename="example.csv",
            outcome_summary={
                "status": "processed",
                "reason": None,
                "source_row_count": 10,
                "candidate_row_count": 8,
                "rows_written": 6,
                "rows_skipped": 2,
                "unmapped_count": 0,
            },
            skip_reasons={"blank_value": 1, "unparseable_value": 1},
            unmapped_identifiers=[("nem12_nmi", "NMI1-E1")],
            unsupported_suffixes=["Z9Z"],
            skipped_samples=[
                {"row": 0, "column": "E1_kWh", "value": "", "reason": "blank_value"},
                {"row": 3, "column": "E1_kWh", "value": "x", "reason": "unparseable_value"},
            ],
            s3_client=s3_client,
        )

        assert key == f"{AUDIT_PREFIX}/2026_Jan_01T00_00_00_000000/example.csv.skipped.json"
        body = s3_resource.Object(AUDIT_BUCKET, key).get()["Body"].read()
        payload = json.loads(body)
        assert payload["source_file"] == "example.csv"
        assert payload["outcome"]["rows_written"] == 6
        assert payload["skip_reasons"] == {"blank_value": 1, "unparseable_value": 1}
        assert payload["unmapped_identifiers"] == [["nem12_nmi", "NMI1-E1"]]
        assert payload["unsupported_suffixes"] == ["Z9Z"]
        assert len(payload["skipped_samples"]) == 2

    @mock_aws
    def test_audit_path_is_correct_format(self) -> None:
        _set_aws_env()
        _make_audit_bucket()
        s3_client = boto3.client("s3", region_name="ap-southeast-2")

        key = write_audit_sidecar(
            batch_ts="2026_May_07T12_34_56_789012",
            source_filename="thingy.txt",
            outcome_summary={},
            skip_reasons={},
            unmapped_identifiers=[],
            unsupported_suffixes=[],
            skipped_samples=[],
            s3_client=s3_client,
        )

        assert key == "audit/2026_May_07T12_34_56_789012/thingy.txt.skipped.json"

    @mock_aws
    def test_special_chars_in_filename_safe(self) -> None:
        _set_aws_env()
        _make_audit_bucket()
        s3_client = boto3.client("s3", region_name="ap-southeast-2")

        key = write_audit_sidecar(
            batch_ts="2026_May_07T00_00_00_000000",
            source_filename="newTBP/sub/dir/file.csv",
            outcome_summary={},
            skip_reasons={},
            unmapped_identifiers=[],
            unsupported_suffixes=[],
            skipped_samples=[],
            s3_client=s3_client,
        )

        # Path separators replaced so the audit key remains a single segment
        # under audit/<batch_ts>/.
        assert key == "audit/2026_May_07T00_00_00_000000/newTBP_sub_dir_file.csv.skipped.json"
        assert "/" not in key.split("/", 2)[2]

    @mock_aws
    def test_truncation_marker_appended_when_caller_oversupplies(self) -> None:
        _set_aws_env()
        s3_resource = _make_audit_bucket()
        s3_client = boto3.client("s3", region_name="ap-southeast-2")

        # Caller supplies more than SAMPLE_CAP samples — defensively the
        # writer must trim to SAMPLE_CAP and append a truncation marker.
        oversized = [
            {"row": i, "column": "E1_kWh", "value": str(i), "reason": "blank_value"} for i in range(SAMPLE_CAP + 25)
        ]
        key = write_audit_sidecar(
            batch_ts="2026_May_07T00_00_00_000000",
            source_filename="big.csv",
            outcome_summary={},
            skip_reasons={"blank_value": SAMPLE_CAP + 25},
            unmapped_identifiers=[],
            unsupported_suffixes=[],
            skipped_samples=oversized,
            s3_client=s3_client,
        )

        body = s3_resource.Object(AUDIT_BUCKET, key).get()["Body"].read()
        payload = json.loads(body)
        # SAMPLE_CAP regular entries + 1 truncation marker.
        assert len(payload["skipped_samples"]) == SAMPLE_CAP + 1
        assert payload["skipped_samples"][-1] == {
            "truncated": True,
            "total_skipped": SAMPLE_CAP + 25,
        }

    @mock_aws
    def test_default_s3_client_is_constructed(self) -> None:
        # Smoke test: when no s3_client is passed, the writer constructs a
        # default boto3 client. moto's default mock catches it.
        _set_aws_env()
        _make_audit_bucket()

        key = write_audit_sidecar(
            batch_ts="2026_May_07T00_00_00_000000",
            source_filename="default.csv",
            outcome_summary={},
            skip_reasons={},
            unmapped_identifiers=[],
            unsupported_suffixes=[],
            skipped_samples=[],
        )
        assert key.endswith("default.csv.skipped.json")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
