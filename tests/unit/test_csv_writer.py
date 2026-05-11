"""Tests for HudiSourceCsvWriter staging/commit/abort lifecycle."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import boto3
import pandas as pd
import pytest
from moto import mock_aws

from functions.file_processor.csv_writer import HudiSourceCsvWriter, StagedCsvUpload
from shared.common import HUDI_BUCKET, HUDI_FINAL_PREFIX, HUDI_STAGING_PREFIX


@pytest.fixture
def hudi_bucket():
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket=HUDI_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )
        yield s3


@pytest.fixture
def executor():
    pool = ThreadPoolExecutor(max_workers=2)
    yield pool
    pool.shutdown(wait=True)


class TestHudiSourceCsvWriter:
    def test_write_row_appends_to_buffer(self, hudi_bucket, executor) -> None:
        writer = HudiSourceCsvWriter(batch_timestamp="2026_May_07T00_00_00_000000", executor=executor)
        ts = pd.Timestamp("2026-05-07 00:00:00")
        writer.write_row("p:bunnings:abc", ts, 1.5, "kwh", "A")
        assert writer.row_count == 1

    def test_flush_uploads_to_staging_prefix(self, hudi_bucket, executor) -> None:
        writer = HudiSourceCsvWriter(batch_timestamp="2026_May_07T00_00_00_000000", executor=executor)
        writer.write_row("p:bunnings:abc", pd.Timestamp("2026-05-07 00:00:00"), 1.5, "kwh", "A")
        writer.flush()

        # Wait for upload to complete by polling .upload_jobs futures
        for job in writer.upload_jobs:
            job.future.result()

        listed = hudi_bucket.list_objects_v2(Bucket=HUDI_BUCKET, Prefix=HUDI_STAGING_PREFIX).get("Contents", [])
        assert len(listed) == 1
        assert listed[0]["Key"].startswith(f"{HUDI_STAGING_PREFIX}/")

    def test_commit_promotes_staging_to_final(self, hudi_bucket, executor) -> None:
        writer = HudiSourceCsvWriter(batch_timestamp="2026_May_07T00_00_00_000000", executor=executor)
        writer.write_row("p:bunnings:abc", pd.Timestamp("2026-05-07 00:00:00"), 1.5, "kwh", "A")
        writer.flush()
        writer.commit()

        final_objs = hudi_bucket.list_objects_v2(Bucket=HUDI_BUCKET, Prefix=HUDI_FINAL_PREFIX).get("Contents", [])
        staging_objs = hudi_bucket.list_objects_v2(Bucket=HUDI_BUCKET, Prefix=HUDI_STAGING_PREFIX).get("Contents", [])
        assert len(final_objs) == 1
        assert len(staging_objs) == 0  # staging cleaned up after copy
        assert final_objs[0]["Key"].startswith(f"{HUDI_FINAL_PREFIX}/")

    def test_abort_deletes_staging_and_final(self, hudi_bucket, executor) -> None:
        writer = HudiSourceCsvWriter(batch_timestamp="2026_May_07T00_00_00_000000", executor=executor)
        writer.write_row("p:bunnings:abc", pd.Timestamp("2026-05-07 00:00:00"), 1.5, "kwh", "A")
        writer.flush()
        writer.commit()
        writer.abort()  # rolls back the committed final keys

        final_objs = hudi_bucket.list_objects_v2(Bucket=HUDI_BUCKET, Prefix=HUDI_FINAL_PREFIX).get("Contents", [])
        assert len(final_objs) == 0

    def test_quality_none_renders_empty_cell(self, hudi_bucket, executor) -> None:
        writer = HudiSourceCsvWriter(batch_timestamp="2026_May_07T00_00_00_000000", executor=executor)
        writer.write_row("p:bunnings:abc", pd.Timestamp("2026-05-07 00:00:00"), 1.5, "kwh", None)
        writer.flush()
        writer.commit()

        final_objs = hudi_bucket.list_objects_v2(Bucket=HUDI_BUCKET, Prefix=HUDI_FINAL_PREFIX).get("Contents", [])
        body = hudi_bucket.get_object(Bucket=HUDI_BUCKET, Key=final_objs[0]["Key"])["Body"].read().decode()
        # Header line + data line with empty trailing quality cell
        assert body.endswith(",\n")  # ends with empty quality cell


class TestStagedCsvUpload:
    def test_dataclass_fields(self) -> None:
        # Quick smoke test: dataclass exists and has the expected fields.
        import dataclasses

        fields = {f.name for f in dataclasses.fields(StagedCsvUpload)}
        assert fields == {"future", "staging_key", "final_key"}
