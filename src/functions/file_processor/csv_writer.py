"""HudiSourceCsvWriter — writes Hudi-shaped CSV objects to S3.

Despite the name "Hudi", this class does NOT write Apache Hudi tables. It
writes plain CSV objects to ``s3://<HUDI_BUCKET>/<HUDI_FINAL_PREFIX>/`` that
the downstream ``DataImportIntoLake`` Glue job consumes into the actual
Hudi table.

Lifecycle:
  - ``write_row`` appends to an in-memory buffer.
  - ``flush`` uploads the buffer to a staging key under HUDI_STAGING_PREFIX
    via a ThreadPoolExecutor (parallelism for large files).
  - ``commit`` copies all staged keys to their final HUDI_FINAL_PREFIX
    locations and deletes the staging copies.
  - ``abort`` deletes everything this writer staged or committed (rollback
    on parse error or downstream-move failure).
"""

from __future__ import annotations

import io
import random
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import boto3
from aws_lambda_powertools import Logger, Tracer

from shared.common import HUDI_BUCKET, HUDI_FINAL_PREFIX, HUDI_STAGING_PREFIX

if TYPE_CHECKING:
    from concurrent.futures import Future, ThreadPoolExecutor

logger = Logger(service="hudi-source-csv-writer", child=True)
tracer = Tracer(service="hudi-source-csv-writer")

s3_resource = boto3.resource("s3")


@dataclass(frozen=True)
class StagedCsvUpload:
    future: Future[None]
    staging_key: str
    final_key: str


def _upload_csv_to_s3(csv_content: str, output_key: str, parent_xray_trace_entity: Any = None) -> None:
    """Upload CSV content to S3. Used by ThreadPoolExecutor.

    Powertools' Tracer auto-instrumentation does not cross thread boundaries.
    To make the parallel S3 PUT subsegments children of the parent ingest_file
    segment (instead of orphan segments), the caller passes the parent X-Ray
    entity captured on the main thread, and we re-attach it inside the worker
    via aws_xray_sdk.core.xray_recorder.set_trace_entity.
    """
    if parent_xray_trace_entity is not None:
        try:
            from aws_xray_sdk.core import xray_recorder

            xray_recorder.set_trace_entity(parent_xray_trace_entity)
        except Exception:  # X-Ray not available in test env — best-effort.
            pass

    s3_resource.Object(HUDI_BUCKET, output_key).put(Body=csv_content)
    logger.debug("Uploaded CSV to S3", extra={"output_key": output_key})


class HudiSourceCsvWriter:
    """Writes Hudi-shaped CSV objects to S3 with a staging/commit/abort lifecycle."""

    CSV_HEADER = "sensorId,ts,val,unit,its,quality\n"
    TS_FORMAT = "%Y-%m-%d %H:%M:%S"

    def __init__(self, batch_timestamp: str, executor: ThreadPoolExecutor) -> None:
        self.batch_timestamp = batch_timestamp
        self.executor = executor
        self.writer_token = uuid.uuid4().hex
        self.buffer = io.StringIO()
        self.buffer.write(self.CSV_HEADER)
        self.row_count = 0
        self.upload_jobs: list[StagedCsvUpload] = []
        self.committed_final_keys: list[str] = []

    def write_row(self, sensor_id: str, ts: Any, val: float, unit: str, quality: str | None = None) -> None:
        """Write a single row to the buffer.

        ``quality=None`` (vendor did not supply a quality value) is serialised
        as an empty cell so Athena/Presto reads the column as NULL. Vendor-
        supplied codes (``A``/``E``/``S14``/etc.) pass through verbatim.
        """
        ts_str = ts.strftime(self.TS_FORMAT) if hasattr(ts, "strftime") else str(ts)
        quality_field = "" if quality is None else quality
        self.buffer.write(f"{sensor_id},{ts_str},{val},{unit},{ts_str},{quality_field}\n")
        self.row_count += 1

    def flush(self) -> None:
        """Upload current buffer to staging asynchronously."""
        if self.row_count == 0:
            return

        csv_content = self.buffer.getvalue()
        batch_file_name = f"batch_{self.batch_timestamp}_{self.writer_token}_{random.randint(1, 1000000)}.csv"
        staging_key = f"{HUDI_STAGING_PREFIX}/{self.writer_token}/{batch_file_name}"
        final_key = f"{HUDI_FINAL_PREFIX}/{batch_file_name}"

        # Capture parent X-Ray entity on the calling thread so the worker
        # can re-attach it (Powertools Tracer does not cross thread boundaries).
        parent_entity = None
        try:
            from aws_xray_sdk.core import xray_recorder

            parent_entity = xray_recorder.get_trace_entity()
        except Exception:
            pass

        future = self.executor.submit(_upload_csv_to_s3, csv_content, staging_key, parent_entity)
        self.upload_jobs.append(StagedCsvUpload(future=future, staging_key=staging_key, final_key=final_key))

        logger.debug("Submitted CSV upload", extra={"output_key": staging_key, "rows": self.row_count})

        self.buffer = io.StringIO()
        self.buffer.write(self.CSV_HEADER)
        self.row_count = 0

    @tracer.capture_method
    def commit(self) -> None:
        """Publish staged uploads to final Hudi keys."""
        jobs = list(self.upload_jobs)
        for job in jobs:
            job.future.result()

        for job in jobs:
            s3_resource.Object(HUDI_BUCKET, job.final_key).copy({"Bucket": HUDI_BUCKET, "Key": job.staging_key})
            self.committed_final_keys.append(job.final_key)
            s3_resource.Object(HUDI_BUCKET, job.staging_key).delete()
            logger.debug(
                "Committed staged CSV upload",
                extra={"staging_key": job.staging_key, "final_key": job.final_key},
            )

        self.upload_jobs.clear()

    @tracer.capture_method
    def abort(self) -> None:
        """Observe pending uploads and delete writer-owned staged/final objects."""
        jobs = list(self.upload_jobs)
        for job in jobs:
            try:
                job.future.result()
            except Exception as e:
                logger.warning(
                    "Staged CSV upload failed during abort",
                    extra={"staging_key": job.staging_key, "final_key": job.final_key, "error": str(e)},
                )

        keys_to_delete = [job.staging_key for job in jobs] + self.committed_final_keys
        seen_keys: set[str] = set()
        for key in keys_to_delete:
            if key in seen_keys:
                continue
            seen_keys.add(key)
            try:
                s3_resource.Object(HUDI_BUCKET, key).delete()
            except Exception as e:
                logger.warning("Failed to delete staged CSV object", extra={"key": key, "error": str(e)})

        self.upload_jobs.clear()
        self.committed_final_keys.clear()
