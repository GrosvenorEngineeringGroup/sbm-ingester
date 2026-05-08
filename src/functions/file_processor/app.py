import io
import json
import os
import random
import shutil
import tempfile
import time
import traceback
import uuid
from collections import Counter
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from itertools import chain
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import boto3
import pandas as pd
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities.idempotency import (
    DynamoDBPersistenceLayer,
    IdempotencyConfig,
    idempotent_function,
)

from shared import (
    HUDI_BUCKET,
    HUDI_FINAL_PREFIX,
    HUDI_STAGING_PREFIX,
    INPUT_BUCKET,
    PARSE_ERR_DIR,
    PARSE_ERROR_LOG_GROUP,
    PROCESSED_DIR,
    UNMAPPED_DIR,
    output_as_data_frames,
    stream_as_data_frames,
)
from shared.audit import SAMPLE_CAP as AUDIT_SAMPLE_CAP
from shared.audit import write_audit_sidecar
from shared.non_nem_parsers import get_non_nem_outcome
from shared.parsers import ParserError, ParserOutcome, ParserReason, ParserStatus, ProcessingError
from shared.parsers.outcome import SkipReason

# File stability check configuration
FILE_STABILITY_CHECK_INTERVAL = 5  # seconds between checks
FILE_STABILITY_MAX_WAIT = 30  # max seconds to wait for file to stabilize
FILE_STABILITY_REQUIRED_CHECKS = 2  # consecutive stable checks required
MAX_REQUEUE_RETRIES = 5  # max times to requeue a message
REQUEUE_DELAY_SECONDS = 60  # delay before requeued message becomes visible

# SQS Queue URL - will be set from environment or constructed
SQS_QUEUE_URL = os.environ.get(
    "SQS_QUEUE_URL",
    "https://sqs.ap-southeast-2.amazonaws.com/318396632821/sbm-files-ingester-queue",
)

# Module-level constants (computed once at import time)
NMI_DATA_STREAM_SUFFIX = [
    "A",
    "B",
    "C",
    "D",
    "E",
    "F",
    "J",
    "K",
    "L",
    "P",
    "Q",
    "R",
    "S",
    "T",
    "U",
    "G",
    "H",
    "Y",
    "M",
    "W",
    "V",
    "Z",
]
NMI_DATA_STREAM_CHANNEL = [
    "1",
    "2",
    "3",
    "4",
    "5",
    "6",
    "7",
    "8",
    "9",
    "A",
    "B",
    "C",
    "D",
    "E",
    "F",
    "G",
    "H",
    "I",
    "J",
    "K",
    "L",
    "M",
    "N",
    "O",
    "P",
    "Q",
    "R",
    "S",
    "T",
    "U",
    "V",
    "W",
    "X",
    "Y",
    "Z",
]
NMI_DATA_STREAM_COMBINED = frozenset(i + j for i in NMI_DATA_STREAM_SUFFIX for j in NMI_DATA_STREAM_CHANNEL)

# Batch size for S3 writes - number of rows before writing
BATCH_SIZE = 50000  # ~50K rows per CSV file

# Parallel S3 write configuration
S3_WRITE_WORKERS = 4  # Number of concurrent S3 upload threads

# Powertools instances
logger = Logger(service="file-processor")
tracer = Tracer(service="file-processor")
metrics = Metrics(namespace="SBM/Ingester")

# Idempotency configuration - prevents duplicate processing of same files
persistence_layer = DynamoDBPersistenceLayer(
    table_name="sbm-ingester-idempotency",
    key_attr="file_key",  # Match DynamoDB table's primary key
)
idempotency_config = IdempotencyConfig(
    expires_after_seconds=43200,  # 12 hours TTL
)

s3_resource = boto3.resource("s3")
s3_client = boto3.client("s3")
sqs_client = boto3.client("sqs")


@dataclass(frozen=True)
class DataFrameCandidate:
    ts: Any
    val: float
    # ``None`` means "vendor did not provide a quality value" — the CSV writer
    # serialises this as an empty cell so Athena/Presto reads it as NULL. A
    # non-None string is forwarded verbatim to preserve vendor codes
    # (``A``/``E``/``S14``/etc.). Spec line 570: never write empty string.
    quality: str | None


@dataclass(frozen=True)
class CSVUploadJob:
    future: Future[None]
    staging_key: str
    final_key: str


def _processed_destination_for_status(status: str) -> str:
    if status in {"processed", "processed_empty", "processed_external"}:
        return PROCESSED_DIR
    if status == "unmapped":
        return UNMAPPED_DIR
    raise ValueError(f"Unsupported parser outcome status: {status}")


def _compute_dataframe_final_status(
    rows_written: int,
    candidate_row_count: int,
    unmapped_count: int,
    unsupported_suffixes: frozenset[str] | set[str],
    rows_skipped: int,
    parser_reason: ParserReason | None,
) -> tuple[ParserStatus, ParserReason | None]:
    """Compute final (status, reason) for the DataFrame path per spec ladder.

    Ladder (in order):
      1. rows_written > 0                                            -> processed
      2. candidate_row_count > 0 and unmapped_count == candidate_row_count
                                                                     -> unmapped
      3. candidate_row_count == 0 and unsupported_suffixes
                                                              -> processed_empty(all_unknown_suffix)
      4. rows_skipped > 0 and rows_written == 0 and candidate_row_count == 0
                                                              -> processed_empty(all_skipped)
      5. else                                                 -> processed_empty(inherit parser_reason)
    """
    if rows_written > 0:
        return ("processed", None)
    if candidate_row_count > 0 and unmapped_count == candidate_row_count:
        return ("unmapped", None)
    if candidate_row_count == 0 and unsupported_suffixes:
        return ("processed_empty", "all_unknown_suffix")
    if rows_skipped > 0 and rows_written == 0 and candidate_row_count == 0:
        return ("processed_empty", "all_skipped")
    return ("processed_empty", parser_reason)


def _is_blank_value(value: Any) -> bool:
    return isinstance(value, str) and value.strip() == ""


def _looks_like_nem_envelope(file_path: str) -> bool:
    """Return True if the file's first line looks like a NEM12 or NEM13 envelope.

    Reads up to ~64 bytes (BOM-stripped via ``utf-8-sig``) and matches the
    prefix ``100,NEM12,`` OR ``100,NEM13,``. Used to short-circuit empty
    NEM-format files (only 100/900 records, no 200/300 payload) to
    ``processed_empty(reason="no_data_sentinel")`` rather than falling through
    to the non-NEM dispatcher, which never matches NEM-format files and would
    incorrectly route them to ``newParseErr/``.

    Defensive: returns ``False`` on any I/O or decoding error so a failing
    helper never crashes the lambda.
    """
    try:
        with Path(file_path).open(encoding="utf-8-sig") as f:
            first_line = f.readline(64)
    except (OSError, UnicodeDecodeError):
        return False
    return first_line.startswith("100,NEM12,") or first_line.startswith("100,NEM13,")


def _candidate_values(
    df: pd.DataFrame,
    col: str,
    t_start_col: pd.Series,
    quality_col: pd.Series | None = None,
    skip_counter: Counter[SkipReason] | None = None,
    samples_sink: list[dict[str, Any]] | None = None,
) -> list[DataFrameCandidate]:
    """Return valid candidate rows; record row-level skips in ``skip_counter``.

    Per the parser-outcome contract, row-level data quality issues never raise.
    Bad rows are skipped silently with the disqualifying reason recorded in
    ``skip_counter`` (mutated in place when supplied).

    Skip taxonomy:
      * ``blank_value`` — value cell is NaN, empty, or whitespace.
      * ``unparseable_value`` — non-empty value cell that fails numeric coercion.
      * ``unparseable_timestamp`` — timestamp cell fails datetime coercion or is NaT.

    Blank-value rows are filtered first so a blank value is not also charged
    against ``unparseable_timestamp``. Cells matched by both ``unparseable_value``
    and ``unparseable_timestamp`` are charged to whichever is detected first
    (timestamp), matching the row-level attribution policy in the spec.

    When ``samples_sink`` is provided, each skipped cell is appended as
    ``{"row": int, "column": str, "value": str, "reason": str}`` until the
    sink reaches ``AUDIT_SAMPLE_CAP`` entries. The cap is enforced here so
    callers can share a single sink across all DataFrames/columns of a file
    without tracking length themselves.
    """
    candidates: list[DataFrameCandidate] = []
    value_col = df[col]
    quality_values = quality_col if quality_col is not None else [None] * len(value_col)

    def _record_sample(row_idx: int, raw: Any, reason: str) -> None:
        if samples_sink is None:
            return
        if len(samples_sink) >= AUDIT_SAMPLE_CAP:
            return
        samples_sink.append(
            {
                "row": row_idx,
                "column": col,
                "value": str(raw),
                "reason": reason,
            }
        )

    for row_idx, (ts_raw, val_raw, quality_raw) in enumerate(zip(t_start_col, value_col, quality_values, strict=False)):
        if pd.isna(val_raw) or _is_blank_value(val_raw):
            if skip_counter is not None:
                skip_counter["blank_value"] += 1
            _record_sample(row_idx, val_raw, "blank_value")
            continue

        ts = pd.to_datetime(ts_raw, errors="coerce")
        if pd.isna(ts):
            if skip_counter is not None:
                skip_counter["unparseable_timestamp"] += 1
            _record_sample(row_idx, ts_raw, "unparseable_timestamp")
            continue

        try:
            val = float(val_raw)
        except (TypeError, ValueError):
            if skip_counter is not None:
                skip_counter["unparseable_value"] += 1
            _record_sample(row_idx, val_raw, "unparseable_value")
            continue

        if pd.isna(val):
            if skip_counter is not None:
                skip_counter["unparseable_value"] += 1
            _record_sample(row_idx, val_raw, "unparseable_value")
            continue

        # Pass through ``None`` for missing vendor quality so the CSV writer
        # emits an empty cell (Athena reads as NULL). See spec line 570.
        quality = None if pd.isna(quality_raw) else str(quality_raw)
        candidates.append(DataFrameCandidate(ts=ts, val=val, quality=quality))

    return candidates


@tracer.capture_method
def check_file_stability(bucket: str, key: str) -> tuple[bool, int]:
    """
    Check if a file has finished uploading by verifying size stability.

    For streaming uploads, files may initially be empty or partially uploaded.
    This function waits for the file size to stabilize before processing.

    Args:
        bucket: S3 bucket name
        key: S3 object key

    Returns:
        tuple: (is_stable, file_size)
            - is_stable: True if file is ready for processing
            - file_size: Final file size in bytes (0 if not stable)
    """
    last_size = -1
    stable_count = 0
    total_wait = 0

    while total_wait < FILE_STABILITY_MAX_WAIT:
        try:
            response = s3_client.head_object(Bucket=bucket, Key=key)
            current_size = response["ContentLength"]

            # File must have content
            if current_size == 0:
                logger.debug(
                    "File is empty, waiting",
                    extra={"bucket": bucket, "key": key, "waited": total_wait},
                )
                time.sleep(FILE_STABILITY_CHECK_INTERVAL)
                total_wait += FILE_STABILITY_CHECK_INTERVAL
                continue

            # Check if size is stable
            if current_size == last_size:
                stable_count += 1
                if stable_count >= FILE_STABILITY_REQUIRED_CHECKS:
                    logger.info(
                        "File is stable",
                        extra={"bucket": bucket, "key": key, "size": current_size},
                    )
                    return True, current_size
            else:
                stable_count = 0

            last_size = current_size
            time.sleep(FILE_STABILITY_CHECK_INTERVAL)
            total_wait += FILE_STABILITY_CHECK_INTERVAL

        except Exception as e:
            # Check if it's a NoSuchKey error
            if hasattr(e, "response") and e.response.get("Error", {}).get("Code") == "NoSuchKey":
                logger.warning("File not found", extra={"bucket": bucket, "key": key})
                return False, 0
            logger.error(
                "Error checking file stability",
                exc_info=True,
                extra={"bucket": bucket, "key": key, "error": str(e)},
            )
            return False, 0

    # Timeout - file may still be uploading
    logger.warning(
        "File stability check timed out",
        extra={"bucket": bucket, "key": key, "last_size": last_size, "waited": total_wait},
    )
    return False, 0


def requeue_message(original_body: dict, retry_count: int) -> bool:
    """
    Requeue a message to SQS with delay for later processing.

    Args:
        original_body: Original SQS message body
        retry_count: Current retry count (will be incremented)

    Returns:
        True if message was successfully requeued
    """
    try:
        # Add retry metadata to message
        new_body = original_body.copy()
        new_body["_retry_count"] = retry_count + 1

        sqs_client.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps(new_body),
            DelaySeconds=REQUEUE_DELAY_SECONDS,
        )

        logger.info(
            "Message requeued for later processing",
            extra={"retry_count": retry_count + 1, "delay_seconds": REQUEUE_DELAY_SECONDS},
        )
        return True

    except Exception as e:
        logger.error("Failed to requeue message", exc_info=True, extra={"error": str(e)})
        return False


@tracer.capture_method
def read_nem12_mappings(bucket_name: str, object_key: str = "nem12_mappings.json") -> dict | None:
    try:
        obj = s3_resource.Object(bucket_name, object_key)
        content = obj.get()["Body"].read().decode("utf-8")
        return json.loads(content)
    except Exception as e:
        logger.error(
            "Failed to read NEM12 mappings",
            exc_info=True,
            extra={"bucket": bucket_name, "key": object_key, "error": str(e)},
        )
        return None


@tracer.capture_method
def download_files_to_tmp(file_list: list[dict[str, str]], tmp_files_folder_path: str) -> list[str]:
    local_paths = []

    for f in file_list:
        bucket = f["bucket"]

        # Always decode key before using with boto3
        key = unquote(f["file_name"].replace("+", "%20"))

        file_name = Path(key).name
        local_path = str(Path(tmp_files_folder_path) / file_name)

        logger.info("Downloading file", extra={"bucket": bucket, "key": key, "local_path": local_path})

        try:
            s3_resource.Bucket(bucket).download_file(key, local_path)
            local_paths.append(local_path)

        except Exception as e:
            logger.error("File download failed", exc_info=True, extra={"key": key, "error": str(e)})
            continue
    return local_paths


def move_s3_file(bucket_name: str, source_key: str, dest_prefix: str) -> str | None:
    # source_key = unquote(source_key.replace("+", "%20"))
    file_name = source_key.split("/")[-1]

    source_key = f"newTBP/{file_name}"
    dest_key = f"{dest_prefix.rstrip('/')}/{file_name}"
    copied_dest = False

    try:
        bucket = s3_resource.Bucket(bucket_name)

        copy_source = {"Bucket": bucket_name, "Key": source_key}
        bucket.Object(dest_key).copy(copy_source)
        copied_dest = True

        bucket.Object(source_key).delete()

        return dest_key

    except Exception as e:
        if copied_dest:
            try:
                s3_resource.Object(bucket_name, dest_key).delete()
            except Exception as cleanup_error:
                logger.warning(
                    "Failed to clean up destination after source move failure",
                    extra={"source": source_key, "dest": dest_key, "error": str(cleanup_error)},
                )
        logger.error("File move failed", exc_info=True, extra={"source": source_key, "dest": dest_key, "error": str(e)})
        return None


def _move_final_source_or_parse_error(
    local_file_path: str,
    dest_prefix: str,
    csv_writer: "DirectCSVWriter",
    logs_dict: dict[str, str],
    timestamp_now: str,
) -> bool:
    """Move source to its final prefix, or roll back writer output and mark parse error."""
    dest_key = move_s3_file(INPUT_BUCKET, local_file_path, dest_prefix)
    if dest_key is not None:
        return True

    csv_writer.abort()
    error_message = f"Failed to move source file to {dest_prefix}"
    logger.error(
        "Final source move failed",
        extra={"file": local_file_path, "dest_prefix": dest_prefix, "error": error_message},
    )
    logs_dict[f"Processing Error: {local_file_path}"] = f"[{timestamp_now}] {error_message}"

    parse_error_key = move_s3_file(INPUT_BUCKET, local_file_path, PARSE_ERR_DIR)
    if parse_error_key is None:
        logger.error(
            "Failed to move source file to parse error after final move failure",
            extra={"file": local_file_path, "dest_prefix": PARSE_ERR_DIR},
        )

    return False


def _upload_csv_to_s3(csv_content: str, output_key: str) -> None:
    """Upload CSV content to S3. Used by ThreadPoolExecutor."""
    s3_resource.Object(HUDI_BUCKET, output_key).put(Body=csv_content)
    logger.debug("Uploaded CSV to S3", extra={"output_key": output_key})


@tracer.capture_method
def _flush_buffer_to_s3(buffer: list, batch_timestamp: str) -> None:
    """Write buffered DataFrames to S3 as a single merged CSV."""
    if not buffer:
        return

    merged_df = pd.concat(buffer, ignore_index=True)
    output_key = f"{HUDI_FINAL_PREFIX}/batch_{batch_timestamp}_{random.randint(1, 1000000)}.csv"
    s3_resource.Object(HUDI_BUCKET, output_key).put(Body=merged_df.to_csv(index=False))
    logger.debug("Flushed buffer to S3", extra={"output_key": output_key, "rows": len(merged_df)})


class DirectCSVWriter:
    """
    Memory-efficient CSV writer that bypasses pandas DataFrame.

    Writes rows directly to a string buffer, then uploads to S3 in parallel.
    Eliminates DataFrame construction, concat, and to_csv overhead.
    """

    CSV_HEADER = "sensorId,ts,val,unit,its,quality\n"
    TS_FORMAT = "%Y-%m-%d %H:%M:%S"

    def __init__(self, batch_timestamp: str, executor: ThreadPoolExecutor) -> None:
        self.batch_timestamp = batch_timestamp
        self.executor = executor
        self.writer_token = uuid.uuid4().hex
        self.buffer = io.StringIO()
        self.buffer.write(self.CSV_HEADER)
        self.row_count = 0
        self.upload_jobs: list[CSVUploadJob] = []
        self.committed_final_keys: list[str] = []

    def write_row(self, sensor_id: str, ts: Any, val: float, unit: str, quality: str | None = None) -> None:
        """Write a single row to the buffer.

        ``quality=None`` (vendor did not supply a quality value) is serialised
        as an empty cell — i.e. zero characters between the trailing commas —
        so Athena/Presto reads the column as NULL. Vendor-supplied codes
        (``A``/``E``/``S14``/etc.) pass through verbatim. Per spec line 570,
        we must never write the literal empty string ``""`` (Athena/Presto
        does NOT coerce ``""`` to NULL).
        """
        ts_str = ts.strftime(self.TS_FORMAT) if hasattr(ts, "strftime") else str(ts)
        # ``str(None)`` would render as "None"; force empty cell instead.
        quality_field = "" if quality is None else quality
        # CSV format: sensorId,ts,val,unit,its,quality (its = ts)
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

        # Submit upload task to thread pool
        future = self.executor.submit(_upload_csv_to_s3, csv_content, staging_key)
        self.upload_jobs.append(CSVUploadJob(future=future, staging_key=staging_key, final_key=final_key))

        logger.debug("Submitted CSV upload", extra={"output_key": staging_key, "rows": self.row_count})

        # Reset buffer for next batch
        self.buffer = io.StringIO()
        self.buffer.write(self.CSV_HEADER)
        self.row_count = 0

    def wait_for_uploads(self) -> None:
        """Wait for uploads and publish staged objects."""
        self.commit()

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


@idempotent_function(persistence_store=persistence_layer, config=idempotency_config, data_keyword_argument="tbp_files")
@tracer.capture_method
def parse_and_write_data(tbp_files: list[dict[str, str]] | None = None) -> int | None:
    tmp_dir = tempfile.gettempdir()
    tmp_files_folder_name = str(uuid.uuid4())
    tmp_files_folder_path = Path(tmp_dir) / tmp_files_folder_name
    tmp_files_folder_path.mkdir(parents=True, exist_ok=True)

    # Generate timestamps once at start (not per-write)
    timestamp_now = pd.Timestamp.now().tz_localize("UTC").tz_convert("Australia/Sydney").isoformat()
    batch_timestamp = pd.Timestamp.now().strftime("%Y_%b_%dT%H_%M_%S_%f")

    # Thread pool for parallel S3 uploads
    executor = ThreadPoolExecutor(max_workers=S3_WRITE_WORKERS)

    try:
        logger.info("Script started", extra={"timestamp": timestamp_now, "files_count": len(tbp_files or [])})

        logs_dict: dict[str, str] = {}
        nem12_mappings = read_nem12_mappings(INPUT_BUCKET)

        if nem12_mappings is None:
            raise Exception("Failed to read NEM12 mappings from S3.")

        download_files_to_tmp(tbp_files or [], str(tmp_files_folder_path))

        valid_processed_files_count = 0
        irrev_files_count = 0
        parse_err_files_count = 0
        processed_monitor_points_count = 0
        total_monitor_points_count = 0
        ftp_files_count = 0

        # Partial-recognition signal accumulators (Task 15 / spec lines 580-624).
        # CloudWatch dimensions are kept batch-level rather than per-file to bound
        # metric cardinality. Ratios are emitted once per affected file with the
        # per-file value; counts are summed across the batch.
        partial_mapped_ratio_total = 0.0
        partial_mapped_ratio_files = 0
        rows_skipped_ratio_total = 0.0
        rows_skipped_ratio_files = 0
        malformed_value_count_total = 0
        unsupported_suffixes_files = 0
        # Accumulator for UnmappedIdentifierKind metric (spec line 600).
        # Holds (kind, value) tuples across all files in the batch so we
        # can emit a per-kind count of distinct unmapped identifiers at
        # batch end. Kept as a set to deduplicate the same identifier
        # appearing in multiple files. Powertools dimensions are not used
        # elsewhere in this file, so kind is encoded in the metric name
        # (``UnmappedIdentifierKind_<kind>``) to keep emission simple and
        # bounded by the small fixed identifier-kind taxonomy.
        batch_unmapped_identifiers: set[tuple[str, str]] = set()

        for file_path in tmp_files_folder_path.iterdir():
            local_file_path = str(file_path)
            outcome: ParserOutcome

            # Try streaming parser first (memory efficient for large files).
            # Use peek pattern: get first item to validate, then chain with rest.
            # The catch is narrowed to exceptions that genuinely mean "not a NEM12
            # file" or "no data" (ValueError, KeyError, IndexError, AssertionError,
            # UnicodeDecodeError, StopIteration). nemreader does not expose its
            # own exception types today; the narrowing is partial pending a
            # nemreader API that surfaces a structured "not a NEM file" signal.
            # Other unexpected exceptions (RuntimeError, AttributeError, etc.)
            # propagate so genuine NEM12 parser bugs surface instead of silently
            # routing to the non-NEM dispatcher.
            _NEM_FALLTHROUGH_ERRORS: tuple[type[BaseException], ...] = (
                ValueError,
                KeyError,
                IndexError,
                AssertionError,
                UnicodeDecodeError,
                StopIteration,
            )
            try:
                stream = stream_as_data_frames(local_file_path, split_days=True)
                first_item = next(stream, None)
                if first_item is None:
                    # Empty NEM envelope (only 100/900 records). Do NOT fall
                    # through to non-NEM parsers — none match NEM-format files
                    # and the file would incorrectly route to newParseErr/.
                    # Short-circuit to processed_empty(no_data_sentinel) so the
                    # source moves to newP/ as a recognised empty payload.
                    if _looks_like_nem_envelope(local_file_path):
                        outcome = ParserOutcome(
                            status="processed_empty",
                            reason="no_data_sentinel",
                            source_row_count=0,
                        )
                    else:
                        raise ValueError("No data parsed from file")
                else:
                    # Chain first item back with the rest of the stream
                    outcome = ParserOutcome(status="processed", dataframes=chain([first_item], stream))
            except _NEM_FALLTHROUGH_ERRORS:
                # Fall back to batch parser
                try:
                    dfs = output_as_data_frames(local_file_path, split_days=True)
                    if not dfs:
                        # Same NEM-envelope short-circuit as above for the
                        # batch parser path: empty NEM12/NEM13 file -> emit
                        # processed_empty(no_data_sentinel) directly.
                        if _looks_like_nem_envelope(local_file_path):
                            outcome = ParserOutcome(
                                status="processed_empty",
                                reason="no_data_sentinel",
                                source_row_count=0,
                            )
                        else:
                            raise ValueError("No data parsed from file")
                    else:
                        outcome = ParserOutcome(status="processed", dataframes=dfs)
                except _NEM_FALLTHROUGH_ERRORS:
                    # Try non-NEM parsers as last resort
                    try:
                        outcome = get_non_nem_outcome(local_file_path, PARSE_ERROR_LOG_GROUP)
                    except (ParserError, ProcessingError) as e:
                        logs_dict[f"Bad File: {local_file_path}"] = f"[{timestamp_now}] {e}"
                        move_s3_file(INPUT_BUCKET, local_file_path, PARSE_ERR_DIR)
                        parse_err_files_count += 1
                        continue

            # Process each NMI's data - write directly to CSV, bypass DataFrame
            csv_writer = DirectCSVWriter(batch_timestamp, executor)
            mapped_monitor_points_for_file = 0
            # Per-file accumulators surfaced for audit sidecar + metrics.
            file_candidate_row_count = 0
            file_source_row_count = outcome.source_row_count
            file_unmapped_count = 0
            file_rows_written = 0
            file_skip_counter: Counter[SkipReason] = Counter()
            file_unsupported_suffixes: set[str] = set()
            file_unmapped_identifiers: set[tuple[str, str]] = set()
            file_skipped_samples: list[dict[str, Any]] = []
            file_total_skipped_seen = 0
            final_reason: ParserReason | None = outcome.reason
            try:
                if outcome.dataframes:
                    candidate_row_count = 0
                    unmapped_count = 0
                    rows_written = 0
                    mapped_monitor_points_count = 0
                    # Skip counts and unsupported suffixes seeded from parser-side
                    # signal so consumer-side accumulation is additive (gap G12).
                    skip_counter: Counter[SkipReason] = Counter(outcome.skip_reasons)
                    unsupported_suffixes: set[str] = set(outcome.unsupported_suffixes)
                    # Seed per-file unmapped identifiers from the parser as well,
                    # so the audit sidecar carries any mapping signal already
                    # captured upstream.
                    unmapped_identifiers: set[tuple[str, str]] = set(outcome.unmapped_identifiers)

                    for nmi, df in outcome.dataframes:
                        # Reset index if t_start is the index
                        if "t_start" not in df.columns and df.index.name == "t_start":
                            df = df.reset_index()

                        if "t_start" not in df.columns:
                            # Parser produced a DataFrame without the required
                            # column; this is a structural parser-output issue,
                            # not an IO failure -> ParserError.
                            raise ParserError(f"Missing t_start column for {nmi}")

                        # Get t_start column for iteration
                        t_start_col = df["t_start"]

                        for col in df.columns:
                            suffix = col.split("_")[0]
                            if suffix not in NMI_DATA_STREAM_COMBINED:
                                # Skip non-data columns silently (t_start, t_end,
                                # quality_*, event_code, event_desc, etc.). Only
                                # record genuine vendor-supplied data columns
                                # whose suffix is unrecognised.
                                if col not in {"t_start", "t_end", "event_code", "event_desc"} and not col.startswith(
                                    "quality_"
                                ):
                                    unsupported_suffixes.add(suffix)
                                continue

                            # Get per-channel quality column if available
                            quality_col_name = f"quality_{suffix}"
                            quality_col = df[quality_col_name] if quality_col_name in df.columns else None
                            candidates = _candidate_values(
                                df,
                                col,
                                t_start_col,
                                quality_col,
                                skip_counter,
                                file_skipped_samples,
                            )

                            if not candidates:
                                continue

                            candidate_row_count += len(candidates)

                            if nmi.startswith("p:"):
                                neptune_id = nmi
                            else:
                                monitor_point_name = f"{nmi}-{suffix}"
                                neptune_id = nem12_mappings.get(monitor_point_name)

                            if neptune_id is None:
                                unmapped_count += len(candidates)
                                # Record (kind, value) pair for audit sidecar
                                # using the canonical identifier taxonomy
                                # (spec: parser-outcome-semantics-design,
                                # identifier-kind table). Direct ``p:``
                                # Neptune IDs use kind ``p_id``; NMI-suffix
                                # lookups against the NEM12 mapping use kind
                                # ``nem12_nmi``. The value carries the full
                                # lookup key (``f"{nmi}-{suffix}"``) so
                                # operators can debug "why didn't this NMI
                                # map" without reconstructing the suffix
                                # separately. Cap at 100 entries per file to
                                # match the spec.
                                if nmi.startswith("p:"):
                                    if len(unmapped_identifiers) < 100:
                                        unmapped_identifiers.add(("p_id", nmi))
                                else:
                                    if len(unmapped_identifiers) < 100:
                                        unmapped_identifiers.add(("nem12_nmi", f"{nmi}-{suffix}"))
                                continue

                            mapped_monitor_points_count += 1

                            # Extract unit from column name (e.g., "E1_kWh" -> "kwh")
                            unit_name = col.split("_")[1].lower() if "_" in col else "kwh"

                            # Write rows directly to CSV buffer (no DataFrame construction)
                            for candidate in candidates:
                                csv_writer.write_row(
                                    neptune_id,
                                    candidate.ts,
                                    candidate.val,
                                    unit_name,
                                    candidate.quality,
                                )
                                rows_written += 1

                                # Flush buffer when it reaches BATCH_SIZE rows
                                if csv_writer.row_count >= BATCH_SIZE:
                                    csv_writer.flush()

                    # Flush and publish only after all validation for this source file succeeds.
                    csv_writer.flush()
                    csv_writer.commit()

                    mapped_monitor_points_for_file = mapped_monitor_points_count

                    rows_skipped_total = sum(skip_counter.values())

                    final_status, final_reason = _compute_dataframe_final_status(
                        rows_written=rows_written,
                        candidate_row_count=candidate_row_count,
                        unmapped_count=unmapped_count,
                        unsupported_suffixes=frozenset(unsupported_suffixes),
                        rows_skipped=rows_skipped_total,
                        parser_reason=outcome.reason,
                    )
                    if final_status == "processed_empty":
                        logger.info(
                            "No valid candidate rows found",
                            extra={
                                "file": local_file_path,
                                "reason": final_reason or "no_valid_candidate_rows",
                            },
                        )
                    # Operator-visibility warning when the calc ladder
                    # routes the file to processed_empty(all_unknown_suffix):
                    # this is the schema-drift signal — the file parsed but
                    # every column suffix was unrecognised. Surface the
                    # offending suffixes so operators can react in real time
                    # without grep'ing the audit sidecar.
                    if final_reason == "all_unknown_suffix":
                        logger.warning(
                            "all_suffixes_unknown",
                            extra={
                                "file": local_file_path,
                                "unsupported_suffixes": sorted(unsupported_suffixes),
                            },
                        )

                    # Promote per-file accumulators for audit + metric emission.
                    file_candidate_row_count = candidate_row_count
                    file_unmapped_count = unmapped_count
                    file_rows_written = rows_written
                    file_skip_counter = skip_counter
                    file_unsupported_suffixes = unsupported_suffixes
                    file_unmapped_identifiers = unmapped_identifiers
                    file_total_skipped_seen = rows_skipped_total
                else:
                    final_status = outcome.status

            except Exception as e:
                # Handle errors during streaming iteration or per-file uploads
                csv_writer.abort()
                logger.error(
                    "Error processing NMI data",
                    exc_info=True,
                    extra={"file": local_file_path, "error": str(e)},
                )
                logs_dict[f"Processing Error: {local_file_path}"] = f"[{timestamp_now}] {e}"
                move_s3_file(INPUT_BUCKET, local_file_path, PARSE_ERR_DIR)
                parse_err_files_count += 1
                continue

            try:
                dest_prefix = _processed_destination_for_status(final_status)
            except ValueError as e:
                logger.error(
                    "Unsupported parser outcome status",
                    exc_info=True,
                    extra={"file": local_file_path, "status": final_status, "error": str(e)},
                )
                logs_dict[f"Processing Error: {local_file_path}"] = f"[{timestamp_now}] {e}"
                move_s3_file(INPUT_BUCKET, local_file_path, PARSE_ERR_DIR)
                parse_err_files_count += 1
                continue

            if not _move_final_source_or_parse_error(
                local_file_path,
                dest_prefix,
                csv_writer,
                logs_dict,
                timestamp_now,
            ):
                parse_err_files_count += 1
                continue

            processed_monitor_points_count += mapped_monitor_points_for_file
            total_monitor_points_count += mapped_monitor_points_for_file
            if dest_prefix == PROCESSED_DIR:
                valid_processed_files_count += 1
            else:
                irrev_files_count += 1

            # Partial-recognition metric accumulation (Task 15).
            if file_candidate_row_count > 0:
                partial_mapped_ratio_total += (file_unmapped_count / file_candidate_row_count) * 100.0
                partial_mapped_ratio_files += 1
            denom_source_rows = max(file_source_row_count, file_candidate_row_count + file_total_skipped_seen)
            if denom_source_rows > 0:
                rows_skipped_ratio_total += (file_total_skipped_seen / denom_source_rows) * 100.0
                rows_skipped_ratio_files += 1
            malformed_value_count_total += int(file_skip_counter.get("unparseable_value", 0))
            if file_unsupported_suffixes:
                unsupported_suffixes_files += 1
            # Accumulate this file's distinct unmapped identifiers into the
            # batch-level set so we can emit per-kind counts at the end.
            batch_unmapped_identifiers.update(file_unmapped_identifiers)

            # Sidecar audit log: emit only when the file shows partial-data-loss
            # signal. Best-effort — failures log and continue.
            if file_total_skipped_seen > 0 or file_unmapped_count > 0 or file_unsupported_suffixes:
                try:
                    write_audit_sidecar(
                        batch_ts=batch_timestamp,
                        source_filename=Path(local_file_path).name,
                        outcome_summary={
                            "status": final_status,
                            "reason": final_reason,
                            "source_row_count": file_source_row_count,
                            "candidate_row_count": file_candidate_row_count,
                            "rows_written": file_rows_written,
                            "rows_skipped": file_total_skipped_seen,
                            "unmapped_count": file_unmapped_count,
                        },
                        skip_reasons=dict(file_skip_counter),
                        unmapped_identifiers=sorted(file_unmapped_identifiers),
                        unsupported_suffixes=sorted(file_unsupported_suffixes),
                        skipped_samples=file_skipped_samples,
                        s3_client=s3_client,
                        total_skipped=file_total_skipped_seen,
                    )
                except Exception as audit_err:
                    logger.warning(
                        "audit_sidecar_write_failed",
                        extra={"file": local_file_path, "error": str(audit_err)},
                    )

        for key, value in logs_dict.items():
            logger.warning("Runtime error", extra={"bad_file": key, "timestamp": value})

        # Record metrics using Powertools
        metrics.add_metric(name="ValidProcessedFiles", unit=MetricUnit.Count, value=valid_processed_files_count)
        metrics.add_metric(name="ParseErrorFiles", unit=MetricUnit.Count, value=parse_err_files_count)
        metrics.add_metric(name="IrrelevantFiles", unit=MetricUnit.Count, value=irrev_files_count)
        metrics.add_metric(name="FTPFiles", unit=MetricUnit.Count, value=ftp_files_count)
        metrics.add_metric(name="ProcessedMonitorPoints", unit=MetricUnit.Count, value=processed_monitor_points_count)
        metrics.add_metric(name="TotalMonitorPoints", unit=MetricUnit.Count, value=total_monitor_points_count)

        # Partial-recognition signals (Task 15 / spec lines 580-624). Ratios are
        # emitted as the batch-mean across files that contributed (i.e. files
        # with non-zero denominators). Counts are batch-level sums. Per-file
        # CloudWatch dimensions are intentionally omitted to bound cardinality.
        if partial_mapped_ratio_files > 0:
            metrics.add_metric(
                name="PartialMappedRatio",
                unit=MetricUnit.Percent,
                value=partial_mapped_ratio_total / partial_mapped_ratio_files,
            )
        if rows_skipped_ratio_files > 0:
            metrics.add_metric(
                name="RowsSkippedRatio",
                unit=MetricUnit.Percent,
                value=rows_skipped_ratio_total / rows_skipped_ratio_files,
            )
        metrics.add_metric(
            name="MalformedValueCount",
            unit=MetricUnit.Count,
            value=malformed_value_count_total,
        )
        metrics.add_metric(
            name="UnsupportedSuffixesFound",
            unit=MetricUnit.Count,
            value=unsupported_suffixes_files,
        )
        # UnmappedIdentifierKind (spec line 600): per-kind count of
        # distinct (kind, value) pairs across the batch. Emitted as one
        # metric per kind with the kind appended to the metric name to
        # avoid relying on Powertools dimensions (not used elsewhere in
        # this file). Skipped when the batch has no unmapped identifiers
        # to keep CloudWatch noise-free.
        if batch_unmapped_identifiers:
            unmapped_kinds: Counter[str] = Counter(kind for kind, _ in batch_unmapped_identifiers)
            for kind, count in unmapped_kinds.items():
                metrics.add_metric(
                    name=f"UnmappedIdentifierKind_{kind}",
                    unit=MetricUnit.Count,
                    value=count,
                )

        processing_end_time = pd.Timestamp.now().tz_localize("UTC").tz_convert("Australia/Sydney").isoformat()
        logger.info("Script finished", extra={"timestamp": processing_end_time})
        shutil.rmtree(tmp_files_folder_path, ignore_errors=True)

        return 1

    except Exception as e:
        err = traceback.format_exc()
        logger.error("Script failed", exc_info=True, extra={"error": str(e), "traceback": err})
        metrics.add_metric(name="ErrorExecutionCount", unit=MetricUnit.Count, value=1)
        shutil.rmtree(tmp_files_folder_path, ignore_errors=True)
        return None
    finally:
        executor.shutdown(wait=False)


@tracer.capture_lambda_handler
@metrics.log_metrics(capture_cold_start_metric=True)
@logger.inject_lambda_context
def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    tbp_files: list[dict[str, str]] = []
    requeued_count = 0
    skipped_count = 0

    for record in event["Records"]:
        try:
            message_body = json.loads(record["body"])

            # Extract retry count from message (default 0 for new messages)
            retry_count = message_body.get("_retry_count", 0)

            # Get S3 event info
            s3_event = message_body["Records"][0]
            bucket_name = s3_event["s3"]["bucket"]["name"]
            file_key = s3_event["s3"]["object"]["key"]

            # Decode key for logging
            decoded_key = unquote(file_key.replace("+", "%20"))

            logger.info(
                "Processing file",
                extra={"bucket": bucket_name, "key": decoded_key, "retry_count": retry_count},
            )

            # Check file stability before processing (use decoded key for S3 API)
            is_stable, _ = check_file_stability(bucket_name, decoded_key)

            if not is_stable:
                if retry_count >= MAX_REQUEUE_RETRIES:
                    # Max retries exceeded - log error and skip
                    logger.error(
                        "Max retries exceeded for unstable file",
                        extra={
                            "bucket": bucket_name,
                            "key": decoded_key,
                            "retry_count": retry_count,
                        },
                    )
                    metrics.add_metric(name="MaxRetriesExceeded", unit=MetricUnit.Count, value=1)
                    skipped_count += 1
                    continue

                # Requeue for later processing
                if requeue_message(message_body, retry_count):
                    requeued_count += 1
                    metrics.add_metric(name="MessagesRequeued", unit=MetricUnit.Count, value=1)
                continue

            # File is stable - add to processing list
            tbp_files.append(
                {
                    "bucket": bucket_name,
                    "file_name": file_key,
                }
            )

        except Exception as e:
            logger.error("Error processing SQS record", exc_info=True, extra={"error": str(e)})
            continue

    if tbp_files:
        parse_and_write_data(tbp_files=tbp_files)

    return {
        "statusCode": 200,
        "body": "Successfully processed files.",
        "processed": len(tbp_files),
        "requeued": requeued_count,
        "skipped": skipped_count,
    }
