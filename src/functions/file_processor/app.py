import json
import os
import random
import shutil
import tempfile
import time
import traceback
import uuid
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
    BUCKET_NAME,
    IRREVFILES_DIR,
    PARSE_ERR_DIR,
    PARSE_ERROR_LOG_GROUP,
    PROCESSED_DIR,
    get_non_nem_df,
    output_as_data_frames,
)

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

# Batch size for S3 writes - merge this many DataFrames before writing
BATCH_SIZE = 50

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
    expires_after_seconds=86400,  # 24 hours TTL
)

s3_resource = boto3.resource("s3")
s3_client = boto3.client("s3")
sqs_client = boto3.client("sqs")


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

    try:
        bucket = s3_resource.Bucket(bucket_name)

        copy_source = {"Bucket": bucket_name, "Key": source_key}
        bucket.Object(dest_key).copy(copy_source)

        bucket.Object(source_key).delete()

        return dest_key

    except Exception as e:
        logger.error("File move failed", exc_info=True, extra={"source": source_key, "dest": dest_key, "error": str(e)})
        return None


@tracer.capture_method
def _flush_buffer_to_s3(buffer: list, batch_timestamp: str) -> None:
    """Write buffered DataFrames to S3 as a single merged CSV."""
    if not buffer:
        return

    merged_df = pd.concat(buffer, ignore_index=True)
    output_key = f"sensorDataFiles/batch_{batch_timestamp}_{random.randint(1, 1000000)}.csv"
    s3_resource.Object("hudibucketsrc", output_key).put(Body=merged_df.to_csv(index=False))
    logger.debug("Flushed buffer to S3", extra={"output_key": output_key, "rows": len(merged_df)})


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

    try:
        logger.info("Script started", extra={"timestamp": timestamp_now, "files_count": len(tbp_files or [])})

        logs_dict: dict[str, str] = {}
        nem12_mappings = read_nem12_mappings(BUCKET_NAME)

        if nem12_mappings is None:
            raise Exception("Failed to read NEM12 mappings from S3.")

        download_files_to_tmp(tbp_files or [], str(tmp_files_folder_path))

        valid_processed_files_count = 0
        irrev_files_count = 0
        parse_err_files_count = 0
        processed_monitor_points_count = 0
        total_monitor_points_count = 0
        ftp_files_count = 0

        # Buffer for batch S3 writes
        write_buffer: list[pd.DataFrame] = []

        for file_path in tmp_files_folder_path.iterdir():
            local_file_path = str(file_path)
            dfs = None

            try:
                dfs = output_as_data_frames(local_file_path, split_days=True)
            except Exception:
                try:
                    dfs = get_non_nem_df(local_file_path, PARSE_ERROR_LOG_GROUP)
                except Exception:
                    logs_dict[f"Bad File: {local_file_path}"] = f"[{timestamp_now}]"
                    move_s3_file(BUCKET_NAME, local_file_path, PARSE_ERR_DIR)
                    parse_err_files_count += 1
                    continue

            file_neptune_ids = []

            for nmi, df in dfs:
                # Reset index if t_start is the index
                if "t_start" not in df.columns and df.index.name == "t_start":
                    df = df.reset_index()

                for col in df.columns:
                    suffix = col.split("_")[0]
                    if suffix not in NMI_DATA_STREAM_COMBINED:
                        continue

                    monitor_point_name = f"{nmi}-{suffix}"
                    neptune_id = nem12_mappings.get(monitor_point_name)

                    if neptune_id is None:
                        continue

                    file_neptune_ids.append(neptune_id)

                    # Extract unit from column name (e.g., "E1_kWh" -> "kwh")
                    unit_name = col.split("_")[1].lower() if "_" in col else "kwh"

                    # Build output DataFrame
                    output_df = df[["t_start", col]].copy()
                    output_df["sensorId"] = neptune_id
                    output_df["unit"] = unit_name
                    output_df = output_df.rename(columns={"t_start": "ts", col: "val"})
                    output_df["its"] = output_df["ts"]
                    output_df = output_df[["sensorId", "ts", "val", "unit", "its"]]

                    # Format timestamps
                    output_df["ts"] = output_df["ts"].dt.strftime("%Y-%m-%d %H:%M:%S")
                    output_df["its"] = output_df["its"].dt.strftime("%Y-%m-%d %H:%M:%S")

                    # Add to buffer instead of immediate write
                    write_buffer.append(output_df)
                    processed_monitor_points_count += 1

                    # Flush buffer when it reaches BATCH_SIZE
                    if len(write_buffer) >= BATCH_SIZE:
                        _flush_buffer_to_s3(write_buffer, batch_timestamp)
                        write_buffer.clear()

            # Move source file based on whether any data was mapped
            if file_neptune_ids:
                total_monitor_points_count += len(file_neptune_ids)
                move_s3_file(BUCKET_NAME, local_file_path, PROCESSED_DIR)
                valid_processed_files_count += 1
            else:
                move_s3_file(BUCKET_NAME, local_file_path, IRREVFILES_DIR)
                irrev_files_count += 1

        # Flush remaining buffer
        _flush_buffer_to_s3(write_buffer, batch_timestamp)

        for key, value in logs_dict.items():
            logger.warning("Runtime error", extra={"bad_file": key, "timestamp": value})

        # Record metrics using Powertools
        metrics.add_metric(name="ValidProcessedFiles", unit=MetricUnit.Count, value=valid_processed_files_count)
        metrics.add_metric(name="ParseErrorFiles", unit=MetricUnit.Count, value=parse_err_files_count)
        metrics.add_metric(name="IrrelevantFiles", unit=MetricUnit.Count, value=irrev_files_count)
        metrics.add_metric(name="FTPFiles", unit=MetricUnit.Count, value=ftp_files_count)
        metrics.add_metric(name="ProcessedMonitorPoints", unit=MetricUnit.Count, value=processed_monitor_points_count)
        metrics.add_metric(name="TotalMonitorPoints", unit=MetricUnit.Count, value=total_monitor_points_count)

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
