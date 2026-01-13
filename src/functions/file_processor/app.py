import json
import random
import shutil
import tempfile
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
persistence_layer = DynamoDBPersistenceLayer(table_name="sbm-ingester-idempotency")
idempotency_config = IdempotencyConfig(
    expires_after_seconds=86400,  # 24 hours TTL
)

s3_resource = boto3.resource("s3")


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
def parseAndWriteData(tbp_files: list[dict[str, str]] | None = None) -> int | None:
    tmp_dir = tempfile.gettempdir()
    tmp_files_folder_name = str(uuid.uuid4())
    tmp_files_folder_path = Path(tmp_dir) / tmp_files_folder_name
    tmp_files_folder_path.mkdir(parents=True, exist_ok=True)

    # Generate timestamps once at start (not per-write)
    timestampNow = pd.Timestamp.now().tz_localize("UTC").tz_convert("Australia/Sydney").isoformat()
    batch_timestamp = pd.Timestamp.now().strftime("%Y_%b_%dT%H_%M_%S_%f")

    try:
        logger.info("Script started", extra={"timestamp": timestampNow, "files_count": len(tbp_files or [])})

        logsDict: dict[str, str] = {}
        nem12_mappings = read_nem12_mappings(BUCKET_NAME)

        if nem12_mappings is None:
            raise Exception("Failed to read NEM12 mappings from S3.")

        download_files_to_tmp(tbp_files or [], str(tmp_files_folder_path))

        validProcessedFilesCount = 0
        irrevFilesCount = 0
        parseErrFilesCount = 0
        processedMonitorPointsCount = 0
        totalMonitorPointsCount = 0
        ftpFilesCount = 0

        # Buffer for batch S3 writes
        write_buffer: list[pd.DataFrame] = []

        for file_path in tmp_files_folder_path.iterdir():
            filePath = str(file_path)
            dfs = None

            try:
                dfs = output_as_data_frames(filePath, split_days=True)
            except Exception:
                try:
                    dfs = get_non_nem_df(filePath, PARSE_ERROR_LOG_GROUP)
                except Exception:
                    logsDict[f"Bad File: {filePath}"] = f"[{timestampNow}]"
                    move_s3_file(BUCKET_NAME, filePath, PARSE_ERR_DIR)
                    parseErrFilesCount += 1
                    continue

            file_neptune_ids = []

            for bufferNMI, bufferDF in dfs:
                # Reset index if t_start is the index
                if "t_start" not in bufferDF.columns and bufferDF.index.name == "t_start":
                    bufferDF = bufferDF.reset_index()

                for reqCol in bufferDF.columns:
                    suffix = reqCol.split("_")[0]
                    if suffix not in NMI_DATA_STREAM_COMBINED:
                        continue

                    monitorPointName = f"{bufferNMI}-{suffix}"
                    neptuneId = nem12_mappings.get(monitorPointName)

                    if neptuneId is None:
                        continue

                    file_neptune_ids.append(neptuneId)

                    # Extract unit from column name (e.g., "E1_kWh" -> "kwh")
                    nem12UnitName = reqCol.split("_")[1].lower() if "_" in reqCol else "kwh"

                    # Build output DataFrame
                    gems2BufferDF = bufferDF[["t_start", reqCol]].copy()
                    gems2BufferDF["sensorId"] = neptuneId
                    gems2BufferDF["unit"] = nem12UnitName
                    gems2BufferDF = gems2BufferDF.rename(columns={"t_start": "ts", reqCol: "val"})
                    gems2BufferDF["its"] = gems2BufferDF["ts"]
                    gems2BufferDF = gems2BufferDF[["sensorId", "ts", "val", "unit", "its"]]

                    # Format timestamps
                    gems2BufferDF["ts"] = gems2BufferDF["ts"].dt.strftime("%Y-%m-%d %H:%M:%S")
                    gems2BufferDF["its"] = gems2BufferDF["its"].dt.strftime("%Y-%m-%d %H:%M:%S")

                    # Add to buffer instead of immediate write
                    write_buffer.append(gems2BufferDF)
                    processedMonitorPointsCount += 1

                    # Flush buffer when it reaches BATCH_SIZE
                    if len(write_buffer) >= BATCH_SIZE:
                        _flush_buffer_to_s3(write_buffer, batch_timestamp)
                        write_buffer.clear()

            # Move source file based on whether any data was mapped
            if file_neptune_ids:
                totalMonitorPointsCount += len(file_neptune_ids)
                move_s3_file(BUCKET_NAME, filePath, PROCESSED_DIR)
                validProcessedFilesCount += 1
            else:
                move_s3_file(BUCKET_NAME, filePath, IRREVFILES_DIR)
                irrevFilesCount += 1

        # Flush remaining buffer
        _flush_buffer_to_s3(write_buffer, batch_timestamp)

        for key, value in logsDict.items():
            logger.warning("Runtime error", extra={"bad_file": key, "timestamp": value})

        # Record metrics using Powertools
        metrics.add_metric(name="ValidProcessedFiles", unit=MetricUnit.Count, value=validProcessedFilesCount)
        metrics.add_metric(name="ParseErrorFiles", unit=MetricUnit.Count, value=parseErrFilesCount)
        metrics.add_metric(name="IrrelevantFiles", unit=MetricUnit.Count, value=irrevFilesCount)
        metrics.add_metric(name="FTPFiles", unit=MetricUnit.Count, value=ftpFilesCount)
        metrics.add_metric(name="ProcessedMonitorPoints", unit=MetricUnit.Count, value=processedMonitorPointsCount)
        metrics.add_metric(name="TotalMonitorPoints", unit=MetricUnit.Count, value=totalMonitorPointsCount)

        processingEndTime = pd.Timestamp.now().tz_localize("UTC").tz_convert("Australia/Sydney").isoformat()
        logger.info("Script finished", extra={"timestamp": processingEndTime})
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
    for record in event["Records"]:
        try:
            message_body = json.loads(record["body"])
            s3_event = message_body["Records"][0]

            bucket_name = s3_event["s3"]["bucket"]["name"]
            file_name = s3_event["s3"]["object"]["key"]
            tbp_files.append(
                {
                    "bucket": bucket_name,
                    "file_name": file_name,
                }
            )

        except Exception as e:
            logger.error("Error processing SQS record", exc_info=True, extra={"error": str(e)})
            continue

    if tbp_files:
        parseAndWriteData(tbp_files=tbp_files)

    return {"statusCode": 200, "body": "Successfully processed files."}
