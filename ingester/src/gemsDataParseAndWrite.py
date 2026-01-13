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
import modules.common as common
import pandas as pd
from modules.common import BUCKET_NAME, CloudWatchLogger
from modules.nem_adapter import output_as_data_frames
from modules.nonNemParserFuncs import nonNemParsersGetDf

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

execution_log = CloudWatchLogger(common.EXECUTION_LOG_GROUP)
error_log = CloudWatchLogger(common.ERROR_LOG_GROUP)
runtime_error_log = CloudWatchLogger(common.RUNTIME_ERROR_LOG_GROUP)
metrics_log = CloudWatchLogger(common.METRICS_LOG_GROUP)

s3_resource = boto3.resource("s3")


def read_nem12_mappings(bucket_name: str, object_key: str = "nem12_mappings.json") -> dict | None:
    try:
        obj = s3_resource.Object(bucket_name, object_key)
        content = obj.get()["Body"].read().decode("utf-8")
        return json.loads(content)
    except Exception as e:
        error_log.log(f"Failed to read NEM12 mappings from {bucket_name}/{object_key}: {e}")
        return None


def download_files_to_tmp(file_list: list[dict[str, str]], tmp_files_folder_path: str) -> list[str]:
    local_paths = []

    for f in file_list:
        bucket = f["bucket"]

        # Always decode key before using with boto3
        key = unquote(f["file_name"].replace("+", "%20"))

        file_name = Path(key).name
        local_path = str(Path(tmp_files_folder_path) / file_name)

        execution_log.log(f"Downloading s3://{bucket}/{key} -> {local_path}")

        try:
            s3_resource.Bucket(bucket).download_file(key, local_path)
            local_paths.append(local_path)

        except Exception as e:
            error_log.log(f"Downloading {key} Failed. File Potentially already processed. Error: {e}")
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
        error_log.log(f"Moving {source_key} -> {dest_key} Failed. File Potentially already processed. Error: {e}")
        return None


def dailyInitializeMetricsDict(metricsDict: dict[str, dict[str, int]], key: str) -> None:
    if key not in metricsDict:
        metricsDict[key] = {
            "calculatedTotalFilesCount": 0,
            "ftpFilesCount": 0,
            "calculatedEmailFilesCount": 0,
            "validProcessedFilesCount": 0,
            "parseErrFilesCount": 0,
            "irrevFilesCount": 0,
            "totalMonitorPointsCount": 0,
            "processedMonitorPointsCount": 0,
            "errorExecutionCount": 0,
        }


def metricsDictPopulateValues(
    metricsDict: dict[str, dict[str, int]],
    key: str,
    ftpFilesCount: int,
    validProcessedFilesCount: int,
    parseErrFilesCount: int,
    irrevFilesCount: int,
    totalMonitorPointsCount: int,
    processedMonitorPointsCount: int,
    errorExecutionCount: int,
) -> None:
    dailyInitializeMetricsDict(metricsDict, key)
    metricsDict[key]["validProcessedFilesCount"] += validProcessedFilesCount
    metricsDict[key]["ftpFilesCount"] += ftpFilesCount
    metricsDict[key]["parseErrFilesCount"] += parseErrFilesCount
    metricsDict[key]["irrevFilesCount"] += irrevFilesCount
    metricsDict[key]["totalMonitorPointsCount"] += totalMonitorPointsCount
    metricsDict[key]["processedMonitorPointsCount"] += processedMonitorPointsCount
    metricsDict[key]["calculatedTotalFilesCount"] = (
        metricsDict[key]["parseErrFilesCount"]
        + metricsDict[key]["irrevFilesCount"]
        + metricsDict[key]["validProcessedFilesCount"]
    )
    metricsDict[key]["calculatedEmailFilesCount"] = (
        metricsDict[key]["calculatedTotalFilesCount"] - metricsDict[key]["ftpFilesCount"]
    )
    metricsDict[key]["errorExecutionCount"] += errorExecutionCount


def _flush_buffer_to_s3(buffer: list, batch_timestamp: str) -> None:
    """Write buffered DataFrames to S3 as a single merged CSV."""
    if not buffer:
        return

    merged_df = pd.concat(buffer, ignore_index=True)
    output_key = f"sensorDataFiles/batch_{batch_timestamp}_{random.randint(1, 1000000)}.csv"
    s3_resource.Object("hudibucketsrc", output_key).put(Body=merged_df.to_csv(index=False))


def parseAndWriteData(tbp_files: list[dict[str, str]] | None = None) -> int | None:
    tmp_dir = tempfile.gettempdir()
    tmp_files_folder_name = str(uuid.uuid4())
    tmp_files_folder_path = Path(tmp_dir) / tmp_files_folder_name
    tmp_files_folder_path.mkdir(parents=True, exist_ok=True)

    # Generate timestamps once at start (not per-write)
    timestampNow = pd.Timestamp.now().tz_localize("UTC").tz_convert("Australia/Sydney").isoformat()
    batch_timestamp = pd.Timestamp.now().strftime("%Y_%b_%dT%H_%M_%S_%f")
    metricsFileKey = timestampNow.split("T")[0] + "D"
    metricsDict: dict[str, dict[str, int]] = {}

    try:
        execution_log.log(f"Script Started Running at: {timestampNow}")

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
                    dfs = nonNemParsersGetDf(filePath, common.PARSE_ERROR_LOG_GROUP)
                except Exception:
                    logsDict[f"Bad File: {filePath}"] = f"[{timestampNow}]"
                    move_s3_file(BUCKET_NAME, filePath, common.PARSE_ERR_DIR)
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
                move_s3_file(BUCKET_NAME, filePath, common.PROCESSED_DIR)
                validProcessedFilesCount += 1
            else:
                move_s3_file(BUCKET_NAME, filePath, common.IRREVFILES_DIR)
                irrevFilesCount += 1

        # Flush remaining buffer
        _flush_buffer_to_s3(write_buffer, batch_timestamp)

        for key, value in logsDict.items():
            runtime_error_log.log(f"{key} at {value}")

        metricsDictPopulateValues(
            metricsDict,
            metricsFileKey,
            ftpFilesCount,
            validProcessedFilesCount,
            parseErrFilesCount,
            irrevFilesCount,
            totalMonitorPointsCount,
            processedMonitorPointsCount,
            0,
        )
        metrics_log.log(json.dumps(metricsDict[metricsFileKey]))

        processingEndTime = pd.Timestamp.now().tz_localize("UTC").tz_convert("Australia/Sydney").isoformat()
        execution_log.log(f"Script Finished Running at: {processingEndTime}")
        shutil.rmtree(tmp_files_folder_path, ignore_errors=True)

        return 1

    except Exception as e:
        err = traceback.format_exc()
        error_log.log(f"Script Failed with Error: {e}\n{err}")
        metricsDictPopulateValues(metricsDict, metricsFileKey, 0, 0, 0, 0, 0, 0, 1)
        metrics_log.log(json.dumps(metricsDict[metricsFileKey]))
        shutil.rmtree(tmp_files_folder_path, ignore_errors=True)
        return None


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
            error_log.log(f"Error processing record: {e}")
            continue

    if tbp_files:
        parseAndWriteData(tbp_files)

    return {"statusCode": 200, "body": "Successfully processed files."}
