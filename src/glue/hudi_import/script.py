"""
Glue ETL job for importing sensor data from CSV files into Apache Hudi data lake.

This job:
1. Lists CSV files from s3://hudibucketsrc/sensorDataFiles/
2. Processes files in batches (BATCH_SIZE files per batch)
3. Upserts data into Hudi table (COPY_ON_WRITE)
4. Archives processed files to s3://hudibucketsrc/sensorDataFilesArchived/

Record key: sensorId + ts
Partition: its (timestamp year)

Configuration:
- BATCH_SIZE: 400 files per batch
- MAX_RUNTIME_SECONDS: 4 hours (14400s)
- ARCHIVE_WORKERS: 10 concurrent operations
- ARCHIVE_MAX_RETRIES: 3 attempts

Optional Testing Parameters (passed via --arguments):
- MAX_FILES: Limit number of files to process (0 = no limit, default)
- DRY_RUN: If "true", process data but don't archive files (default: "false")

Usage:
    # Normal run (all files)
    aws glue start-job-run --job-name DataImportIntoLake

    # Test with 10 files
    aws glue start-job-run --job-name DataImportIntoLake \
        --arguments '{"--MAX_FILES": "10"}'

    # Test with 50 files, no archiving
    aws glue start-job-run --job-name DataImportIntoLake \
        --arguments '{"--MAX_FILES": "50", "--DRY_RUN": "true"}'
"""

import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from urllib.parse import unquote

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from botocore.config import Config
from botocore.exceptions import ClientError
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp
from pyspark.sql.session import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

# ================================
# Configuration
# ================================
SOURCE_BUCKET = "hudibucketsrc"
SOURCE_PREFIX = "sensorDataFiles/"
ARCHIVE_PREFIX = "sensorDataFilesArchived/"

BATCH_SIZE = 400
MAX_BATCHES = 100  # Safety limit: 400 * 100 = 40,000 files max
MAX_RUNTIME_SECONDS = 14400  # 4 hours
ARCHIVE_WORKERS = 10
ARCHIVE_MAX_RETRIES = 3
RETRY_BASE_DELAY = 1  # seconds


# ================================
# Data Classes and Enums
# ================================
class ArchiveResult(Enum):
    """Result status for archive operation."""

    SUCCESS = "success"
    SKIPPED = "skipped"  # File no longer exists (already moved/deleted)
    ERROR = "error"


@dataclass
class BatchResult:
    """Result of processing a single batch."""

    files_processed: int
    rows_upserted: int
    archive_success: int
    archive_skipped: int
    archive_errors: int
    hudi_duration_seconds: float
    archive_duration_seconds: float


# ================================
# Initialize Spark and Glue
# ================================
# Required parameters
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "OUTPUT_BUCKET",
        "HUDI_INIT_SORT_OPTION",
        "HUDI_TABLE_NAME",
        "HUDI_DB_NAME",
    ],
)


def get_optional_arg(argv: list[str], arg_name: str, default: str) -> str:
    """
    Parse optional argument from sys.argv.

    Args:
        argv: Command line arguments (sys.argv)
        arg_name: Argument name without -- prefix
        default: Default value if not found

    Returns:
        Argument value or default
    """
    key = f"--{arg_name}"
    for i, arg in enumerate(argv):
        if arg == key and i + 1 < len(argv):
            return argv[i + 1]
    return default


# Optional testing parameters
MAX_FILES = int(get_optional_arg(sys.argv, "MAX_FILES", "0"))  # 0 = no limit
DRY_RUN = get_optional_arg(sys.argv, "DRY_RUN", "false").lower() == "true"

spark = (
    SparkSession.builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.hive.convertMetastoreParquet", "false")
    .config("spark.sql.legacy.pathOptionBehavior.enabled", "true")
    .getOrCreate()
)

sc = spark.sparkContext
glue_context = GlueContext(sc)
job = Job(glue_context)
logger = glue_context.get_logger()
job.init(args["JOB_NAME"], args)

# S3 client with connection pooling for concurrent archiving
s3_client = boto3.client("s3", config=Config(max_pool_connections=ARCHIVE_WORKERS))


# ================================
# S3 Operations Layer
# ================================
def list_s3_files(bucket: str, prefix: str) -> list[str]:
    """
    List all CSV files in the given S3 prefix.

    Uses pagination to handle large file counts.

    Returns:
        List of S3 URIs (s3://bucket/key format)
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    files: list[str] = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            # Skip directory markers and non-CSV files
            if key.endswith("/"):
                continue
            files.append(f"s3://{bucket}/{key}")

    return files


def archive_single_file(file_uri: str) -> tuple[ArchiveResult, str]:
    """
    Archive a single file from source to archive directory.

    Retries up to ARCHIVE_MAX_RETRIES times with exponential backoff.

    Args:
        file_uri: S3 URI of the file (s3://bucket/key)

    Returns:
        Tuple of (result, message)
    """
    # Parse S3 URI and handle URL encoding
    key = file_uri.replace(f"s3://{SOURCE_BUCKET}/", "")
    key = unquote(key.replace("+", "%20"))

    if not key or not key.startswith(SOURCE_PREFIX):
        return ArchiveResult.SKIPPED, f"Invalid key: {key}"

    archive_key = key.replace(SOURCE_PREFIX, ARCHIVE_PREFIX)

    for attempt in range(ARCHIVE_MAX_RETRIES):
        try:
            # Copy to archive location
            s3_client.copy_object(
                Bucket=SOURCE_BUCKET,
                CopySource={"Bucket": SOURCE_BUCKET, "Key": key},
                Key=archive_key,
            )
            # Delete original
            s3_client.delete_object(Bucket=SOURCE_BUCKET, Key=key)
            return ArchiveResult.SUCCESS, key

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")

            if error_code in ("NoSuchKey", "404"):
                # File was already moved/deleted - this is OK
                return ArchiveResult.SKIPPED, key

            # Real error - retry or fail
            if attempt < ARCHIVE_MAX_RETRIES - 1:
                delay = RETRY_BASE_DELAY * (2**attempt)
                time.sleep(delay)
            else:
                logger.error(
                    f"Archive failed after {ARCHIVE_MAX_RETRIES} attempts: "
                    f"key={key}, error_code={error_code}, error={e}"
                )
                return ArchiveResult.ERROR, f"{key}: {e}"

        except Exception as e:
            if attempt < ARCHIVE_MAX_RETRIES - 1:
                delay = RETRY_BASE_DELAY * (2**attempt)
                time.sleep(delay)
            else:
                logger.error(f"Unexpected archive error: key={key}, error={e}")
                return ArchiveResult.ERROR, f"{key}: {e}"

    # Should not reach here
    return ArchiveResult.ERROR, f"{key}: Unknown error"


def archive_files_concurrent(file_uris: list[str]) -> tuple[int, int, int]:
    """
    Archive multiple files concurrently using ThreadPoolExecutor.

    Args:
        file_uris: List of S3 URIs to archive

    Returns:
        Tuple of (success_count, skipped_count, error_count)
    """
    if not file_uris:
        return 0, 0, 0

    success = 0
    skipped = 0
    errors = 0

    with ThreadPoolExecutor(max_workers=ARCHIVE_WORKERS) as executor:
        futures = [executor.submit(archive_single_file, uri) for uri in file_uris]

        for future in as_completed(futures):
            result, _ = future.result()
            if result == ArchiveResult.SUCCESS:
                success += 1
            elif result == ArchiveResult.SKIPPED:
                skipped += 1
            else:
                errors += 1

    return success, skipped, errors


# ================================
# Hudi Processing Layer
# ================================
def build_hudi_config(
    hudi_init_sort_option: str,
    hudi_output_bucket: str,
    hudi_table_name: str,
    hudi_db_name: str,
) -> dict[str, str]:
    """
    Build Hudi write configuration.

    Returns:
        Dictionary of Hudi configuration options
    """
    hudi_table_name = f"{hudi_table_name.lower()}_{hudi_init_sort_option.lower()}"

    config = {
        "className": "org.apache.hudi",
        # Hive sync settings
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.use_jdbc": "false",
        "hoodie.datasource.hive_sync.support_timestamp": "true",
        "hoodie.datasource.hive_sync.database": hudi_db_name,
        "hoodie.datasource.hive_sync.table": hudi_table_name,
        "hoodie.datasource.hive_sync.partition_fields": "its",
        "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
        # Write settings
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.recordkey.field": "sensorId, ts",
        "hoodie.datasource.write.partitionpath.field": "its:TIMESTAMP",
        "hoodie.datasource.write.hive_style_partitioning": "true",
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.CustomKeyGenerator",
        # Table settings
        "hoodie.table.name": hudi_table_name,
        # Timestamp parsing for partition path
        "hoodie.deltastreamer.keygen.timebased.timestamp.type": "DATE_STRING",
        "hoodie.deltastreamer.keygen.timebased.input.dateformat": "yyyy-MM-dd H:mm:ss",
        "hoodie.deltastreamer.keygen.timebased.output.dateformat": "yyyy",
        "hoodie.deltastreamer.keygen.timebased.timezone": "UTC",
    }

    # Set bulk insert sort mode if specified
    if hudi_init_sort_option.upper() in ["PARTITION_SORT", "NONE"]:
        config["hoodie.bulkinsert.sort.mode"] = hudi_init_sort_option

    return config


def get_schema() -> StructType:
    """Return schema for sensor data CSV files."""
    return StructType(
        [
            StructField("sensorId", StringType(), True),
            StructField("ts", TimestampType(), True),
            StructField("val", DoubleType(), True),
            StructField("unit", StringType(), True),
            StructField("its", StringType(), True),
        ]
    )


def read_csv_batch(file_uris: list[str], schema: StructType) -> DataFrame:
    """
    Read a batch of CSV files into a DataFrame.

    Args:
        file_uris: List of S3 URIs to read
        schema: DataFrame schema

    Returns:
        DataFrame with sensor data and added timestamp column
    """
    return (
        spark.read.schema(schema)
        .format("csv")
        .options(header=True, delimiter=",")
        .load(file_uris)
        .withColumn("ats", current_timestamp())
    )


def process_single_batch(
    file_uris: list[str],
    hudi_config: dict[str, str],
    table_path: str,
    dry_run: bool = False,
) -> BatchResult:
    """
    Process a single batch of files: read, upsert to Hudi, archive.

    Args:
        file_uris: List of S3 URIs in this batch
        hudi_config: Hudi write configuration
        table_path: S3 path to Hudi table
        dry_run: If True, skip archiving files

    Returns:
        BatchResult with processing metrics
    """
    schema = get_schema()

    # Read CSV files
    sensor_df = read_csv_batch(file_uris, schema)

    # Get actual input files (using public API)
    input_files = list(sensor_df.inputFiles())

    # Cache DataFrame for count and write operations
    sensor_df.cache()
    try:
        row_count = sensor_df.count()

        if row_count == 0:
            logger.info(f"Empty batch (0 rows), {len(file_uris)} files without Hudi write")
            # Archive empty files to prevent infinite reprocessing (unless dry_run)
            if dry_run:
                logger.info("DRY_RUN: Skipping archive for empty batch")
                return BatchResult(
                    files_processed=len(file_uris),
                    rows_upserted=0,
                    archive_success=0,
                    archive_skipped=len(input_files),
                    archive_errors=0,
                    hudi_duration_seconds=0.0,
                    archive_duration_seconds=0.0,
                )

            archive_start = datetime.now()
            success, skipped, errors = archive_files_concurrent(input_files)
            archive_duration = (datetime.now() - archive_start).total_seconds()

            return BatchResult(
                files_processed=len(file_uris),
                rows_upserted=0,
                archive_success=success,
                archive_skipped=skipped,
                archive_errors=errors,
                hudi_duration_seconds=0.0,
                archive_duration_seconds=archive_duration,
            )

        # Write to Hudi
        hudi_start = datetime.now()
        sensor_df.write.format("hudi").options(**hudi_config).mode("append").save(table_path)
        hudi_duration = (datetime.now() - hudi_start).total_seconds()

    finally:
        # Always free memory, even on exception
        sensor_df.unpersist()

    # Archive processed files (unless dry_run)
    if dry_run:
        logger.info(f"DRY_RUN: Skipping archive for {len(input_files)} files")
        return BatchResult(
            files_processed=len(file_uris),
            rows_upserted=row_count,
            archive_success=0,
            archive_skipped=len(input_files),
            archive_errors=0,
            hudi_duration_seconds=hudi_duration,
            archive_duration_seconds=0.0,
        )

    archive_start = datetime.now()
    success, skipped, errors = archive_files_concurrent(input_files)
    archive_duration = (datetime.now() - archive_start).total_seconds()

    return BatchResult(
        files_processed=len(file_uris),
        rows_upserted=row_count,
        archive_success=success,
        archive_skipped=skipped,
        archive_errors=errors,
        hudi_duration_seconds=hudi_duration,
        archive_duration_seconds=archive_duration,
    )


# ================================
# Orchestration Layer
# ================================
def chunk_list(items: list, chunk_size: int) -> list[list]:
    """Split a list into chunks of specified size."""
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def process_all_files(
    hudi_config: dict[str, str],
    table_path: str,
    max_files: int = 0,
    dry_run: bool = False,
) -> dict:
    """
    Main orchestrator: list files, process in batches, respect runtime limit.

    Args:
        hudi_config: Hudi write configuration
        table_path: S3 path to Hudi table
        max_files: Maximum files to process (0 = no limit)
        dry_run: If True, process but don't archive files

    Returns:
        Summary dictionary with all metrics
    """
    job_start = datetime.now()
    logger.info(f"Starting job at {job_start}")

    if dry_run:
        logger.info("DRY_RUN mode enabled - files will NOT be archived")

    # List all files upfront (before any processing)
    all_files = list_s3_files(SOURCE_BUCKET, SOURCE_PREFIX)
    total_files = len(all_files)

    # Apply MAX_FILES limit if specified
    if max_files > 0 and total_files > max_files:
        logger.info(f"MAX_FILES={max_files} specified, limiting from {total_files} files")
        all_files = all_files[:max_files]
        total_files = len(all_files)

    if total_files == 0:
        logger.info("No files to process")
        return {
            "total_files": 0,
            "batches_processed": 0,
            "total_rows": 0,
            "total_archived": 0,
            "total_skipped": 0,
            "total_errors": 0,
            "total_runtime_seconds": 0,
        }

    logger.info(f"Found {total_files} files to process")

    # Split into batches
    batches = chunk_list(all_files, BATCH_SIZE)
    if len(batches) > MAX_BATCHES:
        logger.info(f"Limiting to {MAX_BATCHES} batches (out of {len(batches)})")
        batches = batches[:MAX_BATCHES]

    # Aggregate metrics
    total_rows = 0
    total_archived = 0
    total_skipped = 0
    total_errors = 0
    batches_processed = 0

    for batch_num, batch in enumerate(batches, 1):
        # Check runtime limit
        elapsed = (datetime.now() - job_start).total_seconds()
        if elapsed >= MAX_RUNTIME_SECONDS:
            logger.warning(
                f"Max runtime reached ({MAX_RUNTIME_SECONDS}s), processed {batches_processed}/{len(batches)} batches"
            )
            break

        logger.info(f"Processing batch {batch_num}/{len(batches)} ({len(batch)} files, elapsed={elapsed:.0f}s)")

        try:
            result = process_single_batch(batch, hudi_config, table_path, dry_run=dry_run)

            total_rows += result.rows_upserted
            total_archived += result.archive_success
            total_skipped += result.archive_skipped
            total_errors += result.archive_errors
            batches_processed += 1

            archive_total = result.archive_success + result.archive_skipped + result.archive_errors
            logger.info(
                f"Batch {batch_num} complete: "
                f"files={result.files_processed}, "
                f"rows={result.rows_upserted}, "
                f"hudi={result.hudi_duration_seconds:.1f}s, "
                f"archive={result.archive_success}/{archive_total} "
                f"(skipped={result.archive_skipped}, errors={result.archive_errors}), "
                f"archive_time={result.archive_duration_seconds:.1f}s"
            )

        except Exception as e:
            logger.error(f"Batch {batch_num} failed: {e}\n{traceback.format_exc()}")
            # Continue with next batch - files remain for retry on next job run
            continue

    total_runtime = (datetime.now() - job_start).total_seconds()

    summary = {
        "total_files": total_files,
        "batches_processed": batches_processed,
        "batches_total": len(batches),
        "total_rows": total_rows,
        "total_archived": total_archived,
        "total_skipped": total_skipped,
        "total_errors": total_errors,
        "total_runtime_seconds": total_runtime,
    }

    logger.info(f"Job complete: {summary}")
    return summary


# ================================
# Main Entry Point
# ================================
if __name__ == "__main__":
    try:
        # Build configuration
        hudi_config = build_hudi_config(
            args["HUDI_INIT_SORT_OPTION"],
            args["OUTPUT_BUCKET"],
            args["HUDI_TABLE_NAME"],
            args["HUDI_DB_NAME"],
        )

        hudi_table_name = f"{args['HUDI_TABLE_NAME'].lower()}_{args['HUDI_INIT_SORT_OPTION'].lower()}"
        table_path = f"s3://{args['OUTPUT_BUCKET']}/{hudi_table_name}"

        logger.info(f"Job arguments: {args}")
        logger.info(f"Hudi table path: {table_path}")
        logger.info(
            f"Configuration: BATCH_SIZE={BATCH_SIZE}, "
            f"MAX_RUNTIME={MAX_RUNTIME_SECONDS}s, "
            f"ARCHIVE_WORKERS={ARCHIVE_WORKERS}"
        )

        # Log testing parameters if set
        if MAX_FILES > 0:
            logger.info(f"Testing mode: MAX_FILES={MAX_FILES}")
        if DRY_RUN:
            logger.info("Testing mode: DRY_RUN=true (files will NOT be archived)")

        # Process all files
        summary = process_all_files(hudi_config, table_path, max_files=MAX_FILES, dry_run=DRY_RUN)

        if summary["total_errors"] > 0:
            logger.warning(
                f"Job completed with {summary['total_errors']} archive errors. "
                f"Failed files remain in {SOURCE_PREFIX} for retry."
            )

    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise

job.commit()
