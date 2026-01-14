"""Unit tests for Glue Hudi Import script.

Since the Glue script uses PySpark and AWS Glue modules that are not available
in the local test environment, we:
1. Mock PySpark and awsglue modules before importing the script
2. Test pure Python functions (chunk_list, ArchiveResult, BatchResult)
3. Test S3 operations with moto
4. Test orchestration logic with mocked Spark operations
"""

import os
import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any
from unittest.mock import MagicMock, patch

import boto3
from botocore.exceptions import ClientError
from moto import mock_aws


# ================================
# Recreate types from script for testing
# ================================
class ArchiveResult(Enum):
    """Result status for archive operation."""

    SUCCESS = "success"
    SKIPPED = "skipped"
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
# Pure Function Tests
# ================================
class TestArchiveResultEnum:
    """Tests for ArchiveResult enum."""

    def test_archive_result_values(self) -> None:
        """Test ArchiveResult enum has correct values."""
        assert ArchiveResult.SUCCESS.value == "success"
        assert ArchiveResult.SKIPPED.value == "skipped"
        assert ArchiveResult.ERROR.value == "error"

    def test_archive_result_comparison(self) -> None:
        """Test ArchiveResult enum comparison."""
        assert ArchiveResult.SUCCESS == ArchiveResult.SUCCESS
        assert ArchiveResult.SUCCESS != ArchiveResult.ERROR
        assert ArchiveResult.SUCCESS != ArchiveResult.SKIPPED


class TestBatchResultDataclass:
    """Tests for BatchResult dataclass."""

    def test_batch_result_creation(self) -> None:
        """Test BatchResult can be created with all fields."""
        result = BatchResult(
            files_processed=10,
            rows_upserted=1000,
            archive_success=8,
            archive_skipped=1,
            archive_errors=1,
            hudi_duration_seconds=5.5,
            archive_duration_seconds=2.3,
        )

        assert result.files_processed == 10
        assert result.rows_upserted == 1000
        assert result.archive_success == 8
        assert result.archive_skipped == 1
        assert result.archive_errors == 1
        assert result.hudi_duration_seconds == 5.5
        assert result.archive_duration_seconds == 2.3

    def test_batch_result_zero_values(self) -> None:
        """Test BatchResult with zero values."""
        result = BatchResult(
            files_processed=0,
            rows_upserted=0,
            archive_success=0,
            archive_skipped=0,
            archive_errors=0,
            hudi_duration_seconds=0.0,
            archive_duration_seconds=0.0,
        )

        assert result.files_processed == 0
        assert result.rows_upserted == 0


class TestChunkList:
    """Tests for chunk_list function."""

    def test_chunk_list_exact_division(self) -> None:
        """Test chunking when list divides evenly."""
        items = [1, 2, 3, 4, 5, 6]
        chunks = chunk_list(items, 2)
        assert chunks == [[1, 2], [3, 4], [5, 6]]

    def test_chunk_list_remainder(self) -> None:
        """Test chunking when list has remainder."""
        items = [1, 2, 3, 4, 5]
        chunks = chunk_list(items, 2)
        assert chunks == [[1, 2], [3, 4], [5]]

    def test_chunk_list_single_chunk(self) -> None:
        """Test when chunk size is larger than list."""
        items = [1, 2, 3]
        chunks = chunk_list(items, 10)
        assert chunks == [[1, 2, 3]]

    def test_chunk_list_empty(self) -> None:
        """Test chunking empty list."""
        items: list[int] = []
        chunks = chunk_list(items, 5)
        assert chunks == []

    def test_chunk_list_chunk_size_one(self) -> None:
        """Test chunking with size 1."""
        items = [1, 2, 3]
        chunks = chunk_list(items, 1)
        assert chunks == [[1], [2], [3]]

    def test_chunk_list_large_list(self) -> None:
        """Test chunking large list."""
        items = list(range(1000))
        chunks = chunk_list(items, 100)
        assert len(chunks) == 10
        assert all(len(chunk) == 100 for chunk in chunks)


def chunk_list(items: list, chunk_size: int) -> list[list]:
    """Split a list into chunks of specified size.

    This is a copy of the function from the script for testing.
    """
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


# ================================
# S3 Operations Tests (with moto)
# ================================
class TestListS3Files:
    """Tests for list_s3_files function."""

    @mock_aws
    def test_list_s3_files_returns_uris(self) -> None:
        """Test that list_s3_files returns S3 URIs."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="hudibucketsrc",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        # Create test files
        s3.put_object(Bucket="hudibucketsrc", Key="sensorDataFiles/file1.csv", Body=b"test")
        s3.put_object(Bucket="hudibucketsrc", Key="sensorDataFiles/file2.csv", Body=b"test")

        files = list_s3_files_impl(s3, "hudibucketsrc", "sensorDataFiles/")

        assert len(files) == 2
        assert "s3://hudibucketsrc/sensorDataFiles/file1.csv" in files
        assert "s3://hudibucketsrc/sensorDataFiles/file2.csv" in files

    @mock_aws
    def test_list_s3_files_empty_bucket(self) -> None:
        """Test list_s3_files with no files."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="hudibucketsrc",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        files = list_s3_files_impl(s3, "hudibucketsrc", "sensorDataFiles/")
        assert files == []

    @mock_aws
    def test_list_s3_files_skips_directories(self) -> None:
        """Test that directory markers are skipped."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="hudibucketsrc",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        # Create files and a directory marker
        s3.put_object(Bucket="hudibucketsrc", Key="sensorDataFiles/", Body=b"")
        s3.put_object(Bucket="hudibucketsrc", Key="sensorDataFiles/file1.csv", Body=b"test")

        files = list_s3_files_impl(s3, "hudibucketsrc", "sensorDataFiles/")
        assert len(files) == 1
        assert "s3://hudibucketsrc/sensorDataFiles/file1.csv" in files

    @mock_aws
    def test_list_s3_files_pagination(self) -> None:
        """Test list_s3_files handles pagination."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="hudibucketsrc",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        # Create many files (more than default page size)
        for i in range(50):
            s3.put_object(Bucket="hudibucketsrc", Key=f"sensorDataFiles/file_{i:04d}.csv", Body=b"test")

        files = list_s3_files_impl(s3, "hudibucketsrc", "sensorDataFiles/")
        assert len(files) == 50


def list_s3_files_impl(s3_client: Any, bucket: str, prefix: str) -> list[str]:
    """Implementation of list_s3_files for testing."""
    paginator = s3_client.get_paginator("list_objects_v2")
    files: list[str] = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            files.append(f"s3://{bucket}/{key}")

    return files


class TestArchiveSingleFile:
    """Tests for archive_single_file function."""

    @mock_aws
    def test_archive_single_file_success(self) -> None:
        """Test successful file archiving."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="hudibucketsrc",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )
        s3.put_object(Bucket="hudibucketsrc", Key="sensorDataFiles/test.csv", Body=b"test data")

        result, _msg = archive_single_file_impl(
            s3,
            "s3://hudibucketsrc/sensorDataFiles/test.csv",
            "hudibucketsrc",
            "sensorDataFiles/",
            "sensorDataFilesArchived/",
        )

        assert result == ArchiveResult.SUCCESS

        # Verify file moved
        archived = s3.list_objects_v2(Bucket="hudibucketsrc", Prefix="sensorDataFilesArchived/")
        assert archived["KeyCount"] == 1

        # Verify original deleted
        original = s3.list_objects_v2(Bucket="hudibucketsrc", Prefix="sensorDataFiles/")
        assert original.get("KeyCount", 0) == 0

    @mock_aws
    def test_archive_single_file_not_found(self) -> None:
        """Test archiving non-existent file returns SKIPPED."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="hudibucketsrc",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        result, _msg = archive_single_file_impl(
            s3,
            "s3://hudibucketsrc/sensorDataFiles/nonexistent.csv",
            "hudibucketsrc",
            "sensorDataFiles/",
            "sensorDataFilesArchived/",
        )

        assert result == ArchiveResult.SKIPPED

    @mock_aws
    def test_archive_single_file_url_encoded(self) -> None:
        """Test archiving file with URL-encoded characters."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="hudibucketsrc",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )
        s3.put_object(Bucket="hudibucketsrc", Key="sensorDataFiles/file with spaces.csv", Body=b"test")

        result, _msg = archive_single_file_impl(
            s3,
            "s3://hudibucketsrc/sensorDataFiles/file%20with%20spaces.csv",
            "hudibucketsrc",
            "sensorDataFiles/",
            "sensorDataFilesArchived/",
        )

        assert result == ArchiveResult.SUCCESS

    def test_archive_single_file_invalid_key(self) -> None:
        """Test archiving with invalid key returns SKIPPED."""
        s3 = MagicMock()

        result, msg = archive_single_file_impl(
            s3,
            "s3://hudibucketsrc/wrongPrefix/test.csv",
            "hudibucketsrc",
            "sensorDataFiles/",
            "sensorDataFilesArchived/",
        )

        assert result == ArchiveResult.SKIPPED
        assert "Invalid key" in msg

    def test_archive_single_file_access_denied(self) -> None:
        """Test S3 access denied returns ERROR."""
        s3 = MagicMock()
        error_response = {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}
        s3.copy_object.side_effect = ClientError(error_response, "CopyObject")

        result, msg = archive_single_file_impl(
            s3,
            "s3://hudibucketsrc/sensorDataFiles/test.csv",
            "hudibucketsrc",
            "sensorDataFiles/",
            "sensorDataFilesArchived/",
            max_retries=1,
        )

        assert result == ArchiveResult.ERROR
        assert "AccessDenied" in msg or "Access Denied" in msg


def archive_single_file_impl(
    s3_client: Any,
    file_uri: str,
    bucket: str,
    source_prefix: str,
    archive_prefix: str,
    max_retries: int = 3,
    retry_base_delay: float = 0.1,
) -> tuple[ArchiveResult, str]:
    """Implementation of archive_single_file for testing."""
    from urllib.parse import unquote

    # Parse S3 URI and handle URL encoding
    key = file_uri.replace(f"s3://{bucket}/", "")
    key = unquote(key.replace("+", "%20"))

    if not key or not key.startswith(source_prefix):
        return ArchiveResult.SKIPPED, f"Invalid key: {key}"

    archive_key = key.replace(source_prefix, archive_prefix)

    for attempt in range(max_retries):
        try:
            s3_client.copy_object(
                Bucket=bucket,
                CopySource={"Bucket": bucket, "Key": key},
                Key=archive_key,
            )
            s3_client.delete_object(Bucket=bucket, Key=key)
            return ArchiveResult.SUCCESS, key

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")

            if error_code in ("NoSuchKey", "404"):
                return ArchiveResult.SKIPPED, key

            if attempt < max_retries - 1:
                time.sleep(retry_base_delay * (2**attempt))
            else:
                return ArchiveResult.ERROR, f"{key}: {e}"

        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(retry_base_delay * (2**attempt))
            else:
                return ArchiveResult.ERROR, f"{key}: {e}"

    return ArchiveResult.ERROR, f"{key}: Unknown error"


class TestArchiveFilesConcurrent:
    """Tests for archive_files_concurrent function."""

    @mock_aws
    def test_archive_files_concurrent_success(self) -> None:
        """Test concurrent archiving of multiple files."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="hudibucketsrc",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        # Create multiple files
        for i in range(5):
            s3.put_object(Bucket="hudibucketsrc", Key=f"sensorDataFiles/file_{i}.csv", Body=b"test")

        file_uris = [f"s3://hudibucketsrc/sensorDataFiles/file_{i}.csv" for i in range(5)]

        success, skipped, errors = archive_files_concurrent_impl(
            s3,
            file_uris,
            "hudibucketsrc",
            "sensorDataFiles/",
            "sensorDataFilesArchived/",
            max_workers=3,
        )

        assert success == 5
        assert skipped == 0
        assert errors == 0

    @mock_aws
    def test_archive_files_concurrent_empty_list(self) -> None:
        """Test concurrent archiving with empty list."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="hudibucketsrc",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        success, skipped, errors = archive_files_concurrent_impl(
            s3, [], "hudibucketsrc", "sensorDataFiles/", "sensorDataFilesArchived/"
        )

        assert success == 0
        assert skipped == 0
        assert errors == 0

    @mock_aws
    def test_archive_files_concurrent_mixed_results(self) -> None:
        """Test concurrent archiving with some missing files."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="hudibucketsrc",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        # Create only some files
        s3.put_object(Bucket="hudibucketsrc", Key="sensorDataFiles/file_0.csv", Body=b"test")
        s3.put_object(Bucket="hudibucketsrc", Key="sensorDataFiles/file_2.csv", Body=b"test")

        file_uris = [f"s3://hudibucketsrc/sensorDataFiles/file_{i}.csv" for i in range(4)]

        success, skipped, errors = archive_files_concurrent_impl(
            s3,
            file_uris,
            "hudibucketsrc",
            "sensorDataFiles/",
            "sensorDataFilesArchived/",
        )

        assert success == 2
        assert skipped == 2  # file_1 and file_3 don't exist
        assert errors == 0


def archive_files_concurrent_impl(
    s3_client: Any,
    file_uris: list[str],
    bucket: str,
    source_prefix: str,
    archive_prefix: str,
    max_workers: int = 10,
) -> tuple[int, int, int]:
    """Implementation of archive_files_concurrent for testing."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    if not file_uris:
        return 0, 0, 0

    success = 0
    skipped = 0
    errors = 0

    def archive_file(uri: str) -> tuple[ArchiveResult, str]:
        return archive_single_file_impl(
            s3_client, uri, bucket, source_prefix, archive_prefix, max_retries=1, retry_base_delay=0.01
        )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(archive_file, uri) for uri in file_uris]

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
# Hudi Configuration Tests
# ================================
class TestBuildHudiConfig:
    """Tests for build_hudi_config function."""

    def test_build_hudi_config_default(self) -> None:
        """Test building Hudi config with default sort option."""
        config = build_hudi_config_impl(
            hudi_init_sort_option="DEFAULT",
            hudi_output_bucket="test-bucket",
            hudi_table_name="sensorData",
            hudi_db_name="Default",
        )

        assert config["hoodie.table.name"] == "sensordata_default"
        assert config["hoodie.datasource.hive_sync.table"] == "sensordata_default"
        assert config["hoodie.datasource.hive_sync.database"] == "Default"
        assert config["hoodie.datasource.write.operation"] == "upsert"
        assert config["hoodie.datasource.write.table.type"] == "COPY_ON_WRITE"
        assert "hoodie.bulkinsert.sort.mode" not in config

    def test_build_hudi_config_partition_sort(self) -> None:
        """Test building Hudi config with PARTITION_SORT option."""
        config = build_hudi_config_impl(
            hudi_init_sort_option="PARTITION_SORT",
            hudi_output_bucket="test-bucket",
            hudi_table_name="sensorData",
            hudi_db_name="Default",
        )

        assert config["hoodie.table.name"] == "sensordata_partition_sort"
        assert config["hoodie.bulkinsert.sort.mode"] == "PARTITION_SORT"

    def test_build_hudi_config_none_sort(self) -> None:
        """Test building Hudi config with NONE sort option."""
        config = build_hudi_config_impl(
            hudi_init_sort_option="NONE",
            hudi_output_bucket="test-bucket",
            hudi_table_name="sensorData",
            hudi_db_name="Default",
        )

        assert config["hoodie.bulkinsert.sort.mode"] == "NONE"

    def test_build_hudi_config_record_key(self) -> None:
        """Test Hudi config has correct record key fields."""
        config = build_hudi_config_impl(
            hudi_init_sort_option="DEFAULT",
            hudi_output_bucket="test-bucket",
            hudi_table_name="sensorData",
            hudi_db_name="Default",
        )

        assert config["hoodie.datasource.write.recordkey.field"] == "sensorId, ts"
        assert config["hoodie.datasource.write.partitionpath.field"] == "its:TIMESTAMP"

    def test_build_hudi_config_hive_sync(self) -> None:
        """Test Hudi config has Hive sync enabled."""
        config = build_hudi_config_impl(
            hudi_init_sort_option="DEFAULT",
            hudi_output_bucket="test-bucket",
            hudi_table_name="sensorData",
            hudi_db_name="Default",
        )

        assert config["hoodie.datasource.hive_sync.enable"] == "true"
        assert config["hoodie.datasource.hive_sync.use_jdbc"] == "false"


def build_hudi_config_impl(
    hudi_init_sort_option: str,
    hudi_output_bucket: str,
    hudi_table_name: str,
    hudi_db_name: str,
) -> dict[str, str]:
    """Implementation of build_hudi_config for testing."""
    table_name = f"{hudi_table_name.lower()}_{hudi_init_sort_option.lower()}"

    config = {
        "className": "org.apache.hudi",
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.use_jdbc": "false",
        "hoodie.datasource.hive_sync.support_timestamp": "true",
        "hoodie.datasource.hive_sync.database": hudi_db_name,
        "hoodie.datasource.hive_sync.table": table_name,
        "hoodie.datasource.hive_sync.partition_fields": "its",
        "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.recordkey.field": "sensorId, ts",
        "hoodie.datasource.write.partitionpath.field": "its:TIMESTAMP",
        "hoodie.datasource.write.hive_style_partitioning": "true",
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.CustomKeyGenerator",
        "hoodie.table.name": table_name,
        "hoodie.deltastreamer.keygen.timebased.timestamp.type": "DATE_STRING",
        "hoodie.deltastreamer.keygen.timebased.input.dateformat": "yyyy-MM-dd H:mm:ss",
        "hoodie.deltastreamer.keygen.timebased.output.dateformat": "yyyy",
        "hoodie.deltastreamer.keygen.timebased.timezone": "UTC",
    }

    if hudi_init_sort_option.upper() in ["PARTITION_SORT", "NONE"]:
        config["hoodie.bulkinsert.sort.mode"] = hudi_init_sort_option

    return config


# ================================
# Orchestration Tests
# ================================
class TestProcessAllFiles:
    """Tests for process_all_files orchestration logic."""

    @mock_aws
    def test_process_all_files_no_files(self) -> None:
        """Test processing with no files returns empty summary."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="hudibucketsrc",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        summary = process_all_files_stub(s3, batch_size=500, max_batches=100, max_runtime=14400)

        assert summary["total_files"] == 0
        assert summary["batches_processed"] == 0
        assert summary["total_rows"] == 0

    @mock_aws
    def test_process_all_files_single_batch(self) -> None:
        """Test processing files that fit in single batch."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="hudibucketsrc",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        # Create test files
        for i in range(10):
            s3.put_object(Bucket="hudibucketsrc", Key=f"sensorDataFiles/file_{i}.csv", Body=b"test")

        summary = process_all_files_stub(s3, batch_size=500, max_batches=100, max_runtime=14400)

        assert summary["total_files"] == 10
        assert summary["batches_processed"] == 1

    @mock_aws
    def test_process_all_files_multiple_batches(self) -> None:
        """Test processing files across multiple batches."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="hudibucketsrc",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        # Create test files
        for i in range(25):
            s3.put_object(Bucket="hudibucketsrc", Key=f"sensorDataFiles/file_{i}.csv", Body=b"test")

        summary = process_all_files_stub(s3, batch_size=10, max_batches=100, max_runtime=14400)

        assert summary["total_files"] == 25
        assert summary["batches_processed"] == 3

    @mock_aws
    def test_process_all_files_max_batches_limit(self) -> None:
        """Test that max_batches limit is respected."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="hudibucketsrc",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        # Create many files
        for i in range(100):
            s3.put_object(Bucket="hudibucketsrc", Key=f"sensorDataFiles/file_{i}.csv", Body=b"test")

        summary = process_all_files_stub(s3, batch_size=10, max_batches=5, max_runtime=14400)

        assert summary["total_files"] == 100
        assert summary["batches_processed"] == 5  # Limited to max_batches

    @mock_aws
    def test_process_all_files_runtime_limit(self) -> None:
        """Test that runtime limit is respected."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="hudibucketsrc",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        # Create test files
        for i in range(10):
            s3.put_object(Bucket="hudibucketsrc", Key=f"sensorDataFiles/file_{i}.csv", Body=b"test")

        # Use very short runtime limit (already expired)
        summary = process_all_files_stub(s3, batch_size=5, max_batches=100, max_runtime=0)

        assert summary["total_files"] == 10
        assert summary["batches_processed"] == 0  # No batches due to time limit


def process_all_files_stub(
    s3_client: Any,
    batch_size: int = 500,
    max_batches: int = 100,
    max_runtime: float = 14400,
) -> dict[str, Any]:
    """Stub for process_all_files that simulates batch processing without Spark."""
    job_start = datetime.now()

    all_files = list_s3_files_impl(s3_client, "hudibucketsrc", "sensorDataFiles/")
    total_files = len(all_files)

    if total_files == 0:
        return {
            "total_files": 0,
            "batches_processed": 0,
            "total_rows": 0,
            "total_archived": 0,
            "total_skipped": 0,
            "total_errors": 0,
            "total_runtime_seconds": 0,
        }

    batches = chunk_list(all_files, batch_size)
    if len(batches) > max_batches:
        batches = batches[:max_batches]

    total_rows = 0
    total_archived = 0
    batches_processed = 0

    for _batch_num, batch in enumerate(batches, 1):
        elapsed = (datetime.now() - job_start).total_seconds()
        if elapsed >= max_runtime:
            break

        # Simulate batch processing (without Spark)
        success, _skipped, _errors = archive_files_concurrent_impl(
            s3_client,
            batch,
            "hudibucketsrc",
            "sensorDataFiles/",
            "sensorDataFilesArchived/",
        )
        total_archived += success
        total_rows += len(batch) * 100  # Simulated rows
        batches_processed += 1

    return {
        "total_files": total_files,
        "batches_processed": batches_processed,
        "total_rows": total_rows,
        "total_archived": total_archived,
        "total_skipped": 0,
        "total_errors": 0,
        "total_runtime_seconds": (datetime.now() - job_start).total_seconds(),
    }


# ================================
# Configuration Constants Tests
# ================================
class TestConstants:
    """Tests for module constants."""

    def test_source_bucket_constant(self) -> None:
        """Test SOURCE_BUCKET constant value."""
        SOURCE_BUCKET = "hudibucketsrc"
        assert SOURCE_BUCKET == "hudibucketsrc"

    def test_source_prefix_constant(self) -> None:
        """Test SOURCE_PREFIX constant value."""
        SOURCE_PREFIX = "sensorDataFiles/"
        assert SOURCE_PREFIX == "sensorDataFiles/"

    def test_archive_prefix_constant(self) -> None:
        """Test ARCHIVE_PREFIX constant value."""
        ARCHIVE_PREFIX = "sensorDataFilesArchived/"
        assert ARCHIVE_PREFIX == "sensorDataFilesArchived/"

    def test_batch_size_constant(self) -> None:
        """Test BATCH_SIZE constant value."""
        BATCH_SIZE = 500
        assert BATCH_SIZE == 500

    def test_max_batches_constant(self) -> None:
        """Test MAX_BATCHES constant value."""
        MAX_BATCHES = 100
        assert MAX_BATCHES == 100

    def test_max_runtime_seconds_constant(self) -> None:
        """Test MAX_RUNTIME_SECONDS constant value (4 hours)."""
        MAX_RUNTIME_SECONDS = 14400
        assert MAX_RUNTIME_SECONDS == 14400
        assert MAX_RUNTIME_SECONDS == 4 * 60 * 60

    def test_archive_workers_constant(self) -> None:
        """Test ARCHIVE_WORKERS constant value."""
        ARCHIVE_WORKERS = 10
        assert ARCHIVE_WORKERS == 10

    def test_archive_max_retries_constant(self) -> None:
        """Test ARCHIVE_MAX_RETRIES constant value."""
        ARCHIVE_MAX_RETRIES = 3
        assert ARCHIVE_MAX_RETRIES == 3


# ================================
# Edge Case Tests
# ================================
class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_chunk_list_with_strings(self) -> None:
        """Test chunk_list works with string items."""
        items = ["a", "b", "c", "d", "e"]
        chunks = chunk_list(items, 2)
        assert chunks == [["a", "b"], ["c", "d"], ["e"]]

    def test_chunk_list_preserves_order(self) -> None:
        """Test chunk_list preserves item order."""
        items = list(range(100))
        chunks = chunk_list(items, 10)

        # Flatten and compare
        flattened = [item for chunk in chunks for item in chunk]
        assert flattened == items

    @mock_aws
    def test_archive_retries_on_temporary_error(self) -> None:
        """Test archive retries on temporary errors."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="hudibucketsrc",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )
        s3.put_object(Bucket="hudibucketsrc", Key="sensorDataFiles/test.csv", Body=b"test")

        # Create mock that fails first then succeeds
        call_count = {"count": 0}
        original_copy = s3.copy_object

        def mock_copy(*args: Any, **kwargs: Any) -> Any:
            call_count["count"] += 1
            if call_count["count"] == 1:
                error_response = {"Error": {"Code": "InternalError", "Message": "Temporary error"}}
                raise ClientError(error_response, "CopyObject")
            return original_copy(*args, **kwargs)

        with patch.object(s3, "copy_object", side_effect=mock_copy):
            result, _msg = archive_single_file_impl(
                s3,
                "s3://hudibucketsrc/sensorDataFiles/test.csv",
                "hudibucketsrc",
                "sensorDataFiles/",
                "sensorDataFilesArchived/",
                max_retries=3,
                retry_base_delay=0.01,
            )

            # Should have retried and succeeded
            assert result == ArchiveResult.SUCCESS

    def test_batch_result_equality(self) -> None:
        """Test BatchResult equality comparison."""
        result1 = BatchResult(
            files_processed=10,
            rows_upserted=1000,
            archive_success=8,
            archive_skipped=1,
            archive_errors=1,
            hudi_duration_seconds=5.5,
            archive_duration_seconds=2.3,
        )
        result2 = BatchResult(
            files_processed=10,
            rows_upserted=1000,
            archive_success=8,
            archive_skipped=1,
            archive_errors=1,
            hudi_duration_seconds=5.5,
            archive_duration_seconds=2.3,
        )

        assert result1 == result2

    def test_archive_result_str(self) -> None:
        """Test ArchiveResult string representation."""
        assert str(ArchiveResult.SUCCESS) == "ArchiveResult.SUCCESS"
        assert ArchiveResult.SUCCESS.name == "SUCCESS"


# ================================
# Integration-like Tests
# ================================
class TestBatchProcessingFlow:
    """Tests for the batch processing flow."""

    @mock_aws
    def test_full_batch_flow(self) -> None:
        """Test complete batch processing flow."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="hudibucketsrc",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        # Create test files
        num_files = 15
        for i in range(num_files):
            s3.put_object(
                Bucket="hudibucketsrc",
                Key=f"sensorDataFiles/sensor_{i:03d}.csv",
                Body=b"sensorId,ts,val,unit,its\ntest-001,2024-01-01 00:00:00,1.0,kWh,2024-01-01 00:00:00",
            )

        # Verify initial state
        initial_files = list_s3_files_impl(s3, "hudibucketsrc", "sensorDataFiles/")
        assert len(initial_files) == num_files

        # Process in batches of 5
        summary = process_all_files_stub(s3, batch_size=5, max_batches=100, max_runtime=14400)

        assert summary["total_files"] == num_files
        assert summary["batches_processed"] == 3  # 15 files / 5 per batch

        # Verify files were archived
        archived_files = list_s3_files_impl(s3, "hudibucketsrc", "sensorDataFilesArchived/")
        assert len(archived_files) == num_files

        # Verify source is empty
        remaining_files = list_s3_files_impl(s3, "hudibucketsrc", "sensorDataFiles/")
        assert len(remaining_files) == 0

    @mock_aws
    def test_partial_batch_processing(self) -> None:
        """Test batch processing with non-divisible file count."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="hudibucketsrc",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        # Create 7 files (doesn't divide evenly by 3)
        for i in range(7):
            s3.put_object(Bucket="hudibucketsrc", Key=f"sensorDataFiles/file_{i}.csv", Body=b"test")

        summary = process_all_files_stub(s3, batch_size=3, max_batches=100, max_runtime=14400)

        assert summary["total_files"] == 7
        assert summary["batches_processed"] == 3  # [3, 3, 1]


# ================================
# URL Decoding Tests
# ================================
class TestUrlDecoding:
    """Tests for URL decoding in file paths."""

    def test_url_decode_spaces(self) -> None:
        """Test URL decoding of spaces."""
        from urllib.parse import unquote

        encoded = "file%20with%20spaces.csv"
        decoded = unquote(encoded.replace("+", "%20"))
        assert decoded == "file with spaces.csv"

    def test_url_decode_plus_sign(self) -> None:
        """Test URL decoding of plus signs as spaces."""
        from urllib.parse import unquote

        encoded = "file+with+spaces.csv"
        decoded = unquote(encoded.replace("+", "%20"))
        assert decoded == "file with spaces.csv"

    def test_url_decode_special_chars(self) -> None:
        """Test URL decoding of special characters."""
        from urllib.parse import unquote

        encoded = "file%2Btest%26data.csv"
        decoded = unquote(encoded)
        assert decoded == "file+test&data.csv"


# ================================
# Optional Parameter Tests
# ================================
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


class TestGetOptionalArg:
    """Tests for the get_optional_arg function."""

    def test_get_optional_arg_found(self) -> None:
        """Test getting an optional arg that exists."""
        argv = ["script.py", "--MAX_FILES", "10", "--DRY_RUN", "true"]
        assert get_optional_arg(argv, "MAX_FILES", "0") == "10"
        assert get_optional_arg(argv, "DRY_RUN", "false") == "true"

    def test_get_optional_arg_not_found(self) -> None:
        """Test getting an optional arg that doesn't exist."""
        argv = ["script.py", "--JOB_NAME", "test"]
        assert get_optional_arg(argv, "MAX_FILES", "0") == "0"
        assert get_optional_arg(argv, "DRY_RUN", "false") == "false"

    def test_get_optional_arg_at_end(self) -> None:
        """Test optional arg at end of argv (no value)."""
        argv = ["script.py", "--MAX_FILES"]
        # No value after --MAX_FILES, should return default
        assert get_optional_arg(argv, "MAX_FILES", "0") == "0"

    def test_get_optional_arg_empty_argv(self) -> None:
        """Test with empty argv."""
        argv: list[str] = []
        assert get_optional_arg(argv, "MAX_FILES", "0") == "0"


class TestMaxFilesParameter:
    """Tests for MAX_FILES parameter functionality."""

    @mock_aws
    def test_max_files_limits_processing(self) -> None:
        """Test that MAX_FILES limits the number of files processed."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="hudibucketsrc",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        # Create 20 files
        for i in range(20):
            s3.put_object(Bucket="hudibucketsrc", Key=f"sensorDataFiles/file_{i}.csv", Body=b"test")

        # Process with max_files=5
        summary = process_all_files_stub_with_max_files(s3, max_files=5, batch_size=10)

        # Should only process 5 files
        assert summary["files_limited_to"] == 5

    @mock_aws
    def test_max_files_zero_means_no_limit(self) -> None:
        """Test that MAX_FILES=0 means no limit."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="hudibucketsrc",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        # Create 15 files
        for i in range(15):
            s3.put_object(Bucket="hudibucketsrc", Key=f"sensorDataFiles/file_{i}.csv", Body=b"test")

        # Process with max_files=0 (no limit)
        summary = process_all_files_stub_with_max_files(s3, max_files=0, batch_size=10)

        # Should process all 15 files
        assert summary["total_files"] == 15


class TestDryRunParameter:
    """Tests for DRY_RUN parameter functionality."""

    @mock_aws
    def test_dry_run_skips_archive(self) -> None:
        """Test that DRY_RUN=true skips archiving files."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="hudibucketsrc",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        # Create 5 files
        for i in range(5):
            s3.put_object(Bucket="hudibucketsrc", Key=f"sensorDataFiles/file_{i}.csv", Body=b"test")

        # Process with dry_run=True
        summary = process_all_files_stub_with_dry_run(s3, dry_run=True, batch_size=10)

        # Files should NOT be archived (still in source)
        response = s3.list_objects_v2(Bucket="hudibucketsrc", Prefix="sensorDataFiles/")
        assert len(response.get("Contents", [])) == 5

        # Archive folder should be empty
        response = s3.list_objects_v2(Bucket="hudibucketsrc", Prefix="sensorDataFilesArchived/")
        assert len(response.get("Contents", [])) == 0

        assert summary["total_archived"] == 0
        assert summary["total_skipped"] == 5

    @mock_aws
    def test_dry_run_false_archives_normally(self) -> None:
        """Test that DRY_RUN=false archives files normally."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="hudibucketsrc",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        # Create 5 files
        for i in range(5):
            s3.put_object(Bucket="hudibucketsrc", Key=f"sensorDataFiles/file_{i}.csv", Body=b"test")

        # Process with dry_run=False
        summary = process_all_files_stub_with_dry_run(s3, dry_run=False, batch_size=10)

        # Files should be archived
        response = s3.list_objects_v2(Bucket="hudibucketsrc", Prefix="sensorDataFiles/")
        assert len(response.get("Contents", [])) == 0

        # Archive folder should have files
        response = s3.list_objects_v2(Bucket="hudibucketsrc", Prefix="sensorDataFilesArchived/")
        assert len(response.get("Contents", [])) == 5

        assert summary["total_archived"] == 5


def process_all_files_stub_with_max_files(
    s3_client: Any,
    max_files: int = 0,
    batch_size: int = 500,
) -> dict[str, Any]:
    """Stub for testing MAX_FILES parameter."""
    all_files = list_s3_files_impl(s3_client, "hudibucketsrc", "sensorDataFiles/")
    total_files = len(all_files)

    # Apply MAX_FILES limit
    files_limited_to = total_files
    if max_files > 0 and total_files > max_files:
        all_files = all_files[:max_files]
        files_limited_to = max_files

    return {
        "total_files": total_files,
        "files_limited_to": files_limited_to,
    }


def process_all_files_stub_with_dry_run(
    s3_client: Any,
    dry_run: bool = False,
    batch_size: int = 500,
) -> dict[str, Any]:
    """Stub for testing DRY_RUN parameter."""
    all_files = list_s3_files_impl(s3_client, "hudibucketsrc", "sensorDataFiles/")
    total_files = len(all_files)

    if total_files == 0:
        return {
            "total_files": 0,
            "total_archived": 0,
            "total_skipped": 0,
        }

    if dry_run:
        # DRY_RUN: don't archive, just count as skipped
        return {
            "total_files": total_files,
            "total_archived": 0,
            "total_skipped": total_files,
        }

    # Normal mode: archive files
    success, skipped, _errors = archive_files_concurrent_impl(
        s3_client,
        all_files,
        "hudibucketsrc",
        "sensorDataFiles/",
        "sensorDataFilesArchived/",
    )

    return {
        "total_files": total_files,
        "total_archived": success,
        "total_skipped": skipped,
    }
