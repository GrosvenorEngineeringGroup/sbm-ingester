"""Shared pytest fixtures for SBM Ingester tests."""

import json
import sys
import tempfile
from collections.abc import Generator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import boto3
import pandas as pd
import pytest
from moto import mock_aws
from mypy_boto3_logs import CloudWatchLogsClient
from mypy_boto3_s3 import S3Client, S3ServiceResource

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / ".." / "src"))


# ==================== Paths ====================


@pytest.fixture
def fixtures_dir() -> str:
    """Return path to test fixtures directory."""
    return str(Path(__file__).parent / "fixtures")


@pytest.fixture
def nem12_sample_file(fixtures_dir: str) -> str:
    """Return path to NEM12 sample file."""
    return str(Path(fixtures_dir) / "nem12_sample.csv")


@pytest.fixture
def nem13_sample_file(fixtures_dir: str) -> str:
    """Return path to NEM13 sample file."""
    return str(Path(fixtures_dir) / "nem13_sample.csv")


@pytest.fixture
def nem12_multiple_meters_file(fixtures_dir: str) -> str:
    """Return path to NEM12 multiple meters sample file."""
    return str(Path(fixtures_dir) / "nem12_multiple_meters.csv")


@pytest.fixture
def temp_directory() -> Generator[str]:
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


# ==================== AWS Mocks ====================


@pytest.fixture
def aws_credentials() -> None:
    """Mock AWS credentials for moto."""
    import os

    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"


@pytest.fixture
def mock_s3(aws_credentials: None) -> Generator[S3Client]:
    """Create mock S3 service with required buckets."""
    with mock_aws():
        s3 = boto3.client("s3", region_name="ap-southeast-2")

        # Create required buckets
        s3.create_bucket(Bucket="sbm-file-ingester", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"})
        s3.create_bucket(Bucket="hudibucketsrc", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"})

        yield s3


@pytest.fixture
def mock_s3_resource(aws_credentials: None) -> Generator[S3ServiceResource]:
    """Create mock S3 resource with required buckets."""
    with mock_aws():
        s3_resource = boto3.resource("s3", region_name="ap-southeast-2")

        # Create required buckets
        s3_resource.create_bucket(
            Bucket="sbm-file-ingester", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"}
        )
        s3_resource.create_bucket(
            Bucket="hudibucketsrc", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"}
        )

        yield s3_resource


@pytest.fixture
def mock_cloudwatch_logs(aws_credentials: None) -> Generator[CloudWatchLogsClient]:
    """Create mock CloudWatch Logs service."""
    with mock_aws():
        logs = boto3.client("logs", region_name="ap-southeast-2")

        # Create required log groups
        log_groups = [
            "sbm-ingester-parse-error-log",
            "sbm-ingester-runtime-error-log",
            "sbm-ingester-error-log",
            "sbm-ingester-execution-log",
            "sbm-ingester-metrics-log",
        ]
        for log_group in log_groups:
            logs.create_log_group(logGroupName=log_group)

        yield logs


# ==================== Sample Data ====================


@pytest.fixture
def sample_nem12_mappings() -> dict[str, str]:
    """Sample NEM12 ID to Neptune ID mappings."""
    return {
        "NEM1234567890-E1": "neptune-id-001",
        "NEM1234567890-B1": "neptune-id-002",
        "NEM0987654321-E1": "neptune-id-003",
        "Envizi_12345-E1": "neptune-id-004",
        "Optima_6102395013-E1": "neptune-id-005",
    }


@pytest.fixture
def sample_sqs_event() -> dict[str, Any]:
    """Sample SQS event with S3 notification."""
    return {
        "Records": [
            {
                "body": json.dumps(
                    {
                        "Records": [
                            {"s3": {"bucket": {"name": "sbm-file-ingester"}, "object": {"key": "newTBP/test_file.csv"}}}
                        ]
                    }
                )
            }
        ]
    }


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """Sample DataFrame for testing batch writes."""
    from datetime import datetime, timedelta

    base_time = datetime(2024, 1, 1, 0, 0, 0)
    times = [base_time + timedelta(minutes=30 * i) for i in range(48)]

    return pd.DataFrame(
        {
            "sensorId": ["neptune-id-001"] * 48,
            "ts": [t.strftime("%Y-%m-%d %H:%M:%S") for t in times],
            "val": [i * 0.5 for i in range(48)],
            "unit": ["kwh"] * 48,
            "its": [t.strftime("%Y-%m-%d %H:%M:%S") for t in times],
        }
    )


# ==================== Mock CloudWatchLogger ====================


@pytest.fixture
def mock_cloudwatch_logger() -> Generator[MagicMock]:
    """Mock CloudWatchLogger to avoid actual AWS calls."""
    with patch("modules.common.CloudWatchLogger") as MockLogger:
        mock_instance = MagicMock()
        MockLogger.return_value = mock_instance
        yield mock_instance


# ==================== Test Data Generators ====================


def create_envizi_water_csv(filepath: str, serial_numbers: list[str] | None = None, rows_per_meter: int = 10) -> str:
    """Create a sample Envizi water CSV file."""
    from datetime import datetime, timedelta

    if serial_numbers is None:
        serial_numbers = ["12345", "67890"]

    data = []
    base_time = datetime(2024, 1, 1, 0, 0, 0)

    for serial in serial_numbers:
        for i in range(rows_per_meter):
            data.append(
                {
                    "Serial_No": serial,
                    "Interval_Start": (base_time + timedelta(hours=i)).isoformat(),
                    "Interval_End": (base_time + timedelta(hours=i + 1)).isoformat(),
                    "Consumption": i * 0.1,
                    "Consumption Unit": "kL",
                }
            )

    df = pd.DataFrame(data)
    df.to_csv(filepath, index=False)
    return filepath


def create_envizi_electricity_csv(
    filepath: str, serial_numbers: list[str] | None = None, rows_per_meter: int = 10
) -> str:
    """Create a sample Envizi electricity CSV file."""
    from datetime import datetime, timedelta

    if serial_numbers is None:
        serial_numbers = ["E12345", "E67890"]

    data = []
    base_time = datetime(2024, 1, 1, 0, 0, 0)

    for serial in serial_numbers:
        for i in range(rows_per_meter):
            data.append(
                {
                    "Serial_No": serial,
                    "Interval_Start": (base_time + timedelta(hours=i)).isoformat(),
                    "Interval_End": (base_time + timedelta(hours=i + 1)).isoformat(),
                    "kWh": i * 0.5,
                }
            )

    df = pd.DataFrame(data)
    df.to_csv(filepath, index=False)
    return filepath


def create_optima_generation_csv(filepath: str, identifiers: list[str] | None = None, rows_per_id: int = 10) -> str:
    """Create a sample Optima generation data CSV file."""
    from datetime import datetime, timedelta

    if identifiers is None:
        identifiers = ["SOLAR001", "SOLAR002"]

    data = []
    base_time = datetime(2024, 1, 1, 0, 0, 0)

    for identifier in identifiers:
        for i in range(rows_per_id):
            t = base_time + timedelta(hours=i)
            data.append(
                {
                    "Identifier": identifier,
                    "Date": t.strftime("%Y-%m-%d"),
                    "Start Time": t.strftime("%H:%M"),
                    "Generation": i * 0.3,
                }
            )

    df = pd.DataFrame(data)
    df.to_csv(filepath, index=False)
    return filepath
