"""Unit tests for Glue Trigger Lambda.

Tests the Lambda function that checks S3 file count and triggers Glue job.
"""

import os
from unittest.mock import MagicMock, patch

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws


# ================================
# Test Fixtures
# ================================
@pytest.fixture(autouse=True)
def reset_env() -> None:
    """Reset environment variables before each test."""
    os.environ["BUCKET_NAME"] = "test-bucket"
    os.environ["PREFIX"] = "sensorDataFiles/"
    os.environ["FILES_THRESHOLD"] = "2"
    os.environ["GLUE_JOB_NAME"] = "TestGlueJob"
    os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["POWERTOOLS_TRACE_DISABLED"] = "true"
    os.environ["POWERTOOLS_METRICS_NAMESPACE"] = "test"


@pytest.fixture
def mock_context() -> MagicMock:
    """Create a mock Lambda context."""
    context = MagicMock()
    context.function_name = "test-glue-trigger"
    context.memory_limit_in_mb = 128
    context.invoked_function_arn = "arn:aws:lambda:ap-southeast-2:123456789012:function:test"
    context.aws_request_id = "test-request-id"
    return context


# ================================
# count_files_in_prefix Tests
# ================================
class TestCountFilesInPrefix:
    """Tests for count_files_in_prefix function."""

    @mock_aws
    def test_count_files_empty_bucket(self) -> None:
        """Test counting files in empty bucket returns 0."""
        from src.functions.glue_trigger.app import count_files_in_prefix

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="test-bucket",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        count = count_files_in_prefix(s3, "test-bucket", "sensorDataFiles/")
        assert count == 0

    @mock_aws
    def test_count_files_with_files(self) -> None:
        """Test counting files returns correct count."""
        from src.functions.glue_trigger.app import count_files_in_prefix

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="test-bucket",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        # Create test files
        for i in range(5):
            s3.put_object(Bucket="test-bucket", Key=f"sensorDataFiles/file_{i}.csv", Body=b"test")

        count = count_files_in_prefix(s3, "test-bucket", "sensorDataFiles/")
        assert count == 5

    @mock_aws
    def test_count_files_excludes_directory_markers(self) -> None:
        """Test that directory markers are not counted."""
        from src.functions.glue_trigger.app import count_files_in_prefix

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="test-bucket",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        # Create directory marker and files
        s3.put_object(Bucket="test-bucket", Key="sensorDataFiles/", Body=b"")
        s3.put_object(Bucket="test-bucket", Key="sensorDataFiles/subdir/", Body=b"")
        s3.put_object(Bucket="test-bucket", Key="sensorDataFiles/file1.csv", Body=b"test")
        s3.put_object(Bucket="test-bucket", Key="sensorDataFiles/file2.csv", Body=b"test")

        count = count_files_in_prefix(s3, "test-bucket", "sensorDataFiles/")
        assert count == 2

    @mock_aws
    def test_count_files_only_in_prefix(self) -> None:
        """Test that only files in the specified prefix are counted."""
        from src.functions.glue_trigger.app import count_files_in_prefix

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="test-bucket",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        # Create files in different prefixes
        s3.put_object(Bucket="test-bucket", Key="sensorDataFiles/file1.csv", Body=b"test")
        s3.put_object(Bucket="test-bucket", Key="otherPrefix/file2.csv", Body=b"test")
        s3.put_object(Bucket="test-bucket", Key="file3.csv", Body=b"test")

        count = count_files_in_prefix(s3, "test-bucket", "sensorDataFiles/")
        assert count == 1


# ================================
# start_glue_job Tests
# ================================
class TestStartGlueJob:
    """Tests for start_glue_job function."""

    def test_start_glue_job_success(self) -> None:
        """Test successful Glue job start."""
        from src.functions.glue_trigger.app import start_glue_job

        glue = MagicMock()
        glue.start_job_run.return_value = {"JobRunId": "jr_123456"}

        result = start_glue_job(glue, "TestJob")

        assert result["started"] is True
        assert result["job_run_id"] == "jr_123456"
        glue.start_job_run.assert_called_once_with(JobName="TestJob")

    def test_start_glue_job_concurrent_runs_exceeded(self) -> None:
        """Test handling when Glue job is already running."""
        from src.functions.glue_trigger.app import start_glue_job

        glue = MagicMock()
        error_response = {
            "Error": {
                "Code": "ConcurrentRunsExceededException",
                "Message": "Max concurrent runs exceeded",
            }
        }
        glue.start_job_run.side_effect = ClientError(error_response, "StartJobRun")

        result = start_glue_job(glue, "TestJob")

        assert result["started"] is False
        assert result["reason"] == "already_running"

    def test_start_glue_job_other_error_raises(self) -> None:
        """Test that other ClientErrors are re-raised."""
        from src.functions.glue_trigger.app import start_glue_job

        glue = MagicMock()
        error_response = {
            "Error": {
                "Code": "EntityNotFoundException",
                "Message": "Job not found",
            }
        }
        glue.start_job_run.side_effect = ClientError(error_response, "StartJobRun")

        with pytest.raises(ClientError) as exc_info:
            start_glue_job(glue, "NonExistentJob")

        assert exc_info.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_start_glue_job_access_denied_raises(self) -> None:
        """Test that access denied errors are re-raised."""
        from src.functions.glue_trigger.app import start_glue_job

        glue = MagicMock()
        error_response = {
            "Error": {
                "Code": "AccessDeniedException",
                "Message": "Access denied",
            }
        }
        glue.start_job_run.side_effect = ClientError(error_response, "StartJobRun")

        with pytest.raises(ClientError) as exc_info:
            start_glue_job(glue, "TestJob")

        assert exc_info.value.response["Error"]["Code"] == "AccessDeniedException"


# ================================
# lambda_handler Tests
# ================================
class TestLambdaHandler:
    """Tests for lambda_handler function."""

    @mock_aws
    def test_handler_triggers_when_above_threshold(self, mock_context: MagicMock) -> None:
        """Test that Glue job is triggered when file count >= threshold."""
        from src.functions.glue_trigger.app import lambda_handler

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="test-bucket",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        # Create files above threshold (2)
        for i in range(3):
            s3.put_object(Bucket="test-bucket", Key=f"sensorDataFiles/file_{i}.csv", Body=b"test")

        with patch("src.functions.glue_trigger.app.boto3") as mock_boto3:
            mock_s3 = MagicMock()
            mock_glue = MagicMock()
            mock_boto3.client.side_effect = lambda service: mock_s3 if service == "s3" else mock_glue

            # Mock S3 response
            mock_s3.list_objects_v2.return_value = {
                "Contents": [
                    {"Key": "sensorDataFiles/file_0.csv"},
                    {"Key": "sensorDataFiles/file_1.csv"},
                    {"Key": "sensorDataFiles/file_2.csv"},
                ]
            }

            # Mock Glue response
            mock_glue.start_job_run.return_value = {"JobRunId": "jr_test123"}

            result = lambda_handler({}, mock_context)

            assert result["triggered"] is True
            assert result["file_count"] == 3
            assert result["job_run_id"] == "jr_test123"
            mock_glue.start_job_run.assert_called_once_with(JobName="TestGlueJob")

    @mock_aws
    def test_handler_skips_when_below_threshold(self, mock_context: MagicMock) -> None:
        """Test that Glue job is NOT triggered when file count < threshold."""
        from src.functions.glue_trigger.app import lambda_handler

        with patch("src.functions.glue_trigger.app.boto3") as mock_boto3:
            mock_s3 = MagicMock()
            mock_glue = MagicMock()
            mock_boto3.client.side_effect = lambda service: mock_s3 if service == "s3" else mock_glue

            # Mock S3 response with 1 file (below threshold of 2)
            mock_s3.list_objects_v2.return_value = {"Contents": [{"Key": "sensorDataFiles/file_0.csv"}]}

            result = lambda_handler({}, mock_context)

            assert result["triggered"] is False
            assert result["file_count"] == 1
            assert result["reason"] == "below_threshold"
            mock_glue.start_job_run.assert_not_called()

    @mock_aws
    def test_handler_skips_when_empty(self, mock_context: MagicMock) -> None:
        """Test that Glue job is NOT triggered when no files exist."""
        from src.functions.glue_trigger.app import lambda_handler

        with patch("src.functions.glue_trigger.app.boto3") as mock_boto3:
            mock_s3 = MagicMock()
            mock_glue = MagicMock()
            mock_boto3.client.side_effect = lambda service: mock_s3 if service == "s3" else mock_glue

            # Mock empty S3 response
            mock_s3.list_objects_v2.return_value = {}

            result = lambda_handler({}, mock_context)

            assert result["triggered"] is False
            assert result["file_count"] == 0
            assert result["reason"] == "below_threshold"
            mock_glue.start_job_run.assert_not_called()

    @mock_aws
    def test_handler_handles_already_running(self, mock_context: MagicMock) -> None:
        """Test that handler gracefully handles when Glue job is already running."""
        from src.functions.glue_trigger.app import lambda_handler

        with patch("src.functions.glue_trigger.app.boto3") as mock_boto3:
            mock_s3 = MagicMock()
            mock_glue = MagicMock()
            mock_boto3.client.side_effect = lambda service: mock_s3 if service == "s3" else mock_glue

            # Mock S3 response
            mock_s3.list_objects_v2.return_value = {
                "Contents": [
                    {"Key": "sensorDataFiles/file_0.csv"},
                    {"Key": "sensorDataFiles/file_1.csv"},
                ]
            }

            # Mock Glue concurrent runs exceeded
            error_response = {
                "Error": {
                    "Code": "ConcurrentRunsExceededException",
                    "Message": "Max concurrent runs exceeded",
                }
            }
            mock_glue.start_job_run.side_effect = ClientError(error_response, "StartJobRun")

            result = lambda_handler({}, mock_context)

            assert result["triggered"] is False
            assert result["file_count"] == 2
            assert result["reason"] == "already_running"

    @mock_aws
    def test_handler_exact_threshold(self, mock_context: MagicMock) -> None:
        """Test behavior when file count equals threshold exactly."""
        from src.functions.glue_trigger.app import lambda_handler

        with patch("src.functions.glue_trigger.app.boto3") as mock_boto3:
            mock_s3 = MagicMock()
            mock_glue = MagicMock()
            mock_boto3.client.side_effect = lambda service: mock_s3 if service == "s3" else mock_glue

            # Mock S3 response with exactly 2 files (equals threshold)
            mock_s3.list_objects_v2.return_value = {
                "Contents": [
                    {"Key": "sensorDataFiles/file_0.csv"},
                    {"Key": "sensorDataFiles/file_1.csv"},
                ]
            }
            mock_glue.start_job_run.return_value = {"JobRunId": "jr_exact"}

            result = lambda_handler({}, mock_context)

            assert result["triggered"] is True
            assert result["file_count"] == 2
            mock_glue.start_job_run.assert_called_once()


# ================================
# Configuration Tests
# ================================
class TestConfiguration:
    """Tests for configuration and environment variables."""

    def test_default_bucket_name(self) -> None:
        """Test default bucket name value."""
        os.environ.pop("BUCKET_NAME", None)
        # Need to reimport to get default value
        import importlib

        import src.functions.glue_trigger.app as app_module

        importlib.reload(app_module)

        assert app_module.BUCKET_NAME == "hudibucketsrc"

    def test_default_prefix(self) -> None:
        """Test default prefix value."""
        os.environ.pop("PREFIX", None)
        import importlib

        import src.functions.glue_trigger.app as app_module

        importlib.reload(app_module)

        assert app_module.PREFIX == "sensorDataFiles/"

    def test_default_threshold(self) -> None:
        """Test default files threshold value."""
        os.environ.pop("FILES_THRESHOLD", None)
        import importlib

        import src.functions.glue_trigger.app as app_module

        importlib.reload(app_module)

        assert app_module.FILES_THRESHOLD == 2

    def test_default_glue_job_name(self) -> None:
        """Test default Glue job name value."""
        os.environ.pop("GLUE_JOB_NAME", None)
        import importlib

        import src.functions.glue_trigger.app as app_module

        importlib.reload(app_module)

        assert app_module.GLUE_JOB_NAME == "DataImportIntoLake"

    def test_custom_threshold(self) -> None:
        """Test custom files threshold from environment."""
        os.environ["FILES_THRESHOLD"] = "10"
        import importlib

        import src.functions.glue_trigger.app as app_module

        importlib.reload(app_module)

        assert app_module.FILES_THRESHOLD == 10


# ================================
# Edge Cases
# ================================
class TestEdgeCases:
    """Tests for edge cases and error handling."""

    @mock_aws
    def test_handler_with_only_directory_markers(self, mock_context: MagicMock) -> None:
        """Test when bucket only contains directory markers."""
        from src.functions.glue_trigger.app import lambda_handler

        with patch("src.functions.glue_trigger.app.boto3") as mock_boto3:
            mock_s3 = MagicMock()
            mock_glue = MagicMock()
            mock_boto3.client.side_effect = lambda service: mock_s3 if service == "s3" else mock_glue

            # Mock S3 response with only directory markers
            mock_s3.list_objects_v2.return_value = {
                "Contents": [
                    {"Key": "sensorDataFiles/"},
                    {"Key": "sensorDataFiles/subdir/"},
                ]
            }

            result = lambda_handler({}, mock_context)

            assert result["triggered"] is False
            assert result["file_count"] == 0
            assert result["reason"] == "below_threshold"

    def test_count_files_handles_no_contents_key(self) -> None:
        """Test count_files_in_prefix handles missing Contents key."""
        from src.functions.glue_trigger.app import count_files_in_prefix

        s3 = MagicMock()
        s3.list_objects_v2.return_value = {}  # No Contents key

        count = count_files_in_prefix(s3, "test-bucket", "prefix/")
        assert count == 0

    @mock_aws
    def test_handler_glue_error_propagates(self, mock_context: MagicMock) -> None:
        """Test that non-concurrent Glue errors propagate."""
        # Reload module to ensure threshold is reset from fixture
        import importlib

        import src.functions.glue_trigger.app as app_module

        importlib.reload(app_module)
        from src.functions.glue_trigger.app import lambda_handler

        with patch("src.functions.glue_trigger.app.boto3") as mock_boto3:
            mock_s3 = MagicMock()
            mock_glue = MagicMock()
            mock_boto3.client.side_effect = lambda service: mock_s3 if service == "s3" else mock_glue

            mock_s3.list_objects_v2.return_value = {
                "Contents": [
                    {"Key": "sensorDataFiles/file_0.csv"},
                    {"Key": "sensorDataFiles/file_1.csv"},
                ]
            }

            error_response = {
                "Error": {
                    "Code": "InternalServiceException",
                    "Message": "Internal error",
                }
            }
            mock_glue.start_job_run.side_effect = ClientError(error_response, "StartJobRun")

            with pytest.raises(ClientError) as exc_info:
                lambda_handler({}, mock_context)

            assert exc_info.value.response["Error"]["Code"] == "InternalServiceException"

    def test_start_glue_job_empty_response(self) -> None:
        """Test start_glue_job handles empty response."""
        from src.functions.glue_trigger.app import start_glue_job

        glue = MagicMock()
        glue.start_job_run.return_value = {}  # No JobRunId

        result = start_glue_job(glue, "TestJob")

        assert result["started"] is True
        assert result["job_run_id"] is None


# ================================
# Integration-like Tests
# ================================
class TestIntegration:
    """Integration-like tests for the complete flow."""

    @mock_aws
    def test_full_trigger_flow(self, mock_context: MagicMock) -> None:
        """Test complete flow from S3 check to Glue trigger."""
        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="test-bucket",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        # Create test files
        for i in range(5):
            s3.put_object(
                Bucket="test-bucket",
                Key=f"sensorDataFiles/sensor_{i:03d}.csv",
                Body=b"sensorId,ts,val,unit,its\ntest,2024-01-01,1.0,kWh,2024",
            )

        # Import and test count function with real moto S3
        from src.functions.glue_trigger.app import count_files_in_prefix

        count = count_files_in_prefix(s3, "test-bucket", "sensorDataFiles/")
        assert count == 5

    @mock_aws
    def test_mixed_files_and_directories(self, mock_context: MagicMock) -> None:
        """Test counting with mixed files and directory markers."""
        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="test-bucket",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        # Create mix of files and directory markers
        s3.put_object(Bucket="test-bucket", Key="sensorDataFiles/", Body=b"")
        s3.put_object(Bucket="test-bucket", Key="sensorDataFiles/2024/", Body=b"")
        s3.put_object(Bucket="test-bucket", Key="sensorDataFiles/2024/01/", Body=b"")
        s3.put_object(Bucket="test-bucket", Key="sensorDataFiles/file1.csv", Body=b"data")
        s3.put_object(Bucket="test-bucket", Key="sensorDataFiles/2024/file2.csv", Body=b"data")
        s3.put_object(Bucket="test-bucket", Key="sensorDataFiles/2024/01/file3.csv", Body=b"data")

        from src.functions.glue_trigger.app import count_files_in_prefix

        count = count_files_in_prefix(s3, "test-bucket", "sensorDataFiles/")
        assert count == 3  # Only actual files, not directory markers
