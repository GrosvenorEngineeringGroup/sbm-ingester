"""Integration tests for SBM Ingester pipeline."""

import json
import os
import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import boto3
from moto import mock_aws

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

# Import app module early before any Logger patches
import functions.file_processor.app as file_processor_app


class TestLambdaHandler:
    """Tests for lambda_handler entry point."""

    @mock_aws
    def test_lambda_handler_processes_sqs_event(self) -> None:
        """Test that lambda_handler processes SQS event correctly."""
        # Setup mock AWS services
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(Bucket="sbm-file-ingester", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"})
        s3.create_bucket(Bucket="hudibucketsrc", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"})

        # Create mock CloudWatch logs
        logs = boto3.client("logs", region_name="ap-southeast-2")
        for log_group in [
            "sbm-ingester-error-log",
            "sbm-ingester-execution-log",
            "sbm-ingester-metrics-log",
            "sbm-ingester-parse-error-log",
            "sbm-ingester-runtime-error-log",
        ]:
            logs.create_log_group(logGroupName=log_group)

        # Upload NEM12 mappings
        mappings = {"test-nmi-E1": "neptune-id-001"}
        s3.put_object(Bucket="sbm-file-ingester", Key="nem12_mappings.json", Body=json.dumps(mappings))

        # Create SQS event
        sqs_event = {
            "Records": [
                {
                    "body": json.dumps(
                        {
                            "Records": [
                                {
                                    "s3": {
                                        "bucket": {"name": "sbm-file-ingester"},
                                        "object": {"key": "newTBP/test_file.csv"},
                                    }
                                }
                            ]
                        }
                    )
                }
            ]
        }

        # Import and call lambda handler
        # Note: This will fail because test file doesn't exist in S3
        # But it tests the event parsing logic
        # Create mock context for Powertools
        mock_context = MagicMock()
        mock_context.function_name = "test-function"
        mock_context.memory_limit_in_mb = 128
        mock_context.invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:test-function"
        mock_context.aws_request_id = "test-request-id"

        with patch.object(file_processor_app, "parse_and_write_data"):
            result = file_processor_app.lambda_handler(sqs_event, mock_context)

            assert result["statusCode"] == 200

    def test_lambda_handler_returns_success_response(self, sample_sqs_event: dict[str, Any]) -> None:
        """Test that lambda_handler returns success response structure."""
        # Create mock context for Powertools
        mock_context = MagicMock()
        mock_context.function_name = "test-function"
        mock_context.memory_limit_in_mb = 128
        mock_context.invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:test-function"
        mock_context.aws_request_id = "test-request-id"

        with patch.object(file_processor_app, "parse_and_write_data"):
            result = file_processor_app.lambda_handler(sample_sqs_event, mock_context)

            assert "statusCode" in result
            assert "body" in result
            assert result["statusCode"] == 200


class TestFileMovement:
    """Tests for S3 file movement after processing."""

    @mock_aws
    def test_move_s3_file_to_processed(self) -> None:
        """Test that successfully processed files are moved to newP/."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3_resource = boto3.resource("s3", region_name="ap-southeast-2")
        s3_resource.create_bucket(
            Bucket="sbm-file-ingester", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"}
        )

        # Create source file
        bucket = s3_resource.Bucket("sbm-file-ingester")
        bucket.put_object(Key="newTBP/test_file.csv", Body=b"test content")

        with patch("aws_lambda_powertools.Logger"), patch("functions.file_processor.app.s3_resource", s3_resource):
            from functions.file_processor.app import move_s3_file

            move_s3_file("sbm-file-ingester", "newTBP/test_file.csv", "newP/")

            # Verify file moved
            objects_in_newp = list(bucket.objects.filter(Prefix="newP/"))
            objects_in_newtbp = list(bucket.objects.filter(Prefix="newTBP/"))

            assert len(objects_in_newp) == 1
            assert objects_in_newp[0].key == "newP/test_file.csv"
            assert len(objects_in_newtbp) == 0

    @mock_aws
    def test_move_s3_file_to_parse_error(self) -> None:
        """Test that parse error files are moved to newParseErr/."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3_resource = boto3.resource("s3", region_name="ap-southeast-2")
        s3_resource.create_bucket(
            Bucket="sbm-file-ingester", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"}
        )

        bucket = s3_resource.Bucket("sbm-file-ingester")
        bucket.put_object(Key="newTBP/bad_file.csv", Body=b"invalid content")

        with patch("aws_lambda_powertools.Logger"), patch("functions.file_processor.app.s3_resource", s3_resource):
            from functions.file_processor.app import move_s3_file

            move_s3_file("sbm-file-ingester", "newTBP/bad_file.csv", "newParseErr/")

            objects_in_err = list(bucket.objects.filter(Prefix="newParseErr/"))
            assert len(objects_in_err) == 1
            assert objects_in_err[0].key == "newParseErr/bad_file.csv"

    @mock_aws
    def test_move_s3_file_to_irrelevant(self) -> None:
        """Test that unmapped files are moved to newIrrevFiles/."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3_resource = boto3.resource("s3", region_name="ap-southeast-2")
        s3_resource.create_bucket(
            Bucket="sbm-file-ingester", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"}
        )

        bucket = s3_resource.Bucket("sbm-file-ingester")
        bucket.put_object(Key="newTBP/unmapped_file.csv", Body=b"content")

        with patch("aws_lambda_powertools.Logger"), patch("functions.file_processor.app.s3_resource", s3_resource):
            from functions.file_processor.app import move_s3_file

            move_s3_file("sbm-file-ingester", "newTBP/unmapped_file.csv", "newIrrevFiles/")

            objects_in_irrev = list(bucket.objects.filter(Prefix="newIrrevFiles/"))
            assert len(objects_in_irrev) == 1
            assert objects_in_irrev[0].key == "newIrrevFiles/unmapped_file.csv"


class TestNem12MappingsRead:
    """Tests for NEM12 mappings file reading."""

    @mock_aws
    def test_read_nem12_mappings_success(self) -> None:
        """Test successful reading of NEM12 mappings."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3_resource = boto3.resource("s3", region_name="ap-southeast-2")
        s3_resource.create_bucket(
            Bucket="sbm-file-ingester", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"}
        )

        # Create mappings file
        mappings = {
            "NMI123-E1": "neptune-001",
            "NMI456-B1": "neptune-002",
        }
        s3_resource.Object("sbm-file-ingester", "nem12_mappings.json").put(Body=json.dumps(mappings))

        with patch("aws_lambda_powertools.Logger"), patch("functions.file_processor.app.s3_resource", s3_resource):
            from functions.file_processor.app import read_nem12_mappings

            result = read_nem12_mappings("sbm-file-ingester")

            assert result == mappings
            assert result["NMI123-E1"] == "neptune-001"

    @mock_aws
    def test_read_nem12_mappings_file_not_found(self) -> None:
        """Test handling of missing mappings file."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3_resource = boto3.resource("s3", region_name="ap-southeast-2")
        s3_resource.create_bucket(
            Bucket="sbm-file-ingester", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"}
        )

        with patch("aws_lambda_powertools.Logger"), patch("functions.file_processor.app.s3_resource", s3_resource):
            from functions.file_processor.app import read_nem12_mappings

            result = read_nem12_mappings("sbm-file-ingester")

            # Should return None on error
            assert result is None


# TestMetricsPopulation removed - these functions (dailyInitializeMetricsDict, metricsDictPopulateValues)
# have been replaced by Powertools Metrics and no longer exist


class TestDownloadFilesToTmp:
    """Tests for downloading files to temp directory."""

    @mock_aws
    def test_download_files_success(self, temp_directory: str) -> None:
        """Test successful file download from S3."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3_resource = boto3.resource("s3", region_name="ap-southeast-2")
        s3_resource.create_bucket(
            Bucket="sbm-file-ingester", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"}
        )

        # Upload test file
        s3_resource.Object("sbm-file-ingester", "newTBP/test.csv").put(Body=b"test,data\n1,2")

        with patch("aws_lambda_powertools.Logger"), patch("functions.file_processor.app.s3_resource", s3_resource):
            from functions.file_processor.app import download_files_to_tmp

            files = [{"bucket": "sbm-file-ingester", "file_name": "newTBP/test.csv"}]
            result = download_files_to_tmp(files, temp_directory)

            assert len(result) == 1
            assert Path(result[0]).exists()

    @mock_aws
    def test_download_files_handles_url_encoding(self, temp_directory: str) -> None:
        """Test that URL-encoded file names are handled correctly."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3_resource = boto3.resource("s3", region_name="ap-southeast-2")
        s3_resource.create_bucket(
            Bucket="sbm-file-ingester", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"}
        )

        # Upload file with space in name
        s3_resource.Object("sbm-file-ingester", "newTBP/test file.csv").put(Body=b"test,data\n1,2")

        with patch("aws_lambda_powertools.Logger"), patch("functions.file_processor.app.s3_resource", s3_resource):
            from functions.file_processor.app import download_files_to_tmp

            # URL-encoded filename (+ represents space)
            files = [{"bucket": "sbm-file-ingester", "file_name": "newTBP/test+file.csv"}]
            result = download_files_to_tmp(files, temp_directory)

            # Should handle URL decoding
            assert len(result) == 1


class TestFullPipeline:
    """End-to-end integration tests."""

    @mock_aws
    def test_full_pipeline_with_nem12_file(self, nem12_sample_file: str, temp_directory: str) -> None:
        """Test full pipeline with real NEM12 file."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        # Setup S3
        s3_resource = boto3.resource("s3", region_name="ap-southeast-2")
        s3_resource.create_bucket(
            Bucket="sbm-file-ingester", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"}
        )
        s3_resource.create_bucket(
            Bucket="hudibucketsrc", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"}
        )

        # Setup CloudWatch logs
        logs = boto3.client("logs", region_name="ap-southeast-2")
        for log_group in [
            "sbm-ingester-error-log",
            "sbm-ingester-execution-log",
            "sbm-ingester-metrics-log",
            "sbm-ingester-parse-error-log",
            "sbm-ingester-runtime-error-log",
        ]:
            logs.create_log_group(logGroupName=log_group)

        # Read NEM12 file and upload to S3
        with Path(nem12_sample_file).open("rb") as f:
            s3_resource.Object("sbm-file-ingester", "newTBP/nem12_test.csv").put(Body=f.read())

        # Upload mappings (empty - file will go to irrelevant)
        s3_resource.Object("sbm-file-ingester", "nem12_mappings.json").put(Body=json.dumps({}))

        # Run the pipeline
        with patch("functions.file_processor.app.s3_resource", s3_resource):
            from functions.file_processor.app import parse_and_write_data

            files = [{"bucket": "sbm-file-ingester", "file_name": "newTBP/nem12_test.csv"}]
            result = parse_and_write_data(tbp_files=files)

            # Should complete without error
            # File should be moved to irrelevant (no mappings)
            bucket = s3_resource.Bucket("sbm-file-ingester")
            irrev_objects = list(bucket.objects.filter(Prefix="newIrrevFiles/"))

            # Either processed successfully or moved to appropriate folder
            assert result == 1 or len(irrev_objects) > 0
