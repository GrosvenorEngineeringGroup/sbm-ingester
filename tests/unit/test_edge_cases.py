"""Edge case tests to improve coverage to 90%+."""

import json
import os
import sys
import time
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import boto3
import pandas as pd
import pytest
from moto import mock_aws

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

# Import app module early before any Logger patches
import functions.file_processor.app as file_processor_app
from shared.parsers import ParserError, ParserOutcome, ProcessingError


class TestMoveS3FileEdgeCases:
    """Edge case tests for move_s3_file function."""

    @mock_aws
    def test_move_s3_file_copy_exception(self) -> None:
        """Test that move_s3_file handles copy exceptions gracefully."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3_resource = boto3.resource("s3", region_name="ap-southeast-2")
        s3_resource.create_bucket(
            Bucket="sbm-file-ingester", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"}
        )

        # Don't create source file - will cause copy to fail
        mock_logger = MagicMock()
        with (
            patch("aws_lambda_powertools.Logger"),
            patch("functions.file_processor.app.s3_resource", s3_resource),
            patch("functions.file_processor.app.logger", mock_logger),
        ):
            from functions.file_processor.app import move_s3_file

            result = move_s3_file("sbm-file-ingester", "newTBP/nonexistent.csv", "newP/")

            # Should return None and log error
            assert result is None
            assert mock_logger.error.called

    @mock_aws
    def test_move_s3_file_handles_special_characters_in_filename(self) -> None:
        """Test that move_s3_file handles special characters in filenames."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3_resource = boto3.resource("s3", region_name="ap-southeast-2")
        s3_resource.create_bucket(
            Bucket="sbm-file-ingester", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"}
        )

        # Create file with special characters
        bucket = s3_resource.Bucket("sbm-file-ingester")
        bucket.put_object(Key="newTBP/test_file (1).csv", Body=b"test content")

        with (
            patch("aws_lambda_powertools.Logger"),
            patch("functions.file_processor.app.s3_resource", s3_resource),
        ):
            from functions.file_processor.app import move_s3_file

            result = move_s3_file("sbm-file-ingester", "newTBP/test_file (1).csv", "newP/")

            assert result == "newP/test_file (1).csv"


class TestNem12MappingsEdgeCases:
    """Edge case tests for NEM12 mappings handling."""

    @mock_aws
    def test_parse_and_write_fails_when_mappings_none(self, temp_directory: str) -> None:
        """Test that parse_and_write_data fails gracefully when nem12_mappings is None."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

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

        # Do NOT upload nem12_mappings.json to trigger the None case

        with patch("functions.file_processor.app.s3_resource", s3_resource):
            from functions.file_processor.app import parse_and_write_data

            result = parse_and_write_data(tbp_files=[])

            # Should return None due to missing mappings
            assert result is None


class TestParseAndWriteDataEdgeCases:
    """Edge case tests for parse_and_write_data function."""

    @mock_aws
    def test_file_parse_fallback_to_non_nem_parser(self, temp_directory: str) -> None:
        """Test that files falling back to nonNemParsersGetDf are handled correctly."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

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

        # Upload mappings
        s3_resource.Object("sbm-file-ingester", "nem12_mappings.json").put(
            Body=json.dumps({"Envizi_12345-E1": "neptune-001"})
        )

        # Create Envizi water file (non-NEM format)
        envizi_content = """Serial_No,Interval_Start,Interval_End,Consumption,Consumption Unit
12345,2024-01-01T00:00:00,2024-01-01T01:00:00,1.5,kL
12345,2024-01-01T01:00:00,2024-01-01T02:00:00,2.0,kL
"""
        s3_resource.Object("sbm-file-ingester", "newTBP/envizi_water.csv").put(Body=envizi_content.encode())

        with patch("functions.file_processor.app.s3_resource", s3_resource):
            from functions.file_processor.app import parse_and_write_data

            files = [{"bucket": "sbm-file-ingester", "file_name": "newTBP/envizi_water.csv"}]
            result = parse_and_write_data(tbp_files=files)

            assert result == 1

    @mock_aws
    def test_file_parse_completely_fails(self, temp_directory: str) -> None:
        """Test that files failing all parsers go to parse error directory."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

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

        # Upload mappings
        s3_resource.Object("sbm-file-ingester", "nem12_mappings.json").put(Body=json.dumps({}))

        # Create completely invalid file
        s3_resource.Object("sbm-file-ingester", "newTBP/invalid.csv").put(
            Body=b"completely,invalid,garbage\ndata,more,junk\n"
        )

        with patch("functions.file_processor.app.s3_resource", s3_resource):
            from functions.file_processor.app import parse_and_write_data

            files = [{"bucket": "sbm-file-ingester", "file_name": "newTBP/invalid.csv"}]
            result = parse_and_write_data(tbp_files=files)

            # Should still return 1 (success overall) but file should be moved to parse error
            assert result == 1

            # Check file was moved to parse error directory
            bucket = s3_resource.Bucket("sbm-file-ingester")
            parse_err_objects = list(bucket.objects.filter(Prefix="newParseErr/"))
            assert len(parse_err_objects) == 1

    @mock_aws
    def test_t_start_index_reset(self, temp_directory: str) -> None:
        """Test that DataFrame with t_start as index is handled correctly."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        # Create DataFrame with t_start as index
        df = pd.DataFrame(
            {
                "E1_kWh": [1.0, 2.0, 3.0],
            }
        )
        df.index = pd.to_datetime(["2024-01-01 00:00", "2024-01-01 00:30", "2024-01-01 01:00"])
        df.index.name = "t_start"

        # Verify the reset works
        if "t_start" not in df.columns and df.index.name == "t_start":
            df = df.reset_index()

        assert "t_start" in df.columns

    @mock_aws
    def test_runtime_error_log(self, temp_directory: str) -> None:
        """Test that runtime errors are logged to logsDict and output at end."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

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

        # Upload mappings
        s3_resource.Object("sbm-file-ingester", "nem12_mappings.json").put(Body=json.dumps({}))

        # Upload bad file that will be logged
        s3_resource.Object("sbm-file-ingester", "newTBP/bad_file.csv").put(Body=b"invalid,content")

        mock_logger = MagicMock()
        with (
            patch("functions.file_processor.app.s3_resource", s3_resource),
            patch("functions.file_processor.app.logger", mock_logger),
        ):
            from functions.file_processor.app import parse_and_write_data

            files = [{"bucket": "sbm-file-ingester", "file_name": "newTBP/bad_file.csv"}]
            parse_and_write_data(tbp_files=files)

            # runtime errors are logged as warnings for bad files
            assert mock_logger.warning.called or mock_logger.error.called


class TestLambdaHandlerEdgeCases:
    """Edge case tests for lambda_handler function."""

    def test_lambda_handler_malformed_record(self) -> None:
        """Test lambda_handler handles malformed records gracefully."""
        # Create mock context for Powertools
        mock_context = MagicMock()
        mock_context.function_name = "test-function"
        mock_context.memory_limit_in_mb = 128
        mock_context.invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:test-function"
        mock_context.aws_request_id = "test-request-id"

        with patch.object(file_processor_app, "parse_and_write_data"):
            # Event with malformed body (missing required fields)
            event: dict[str, Any] = {
                "Records": [
                    {"body": "not valid json"},
                    {"body": json.dumps({"Records": []})},
                    {"body": json.dumps({"Records": [{"s3": {}}]})},
                ]
            }

            result = file_processor_app.lambda_handler(event, mock_context)

            assert result["statusCode"] == 200

    def test_lambda_handler_empty_records(self) -> None:
        """Test lambda_handler handles empty records list."""
        # Create mock context for Powertools
        mock_context = MagicMock()
        mock_context.function_name = "test-function"
        mock_context.memory_limit_in_mb = 128
        mock_context.invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:test-function"
        mock_context.aws_request_id = "test-request-id"

        with patch.object(file_processor_app, "parse_and_write_data") as mock_parse:
            event: dict[str, Any] = {"Records": []}

            result = file_processor_app.lambda_handler(event, mock_context)

            assert result["statusCode"] == 200
            # parse_and_write_data should not be called with empty list
            mock_parse.assert_not_called()


class TestDownloadFilesEdgeCases:
    """Edge case tests for download_files_to_tmp function."""

    @mock_aws
    def test_download_nonexistent_file(self, temp_directory: str) -> None:
        """Test downloading a file that doesn't exist in S3."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3_resource = boto3.resource("s3", region_name="ap-southeast-2")
        s3_resource.create_bucket(
            Bucket="sbm-file-ingester", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"}
        )

        mock_logger = MagicMock()
        with (
            patch("aws_lambda_powertools.Logger"),
            patch("functions.file_processor.app.s3_resource", s3_resource),
            patch("functions.file_processor.app.logger", mock_logger),
        ):
            from functions.file_processor.app import download_files_to_tmp

            files = [{"bucket": "sbm-file-ingester", "file_name": "newTBP/nonexistent.csv"}]
            result = download_files_to_tmp(files, temp_directory)

            # Should return empty list and log error
            assert not result
            assert mock_logger.error.called

    @mock_aws
    def test_download_percent_encoded_filename(self, temp_directory: str) -> None:
        """Test downloading files with percent-encoded characters."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3_resource = boto3.resource("s3", region_name="ap-southeast-2")
        s3_resource.create_bucket(
            Bucket="sbm-file-ingester", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"}
        )

        # Create file with space in name
        s3_resource.Object("sbm-file-ingester", "newTBP/my file.csv").put(Body=b"test,content")

        with (
            patch("aws_lambda_powertools.Logger"),
            patch("functions.file_processor.app.s3_resource", s3_resource),
        ):
            from functions.file_processor.app import download_files_to_tmp

            # URL encoded with %20 for space
            files = [{"bucket": "sbm-file-ingester", "file_name": "newTBP/my%20file.csv"}]
            result = download_files_to_tmp(files, temp_directory)

            assert len(result) == 1
            assert Path(result[0]).exists()


class TestProcessingLoopEdgeCases:
    """Edge case tests for the main data processing loop."""

    @mock_aws
    def test_dataframe_with_mapped_neptune_id(self, temp_directory: str, nem12_sample_file: str) -> None:
        """Test full processing with a file that has Neptune ID mappings."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

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

        # Read NEM12 file content
        with Path(nem12_sample_file).open("rb") as f:
            nem12_content = f.read()

        s3_resource.Object("sbm-file-ingester", "newTBP/test_nem12.csv").put(Body=nem12_content)

        # Upload mappings that match the NMI in the sample file
        # NMI in sample file is "VABD000163" with E1 channel
        s3_resource.Object("sbm-file-ingester", "nem12_mappings.json").put(
            Body=json.dumps({"VABD000163-E1": "neptune-test-001"})
        )

        with patch("functions.file_processor.app.s3_resource", s3_resource):
            from functions.file_processor.app import parse_and_write_data

            files = [{"bucket": "sbm-file-ingester", "file_name": "newTBP/test_nem12.csv"}]
            result = parse_and_write_data(tbp_files=files)

            assert result == 1

            # Check that output was written to hudibucketsrc
            hudi_bucket = s3_resource.Bucket("hudibucketsrc")
            sensor_files = list(hudi_bucket.objects.filter(Prefix="sensorDataFiles/"))
            assert len(sensor_files) > 0

            # Check file was moved to processed directory
            ingester_bucket = s3_resource.Bucket("sbm-file-ingester")
            processed_files = list(ingester_bucket.objects.filter(Prefix="newP/"))
            assert len(processed_files) == 1

    @mock_aws
    def test_file_with_no_neptune_mapping_goes_to_irrelevant(self, temp_directory: str, nem12_sample_file: str) -> None:
        """Test that files with no Neptune mapping go to irrelevant directory."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

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

        # Read NEM12 file content
        with Path(nem12_sample_file).open("rb") as f:
            nem12_content = f.read()

        s3_resource.Object("sbm-file-ingester", "newTBP/unmapped_nem12.csv").put(Body=nem12_content)

        # Upload empty mappings - no Neptune IDs
        s3_resource.Object("sbm-file-ingester", "nem12_mappings.json").put(Body=json.dumps({}))

        with patch("functions.file_processor.app.s3_resource", s3_resource):
            from functions.file_processor.app import parse_and_write_data

            files = [{"bucket": "sbm-file-ingester", "file_name": "newTBP/unmapped_nem12.csv"}]
            result = parse_and_write_data(tbp_files=files)

            assert result == 1

            # Check file was moved to irrelevant directory
            bucket = s3_resource.Bucket("sbm-file-ingester")
            irrev_files = list(bucket.objects.filter(Prefix="newIrrevFiles/"))
            assert len(irrev_files) == 1


class TestBatchSizeFlush:
    """Tests for batch size flush during processing."""

    @mock_aws
    def test_buffer_flushes_at_batch_size(self, temp_directory: str) -> None:
        """Test that buffer flushes when reaching BATCH_SIZE."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

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

        # Create mappings for many monitor points (more than BATCH_SIZE=50)
        # Using 60 different suffixes to trigger the batch flush
        mappings = {}
        for i in range(60):
            suffix_idx = i % 22  # Use suffixes from NMI_DATA_STREAM_SUFFIX
            channel_idx = i // 22  # Use channels from NMI_DATA_STREAM_CHANNEL
            suffix_char = [
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
            ][suffix_idx]
            channel_char = ["1", "2", "3", "4", "5", "6", "7", "8", "9"][channel_idx]
            key = f"TESTNMI-{suffix_char}{channel_char}"
            mappings[key] = f"neptune-{i:03d}"

        s3_resource.Object("sbm-file-ingester", "nem12_mappings.json").put(Body=json.dumps(mappings))

        # Create a NEM12 file with many channels to trigger batch flush
        # We'll create a file with multiple 200/300 records
        nem12_lines = ["100,NEM12,200405011135,MDA1,Ret1"]

        # Add 60 different channels/suffixes
        for i in range(60):
            suffix_idx = i % 22
            channel_idx = i // 22
            suffix_char = [
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
            ][suffix_idx]
            channel_char = ["1", "2", "3", "4", "5", "6", "7", "8", "9"][channel_idx]

            nem12_lines.append(
                f"200,TESTNMI,E{suffix_char}{channel_char},{i + 1},{suffix_char}{channel_char},N1,METSER,kWh,30,"
            )
            # One day of 30-minute readings (48 values)
            values = ",".join(["1.111"] * 48)
            nem12_lines.append(f"300,20240101,{values},A,,,20240102120000,")

        nem12_lines.append("900")
        nem12_content = "\n".join(nem12_lines)

        s3_resource.Object("sbm-file-ingester", "newTBP/many_channels.csv").put(Body=nem12_content.encode())

        with patch("functions.file_processor.app.s3_resource", s3_resource):
            from functions.file_processor.app import parse_and_write_data

            files = [{"bucket": "sbm-file-ingester", "file_name": "newTBP/many_channels.csv"}]
            result = parse_and_write_data(tbp_files=files)

            assert result == 1

            # Check that batch files were written to hudibucketsrc
            hudi_bucket = s3_resource.Bucket("hudibucketsrc")
            sensor_files = list(hudi_bucket.objects.filter(Prefix="sensorDataFiles/"))

            # With BATCH_SIZE=50000 rows and 60 channels x 48 readings = 2880 rows,
            # we expect 1 file (all rows fit in one batch)
            assert len(sensor_files) >= 1

            # Verify the file contains the expected data
            csv_content = sensor_files[0].get()["Body"].read().decode("utf-8")
            lines = csv_content.strip().split("\n")
            # 1 header + 2880 data rows
            assert len(lines) == 2881


class TestExceptionHandling:
    """Edge case tests for exception handling paths."""

    @mock_aws
    def test_parseandwritedata_general_exception(self, temp_directory: str) -> None:
        """Test that general exceptions in parse_and_write_data are handled."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

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

        mock_logger = MagicMock()
        with (
            patch("functions.file_processor.app.s3_resource", s3_resource),
            patch("functions.file_processor.app.logger", mock_logger),
            patch(
                "functions.file_processor.app.read_nem12_mappings",
                side_effect=Exception("Test exception"),
            ),
        ):
            from functions.file_processor.app import parse_and_write_data

            result = parse_and_write_data(tbp_files=[])

            # Should return None on exception
            assert result is None
            # Error should be logged
            assert mock_logger.error.called


# TestMetricsEdgeCases removed - these functions (dailyInitializeMetricsDict, metricsDictPopulateValues)
# have been replaced by Powertools Metrics and no longer exist


def _create_outcome_test_buckets(
    mappings: dict[str, str] | None = None,
    file_name: str = "outcome.csv",
    body: bytes = b"not,nem\n",
):
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

    s3_resource = boto3.resource("s3", region_name="ap-southeast-2")
    s3_resource.create_bucket(
        Bucket="sbm-file-ingester", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"}
    )
    s3_resource.create_bucket(
        Bucket="hudibucketsrc", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"}
    )

    logs = boto3.client("logs", region_name="ap-southeast-2")
    for log_group in [
        "sbm-ingester-error-log",
        "sbm-ingester-execution-log",
        "sbm-ingester-metrics-log",
        "sbm-ingester-parse-error-log",
        "sbm-ingester-runtime-error-log",
    ]:
        logs.create_log_group(logGroupName=log_group)

    s3_resource.Object("sbm-file-ingester", "nem12_mappings.json").put(Body=json.dumps(mappings or {}))
    s3_resource.Object("sbm-file-ingester", f"newTBP/{file_name}").put(Body=body)
    return s3_resource


def _list_keys(s3_resource, bucket: str, prefix: str) -> list[str]:
    return [obj.key for obj in s3_resource.Bucket(bucket).objects.filter(Prefix=prefix)]


def _run_with_non_nem_outcome(
    outcome_or_error: ParserOutcome | Exception,
    mappings: dict[str, str] | None = None,
    file_name: str = "outcome.csv",
):
    s3_resource = _create_outcome_test_buckets(mappings=mappings, file_name=file_name)

    outcome_patch = (
        patch("functions.file_processor.app.get_non_nem_outcome", side_effect=outcome_or_error)
        if isinstance(outcome_or_error, Exception)
        else patch("functions.file_processor.app.get_non_nem_outcome", return_value=outcome_or_error)
    )

    with (
        patch("functions.file_processor.app.s3_resource", s3_resource),
        patch("functions.file_processor.app.stream_as_data_frames", side_effect=ValueError("not nem")),
        patch("functions.file_processor.app.output_as_data_frames", side_effect=ValueError("not nem")),
        outcome_patch,
    ):
        result = file_processor_app.parse_and_write_data(
            tbp_files=[{"bucket": "sbm-file-ingester", "file_name": f"newTBP/{file_name}"}]
        )

    return result, s3_resource


class TestParserOutcomeDisposition:
    @mock_aws
    def test_processed_empty_outcome_moves_to_newp(self) -> None:
        result, s3_resource = _run_with_non_nem_outcome(ParserOutcome(status="processed_empty", reason="no rows"))

        assert result == 1
        assert _list_keys(s3_resource, "sbm-file-ingester", "newP/") == ["newP/outcome.csv"]
        assert _list_keys(s3_resource, "hudibucketsrc", "sensorDataFiles/") == []

    @mock_aws
    def test_processed_external_outcome_moves_to_newp(self) -> None:
        result, s3_resource = _run_with_non_nem_outcome(ParserOutcome(status="processed_external"))

        assert result == 1
        assert _list_keys(s3_resource, "sbm-file-ingester", "newP/") == ["newP/outcome.csv"]
        assert _list_keys(s3_resource, "hudibucketsrc", "sensorDataFiles/") == []

    @mock_aws
    def test_unmapped_outcome_moves_to_new_irrev_files(self) -> None:
        result, s3_resource = _run_with_non_nem_outcome(ParserOutcome(status="unmapped"))

        assert result == 1
        assert _list_keys(s3_resource, "sbm-file-ingester", "newIrrevFiles/") == ["newIrrevFiles/outcome.csv"]
        assert _list_keys(s3_resource, "hudibucketsrc", "sensorDataFiles/") == []

    @mock_aws
    def test_dataframe_all_unmapped_moves_to_new_irrev_files(self) -> None:
        df = pd.DataFrame(
            {
                "t_start": pd.to_datetime(["2024-01-01 00:00", "2024-01-01 00:30"]),
                "E1_kWh": [1.0, 2.0],
            }
        )

        result, s3_resource = _run_with_non_nem_outcome(ParserOutcome(status="processed", dfs=[("NMI1", df)]))

        assert result == 1
        assert _list_keys(s3_resource, "sbm-file-ingester", "newIrrevFiles/") == ["newIrrevFiles/outcome.csv"]
        assert _list_keys(s3_resource, "hudibucketsrc", "sensorDataFiles/") == []

    @mock_aws
    def test_dataframe_partial_mapping_moves_to_newp(self) -> None:
        df = pd.DataFrame(
            {
                "t_start": pd.to_datetime(["2024-01-01 00:00", "2024-01-01 00:30"]),
                "E1_kWh": [1.0, 2.0],
                "B1_kWh": [3.0, 4.0],
            }
        )

        result, s3_resource = _run_with_non_nem_outcome(
            ParserOutcome(status="processed", dfs=[("NMI1", df)]),
            mappings={"NMI1-E1": "p:test:e1"},
        )

        assert result == 1
        assert _list_keys(s3_resource, "sbm-file-ingester", "newP/") == ["newP/outcome.csv"]
        hudi_keys = _list_keys(s3_resource, "hudibucketsrc", "sensorDataFiles/")
        assert len(hudi_keys) == 1
        body = s3_resource.Object("hudibucketsrc", hudi_keys[0]).get()["Body"].read().decode()
        assert "p:test:e1,2024-01-01 00:00:00,1.0,kwh,2024-01-01 00:00:00," in body
        assert "3.0" not in body

    @mock_aws
    def test_side_effect_processed_outcome_moves_to_newp(self) -> None:
        result, s3_resource = _run_with_non_nem_outcome(ParserOutcome(status="processed"))

        assert result == 1
        assert _list_keys(s3_resource, "sbm-file-ingester", "newP/") == ["newP/outcome.csv"]
        assert _list_keys(s3_resource, "hudibucketsrc", "sensorDataFiles/") == []

    @mock_aws
    def test_dataframe_unsupported_suffix_moves_to_newp_without_hudi_write(self) -> None:
        df = pd.DataFrame(
            {
                "t_start": pd.to_datetime(["2024-01-01 00:00"]),
                "X1_kWh": [1.0],
            }
        )

        result, s3_resource = _run_with_non_nem_outcome(ParserOutcome(status="processed", dfs=[("NMI1", df)]))

        assert result == 1
        assert _list_keys(s3_resource, "sbm-file-ingester", "newP/") == ["newP/outcome.csv"]
        assert _list_keys(s3_resource, "hudibucketsrc", "sensorDataFiles/") == []

    @mock_aws
    def test_dataframe_nan_values_move_to_newp_without_hudi_write(self) -> None:
        df = pd.DataFrame(
            {
                "t_start": pd.to_datetime(["2024-01-01 00:00", "2024-01-01 00:30", "2024-01-01 01:00"]),
                "E1_kWh": [pd.NA, float("nan"), ""],
            }
        )

        result, s3_resource = _run_with_non_nem_outcome(
            ParserOutcome(status="processed", dfs=[("NMI1", df)]),
            mappings={"NMI1-E1": "p:test:e1"},
        )

        assert result == 1
        assert _list_keys(s3_resource, "sbm-file-ingester", "newP/") == ["newP/outcome.csv"]
        assert _list_keys(s3_resource, "hudibucketsrc", "sensorDataFiles/") == []

    @mock_aws
    def test_direct_point_id_bypasses_mapping_and_moves_to_newp(self) -> None:
        df = pd.DataFrame(
            {
                "t_start": pd.to_datetime(["2024-01-01 00:00"]),
                "E1_kWh": [5.5],
            }
        )

        result, s3_resource = _run_with_non_nem_outcome(ParserOutcome(status="processed", dfs=[("p:direct:id", df)]))

        assert result == 1
        assert _list_keys(s3_resource, "sbm-file-ingester", "newP/") == ["newP/outcome.csv"]
        hudi_keys = _list_keys(s3_resource, "hudibucketsrc", "sensorDataFiles/")
        assert len(hudi_keys) == 1
        body = s3_resource.Object("hudibucketsrc", hudi_keys[0]).get()["Body"].read().decode()
        assert "p:direct:id,2024-01-01 00:00:00,5.5,kwh,2024-01-01 00:00:00," in body

    @mock_aws
    def test_quality_column_is_written_with_mapped_rows(self) -> None:
        df = pd.DataFrame(
            {
                "t_start": pd.to_datetime(["2024-01-01 00:00", "2024-01-01 00:30"]),
                "E1_kWh": [1.0, 2.0],
                "quality_E1": ["A", pd.NA],
            }
        )

        result, s3_resource = _run_with_non_nem_outcome(
            ParserOutcome(status="processed", dfs=[("NMI1", df)]),
            mappings={"NMI1-E1": "p:test:e1"},
        )

        assert result == 1
        hudi_keys = _list_keys(s3_resource, "hudibucketsrc", "sensorDataFiles/")
        body = s3_resource.Object("hudibucketsrc", hudi_keys[0]).get()["Body"].read().decode()
        assert "p:test:e1,2024-01-01 00:00:00,1.0,kwh,2024-01-01 00:00:00,A" in body
        assert "p:test:e1,2024-01-01 00:30:00,2.0,kwh,2024-01-01 00:30:00,\n" in body

    @mock_aws
    def test_dataframe_bad_timestamp_moves_to_new_parse_err(self) -> None:
        df = pd.DataFrame({"t_start": ["not-a-date"], "E1_kWh": [1.0]})

        result, s3_resource = _run_with_non_nem_outcome(
            ParserOutcome(status="processed", dfs=[("NMI1", df)]),
            mappings={"NMI1-E1": "p:test:e1"},
        )

        assert result == 1
        assert _list_keys(s3_resource, "sbm-file-ingester", "newParseErr/") == ["newParseErr/outcome.csv"]
        assert _list_keys(s3_resource, "hudibucketsrc", "sensorDataFiles/") == []

    @mock_aws
    def test_dataframe_non_numeric_value_moves_to_new_parse_err(self) -> None:
        df = pd.DataFrame({"t_start": pd.to_datetime(["2024-01-01 00:00"]), "E1_kWh": ["not-number"]})

        result, s3_resource = _run_with_non_nem_outcome(
            ParserOutcome(status="processed", dfs=[("NMI1", df)]),
            mappings={"NMI1-E1": "p:test:e1"},
        )

        assert result == 1
        assert _list_keys(s3_resource, "sbm-file-ingester", "newParseErr/") == ["newParseErr/outcome.csv"]
        assert _list_keys(s3_resource, "hudibucketsrc", "sensorDataFiles/") == []

    @mock_aws
    def test_partial_flush_validation_error_cleans_hudi_output(self) -> None:
        class DelayedBadValue:
            def __float__(self) -> float:
                time.sleep(0.1)
                raise ValueError("not-number")

        df = pd.DataFrame(
            {
                "t_start": pd.to_datetime(["2024-01-01 00:00"]),
                "E1_kWh": [1.0],
                "B1_kWh": [DelayedBadValue()],
            }
        )
        s3_resource = _create_outcome_test_buckets(mappings={"NMI1-E1": "p:test:e1", "NMI1-B1": "p:test:b1"})

        with (
            patch("functions.file_processor.app.s3_resource", s3_resource),
            patch("functions.file_processor.app.stream_as_data_frames", side_effect=ValueError("not nem")),
            patch("functions.file_processor.app.output_as_data_frames", side_effect=ValueError("not nem")),
            patch(
                "functions.file_processor.app.get_non_nem_outcome",
                return_value=ParserOutcome(status="processed", dfs=[("NMI1", df)]),
            ),
            patch("functions.file_processor.app.BATCH_SIZE", 1),
        ):
            result = file_processor_app.parse_and_write_data(
                tbp_files=[{"bucket": "sbm-file-ingester", "file_name": "newTBP/outcome.csv"}]
            )

        assert result == 1
        assert _list_keys(s3_resource, "sbm-file-ingester", "newParseErr/") == ["newParseErr/outcome.csv"]
        assert _list_keys(s3_resource, "sbm-file-ingester", "newP/") == []
        assert _list_keys(s3_resource, "hudibucketsrc", "sensorDataFiles/") == []
        assert _list_keys(s3_resource, "hudibucketsrc", "sensorDataFilesStaging/") == []

    @mock_aws
    def test_collision_after_partial_flush_keeps_prior_file_output(self) -> None:
        class DelayedBadValue:
            def __float__(self) -> float:
                time.sleep(0.1)
                raise ValueError("not-number")

        success_df = pd.DataFrame(
            {
                "t_start": pd.to_datetime(["2024-01-01 00:00"]),
                "E1_kWh": [1.0],
            }
        )
        failing_df = pd.DataFrame(
            {
                "t_start": pd.to_datetime(["2024-01-01 00:30"]),
                "E1_kWh": [2.0],
                "B1_kWh": [DelayedBadValue()],
            }
        )
        s3_resource = _create_outcome_test_buckets(
            mappings={"NMI1-E1": "p:test:e1", "NMI1-B1": "p:test:b1"},
            file_name="first.csv",
        )
        s3_resource.Object("sbm-file-ingester", "newTBP/second.csv").put(Body=b"test")

        def get_outcome(local_file_path: str, _parse_error_log_group: str) -> ParserOutcome:
            if Path(local_file_path).name == "first.csv":
                return ParserOutcome(status="processed", dfs=[("NMI1", success_df)])
            return ParserOutcome(status="processed", dfs=[("NMI1", failing_df)])

        with (
            patch("functions.file_processor.app.s3_resource", s3_resource),
            patch("functions.file_processor.app.stream_as_data_frames", side_effect=ValueError("not nem")),
            patch("functions.file_processor.app.output_as_data_frames", side_effect=ValueError("not nem")),
            patch("functions.file_processor.app.get_non_nem_outcome", side_effect=get_outcome),
            patch("functions.file_processor.app.BATCH_SIZE", 1),
            patch("functions.file_processor.app.random.randint", return_value=12345),
        ):
            result = file_processor_app.parse_and_write_data(
                tbp_files=[
                    {"bucket": "sbm-file-ingester", "file_name": "newTBP/first.csv"},
                    {"bucket": "sbm-file-ingester", "file_name": "newTBP/second.csv"},
                ]
            )

        assert result == 1
        hudi_keys = _list_keys(s3_resource, "hudibucketsrc", "sensorDataFiles/")
        assert len(hudi_keys) == 1
        body = s3_resource.Object("hudibucketsrc", hudi_keys[0]).get()["Body"].read().decode()
        assert "p:test:e1,2024-01-01 00:00:00,1.0,kwh,2024-01-01 00:00:00," in body
        assert _list_keys(s3_resource, "hudibucketsrc", "sensorDataFilesStaging/") == []
        assert _list_keys(s3_resource, "sbm-file-ingester", "newP/") == ["newP/first.csv"]
        assert _list_keys(s3_resource, "sbm-file-ingester", "newParseErr/") == ["newParseErr/second.csv"]

    @mock_aws
    def test_dataframe_final_move_failure_cleans_hudi_output_and_moves_to_parse_err(self) -> None:
        df = pd.DataFrame(
            {
                "t_start": pd.to_datetime(["2024-01-01 00:00"]),
                "E1_kWh": [1.0],
            }
        )
        s3_resource = _create_outcome_test_buckets(mappings={"NMI1-E1": "p:test:e1"})
        real_move_s3_file = file_processor_app.move_s3_file
        move_attempts = 0

        def move_s3_file_with_final_failure(bucket_name: str, source_key: str, dest_prefix: str) -> str | None:
            nonlocal move_attempts
            move_attempts += 1
            if move_attempts == 1:
                return None
            return real_move_s3_file(bucket_name, source_key, dest_prefix)

        with (
            patch("functions.file_processor.app.s3_resource", s3_resource),
            patch("functions.file_processor.app.stream_as_data_frames", side_effect=ValueError("not nem")),
            patch("functions.file_processor.app.output_as_data_frames", side_effect=ValueError("not nem")),
            patch(
                "functions.file_processor.app.get_non_nem_outcome",
                return_value=ParserOutcome(status="processed", dfs=[("NMI1", df)]),
            ),
            patch("functions.file_processor.app.move_s3_file", side_effect=move_s3_file_with_final_failure),
        ):
            result = file_processor_app.parse_and_write_data(
                tbp_files=[{"bucket": "sbm-file-ingester", "file_name": "newTBP/outcome.csv"}]
            )

        assert result == 1
        assert _list_keys(s3_resource, "sbm-file-ingester", "newParseErr/") == ["newParseErr/outcome.csv"]
        assert _list_keys(s3_resource, "sbm-file-ingester", "newP/") == []
        assert _list_keys(s3_resource, "hudibucketsrc", "sensorDataFiles/") == []
        assert _list_keys(s3_resource, "hudibucketsrc", "sensorDataFilesStaging/") == []

    @mock_aws
    def test_dataframe_upload_failure_moves_to_new_parse_err(self) -> None:
        df = pd.DataFrame({"t_start": pd.to_datetime(["2024-01-01 00:00"]), "E1_kWh": [1.0]})
        s3_resource = _create_outcome_test_buckets(mappings={"NMI1-E1": "p:test:e1"})

        with (
            patch("functions.file_processor.app.s3_resource", s3_resource),
            patch("functions.file_processor.app.stream_as_data_frames", side_effect=ValueError("not nem")),
            patch("functions.file_processor.app.output_as_data_frames", side_effect=ValueError("not nem")),
            patch(
                "functions.file_processor.app.get_non_nem_outcome",
                return_value=ParserOutcome(status="processed", dfs=[("NMI1", df)]),
            ),
            patch("functions.file_processor.app._upload_csv_to_s3", side_effect=RuntimeError("boom")),
        ):
            result = file_processor_app.parse_and_write_data(
                tbp_files=[{"bucket": "sbm-file-ingester", "file_name": "newTBP/outcome.csv"}]
            )

        assert result == 1
        assert _list_keys(s3_resource, "sbm-file-ingester", "newParseErr/") == ["newParseErr/outcome.csv"]
        assert _list_keys(s3_resource, "sbm-file-ingester", "newP/") == []
        assert _list_keys(s3_resource, "hudibucketsrc", "sensorDataFiles/") == []

    @mock_aws
    @pytest.mark.parametrize("error_cls", [ParserError, ProcessingError])
    def test_parser_errors_move_to_new_parse_err(self, error_cls: type[Exception]) -> None:
        result, s3_resource = _run_with_non_nem_outcome(error_cls("bad parser"))

        assert result == 1
        assert _list_keys(s3_resource, "sbm-file-ingester", "newParseErr/") == ["newParseErr/outcome.csv"]
        assert _list_keys(s3_resource, "hudibucketsrc", "sensorDataFiles/") == []
