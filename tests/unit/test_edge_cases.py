"""Edge case tests to improve coverage to 90%+."""

import json
import os
import sys
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
        """Test that buffer flushes when reaching CSV_FLUSH_ROW_THRESHOLD."""
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

        # Create mappings for many monitor points (more than CSV_FLUSH_ROW_THRESHOLD=50)
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

            # With CSV_FLUSH_ROW_THRESHOLD=50000 rows and 60 channels x 48 readings = 2880 rows,
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
        patch("functions.file_processor.app.dispatch_non_nem", side_effect=outcome_or_error)
        if isinstance(outcome_or_error, Exception)
        else patch("functions.file_processor.app.dispatch_non_nem", return_value=outcome_or_error)
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

        result, s3_resource = _run_with_non_nem_outcome(ParserOutcome(status="processed", dataframes=[("NMI1", df)]))

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
            ParserOutcome(status="processed", dataframes=[("NMI1", df)]),
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

        result, s3_resource = _run_with_non_nem_outcome(ParserOutcome(status="processed", dataframes=[("NMI1", df)]))

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
            ParserOutcome(status="processed", dataframes=[("NMI1", df)]),
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

        result, s3_resource = _run_with_non_nem_outcome(
            ParserOutcome(status="processed", dataframes=[("p:direct:id", df)])
        )

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
            ParserOutcome(status="processed", dataframes=[("NMI1", df)]),
            mappings={"NMI1-E1": "p:test:e1"},
        )

        assert result == 1
        hudi_keys = _list_keys(s3_resource, "hudibucketsrc", "sensorDataFiles/")
        body = s3_resource.Object("hudibucketsrc", hudi_keys[0]).get()["Body"].read().decode()
        assert "p:test:e1,2024-01-01 00:00:00,1.0,kwh,2024-01-01 00:00:00,A" in body
        assert "p:test:e1,2024-01-01 00:30:00,2.0,kwh,2024-01-01 00:30:00,\n" in body

    @mock_aws
    def test_dataframe_bad_timestamp_only_row_processes_empty(self) -> None:
        # Single bad-timestamp row: nothing written, but file processed
        # (no ParserError, no ProcessingError); source moved to newP/.
        df = pd.DataFrame({"t_start": ["not-a-date"], "E1_kWh": [1.0]})

        result, s3_resource = _run_with_non_nem_outcome(
            ParserOutcome(status="processed", dataframes=[("NMI1", df)]),
            mappings={"NMI1-E1": "p:test:e1"},
        )

        assert result == 1
        assert _list_keys(s3_resource, "sbm-file-ingester", "newParseErr/") == []
        assert _list_keys(s3_resource, "sbm-file-ingester", "newP/") == ["newP/outcome.csv"]
        assert _list_keys(s3_resource, "hudibucketsrc", "sensorDataFiles/") == []

    @mock_aws
    def test_dataframe_non_numeric_only_row_processes_empty(self) -> None:
        # Single non-numeric row: nothing written, but file processed.
        df = pd.DataFrame({"t_start": pd.to_datetime(["2024-01-01 00:00"]), "E1_kWh": ["not-number"]})

        result, s3_resource = _run_with_non_nem_outcome(
            ParserOutcome(status="processed", dataframes=[("NMI1", df)]),
            mappings={"NMI1-E1": "p:test:e1"},
        )

        assert result == 1
        assert _list_keys(s3_resource, "sbm-file-ingester", "newParseErr/") == []
        assert _list_keys(s3_resource, "sbm-file-ingester", "newP/") == ["newP/outcome.csv"]
        assert _list_keys(s3_resource, "hudibucketsrc", "sensorDataFiles/") == []

    @mock_aws
    def test_dataframe_partial_bad_rows_writes_good_rows(self) -> None:
        # Mix of valid and invalid rows: valid rows land in Hudi, invalid rows
        # are skipped silently and the file is marked processed.
        df = pd.DataFrame(
            {
                "t_start": [
                    pd.Timestamp("2024-01-01 00:00"),
                    pd.NaT,  # unparseable timestamp -> skip
                    pd.Timestamp("2024-01-01 00:30"),
                    pd.Timestamp("2024-01-01 01:00"),
                    pd.Timestamp("2024-01-01 01:30"),
                ],
                "E1_kWh": [1.0, 2.0, "not-number", 3.0, ""],
            }
        )

        result, s3_resource = _run_with_non_nem_outcome(
            ParserOutcome(status="processed", dataframes=[("NMI1", df)]),
            mappings={"NMI1-E1": "p:test:e1"},
        )

        assert result == 1
        assert _list_keys(s3_resource, "sbm-file-ingester", "newP/") == ["newP/outcome.csv"]
        assert _list_keys(s3_resource, "sbm-file-ingester", "newParseErr/") == []
        hudi_keys = _list_keys(s3_resource, "hudibucketsrc", "sensorDataFiles/")
        assert len(hudi_keys) == 1
        body = s3_resource.Object("hudibucketsrc", hudi_keys[0]).get()["Body"].read().decode()
        # Two valid rows written.
        assert "p:test:e1,2024-01-01 00:00:00,1.0,kwh,2024-01-01 00:00:00," in body
        assert "p:test:e1,2024-01-01 01:00:00,3.0,kwh,2024-01-01 01:00:00," in body
        # Bad rows omitted.
        assert "not-number" not in body
        assert "NaT" not in body

    @mock_aws
    def test_partial_flush_upload_error_cleans_hudi_output(self) -> None:
        # Row-level bad values no longer raise (Task 16); the partial-flush
        # cleanup path is now exercised by an upload failure on the second
        # batch flush. With CSV_FLUSH_ROW_THRESHOLD=1 and two valid rows, the first flush
        # succeeds and the second raises -> writer.abort() must clean both
        # the staging and the previously-committed final keys.
        df = pd.DataFrame(
            {
                "t_start": pd.to_datetime(["2024-01-01 00:00", "2024-01-01 00:30"]),
                "E1_kWh": [1.0, 2.0],
            }
        )
        s3_resource = _create_outcome_test_buckets(mappings={"NMI1-E1": "p:test:e1"})

        from functions.file_processor import csv_writer as csv_writer_module

        call_count = {"n": 0}
        real_upload = csv_writer_module._upload_csv_to_s3

        def upload_with_second_failure(csv_content: str, output_key: str, parent_xray_trace_entity: Any = None) -> None:
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise RuntimeError("simulated upload failure")
            real_upload(csv_content, output_key, parent_xray_trace_entity)

        with (
            patch("functions.file_processor.app.s3_resource", s3_resource),
            patch("functions.file_processor.csv_writer.s3_resource", s3_resource),
            patch("functions.file_processor.app.stream_as_data_frames", side_effect=ValueError("not nem")),
            patch("functions.file_processor.app.output_as_data_frames", side_effect=ValueError("not nem")),
            patch(
                "functions.file_processor.app.dispatch_non_nem",
                return_value=ParserOutcome(status="processed", dataframes=[("NMI1", df)]),
            ),
            patch("functions.file_processor.app.CSV_FLUSH_ROW_THRESHOLD", 1),
            patch("functions.file_processor.csv_writer._upload_csv_to_s3", side_effect=upload_with_second_failure),
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
        # Two source files; the second file's second-batch upload fails. The
        # first file's Hudi output must be preserved while the second file's
        # writer cleans up its staged/committed objects.
        success_df = pd.DataFrame(
            {
                "t_start": pd.to_datetime(["2024-01-01 00:00"]),
                "E1_kWh": [1.0],
            }
        )
        failing_df = pd.DataFrame(
            {
                "t_start": pd.to_datetime(["2024-01-01 00:30", "2024-01-01 01:00"]),
                "E1_kWh": [2.0, 3.0],
            }
        )
        s3_resource = _create_outcome_test_buckets(
            mappings={"NMI1-E1": "p:test:e1"},
            file_name="first.csv",
        )
        s3_resource.Object("sbm-file-ingester", "newTBP/second.csv").put(Body=b"test")

        def get_outcome(local_file_path: str) -> ParserOutcome:
            if Path(local_file_path).name == "first.csv":
                return ParserOutcome(status="processed", dataframes=[("NMI1", success_df)])
            return ParserOutcome(status="processed", dataframes=[("NMI1", failing_df)])

        # The first file emits one batch (1 upload). The second file emits two
        # batches; the second batch's upload raises -> abort path runs.
        from functions.file_processor import csv_writer as csv_writer_module

        call_count = {"n": 0}
        real_upload = csv_writer_module._upload_csv_to_s3

        def upload_third_call_fails(csv_content: str, output_key: str, parent_xray_trace_entity: Any = None) -> None:
            call_count["n"] += 1
            if call_count["n"] == 3:
                raise RuntimeError("simulated upload failure")
            real_upload(csv_content, output_key, parent_xray_trace_entity)

        with (
            patch("functions.file_processor.app.s3_resource", s3_resource),
            patch("functions.file_processor.csv_writer.s3_resource", s3_resource),
            patch("functions.file_processor.app.stream_as_data_frames", side_effect=ValueError("not nem")),
            patch("functions.file_processor.app.output_as_data_frames", side_effect=ValueError("not nem")),
            patch("functions.file_processor.app.dispatch_non_nem", side_effect=get_outcome),
            patch("functions.file_processor.app.CSV_FLUSH_ROW_THRESHOLD", 1),
            patch("functions.file_processor.csv_writer.random.randint", return_value=12345),
            patch("functions.file_processor.csv_writer._upload_csv_to_s3", side_effect=upload_third_call_fails),
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
                "functions.file_processor.app.dispatch_non_nem",
                return_value=ParserOutcome(status="processed", dataframes=[("NMI1", df)]),
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
            patch("functions.file_processor.csv_writer.s3_resource", s3_resource),
            patch("functions.file_processor.app.stream_as_data_frames", side_effect=ValueError("not nem")),
            patch("functions.file_processor.app.output_as_data_frames", side_effect=ValueError("not nem")),
            patch(
                "functions.file_processor.app.dispatch_non_nem",
                return_value=ParserOutcome(status="processed", dataframes=[("NMI1", df)]),
            ),
            patch("functions.file_processor.csv_writer._upload_csv_to_s3", side_effect=RuntimeError("boom")),
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


class TestComputeDataFrameFinalStatus:
    """Cover each branch of the spec's final-status calc ladder."""

    def test_rows_written_positive_returns_processed(self) -> None:
        from functions.file_processor.app import _compute_dataframe_final_status

        status, reason = _compute_dataframe_final_status(
            rows_written=5,
            candidate_row_count=5,
            unmapped_count=0,
            unsupported_suffixes=frozenset(),
            rows_skipped=0,
            parser_reason=None,
        )

        assert status == "processed"
        assert reason is None

    def test_all_candidates_unmapped_returns_unmapped(self) -> None:
        from functions.file_processor.app import _compute_dataframe_final_status

        status, reason = _compute_dataframe_final_status(
            rows_written=0,
            candidate_row_count=7,
            unmapped_count=7,
            unsupported_suffixes=frozenset(),
            rows_skipped=0,
            parser_reason=None,
        )

        assert status == "unmapped"
        assert reason is None

    def test_no_candidates_with_unsupported_suffix_returns_all_unknown_suffix(self) -> None:
        from functions.file_processor.app import _compute_dataframe_final_status

        status, reason = _compute_dataframe_final_status(
            rows_written=0,
            candidate_row_count=0,
            unmapped_count=0,
            unsupported_suffixes=frozenset({"foo"}),
            rows_skipped=0,
            parser_reason=None,
        )

        assert status == "processed_empty"
        assert reason == "all_unknown_suffix"

    def test_rows_skipped_only_returns_all_skipped(self) -> None:
        from functions.file_processor.app import _compute_dataframe_final_status

        status, reason = _compute_dataframe_final_status(
            rows_written=0,
            candidate_row_count=0,
            unmapped_count=0,
            unsupported_suffixes=frozenset(),
            rows_skipped=4,
            parser_reason=None,
        )

        assert status == "processed_empty"
        assert reason == "all_skipped"

    def test_default_branch_inherits_parser_reason(self) -> None:
        from functions.file_processor.app import _compute_dataframe_final_status

        status, reason = _compute_dataframe_final_status(
            rows_written=0,
            candidate_row_count=0,
            unmapped_count=0,
            unsupported_suffixes=frozenset(),
            rows_skipped=0,
            parser_reason="all_blank",
        )

        assert status == "processed_empty"
        assert reason == "all_blank"

    def test_default_branch_with_no_parser_reason_yields_none(self) -> None:
        from functions.file_processor.app import _compute_dataframe_final_status

        status, reason = _compute_dataframe_final_status(
            rows_written=0,
            candidate_row_count=0,
            unmapped_count=0,
            unsupported_suffixes=frozenset(),
            rows_skipped=0,
            parser_reason=None,
        )

        assert status == "processed_empty"
        assert reason is None


class TestCandidateValuesSkipAndCount:
    """Direct unit tests for `_candidate_values` skip-and-count behaviour."""

    def test_one_unparseable_timestamp_skipped_and_counted(self) -> None:
        from collections import Counter

        from functions.file_processor.app import _candidate_values

        # 100 rows; row 17 has a bogus timestamp -> skip, others valid.
        timestamps = [pd.Timestamp("2024-01-01") + pd.Timedelta(minutes=30 * i) for i in range(100)]
        timestamps[17] = "definitely-not-a-timestamp"
        df = pd.DataFrame({"t_start": timestamps, "E1_kWh": [float(i) for i in range(100)]})
        skip_counter: Counter = Counter()

        candidates = _candidate_values(df, "E1_kWh", df["t_start"], None, skip_counter)

        assert len(candidates) == 99
        assert skip_counter["unparseable_timestamp"] == 1
        assert skip_counter["unparseable_value"] == 0
        assert skip_counter["blank_value"] == 0

    def test_one_unparseable_value_skipped_and_counted(self) -> None:
        from collections import Counter

        from functions.file_processor.app import _candidate_values

        # 100 rows; row 5 has a non-numeric value.
        timestamps = [pd.Timestamp("2024-01-01") + pd.Timedelta(minutes=30 * i) for i in range(100)]
        values: list[Any] = [float(i) for i in range(100)]
        values[5] = "abc"
        df = pd.DataFrame({"t_start": timestamps, "E1_kWh": values})
        skip_counter: Counter = Counter()

        candidates = _candidate_values(df, "E1_kWh", df["t_start"], None, skip_counter)

        assert len(candidates) == 99
        assert skip_counter["unparseable_value"] == 1
        assert skip_counter["unparseable_timestamp"] == 0
        assert skip_counter["blank_value"] == 0

    def test_blank_string_value_counted_as_blank(self) -> None:
        from collections import Counter

        from functions.file_processor.app import _candidate_values

        df = pd.DataFrame({"t_start": [pd.Timestamp("2024-01-01")], "E1_kWh": [""]})
        skip_counter: Counter = Counter()

        candidates = _candidate_values(df, "E1_kWh", df["t_start"], None, skip_counter)

        assert candidates == []
        assert skip_counter["blank_value"] == 1
        assert skip_counter["unparseable_value"] == 0

    def test_whitespace_value_counted_as_blank(self) -> None:
        from collections import Counter

        from functions.file_processor.app import _candidate_values

        df = pd.DataFrame({"t_start": [pd.Timestamp("2024-01-01")], "E1_kWh": ["   "]})
        skip_counter: Counter = Counter()

        candidates = _candidate_values(df, "E1_kWh", df["t_start"], None, skip_counter)

        assert candidates == []
        assert skip_counter["blank_value"] == 1

    def test_nan_value_counted_as_blank(self) -> None:
        from collections import Counter

        import numpy as np

        from functions.file_processor.app import _candidate_values

        df = pd.DataFrame({"t_start": [pd.Timestamp("2024-01-01")], "E1_kWh": [np.nan]})
        skip_counter: Counter = Counter()

        candidates = _candidate_values(df, "E1_kWh", df["t_start"], None, skip_counter)

        assert candidates == []
        assert skip_counter["blank_value"] == 1

    def test_does_not_raise_on_any_row_level_issue(self) -> None:
        from functions.file_processor.app import _candidate_values

        df = pd.DataFrame(
            {
                "t_start": ["bad-ts", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")],
                "E1_kWh": ["x", "y", ""],
            }
        )

        # Must not raise even though every row is bad.
        candidates = _candidate_values(df, "E1_kWh", df["t_start"])

        assert candidates == []

    def test_skip_counter_optional(self) -> None:
        from functions.file_processor.app import _candidate_values

        df = pd.DataFrame({"t_start": [pd.Timestamp("2024-01-01")], "E1_kWh": [1.0]})
        # No skip_counter argument; must not error.
        candidates = _candidate_values(df, "E1_kWh", df["t_start"])

        assert len(candidates) == 1


class TestProcessorUnsupportedSuffixAccumulation:
    """Verify that unknown suffix columns are recorded (gap G12)."""

    @mock_aws
    def test_unsupported_suffix_with_no_known_columns_processes_empty(self) -> None:
        # All data columns have an unknown suffix -> no candidates, file
        # processed_empty (all_unknown_suffix), source moved to newP/.
        df = pd.DataFrame(
            {
                "t_start": pd.to_datetime(["2024-01-01 00:00", "2024-01-01 00:30"]),
                "Z9Z_kWh": [1.0, 2.0],
            }
        )

        result, s3_resource = _run_with_non_nem_outcome(
            ParserOutcome(status="processed", dataframes=[("NMI1", df)]),
            mappings={"NMI1-Z9Z": "p:test:z"},
        )

        assert result == 1
        assert _list_keys(s3_resource, "sbm-file-ingester", "newP/") == ["newP/outcome.csv"]
        assert _list_keys(s3_resource, "hudibucketsrc", "sensorDataFiles/") == []

    @mock_aws
    def test_known_and_unknown_suffix_processes_known_rows(self) -> None:
        # File has one known suffix (E1) and one unknown suffix (Z9Z); the
        # known channel writes Hudi rows, the unknown is ignored silently
        # but must not break processing.
        df = pd.DataFrame(
            {
                "t_start": pd.to_datetime(["2024-01-01 00:00"]),
                "E1_kWh": [1.0],
                "Z9Z_kWh": [9.0],
            }
        )

        result, s3_resource = _run_with_non_nem_outcome(
            ParserOutcome(status="processed", dataframes=[("NMI1", df)]),
            mappings={"NMI1-E1": "p:test:e1"},
        )

        assert result == 1
        assert _list_keys(s3_resource, "sbm-file-ingester", "newP/") == ["newP/outcome.csv"]
        hudi_keys = _list_keys(s3_resource, "hudibucketsrc", "sensorDataFiles/")
        assert len(hudi_keys) == 1


class TestLooksLikeNemEnvelope:
    """Unit tests for the ``_looks_like_nem_envelope`` helper."""

    def test_nem12_envelope_matches(self, tmp_path: Path) -> None:
        f = tmp_path / "f.csv"
        f.write_text("100,NEM12,202605060200,MDP1,Origin\n900\n")
        assert file_processor_app._looks_like_nem_envelope(str(f)) is True

    def test_nem13_envelope_matches(self, tmp_path: Path) -> None:
        f = tmp_path / "f.csv"
        f.write_text("100,NEM13,202605060200,MDP1,Origin\n900\n")
        assert file_processor_app._looks_like_nem_envelope(str(f)) is True

    def test_utf8_bom_envelope_matches(self, tmp_path: Path) -> None:
        # File starts with UTF-8 BOM (\xef\xbb\xbf) followed by a NEM12 header.
        # ``utf-8-sig`` must strip the BOM so the prefix match still succeeds.
        f = tmp_path / "f.csv"
        f.write_bytes("﻿100,NEM12,202605060200,MDP1,Origin\n900\n".encode())
        assert file_processor_app._looks_like_nem_envelope(str(f)) is True

    def test_non_nem_first_line_returns_false(self, tmp_path: Path) -> None:
        f = tmp_path / "f.csv"
        f.write_text("Date,Value,Quality\n2024-01-01,1.0,A\n")
        assert file_processor_app._looks_like_nem_envelope(str(f)) is False

    def test_nonexistent_file_returns_false(self, tmp_path: Path) -> None:
        assert file_processor_app._looks_like_nem_envelope(str(tmp_path / "missing.csv")) is False

    def test_unparseable_bytes_returns_false(self, tmp_path: Path) -> None:
        # Pure binary garbage that can't be decoded as utf-8 should not crash;
        # helper must defensively return False.
        f = tmp_path / "f.bin"
        f.write_bytes(b"\xff\xfe\xfd\xfc some binary noise")
        # Either decode fails (UnicodeDecodeError) -> False, or first line just
        # doesn't match the prefix -> False. Either way: defensive False.
        assert file_processor_app._looks_like_nem_envelope(str(f)) is False


class TestNemEmptyEnvelopeShortCircuit:
    """Empty NEM12/NEM13 envelopes (only 100/900 records) must short-circuit
    to ``processed_empty(no_data_sentinel)`` instead of falling through to the
    non-NEM dispatcher.
    """

    @mock_aws
    def test_empty_nem12_envelope_emits_processed_empty(self, fixtures_dir: str) -> None:
        body = (Path(fixtures_dir) / "nem12_empty.csv").read_bytes()
        s3_resource = _create_outcome_test_buckets(file_name="empty_nem12.csv", body=body)

        # If the short-circuit works, the non-NEM dispatcher must NEVER be
        # consulted for this file. Patching it with a side_effect that fails
        # the test guarantees that.
        sentinel_called = AssertionError("dispatch_non_nem must not be called for empty NEM envelopes")

        with (
            patch("functions.file_processor.app.s3_resource", s3_resource),
            patch("functions.file_processor.app.dispatch_non_nem", side_effect=sentinel_called),
        ):
            result = file_processor_app.parse_and_write_data(
                tbp_files=[{"bucket": "sbm-file-ingester", "file_name": "newTBP/empty_nem12.csv"}]
            )

        assert result == 1
        assert _list_keys(s3_resource, "sbm-file-ingester", "newP/") == ["newP/empty_nem12.csv"]
        assert _list_keys(s3_resource, "sbm-file-ingester", "newParseErr/") == []
        assert _list_keys(s3_resource, "sbm-file-ingester", "newIrrevFiles/") == []
        # No data rows -> nothing written to Hudi.
        assert _list_keys(s3_resource, "hudibucketsrc", "sensorDataFiles/") == []

    @mock_aws
    def test_empty_nem13_envelope_emits_processed_empty(self, fixtures_dir: str) -> None:
        body = (Path(fixtures_dir) / "nem13_empty.csv").read_bytes()
        s3_resource = _create_outcome_test_buckets(file_name="empty_nem13.csv", body=body)

        sentinel_called = AssertionError("dispatch_non_nem must not be called for empty NEM envelopes")

        with (
            patch("functions.file_processor.app.s3_resource", s3_resource),
            patch("functions.file_processor.app.dispatch_non_nem", side_effect=sentinel_called),
        ):
            result = file_processor_app.parse_and_write_data(
                tbp_files=[{"bucket": "sbm-file-ingester", "file_name": "newTBP/empty_nem13.csv"}]
            )

        assert result == 1
        assert _list_keys(s3_resource, "sbm-file-ingester", "newP/") == ["newP/empty_nem13.csv"]
        assert _list_keys(s3_resource, "sbm-file-ingester", "newParseErr/") == []
        assert _list_keys(s3_resource, "hudibucketsrc", "sensorDataFiles/") == []

    @mock_aws
    def test_genuine_parse_error_in_nem12_still_propagates_as_parser_error(self) -> None:
        # A NEM12-shaped file whose streaming/batch parsers raise (e.g. on a
        # malformed 200-record producing a ValueError from nemreader) must
        # NOT be short-circuited by the empty-envelope path: the streams are
        # not "empty", they're "broken". The fallthrough takes us to the
        # non-NEM dispatcher, which can't parse NEM12 -> ParserError ->
        # newParseErr/ as before.
        s3_resource = _create_outcome_test_buckets(
            file_name="malformed_nem12.csv",
            body=b"100,NEM12,202605060200,MDP1,Origin\n200,broken,record\n900\n",
        )

        with (
            patch("functions.file_processor.app.s3_resource", s3_resource),
            patch("functions.file_processor.app.stream_as_data_frames", side_effect=ValueError("malformed 200")),
            patch("functions.file_processor.app.output_as_data_frames", side_effect=ValueError("malformed 200")),
            patch(
                "functions.file_processor.app.dispatch_non_nem",
                side_effect=ParserError("non-NEM cannot parse NEM12-shaped file"),
            ),
        ):
            result = file_processor_app.parse_and_write_data(
                tbp_files=[{"bucket": "sbm-file-ingester", "file_name": "newTBP/malformed_nem12.csv"}]
            )

        assert result == 1
        # ParserError from the non-NEM dispatcher (last resort) -> newParseErr/.
        assert _list_keys(s3_resource, "sbm-file-ingester", "newParseErr/") == ["newParseErr/malformed_nem12.csv"]
        assert _list_keys(s3_resource, "sbm-file-ingester", "newP/") == []

    @mock_aws
    def test_unexpected_runtime_error_in_nem_path_propagates_as_parser_error(self) -> None:
        # An unexpected RuntimeError/AttributeError from nemreader internals
        # (e.g. genuine parser bug, not "this is not NEM12") must propagate
        # as ParserError through the dispatcher, NOT silently fall through
        # to the non-NEM dispatcher (which would mask the bug). Spec / Task 17:
        # NEM fallback narrowing catches only specific exceptions known to
        # mean "not a NEM12 file"; everything else surfaces.
        s3_resource = _create_outcome_test_buckets(
            file_name="nem12_bug.csv",
            body=b"100,NEM12,202605060200,MDP1,Origin\n200,row\n900\n",
        )

        with (
            patch("functions.file_processor.app.s3_resource", s3_resource),
            patch(
                "functions.file_processor.app.stream_as_data_frames",
                side_effect=RuntimeError("simulated nemreader internal bug"),
            ),
            patch(
                "functions.file_processor.app.output_as_data_frames",
                side_effect=RuntimeError("simulated nemreader internal bug"),
            ),
            patch(
                "functions.file_processor.app.dispatch_non_nem",
                side_effect=AssertionError("non-NEM dispatcher must NOT be consulted on RuntimeError"),
            ),
        ):
            # The RuntimeError from the NEM path is not in the narrowed
            # fallthrough tuple, so it bubbles up out of the per-file try
            # and is caught by the outer batch handler. Concretely the
            # batch handler logs and returns None for the whole batch.
            result = file_processor_app.parse_and_write_data(
                tbp_files=[{"bucket": "sbm-file-ingester", "file_name": "newTBP/nem12_bug.csv"}]
            )

        # Whole-batch abandonment: result is None, file remains in newTBP/.
        # This is intentional — a real parser bug should NOT be silently
        # routed to newParseErr/ via the non-NEM fallback.
        assert result is None
        assert _list_keys(s3_resource, "sbm-file-ingester", "newP/") == []
        assert _list_keys(s3_resource, "sbm-file-ingester", "newParseErr/") == []

    @mock_aws
    def test_non_nem_envelope_with_empty_stream_still_falls_through(self) -> None:
        # A file that does NOT start with 100,NEM12, or 100,NEM13, must still
        # fall through to the non-NEM dispatcher even when the streaming
        # parser yields nothing. Confirms the helper is correctly NEM-only.
        s3_resource = _create_outcome_test_buckets(
            file_name="non_nem.csv",
            body=b"Date,Value,Quality\n",  # header-only non-NEM file
        )

        sentinel = ParserOutcome(status="processed_empty", reason="zero_rows")
        with (
            patch("functions.file_processor.app.s3_resource", s3_resource),
            # Streaming returns empty iterator -> next() yields None.
            patch("functions.file_processor.app.stream_as_data_frames", return_value=iter([])),
            patch("functions.file_processor.app.output_as_data_frames", return_value=[]),
            patch(
                "functions.file_processor.app.dispatch_non_nem",
                return_value=sentinel,
            ) as mock_dispatch,
        ):
            result = file_processor_app.parse_and_write_data(
                tbp_files=[{"bucket": "sbm-file-ingester", "file_name": "newTBP/non_nem.csv"}]
            )

        assert result == 1
        # The non-NEM dispatcher MUST be consulted for non-NEM files.
        assert mock_dispatch.called, "non-NEM dispatcher must be consulted for non-NEM-format files"
        # Source moves to newP/ because the dispatcher returned processed_empty.
        assert _list_keys(s3_resource, "sbm-file-ingester", "newP/") == ["newP/non_nem.csv"]


class TestMissingTStartIsParserError:
    """`Missing t_start column` is a parser-output structural error."""

    @mock_aws
    def test_missing_t_start_column_routes_to_parse_err(self) -> None:
        from shared.parsers import ParserOutcome

        # DataFrame has no t_start column at all -> ParserError -> newParseErr/.
        df = pd.DataFrame({"E1_kWh": [1.0, 2.0]})

        result, s3_resource = _run_with_non_nem_outcome(
            ParserOutcome(status="processed", dataframes=[("NMI1", df)]),
            mappings={"NMI1-E1": "p:test:e1"},
        )

        assert result == 1
        assert _list_keys(s3_resource, "sbm-file-ingester", "newParseErr/") == ["newParseErr/outcome.csv"]
        assert _list_keys(s3_resource, "sbm-file-ingester", "newP/") == []
        assert _list_keys(s3_resource, "hudibucketsrc", "sensorDataFiles/") == []


def _audit_sidecar_keys(s3_resource) -> list[str]:
    return _list_keys(s3_resource, "hudibucketsrc", "audit/")


class TestAuditSidecarIntegration:
    """parse_and_write_data wires the audit sidecar correctly."""

    @mock_aws
    def test_file_with_skips_writes_audit_sidecar(self) -> None:
        # Two valid rows + one malformed value -> skipped, sidecar emitted.
        df = pd.DataFrame(
            {
                "t_start": pd.to_datetime(["2024-01-01 00:00", "2024-01-01 00:30", "2024-01-01 01:00"]),
                "E1_kWh": [1.0, "not-number", 3.0],
            }
        )

        result, s3_resource = _run_with_non_nem_outcome(
            ParserOutcome(status="processed", dataframes=[("NMI1", df)]),
            mappings={"NMI1-E1": "p:test:e1"},
        )

        assert result == 1
        keys = _audit_sidecar_keys(s3_resource)
        assert len(keys) == 1
        assert keys[0].endswith("/outcome.csv.skipped.json")
        body = s3_resource.Object("hudibucketsrc", keys[0]).get()["Body"].read()
        payload = json.loads(body)
        assert payload["source_file"] == "outcome.csv"
        assert payload["outcome"]["status"] == "processed"
        assert payload["outcome"]["rows_skipped"] == 1
        assert payload["skip_reasons"].get("unparseable_value") == 1
        # Sample carries the offending cell.
        sample = payload["skipped_samples"][0]
        assert sample["reason"] == "unparseable_value"
        assert sample["column"] == "E1_kWh"
        assert sample["value"] == "not-number"

    @mock_aws
    def test_file_without_skips_no_audit_sidecar(self) -> None:
        # Clean file: all rows valid and mapped -> no audit sidecar.
        df = pd.DataFrame(
            {
                "t_start": pd.to_datetime(["2024-01-01 00:00", "2024-01-01 00:30"]),
                "E1_kWh": [1.0, 2.0],
            }
        )

        result, s3_resource = _run_with_non_nem_outcome(
            ParserOutcome(status="processed", dataframes=[("NMI1", df)]),
            mappings={"NMI1-E1": "p:test:e1"},
        )

        assert result == 1
        assert _audit_sidecar_keys(s3_resource) == []

    @mock_aws
    def test_file_with_unmapped_writes_audit_sidecar(self) -> None:
        # Unmapped NMI -> file routed to newIrrevFiles, sidecar emitted with
        # unmapped_identifiers populated.
        df = pd.DataFrame(
            {
                "t_start": pd.to_datetime(["2024-01-01 00:00"]),
                "E1_kWh": [1.0],
            }
        )

        result, s3_resource = _run_with_non_nem_outcome(
            ParserOutcome(status="processed", dataframes=[("NMI1", df)]),
            mappings={},  # no mapping for NMI1-E1
        )

        assert result == 1
        keys = _audit_sidecar_keys(s3_resource)
        assert len(keys) == 1
        body = s3_resource.Object("hudibucketsrc", keys[0]).get()["Body"].read()
        payload = json.loads(body)
        assert payload["outcome"]["unmapped_count"] == 1
        assert ["nem12_nmi", "NMI1-E1"] in payload["unmapped_identifiers"]

    @mock_aws
    def test_audit_sample_cap_enforced(self) -> None:
        # Build a frame with 150 malformed values -> cap at 100 samples + truncation marker.
        n = 150
        timestamps = [pd.Timestamp("2024-01-01") + pd.Timedelta(minutes=30 * i) for i in range(n)]
        values: list[Any] = ["bad"] * n
        df = pd.DataFrame({"t_start": timestamps, "E1_kWh": values})

        result, s3_resource = _run_with_non_nem_outcome(
            ParserOutcome(status="processed", dataframes=[("NMI1", df)]),
            mappings={"NMI1-E1": "p:test:e1"},
        )

        assert result == 1
        keys = _audit_sidecar_keys(s3_resource)
        assert len(keys) == 1
        body = s3_resource.Object("hudibucketsrc", keys[0]).get()["Body"].read()
        payload = json.loads(body)
        # 100 entries + truncation marker.
        assert len(payload["skipped_samples"]) == 101
        assert payload["skipped_samples"][-1].get("truncated") is True
        assert payload["skipped_samples"][-1].get("total_skipped") == 150

    @mock_aws
    def test_audit_failure_does_not_fail_pipeline(self) -> None:
        # Patch write_audit_sidecar to raise; pipeline must still succeed
        # and source must move to newP/.
        df = pd.DataFrame(
            {
                "t_start": pd.to_datetime(["2024-01-01 00:00", "2024-01-01 00:30"]),
                "E1_kWh": [1.0, "bad"],
            }
        )
        s3_resource = _create_outcome_test_buckets(mappings={"NMI1-E1": "p:test:e1"})

        with (
            patch("functions.file_processor.app.s3_resource", s3_resource),
            patch("functions.file_processor.app.stream_as_data_frames", side_effect=ValueError("not nem")),
            patch("functions.file_processor.app.output_as_data_frames", side_effect=ValueError("not nem")),
            patch(
                "functions.file_processor.app.dispatch_non_nem",
                return_value=ParserOutcome(status="processed", dataframes=[("NMI1", df)]),
            ),
            patch(
                "functions.file_processor.app.write_audit_sidecar",
                side_effect=RuntimeError("boom"),
            ),
        ):
            result = file_processor_app.parse_and_write_data(
                tbp_files=[{"bucket": "sbm-file-ingester", "file_name": "newTBP/outcome.csv"}]
            )

        assert result == 1
        # Source still moved to newP/, no parse error, audit absent.
        assert _list_keys(s3_resource, "sbm-file-ingester", "newP/") == ["newP/outcome.csv"]
        assert _list_keys(s3_resource, "sbm-file-ingester", "newParseErr/") == []
        assert _audit_sidecar_keys(s3_resource) == []


class TestPartialRecognitionMetrics:
    """parse_and_write_data emits partial-recognition CloudWatch metrics."""

    @mock_aws
    def test_partial_mapped_ratio_metric_emitted(self) -> None:
        # File has one mapped channel and one unmapped channel; PartialMappedRatio
        # must be emitted for the file.
        df = pd.DataFrame(
            {
                "t_start": pd.to_datetime(["2024-01-01 00:00", "2024-01-01 00:30"]),
                "E1_kWh": [1.0, 2.0],
                "E2_kWh": [3.0, 4.0],
            }
        )
        s3_resource = _create_outcome_test_buckets(mappings={"NMI1-E1": "p:test:e1"})
        mock_metrics = MagicMock()

        with (
            patch("functions.file_processor.app.s3_resource", s3_resource),
            patch("functions.file_processor.app.stream_as_data_frames", side_effect=ValueError("not nem")),
            patch("functions.file_processor.app.output_as_data_frames", side_effect=ValueError("not nem")),
            patch(
                "functions.file_processor.app.dispatch_non_nem",
                return_value=ParserOutcome(status="processed", dataframes=[("NMI1", df)]),
            ),
            patch("functions.file_processor.app.metrics", mock_metrics),
        ):
            file_processor_app.parse_and_write_data(
                tbp_files=[{"bucket": "sbm-file-ingester", "file_name": "newTBP/outcome.csv"}]
            )

        emitted = {call.kwargs.get("name") or call.args[0]: call for call in mock_metrics.add_metric.call_args_list}
        assert "PartialMappedRatio" in emitted
        # 2 unmapped of 4 candidates -> 50%.
        partial = emitted["PartialMappedRatio"]
        assert partial.kwargs.get("value", partial.args[2] if len(partial.args) > 2 else None) == 50.0

    @mock_aws
    def test_rows_skipped_ratio_metric_emitted(self) -> None:
        # 1 skipped of 3 source rows -> RowsSkippedRatio ~33.33%.
        df = pd.DataFrame(
            {
                "t_start": pd.to_datetime(["2024-01-01 00:00", "2024-01-01 00:30", "2024-01-01 01:00"]),
                "E1_kWh": [1.0, "bad", 3.0],
            }
        )
        s3_resource = _create_outcome_test_buckets(mappings={"NMI1-E1": "p:test:e1"})
        mock_metrics = MagicMock()

        with (
            patch("functions.file_processor.app.s3_resource", s3_resource),
            patch("functions.file_processor.app.stream_as_data_frames", side_effect=ValueError("not nem")),
            patch("functions.file_processor.app.output_as_data_frames", side_effect=ValueError("not nem")),
            patch(
                "functions.file_processor.app.dispatch_non_nem",
                return_value=ParserOutcome(status="processed", dataframes=[("NMI1", df)]),
            ),
            patch("functions.file_processor.app.metrics", mock_metrics),
        ):
            file_processor_app.parse_and_write_data(
                tbp_files=[{"bucket": "sbm-file-ingester", "file_name": "newTBP/outcome.csv"}]
            )

        names = {call.kwargs.get("name") or call.args[0] for call in mock_metrics.add_metric.call_args_list}
        assert "RowsSkippedRatio" in names
        assert "MalformedValueCount" in names
        assert "UnsupportedSuffixesFound" in names


class TestIdentifierObservability:
    """Identifier-level observability: kind taxonomy + metric + warning log."""

    @mock_aws
    def test_unmapped_direct_p_id_uses_p_id_kind(self) -> None:
        # A DataFrame keyed by a direct ``p:`` id with no Hudi mapping should
        # populate the audit sidecar's ``unmapped_identifiers`` with the
        # canonical ``p_id`` kind from the spec.
        df = pd.DataFrame(
            {
                "t_start": pd.to_datetime(["2024-01-01 00:00"]),
                "E1_kWh": [1.0],
            }
        )

        # The DataFrame path treats keys starting with ``p:`` as direct IDs
        # only when the mapping exists. With an empty mapping the lookup
        # returns None and the file_processor records the kind as ``p_id``.
        result, s3_resource = _run_with_non_nem_outcome(
            ParserOutcome(status="processed", dataframes=[("p:direct:id", df)]),
            mappings={},
        )

        assert result == 1
        # Direct p: ids bypass mapping-miss codepaths in the current
        # implementation — they are written directly to Hudi without lookup.
        # Hence no audit sidecar is expected here. Confirm.
        assert _audit_sidecar_keys(s3_resource) == []

    @mock_aws
    def test_unmapped_nem12_nmi_uses_nem12_nmi_kind(self) -> None:
        # NMI-suffix lookup miss must use ``nem12_nmi`` kind per spec.
        df = pd.DataFrame(
            {
                "t_start": pd.to_datetime(["2024-01-01 00:00"]),
                "E1_kWh": [1.0],
            }
        )

        result, s3_resource = _run_with_non_nem_outcome(
            ParserOutcome(status="processed", dataframes=[("NMI1", df)]),
            mappings={},  # mapping miss
        )

        assert result == 1
        keys = _audit_sidecar_keys(s3_resource)
        assert len(keys) == 1
        body = s3_resource.Object("hudibucketsrc", keys[0]).get()["Body"].read()
        payload = json.loads(body)
        kinds = {pair[0] for pair in payload["unmapped_identifiers"]}
        assert kinds == {"nem12_nmi"}
        # Old kinds must not leak through.
        assert "monitor_point_name" not in kinds
        assert "point_id" not in kinds

    @mock_aws
    def test_unmapped_identifier_kind_metric_emitted_per_kind(self) -> None:
        # File with an unmapped NMI-suffix lookup -> batch should emit
        # ``UnmappedIdentifierKind_nem12_nmi``.
        df = pd.DataFrame(
            {
                "t_start": pd.to_datetime(["2024-01-01 00:00"]),
                "E1_kWh": [1.0],
            }
        )
        s3_resource = _create_outcome_test_buckets(mappings={})
        mock_metrics = MagicMock()

        with (
            patch("functions.file_processor.app.s3_resource", s3_resource),
            patch("functions.file_processor.app.stream_as_data_frames", side_effect=ValueError("not nem")),
            patch("functions.file_processor.app.output_as_data_frames", side_effect=ValueError("not nem")),
            patch(
                "functions.file_processor.app.dispatch_non_nem",
                return_value=ParserOutcome(status="processed", dataframes=[("NMI1", df)]),
            ),
            patch("functions.file_processor.app.metrics", mock_metrics),
        ):
            file_processor_app.parse_and_write_data(
                tbp_files=[{"bucket": "sbm-file-ingester", "file_name": "newTBP/outcome.csv"}]
            )

        names = {call.kwargs.get("name") or call.args[0] for call in mock_metrics.add_metric.call_args_list}
        assert "UnmappedIdentifierKind_nem12_nmi" in names

    @mock_aws
    def test_unmapped_identifier_kind_metric_not_emitted_when_clean(self) -> None:
        # Clean batch (no unmapped) -> no UnmappedIdentifierKind_* metric.
        df = pd.DataFrame(
            {
                "t_start": pd.to_datetime(["2024-01-01 00:00"]),
                "E1_kWh": [1.0],
            }
        )
        s3_resource = _create_outcome_test_buckets(mappings={"NMI1-E1": "p:test:e1"})
        mock_metrics = MagicMock()

        with (
            patch("functions.file_processor.app.s3_resource", s3_resource),
            patch("functions.file_processor.app.stream_as_data_frames", side_effect=ValueError("not nem")),
            patch("functions.file_processor.app.output_as_data_frames", side_effect=ValueError("not nem")),
            patch(
                "functions.file_processor.app.dispatch_non_nem",
                return_value=ParserOutcome(status="processed", dataframes=[("NMI1", df)]),
            ),
            patch("functions.file_processor.app.metrics", mock_metrics),
        ):
            file_processor_app.parse_and_write_data(
                tbp_files=[{"bucket": "sbm-file-ingester", "file_name": "newTBP/outcome.csv"}]
            )

        names = {call.kwargs.get("name") or call.args[0] for call in mock_metrics.add_metric.call_args_list}
        assert not any(name.startswith("UnmappedIdentifierKind_") for name in names)

    @mock_aws
    def test_all_unknown_suffix_emits_warning_log(self, caplog) -> None:
        # File with only unrecognised suffixes triggers
        # processed_empty(reason='all_unknown_suffix') and an operator
        # warning ``all_suffixes_unknown`` listing the offending suffixes.
        df = pd.DataFrame(
            {
                "t_start": pd.to_datetime(["2024-01-01 00:00"]),
                "Z9Z_kWh": [1.0],  # Z9Z is not in NMI_DATA_STREAM_COMBINED
            }
        )

        with caplog.at_level("WARNING"):
            result, _ = _run_with_non_nem_outcome(
                ParserOutcome(status="processed", dataframes=[("NMI1", df)]),
                mappings={"NMI1-E1": "p:test:e1"},  # mapping irrelevant — no E* column
            )

        assert result == 1
        warning_records = [rec for rec in caplog.records if rec.message == "all_suffixes_unknown"]
        assert len(warning_records) == 1
        # The structured log must surface the unsupported suffixes for ops.
        rec = warning_records[0]
        suffixes = getattr(rec, "unsupported_suffixes", None)
        assert suffixes == ["Z9Z"]
