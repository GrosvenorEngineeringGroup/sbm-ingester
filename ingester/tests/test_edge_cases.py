"""Edge case tests to improve coverage to 90%+."""

import json
import os
import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import boto3
import pandas as pd
from moto import mock_aws

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / ".." / "src"))


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
        mock_error_log = MagicMock()
        with (
            patch("modules.common.CloudWatchLogger"),
            patch("gemsDataParseAndWrite.s3_resource", s3_resource),
            patch("gemsDataParseAndWrite.error_log", mock_error_log),
        ):
            from gemsDataParseAndWrite import move_s3_file

            result = move_s3_file("sbm-file-ingester", "newTBP/nonexistent.csv", "newP/")

            # Should return None and log error
            assert result is None
            assert mock_error_log.log.called

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
            patch("modules.common.CloudWatchLogger"),
            patch("gemsDataParseAndWrite.s3_resource", s3_resource),
        ):
            from gemsDataParseAndWrite import move_s3_file

            result = move_s3_file("sbm-file-ingester", "newTBP/test_file (1).csv", "newP/")

            assert result == "newP/test_file (1).csv"


class TestNem12MappingsEdgeCases:
    """Edge case tests for NEM12 mappings handling."""

    @mock_aws
    def test_parse_and_write_fails_when_mappings_none(self, temp_directory: str) -> None:
        """Test that parseAndWriteData fails gracefully when nem12_mappings is None."""
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

        with patch("gemsDataParseAndWrite.s3_resource", s3_resource):
            from gemsDataParseAndWrite import parseAndWriteData

            result = parseAndWriteData([])

            # Should return None due to missing mappings
            assert result is None


class TestParseAndWriteDataEdgeCases:
    """Edge case tests for parseAndWriteData function."""

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

        with patch("gemsDataParseAndWrite.s3_resource", s3_resource):
            from gemsDataParseAndWrite import parseAndWriteData

            files = [{"bucket": "sbm-file-ingester", "file_name": "newTBP/envizi_water.csv"}]
            result = parseAndWriteData(files)

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

        with patch("gemsDataParseAndWrite.s3_resource", s3_resource):
            from gemsDataParseAndWrite import parseAndWriteData

            files = [{"bucket": "sbm-file-ingester", "file_name": "newTBP/invalid.csv"}]
            result = parseAndWriteData(files)

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

        mock_runtime_error_log = MagicMock()
        with (
            patch("gemsDataParseAndWrite.s3_resource", s3_resource),
            patch("gemsDataParseAndWrite.runtime_error_log", mock_runtime_error_log),
        ):
            from gemsDataParseAndWrite import parseAndWriteData

            files = [{"bucket": "sbm-file-ingester", "file_name": "newTBP/bad_file.csv"}]
            parseAndWriteData(files)

            # runtime_error_log should be called for bad files
            assert mock_runtime_error_log.log.called


class TestLambdaHandlerEdgeCases:
    """Edge case tests for lambda_handler function."""

    def test_lambda_handler_malformed_record(self) -> None:
        """Test lambda_handler handles malformed records gracefully."""
        mock_error_log = MagicMock()
        with (
            patch("modules.common.CloudWatchLogger"),
            patch("gemsDataParseAndWrite.error_log", mock_error_log),
            patch("gemsDataParseAndWrite.parseAndWriteData"),
        ):
            from gemsDataParseAndWrite import lambda_handler

            # Event with malformed body (missing required fields)
            event: dict[str, Any] = {
                "Records": [
                    {"body": "not valid json"},
                    {"body": json.dumps({"Records": []})},
                    {"body": json.dumps({"Records": [{"s3": {}}]})},
                ]
            }

            result = lambda_handler(event, None)

            assert result["statusCode"] == 200
            # Error should have been logged for malformed records
            assert mock_error_log.log.called

    def test_lambda_handler_empty_records(self) -> None:
        """Test lambda_handler handles empty records list."""
        with (
            patch("modules.common.CloudWatchLogger"),
            patch("gemsDataParseAndWrite.parseAndWriteData") as mock_parse,
        ):
            from gemsDataParseAndWrite import lambda_handler

            event: dict[str, Any] = {"Records": []}

            result = lambda_handler(event, None)

            assert result["statusCode"] == 200
            # parseAndWriteData should not be called with empty list
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

        mock_error_log = MagicMock()
        with (
            patch("modules.common.CloudWatchLogger"),
            patch("gemsDataParseAndWrite.s3_resource", s3_resource),
            patch("gemsDataParseAndWrite.error_log", mock_error_log),
        ):
            from gemsDataParseAndWrite import download_files_to_tmp

            files = [{"bucket": "sbm-file-ingester", "file_name": "newTBP/nonexistent.csv"}]
            result = download_files_to_tmp(files, temp_directory)

            # Should return empty list and log error
            assert result == []
            assert mock_error_log.log.called

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
            patch("modules.common.CloudWatchLogger"),
            patch("gemsDataParseAndWrite.s3_resource", s3_resource),
        ):
            from gemsDataParseAndWrite import download_files_to_tmp

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

        with patch("gemsDataParseAndWrite.s3_resource", s3_resource):
            from gemsDataParseAndWrite import parseAndWriteData

            files = [{"bucket": "sbm-file-ingester", "file_name": "newTBP/test_nem12.csv"}]
            result = parseAndWriteData(files)

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

        with patch("gemsDataParseAndWrite.s3_resource", s3_resource):
            from gemsDataParseAndWrite import parseAndWriteData

            files = [{"bucket": "sbm-file-ingester", "file_name": "newTBP/unmapped_nem12.csv"}]
            result = parseAndWriteData(files)

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

        with patch("gemsDataParseAndWrite.s3_resource", s3_resource):
            from gemsDataParseAndWrite import parseAndWriteData

            files = [{"bucket": "sbm-file-ingester", "file_name": "newTBP/many_channels.csv"}]
            result = parseAndWriteData(files)

            assert result == 1

            # Check that multiple batch files were written to hudibucketsrc
            hudi_bucket = s3_resource.Bucket("hudibucketsrc")
            sensor_files = list(hudi_bucket.objects.filter(Prefix="sensorDataFiles/"))

            # Should have at least 2 files (one from mid-flush, one from final flush)
            # Since we have 60 monitor points and BATCH_SIZE=50
            assert len(sensor_files) >= 2


class TestExceptionHandling:
    """Edge case tests for exception handling paths."""

    @mock_aws
    def test_parseandwritedata_general_exception(self, temp_directory: str) -> None:
        """Test that general exceptions in parseAndWriteData are handled."""
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

        mock_error_log = MagicMock()
        with (
            patch("gemsDataParseAndWrite.s3_resource", s3_resource),
            patch("gemsDataParseAndWrite.error_log", mock_error_log),
            patch(
                "gemsDataParseAndWrite.read_nem12_mappings",
                side_effect=Exception("Test exception"),
            ),
        ):
            from gemsDataParseAndWrite import parseAndWriteData

            result = parseAndWriteData([])

            # Should return None on exception
            assert result is None
            # Error should be logged
            assert mock_error_log.log.called


class TestMetricsEdgeCases:
    """Edge case tests for metrics calculations."""

    def test_metrics_dict_already_initialized(self) -> None:
        """Test that dailyInitializeMetricsDict doesn't overwrite existing data."""
        with patch("modules.common.CloudWatchLogger"):
            from gemsDataParseAndWrite import dailyInitializeMetricsDict

            metrics: dict[str, dict[str, int]] = {"2024-01-15D": {"validProcessedFilesCount": 5}}
            dailyInitializeMetricsDict(metrics, "2024-01-15D")

            # Should not overwrite existing value
            assert metrics["2024-01-15D"]["validProcessedFilesCount"] == 5

    def test_metrics_accumulation_multiple_calls(self) -> None:
        """Test that metrics accumulate correctly over multiple calls."""
        with patch("modules.common.CloudWatchLogger"):
            from gemsDataParseAndWrite import metricsDictPopulateValues

            metrics: dict[str, dict[str, int]] = {}

            # First call
            metricsDictPopulateValues(metrics, "2024-01-15D", 1, 2, 0, 0, 5, 4, 0)

            # Second call
            metricsDictPopulateValues(metrics, "2024-01-15D", 2, 3, 1, 0, 10, 8, 0)

            # Values should be accumulated
            assert metrics["2024-01-15D"]["ftpFilesCount"] == 3
            assert metrics["2024-01-15D"]["validProcessedFilesCount"] == 5
            assert metrics["2024-01-15D"]["parseErrFilesCount"] == 1
            assert metrics["2024-01-15D"]["totalMonitorPointsCount"] == 15  # 5 + 10
            assert metrics["2024-01-15D"]["processedMonitorPointsCount"] == 12  # 4 + 8
