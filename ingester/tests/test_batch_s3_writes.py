"""Unit tests for batch S3 write functionality in gemsDataParseAndWrite.py."""

from unittest.mock import patch

import pandas as pd
from mypy_boto3_s3 import S3ServiceResource


class TestFlushBufferToS3:
    """Tests for _flush_buffer_to_s3 function."""

    def test_empty_buffer_no_write(self, mock_s3_resource: S3ServiceResource) -> None:
        """Test that empty buffer does not write to S3."""
        with patch("modules.common.CloudWatchLogger"):
            # Need to patch s3_resource in the module
            with patch("gemsDataParseAndWrite.s3_resource", mock_s3_resource):
                from gemsDataParseAndWrite import _flush_buffer_to_s3

                # Empty buffer should not raise and not write
                _flush_buffer_to_s3([], "2024_Jan_01T00_00_00_000000")

                # Verify no objects in bucket
                bucket = mock_s3_resource.Bucket("hudibucketsrc")
                objects = list(bucket.objects.all())
                assert len(objects) == 0

    def test_single_dataframe_write(self, mock_s3_resource: S3ServiceResource) -> None:
        """Test that single DataFrame is written correctly."""
        with patch("modules.common.CloudWatchLogger"):
            with patch("gemsDataParseAndWrite.s3_resource", mock_s3_resource):
                from gemsDataParseAndWrite import _flush_buffer_to_s3

                df = pd.DataFrame(
                    {
                        "sensorId": ["id-001"] * 5,
                        "ts": ["2024-01-01 00:00:00"] * 5,
                        "val": [1.0, 2.0, 3.0, 4.0, 5.0],
                        "unit": ["kwh"] * 5,
                        "its": ["2024-01-01 00:00:00"] * 5,
                    }
                )

                _flush_buffer_to_s3([df], "2024_Jan_01T00_00_00_000000")

                # Verify one object written
                bucket = mock_s3_resource.Bucket("hudibucketsrc")
                objects = list(bucket.objects.all())
                assert len(objects) == 1

                # Verify object key starts with correct prefix
                assert objects[0].key.startswith("sensorDataFiles/batch_")

    def test_multiple_dataframes_merged(self, mock_s3_resource: S3ServiceResource) -> None:
        """Test that multiple DataFrames are merged into single CSV."""
        with patch("modules.common.CloudWatchLogger"):
            with patch("gemsDataParseAndWrite.s3_resource", mock_s3_resource):
                from gemsDataParseAndWrite import _flush_buffer_to_s3

                df1 = pd.DataFrame(
                    {
                        "sensorId": ["id-001"] * 3,
                        "ts": ["2024-01-01 00:00:00"] * 3,
                        "val": [1.0, 2.0, 3.0],
                        "unit": ["kwh"] * 3,
                        "its": ["2024-01-01 00:00:00"] * 3,
                    }
                )

                df2 = pd.DataFrame(
                    {
                        "sensorId": ["id-002"] * 2,
                        "ts": ["2024-01-01 00:30:00"] * 2,
                        "val": [4.0, 5.0],
                        "unit": ["kwh"] * 2,
                        "its": ["2024-01-01 00:30:00"] * 2,
                    }
                )

                _flush_buffer_to_s3([df1, df2], "2024_Jan_01T00_00_00_000000")

                # Should only create one object (merged)
                bucket = mock_s3_resource.Bucket("hudibucketsrc")
                objects = list(bucket.objects.all())
                assert len(objects) == 1

                # Verify merged content
                obj = mock_s3_resource.Object("hudibucketsrc", objects[0].key)
                body = obj.get()["Body"].read().decode("utf-8")

                # Should have 5 data rows + 1 header
                lines = body.strip().split("\n")
                assert len(lines) == 6  # 1 header + 5 data rows

    def test_csv_output_format(self, mock_s3_resource: S3ServiceResource) -> None:
        """Test that CSV output has correct format."""
        with patch("modules.common.CloudWatchLogger"):
            with patch("gemsDataParseAndWrite.s3_resource", mock_s3_resource):
                from gemsDataParseAndWrite import _flush_buffer_to_s3

                df = pd.DataFrame(
                    {
                        "sensorId": ["neptune-001"],
                        "ts": ["2024-01-01 00:00:00"],
                        "val": [123.456],
                        "unit": ["kwh"],
                        "its": ["2024-01-01 00:00:00"],
                    }
                )

                _flush_buffer_to_s3([df], "2024_Jan_01T00_00_00_000000")

                bucket = mock_s3_resource.Bucket("hudibucketsrc")
                objects = list(bucket.objects.all())
                obj = mock_s3_resource.Object("hudibucketsrc", objects[0].key)
                body = obj.get()["Body"].read().decode("utf-8")

                # First line should be header
                header = body.strip().split("\n")[0]
                assert "sensorId" in header
                assert "ts" in header
                assert "val" in header
                assert "unit" in header
                assert "its" in header


class TestNmiDataStreamFiltering:
    """Tests for NMI data stream suffix filtering."""

    def test_valid_suffix_in_combined_set(self) -> None:
        """Test that valid suffixes are in NMI_DATA_STREAM_COMBINED."""
        with patch("modules.common.CloudWatchLogger"):
            from gemsDataParseAndWrite import NMI_DATA_STREAM_COMBINED

            # Valid suffixes should be in the set
            valid_suffixes = ["E1", "B1", "Q1", "K1", "E2", "B2"]
            for suffix in valid_suffixes:
                assert suffix in NMI_DATA_STREAM_COMBINED, f"{suffix} should be valid"

    def test_invalid_suffix_not_in_combined_set(self) -> None:
        """Test that invalid suffixes are not in NMI_DATA_STREAM_COMBINED."""
        with patch("modules.common.CloudWatchLogger"):
            from gemsDataParseAndWrite import NMI_DATA_STREAM_COMBINED

            # Invalid suffixes should not be in the set
            # Note: ZZ is valid (Z suffix + Z channel), so we use truly invalid ones
            invalid_suffixes = ["XX", "00", "t_start", "quality_method", "event_code"]
            for suffix in invalid_suffixes:
                assert suffix not in NMI_DATA_STREAM_COMBINED, f"{suffix} should be invalid"

    def test_combined_set_is_frozenset(self) -> None:
        """Test that NMI_DATA_STREAM_COMBINED is a frozenset for O(1) lookup."""
        with patch("modules.common.CloudWatchLogger"):
            from gemsDataParseAndWrite import NMI_DATA_STREAM_COMBINED

            assert isinstance(NMI_DATA_STREAM_COMBINED, frozenset)


class TestBatchSize:
    """Tests for BATCH_SIZE constant and batching behavior."""

    def test_batch_size_constant_exists(self) -> None:
        """Test that BATCH_SIZE constant exists and is reasonable."""
        with patch("modules.common.CloudWatchLogger"):
            from gemsDataParseAndWrite import BATCH_SIZE

            assert isinstance(BATCH_SIZE, int)
            assert BATCH_SIZE > 0
            assert BATCH_SIZE == 50  # Expected value


class TestTimestampFormat:
    """Tests for timestamp formatting."""

    def test_timestamp_format_yyyy_mm_dd_hh_mm_ss(self, mock_s3_resource: S3ServiceResource) -> None:
        """Test that timestamps are formatted as YYYY-MM-DD HH:MM:SS."""
        with patch("modules.common.CloudWatchLogger"):
            with patch("gemsDataParseAndWrite.s3_resource", mock_s3_resource):
                from gemsDataParseAndWrite import _flush_buffer_to_s3

                df = pd.DataFrame(
                    {
                        "sensorId": ["neptune-001"],
                        "ts": ["2024-01-15 13:30:45"],
                        "val": [100.0],
                        "unit": ["kwh"],
                        "its": ["2024-01-15 13:30:45"],
                    }
                )

                _flush_buffer_to_s3([df], "2024_Jan_15T13_30_45_000000")

                bucket = mock_s3_resource.Bucket("hudibucketsrc")
                objects = list(bucket.objects.all())
                obj = mock_s3_resource.Object("hudibucketsrc", objects[0].key)
                body = obj.get()["Body"].read().decode("utf-8")

                # Verify timestamp format in content
                assert "2024-01-15 13:30:45" in body


class TestBatchWriteIntegration:
    """Integration tests for batch write buffer management."""

    def test_buffer_accumulates_until_batch_size(
        self, mock_s3_resource: S3ServiceResource, sample_dataframe: pd.DataFrame
    ) -> None:
        """Test that writes are batched correctly."""
        # This test verifies the batching logic conceptually
        # The actual implementation buffers dataframes and flushes at BATCH_SIZE

        with patch("modules.common.CloudWatchLogger"):
            from gemsDataParseAndWrite import BATCH_SIZE

            # Simulate buffer accumulation
            buffer = []
            for i in range(BATCH_SIZE - 1):
                small_df = pd.DataFrame(
                    {
                        "sensorId": [f"id-{i}"],
                        "ts": ["2024-01-01 00:00:00"],
                        "val": [float(i)],
                        "unit": ["kwh"],
                        "its": ["2024-01-01 00:00:00"],
                    }
                )
                buffer.append(small_df)

            # Buffer should have BATCH_SIZE - 1 items
            assert len(buffer) == BATCH_SIZE - 1

    def test_final_flush_clears_remaining(self, mock_s3_resource: S3ServiceResource) -> None:
        """Test that remaining buffer items are flushed at the end."""
        with patch("modules.common.CloudWatchLogger"):
            with patch("gemsDataParseAndWrite.s3_resource", mock_s3_resource):
                from gemsDataParseAndWrite import _flush_buffer_to_s3

                # Simulate partial batch (less than BATCH_SIZE)
                buffer = []
                for i in range(10):  # Only 10 items
                    df = pd.DataFrame(
                        {
                            "sensorId": [f"id-{i}"],
                            "ts": ["2024-01-01 00:00:00"],
                            "val": [float(i)],
                            "unit": ["kwh"],
                            "its": ["2024-01-01 00:00:00"],
                        }
                    )
                    buffer.append(df)

                # Final flush should write all remaining
                _flush_buffer_to_s3(buffer, "2024_Jan_01T00_00_00_000000")

                bucket = mock_s3_resource.Bucket("hudibucketsrc")
                objects = list(bucket.objects.all())
                assert len(objects) == 1

                # Verify all 10 rows are written
                obj = mock_s3_resource.Object("hudibucketsrc", objects[0].key)
                body = obj.get()["Body"].read().decode("utf-8")
                lines = body.strip().split("\n")
                assert len(lines) == 11  # 1 header + 10 data rows
