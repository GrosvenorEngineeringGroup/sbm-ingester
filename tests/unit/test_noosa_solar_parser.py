"""Comprehensive tests for the Noosa Solar parser."""

import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import boto3
import pandas as pd
import pytest
from moto import mock_aws

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))


# ==================== Helpers ====================


def _create_noosa_csv(
    filepath: str,
    *,
    columns: dict[str, list[str]] | None = None,
    timestamps: list[str] | None = None,
    include_bom: bool = True,
) -> str:
    """Create a sample RACV_Noosa_Solar CSV file.

    Args:
        filepath: Path to write the CSV file.
        columns: Dict mapping column header (sensor ID) to list of values.
        timestamps: List of timestamp strings.  Defaults to three rows.
        include_bom: Whether to write a UTF-8 BOM (matches real-world files).
    """
    if timestamps is None:
        timestamps = [
            "31-Mar-26 8:00 AM AEST",
            "31-Mar-26 8:30 AM AEST",
            "31-Mar-26 9:00 AM AEST",
        ]

    if columns is None:
        columns = {
            "p:racv:r:abc123-energy": ["1.5", "2.0", "3.5"],
        }

    # Build a DataFrame and use pandas to_csv to handle quoting correctly
    data: dict[str, list[str]] = {"timestamp": timestamps}
    for col_name, col_values in columns.items():
        data[col_name] = col_values

    df = pd.DataFrame(data)
    df.to_csv(filepath, index=False, encoding="utf-8-sig" if include_bom else "utf-8")
    return filepath


# ==================== Tests ====================


class TestParseNumericColumns:
    """Tests for numeric kWh value parsing."""

    def test_parse_numeric_columns(self, tmp_path: Path) -> None:
        """Numeric kWh values parsed correctly, column name is E1_kWh."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        _create_noosa_csv(
            filepath,
            columns={"p:racv:r:sensor1": ["10.5", "20.0", "30.5"]},
        )

        with patch("shared.noosa_solar_parser.logger"):
            from shared.noosa_solar_parser import noosa_solar_parser

            result = noosa_solar_parser(filepath, "error_log")

        assert len(result) == 1
        sensor_id, df = result[0]
        assert sensor_id == "p:racv:r:sensor1"
        assert "E1_kWh" in df.columns
        assert list(df["E1_kWh"]) == [10.5, 20.0, 30.5]


class TestParseStatusColumns:
    """Tests for Fronius status string mapping."""

    def test_parse_status_columns(self, tmp_path: Path) -> None:
        """Status strings mapped to codes via FRONIUS_MODE_MAP, column name is E1_mode."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        _create_noosa_csv(
            filepath,
            columns={
                "p:racv:r:status1": [
                    "Normal Operation",
                    "Standby",
                    "Error Exists",
                ],
            },
        )

        with patch("shared.noosa_solar_parser.logger"):
            from shared.noosa_solar_parser import noosa_solar_parser

            result = noosa_solar_parser(filepath, "error_log")

        assert len(result) == 1
        sensor_id, df = result[0]
        assert sensor_id == "p:racv:r:status1"
        assert "E1_mode" in df.columns
        assert list(df["E1_mode"]) == [4.0, 8.0, 7.0]


class TestRejectsNonMatchingFile:
    """Tests for filename validation."""

    def test_rejects_non_matching_file(self, tmp_path: Path) -> None:
        """Non-RACV_Noosa_Solar files raise exception."""
        filepath = str(tmp_path / "some_other_file.csv")
        _create_noosa_csv(filepath)

        with patch("shared.noosa_solar_parser.logger"):
            from shared.noosa_solar_parser import noosa_solar_parser

            with pytest.raises(Exception, match="Not a Noosa Solar file"):
                noosa_solar_parser(filepath, "error_log")


class TestTimestampParsing:
    """Tests for timestamp format handling."""

    def test_timestamp_parsing(self, tmp_path: Path) -> None:
        """'31-Mar-26 8:00 AM AEST' parsed correctly."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        _create_noosa_csv(
            filepath,
            timestamps=["31-Mar-26 8:00 AM AEST"],
            columns={"p:racv:r:s1": ["5.0"]},
        )

        with patch("shared.noosa_solar_parser.logger"):
            from shared.noosa_solar_parser import noosa_solar_parser

            result = noosa_solar_parser(filepath, "error_log")

        _, df = result[0]
        ts = df.index[0]
        assert ts == pd.Timestamp("2026-03-31 08:00:00")


class TestTimezoneWarning:
    """Tests for non-AEST timezone handling."""

    def test_timezone_warning(self, tmp_path: Path) -> None:
        """Non-AEST timezone logs warning but still parses."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        _create_noosa_csv(
            filepath,
            timestamps=[
                "31-Mar-26 8:00 AM AEDT",
                "31-Mar-26 8:30 AM AEDT",
                "31-Mar-26 9:00 AM AEDT",
            ],
            columns={"p:racv:r:s1": ["1.0", "2.0", "3.0"]},
        )

        mock_log = MagicMock()
        with patch("shared.noosa_solar_parser.logger", mock_log):
            from shared.noosa_solar_parser import noosa_solar_parser

            result = noosa_solar_parser(filepath, "error_log")

        # Should still return valid data
        assert len(result) == 1
        # Should have logged a warning
        mock_log.warning.assert_called_once()
        call_kwargs = mock_log.warning.call_args
        assert "Unexpected timezone" in call_kwargs[0][0]


class TestNanValuesDropped:
    """Tests for NaN value filtering."""

    def test_nan_values_dropped(self, tmp_path: Path) -> None:
        """NaN and empty values filtered out."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        _create_noosa_csv(
            filepath,
            timestamps=[
                "31-Mar-26 8:00 AM AEST",
                "31-Mar-26 8:30 AM AEST",
                "31-Mar-26 9:00 AM AEST",
                "31-Mar-26 9:30 AM AEST",
            ],
            columns={"p:racv:r:s1": ["1.0", "nan", "3.0", "nan"]},
        )

        with patch("shared.noosa_solar_parser.logger"):
            from shared.noosa_solar_parser import noosa_solar_parser

            result = noosa_solar_parser(filepath, "error_log")

        _, df = result[0]
        assert len(df) == 2
        assert list(df["E1_kWh"]) == [1.0, 3.0]


class TestMixedEmptyAndNanValues:
    """Tests for both empty cells and nan strings."""

    def test_mixed_empty_and_nan_values(self, tmp_path: Path) -> None:
        """Both empty cells (,,) and 'nan' strings are handled identically (dropped)."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        # Write manually to include truly empty cells
        content = (
            "timestamp,p:racv:r:s1\n"
            "31-Mar-26 8:00 AM AEST,1.0\n"
            "31-Mar-26 8:30 AM AEST,\n"
            "31-Mar-26 9:00 AM AEST,nan\n"
            "31-Mar-26 9:30 AM AEST,5.0\n"
        )
        Path(filepath).write_text(content, encoding="utf-8-sig")

        with patch("shared.noosa_solar_parser.logger"):
            from shared.noosa_solar_parser import noosa_solar_parser

            result = noosa_solar_parser(filepath, "error_log")

        _, df = result[0]
        # Both empty and nan rows should be dropped
        assert len(df) == 2
        assert list(df["E1_kWh"]) == [1.0, 5.0]


class TestUnknownStatusWarning:
    """Tests for unknown Fronius status strings."""

    def test_unknown_status_warning(self, tmp_path: Path) -> None:
        """Unknown status strings logged as warning and dropped."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        _create_noosa_csv(
            filepath,
            columns={
                "p:racv:r:status1": [
                    "Normal Operation",
                    "TotallyUnknownStatus",
                    "Error Exists",
                ],
            },
        )

        mock_log = MagicMock()
        with patch("shared.noosa_solar_parser.logger", mock_log):
            from shared.noosa_solar_parser import noosa_solar_parser

            result = noosa_solar_parser(filepath, "error_log")

        # Unknown status should be dropped (mapped to NaN then dropped)
        _, df = result[0]
        assert len(df) == 2
        assert list(df["E1_mode"]) == [4.0, 7.0]

        # Should have logged a warning about unknown values
        mock_log.warning.assert_called_once()
        call_kwargs = mock_log.warning.call_args
        assert "Unknown Fronius mode" in call_kwargs[0][0]


class TestAllColumnsReturnTStart:
    """Tests for DataFrame index format."""

    def test_all_columns_return_t_start(self, tmp_path: Path) -> None:
        """All returned DataFrames have t_start as index."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        _create_noosa_csv(
            filepath,
            columns={
                "p:racv:r:energy1": ["1.0", "2.0", "3.0"],
                "p:racv:r:status1": [
                    "Normal Operation",
                    "Standby",
                    "Error Exists",
                ],
            },
        )

        with patch("shared.noosa_solar_parser.logger"):
            from shared.noosa_solar_parser import noosa_solar_parser

            result = noosa_solar_parser(filepath, "error_log")

        assert len(result) == 2
        for _, df in result:
            assert df.index.name == "t_start"


class TestSensorIdFormat:
    """Tests for sensor ID prefix format."""

    def test_sensor_id_format(self, tmp_path: Path) -> None:
        """All returned identifiers start with 'p:racv:r:'."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        _create_noosa_csv(
            filepath,
            columns={
                "p:racv:r:abc-energy": ["1.0", "2.0", "3.0"],
                "p:racv:r:def-status": [
                    "Normal Operation",
                    "Standby",
                    "Error Exists",
                ],
            },
        )

        with patch("shared.noosa_solar_parser.logger"):
            from shared.noosa_solar_parser import noosa_solar_parser

            result = noosa_solar_parser(filepath, "error_log")

        for sensor_id, _ in result:
            assert sensor_id.startswith("p:racv:r:")


class TestEmptyFile:
    """Tests for empty file handling."""

    def test_empty_file(self, tmp_path: Path) -> None:
        """File with only headers raises exception."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        content = "timestamp,p:racv:r:s1\n"
        Path(filepath).write_text(content, encoding="utf-8-sig")

        with patch("shared.noosa_solar_parser.logger"):
            from shared.noosa_solar_parser import noosa_solar_parser

            with pytest.raises(Exception, match="No valid data"):
                noosa_solar_parser(filepath, "error_log")


class TestMissingTimestampColumn:
    """Tests for missing timestamp column validation."""

    def test_missing_timestamp_column(self, tmp_path: Path) -> None:
        """File with wrong first column name raises exception."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        content = "wrong_header,p:racv:r:s1\n31-Mar-26 8:00 AM AEST,1.0\n"
        Path(filepath).write_text(content, encoding="utf-8-sig")

        with patch("shared.noosa_solar_parser.logger"):
            from shared.noosa_solar_parser import noosa_solar_parser

            with pytest.raises(Exception, match="Missing timestamp column"):
                noosa_solar_parser(filepath, "error_log")


class TestAllNanColumnSkipped:
    """Tests for columns with all NaN values."""

    def test_all_nan_column_skipped(self, tmp_path: Path) -> None:
        """Column where every value is NaN is excluded from results."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        _create_noosa_csv(
            filepath,
            columns={
                "p:racv:r:good": ["1.0", "2.0", "3.0"],
                "p:racv:r:bad": ["nan", "nan", "nan"],
            },
        )

        with patch("shared.noosa_solar_parser.logger"):
            from shared.noosa_solar_parser import noosa_solar_parser

            result = noosa_solar_parser(filepath, "error_log")

        # Only the good column should be in results
        sensor_ids = [sid for sid, _ in result]
        assert "p:racv:r:good" in sensor_ids
        assert "p:racv:r:bad" not in sensor_ids


class TestAllZeroColumnPreserved:
    """Tests for columns with all zero values."""

    def test_all_zero_column_preserved(self, tmp_path: Path) -> None:
        """Column with all 0 values is correctly classified as numeric and preserved."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        _create_noosa_csv(
            filepath,
            columns={"p:racv:r:zeros": ["0", "0", "0"]},
        )

        with patch("shared.noosa_solar_parser.logger"):
            from shared.noosa_solar_parser import noosa_solar_parser

            result = noosa_solar_parser(filepath, "error_log")

        assert len(result) == 1
        sensor_id, df = result[0]
        assert sensor_id == "p:racv:r:zeros"
        assert "E1_kWh" in df.columns
        assert list(df["E1_kWh"]) == [0.0, 0.0, 0.0]


class TestMultipleStatusValues:
    """Tests for multiple different status strings in one column."""

    def test_multiple_status_values(self, tmp_path: Path) -> None:
        """Column with mixed status strings all mapped correctly."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        statuses = [
            "Off",
            "In Operation, No Feed In",
            "Run Up Phase",
            "Normal Operation",
            "Power Reduction",
            "Switch Off Phase",
            "Error Exists",
            "Standby",
            "No Fronius Solar Net Comm",
            "No Comm with Inverter",
            "Overcurrent detected in Fronius Solar Net",
            "Inverter Update being Processed",
            "AFCI Event",
        ]
        # Generate 13 valid 12-hour timestamps (1:00 AM through 1:00 PM)
        timestamps = []
        for i in range(13):
            hour = i + 1  # 1..13
            if hour <= 11:
                timestamps.append(f"31-Mar-26 {hour}:00 AM AEST")
            elif hour == 12:
                timestamps.append("31-Mar-26 12:00 PM AEST")
            else:
                timestamps.append(f"31-Mar-26 {hour - 12}:00 PM AEST")

        _create_noosa_csv(
            filepath,
            timestamps=timestamps,
            columns={"p:racv:r:all_statuses": statuses},
        )

        with patch("shared.noosa_solar_parser.logger"):
            from shared.noosa_solar_parser import noosa_solar_parser

            result = noosa_solar_parser(filepath, "error_log")

        assert len(result) == 1
        _, df = result[0]
        assert "E1_mode" in df.columns
        expected_codes = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0]
        assert list(df["E1_mode"]) == expected_codes


class TestDispatcherIntegration:
    """Tests for get_non_nem_df routing to the Noosa Solar parser."""

    def test_dispatcher_integration(self, tmp_path: Path) -> None:
        """get_non_nem_df() correctly routes RACV_Noosa_Solar files to the Noosa parser."""
        filepath = str(tmp_path / "RACV_Noosa_Solar_20260331.csv")
        _create_noosa_csv(
            filepath,
            columns={"p:racv:r:s1": ["10.0", "20.0", "30.0"]},
        )

        with patch("shared.noosa_solar_parser.logger"), patch("shared.non_nem_parsers.logger"):
            from shared.non_nem_parsers import get_non_nem_df

            result = get_non_nem_df(filepath, "error_log")

        assert len(result) == 1
        sensor_id, df = result[0]
        assert sensor_id == "p:racv:r:s1"
        assert "E1_kWh" in df.columns


class TestPPrefixBypassInFileProcessor:
    """Integration test: sensors with 'p:' prefix bypass Neptune mapping."""

    @mock_aws
    def test_p_prefix_bypass_in_file_processor(self, tmp_path: Path) -> None:
        """Integration: p: prefix sensors bypass Neptune mapping."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

        s3_resource = boto3.resource("s3", region_name="ap-southeast-2")
        s3_resource.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )
        s3_resource.create_bucket(
            Bucket="hudibucketsrc",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
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

        # Upload empty mappings -- the p: sensor should still be processed
        s3_resource.Object("sbm-file-ingester", "nem12_mappings.json").put(Body=json.dumps({}))

        # Create a Noosa Solar CSV and upload to S3
        local_csv = str(tmp_path / "RACV_Noosa_Solar_20260331.csv")
        _create_noosa_csv(
            local_csv,
            columns={"p:racv:r:abc123-energy": ["1.0", "2.0", "3.0"]},
        )
        csv_content = Path(local_csv).read_bytes()
        s3_resource.Object("sbm-file-ingester", "newTBP/RACV_Noosa_Solar_20260331.csv").put(Body=csv_content)

        with patch("functions.file_processor.app.s3_resource", s3_resource):
            from functions.file_processor.app import parse_and_write_data

            files = [{"bucket": "sbm-file-ingester", "file_name": "newTBP/RACV_Noosa_Solar_20260331.csv"}]
            result = parse_and_write_data(tbp_files=files)

        assert result == 1

        # Verify data was written to hudibucketsrc (bypassed Neptune lookup)
        hudi_bucket = s3_resource.Bucket("hudibucketsrc")
        sensor_files = list(hudi_bucket.objects.filter(Prefix="sensorDataFiles/"))
        assert len(sensor_files) >= 1

        # Verify the CSV content uses the raw p: sensor ID
        csv_output = sensor_files[0].get()["Body"].read().decode("utf-8")
        assert "p:racv:r:abc123-energy" in csv_output

        # File should be moved to processed (not irrelevant)
        ingester_bucket = s3_resource.Bucket("sbm-file-ingester")
        processed_files = list(ingester_bucket.objects.filter(Prefix="newP/"))
        assert len(processed_files) == 1
