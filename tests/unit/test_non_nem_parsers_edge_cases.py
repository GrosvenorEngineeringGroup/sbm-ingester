"""Edge case tests for non_nem_parsers.py to improve coverage."""

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))


class TestEnviziVerticalParserWaterBulk:
    """Tests for envizi_vertical_parser_water_bulk function."""

    def test_parses_bulk_water_data_correctly(self, temp_directory: str) -> None:
        """Test that bulk water data with Date_Time column is parsed correctly."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.non_nem_parsers import envizi_vertical_parser_water_bulk

            # Create CSV with Date_Time column (bulk format)
            filepath = str(Path(temp_directory) / "water_bulk.csv")
            df = pd.DataFrame(
                {
                    "Serial_No": ["12345", "12345", "67890"],
                    "Date_Time": ["2024-01-01 00:00:00", "2024-01-01 01:00:00", "2024-01-01 00:00:00"],
                    "kL": [1.5, 2.0, 3.0],
                }
            )
            df.to_csv(filepath, index=False)

            result = envizi_vertical_parser_water_bulk(filepath, "error_log")

            assert isinstance(result, list)
            assert len(result) == 2  # Two unique serial numbers

            nmis = [nmi for nmi, _ in result]
            assert "Envizi_12345" in nmis
            assert "Envizi_67890" in nmis

            # Check column naming
            _, df_result = result[0]
            assert "E1_kL" in df_result.columns

    def test_bulk_water_rejects_optima_generation_file(self, temp_directory: str) -> None:
        """Test that OptimaGenerationData files are rejected by bulk water parser."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.non_nem_parsers import envizi_vertical_parser_water_bulk

            # Create file with OptimaGenerationData in name
            filepath = str(Path(temp_directory) / "OptimaGenerationData_water.csv")
            df = pd.DataFrame(
                {
                    "Serial_No": ["12345"],
                    "Date_Time": ["2024-01-01 00:00:00"],
                    "kL": [1.5],
                }
            )
            df.to_csv(filepath, index=False)

            with pytest.raises(Exception, match="Not Relevant Parser"):
                envizi_vertical_parser_water_bulk(filepath, "error_log")

    def test_bulk_water_handles_multiple_meters(self, temp_directory: str) -> None:
        """Test that bulk water parser handles multiple meters correctly."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.non_nem_parsers import envizi_vertical_parser_water_bulk

            filepath = str(Path(temp_directory) / "multi_meter_bulk.csv")
            df = pd.DataFrame(
                {
                    "Serial_No": ["111", "222", "333", "111", "222"],
                    "Date_Time": [
                        "2024-01-01 00:00:00",
                        "2024-01-01 00:00:00",
                        "2024-01-01 00:00:00",
                        "2024-01-01 01:00:00",
                        "2024-01-01 01:00:00",
                    ],
                    "kL": [1.0, 2.0, 3.0, 1.5, 2.5],
                }
            )
            df.to_csv(filepath, index=False)

            result = envizi_vertical_parser_water_bulk(filepath, "error_log")

            assert len(result) == 3
            nmis = sorted([nmi for nmi, _ in result])
            assert nmis == ["Envizi_111", "Envizi_222", "Envizi_333"]


class TestOptimaUsageAndSpendToS3:
    """Tests for optima_usage_and_spend_to_s3 function."""

    def test_rejects_optima_generation_file(self, temp_directory: str) -> None:
        """Test that OptimaGenerationData files are rejected."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.non_nem_parsers import optima_usage_and_spend_to_s3

            # File with OptimaGenerationData in name
            filepath = str(Path(temp_directory) / "OptimaGenerationData.csv")
            Path(filepath).write_text("dummy content")

            with pytest.raises(Exception, match="Not Relevant Parser"):
                optima_usage_and_spend_to_s3(filepath, "error_log")

    def test_rejects_non_racv_usage_file(self, temp_directory: str) -> None:
        """Test that non-RACV Usage and Spend files are rejected."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.non_nem_parsers import optima_usage_and_spend_to_s3

            # File without "RACV-Usage and Spend Report" in name
            filepath = str(Path(temp_directory) / "other_report.csv")
            Path(filepath).write_text("dummy content")

            with pytest.raises(Exception, match="Not Valid Optima Usage And Spend File"):
                optima_usage_and_spend_to_s3(filepath, "error_log")

    @pytest.fixture
    def aws_env(self) -> None:
        """Set up AWS environment variables."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

    def test_uploads_racv_usage_and_spend_file(self, temp_directory: str, aws_env: None) -> None:
        """Test that valid RACV Usage and Spend files are uploaded to S3."""
        import boto3
        from moto import mock_aws

        with mock_aws():
            # Create the target bucket
            s3 = boto3.client("s3", region_name="ap-southeast-2")
            s3.create_bucket(
                Bucket="gegoptimareports", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"}
            )

            with patch("shared.non_nem_parsers.logger"):
                from shared.non_nem_parsers import optima_usage_and_spend_to_s3

                # Create file with correct name pattern
                filepath = str(Path(temp_directory) / "RACV-Usage and Spend Report.csv")
                Path(filepath).write_text("date,usage,spend\n2024-01-01,100,50.00")

                result = optima_usage_and_spend_to_s3(filepath, "error_log")

                # Should return empty list
                assert result == []

                # Verify file was uploaded
                response = s3.get_object(Bucket="gegoptimareports", Key="usageAndSpendReports/racvUsageAndSpend.csv")
                body = response["Body"].read().decode("utf-8")
                assert "date,usage,spend" in body


class TestRacvElecParserEdgeCases:
    """Edge case tests for racv_elec_parser function."""

    def test_raises_exception_when_all_zeros(self, temp_directory: str) -> None:
        """Test that racv_elec_parser raises exception when all data is zero."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.non_nem_parsers import racv_elec_parser

            # Create file with all zeros - no valid data
            filepath = str(Path(temp_directory) / "all_zeros.csv")
            content = """Header Row 1
Header Row 2
Date,Start Time,Meter1 kWh
2024-01-01,00:00,0.0
2024-01-01,00:30,0.0
2024-01-02,00:00,0.0
2024-01-02,00:30,0.0
"""
            with Path(filepath).open("w") as f:
                f.write(content)

            with pytest.raises(Exception, match="No Valid Data"):
                racv_elec_parser(filepath, "error_log")

    def test_handles_mixed_zero_nonzero_meters(self, temp_directory: str) -> None:
        """Test that racv_elec_parser handles files with some zero and some non-zero meters."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.non_nem_parsers import racv_elec_parser

            filepath = str(Path(temp_directory) / "mixed_meters.csv")
            content = """Header Row 1
Header Row 2
Date,Start Time,ZeroMeter kWh,NonZeroMeter kWh
2024-01-01,00:00,0.0,10.0
2024-01-01,00:30,0.0,11.0
2024-01-02,00:00,0.0,12.0
2024-01-02,00:30,0.0,13.0
"""
            with Path(filepath).open("w") as f:
                f.write(content)

            result = racv_elec_parser(filepath, "error_log")

            # Should only have nonzero meter
            assert len(result) == 1
            nmi, _ = result[0]
            assert "NonZeroMeter" in nmi


class TestGreenSquareComXParserEdgeCases:
    """Edge case tests for green_square_private_wire_schneider_comx_parser function."""

    def test_handles_kwh_column_directly(self, temp_directory: str) -> None:
        """Test that ComX parser handles Active energy (kWh) column without conversion."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.non_nem_parsers import green_square_private_wire_schneider_comx_parser

            filepath = str(Path(temp_directory) / "comx_kwh.csv")
            content = """Row1,col2,col3,col4,TestSite
ComX510_Green_Square,data,data,data,TestSite
Row3,col2,col3,col4,col5
Row4,col2,col3,col4,col5
Row5,col2,col3,col4,col5
Row6,col2,col3,col4,col5
Local Time Stamp,Active energy (kWh),Other,col4,col5
01/01/2024 00:00,1.0,data,col4,col5
01/01/2024 00:30,2.0,data,col4,col5
"""
            with Path(filepath).open("w") as f:
                f.write(content)

            result = green_square_private_wire_schneider_comx_parser(filepath, "error_log")

            assert len(result) == 1
            _, df = result[0]

            # Values should be unchanged (kWh, no conversion)
            assert "E1_kWh" in df.columns
            assert df["E1_kWh"].iloc[0] == 1.0
            assert df["E1_kWh"].iloc[1] == 2.0

    def test_raises_exception_missing_energy_column(self, temp_directory: str) -> None:
        """Test that ComX parser raises exception when energy column is missing."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.non_nem_parsers import green_square_private_wire_schneider_comx_parser

            filepath = str(Path(temp_directory) / "comx_no_energy.csv")
            content = """Row1,col2,col3,col4,TestSite
ComX510_Green_Square,data,data,data,TestSite
Row3,col2,col3,col4,col5
Row4,col2,col3,col4,col5
Row5,col2,col3,col4,col5
Row6,col2,col3,col4,col5
Local Time Stamp,Other Column,col3,col4,col5
01/01/2024 00:00,data,data,col4,col5
"""
            with Path(filepath).open("w") as f:
                f.write(content)

            with pytest.raises(Exception, match="Missing Active energy column"):
                green_square_private_wire_schneider_comx_parser(filepath, "error_log")

    def test_extracts_site_name_correctly(self, temp_directory: str) -> None:
        """Test that ComX parser extracts site name correctly."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.non_nem_parsers import green_square_private_wire_schneider_comx_parser

            filepath = str(Path(temp_directory) / "comx_site.csv")
            content = """Row1,col2,col3,col4,Test Site Name
ComX510_Green_Square,data,data,data,Test Site Name
Row3,col2,col3,col4,col5
Row4,col2,col3,col4,col5
Row5,col2,col3,col4,col5
Row6,col2,col3,col4,col5
Local Time Stamp,Active energy (kWh),Other,col4,col5
01/01/2024 00:00,1.0,data,col4,col5
"""
            with Path(filepath).open("w") as f:
                f.write(content)

            result = green_square_private_wire_schneider_comx_parser(filepath, "error_log")

            nmi, _ = result[0]
            # Site name should have spaces removed
            assert nmi == "GPWComX_TestSiteName"


class TestGetNonNemDfEdgeCases:
    """Edge case tests for get_non_nem_df dispatcher function."""

    def test_stops_at_first_successful_parser(self, temp_directory: str) -> None:
        """Test that dispatcher stops after first successful parser."""
        mock_log = MagicMock()
        with patch("shared.non_nem_parsers.logger", mock_log):
            from shared.non_nem_parsers import get_non_nem_df

            # Create valid Envizi water file
            filepath = str(Path(temp_directory) / "water.csv")
            df = pd.DataFrame(
                {
                    "Serial_No": ["12345"],
                    "Interval_Start": ["2024-01-01T00:00:00"],
                    "Interval_End": ["2024-01-01T01:00:00"],
                    "Consumption": [1.5],
                    "Consumption Unit": ["kL"],
                }
            )
            df.to_csv(filepath, index=False)

            result = get_non_nem_df(filepath, "error_log")

            # Should successfully parse with first valid parser
            assert len(result) == 1
            assert result[0][0] == "Envizi_12345"

    def test_bulk_water_parser_is_tried(self, temp_directory: str) -> None:
        """Test that bulk water parser is tried in the dispatcher."""
        mock_log = MagicMock()
        with patch("shared.non_nem_parsers.logger", mock_log):
            from shared.non_nem_parsers import get_non_nem_df

            # Create valid bulk water file
            filepath = str(Path(temp_directory) / "bulk_water.csv")
            df = pd.DataFrame(
                {
                    "Serial_No": ["BULK123"],
                    "Date_Time": ["2024-01-01 00:00:00"],
                    "kL": [5.0],
                }
            )
            df.to_csv(filepath, index=False)

            result = get_non_nem_df(filepath, "error_log")

            # Should successfully parse with bulk water parser
            assert len(result) == 1
            assert result[0][0] == "Envizi_BULK123"


class TestParserOutputConsistency:
    """Tests to ensure all parsers have consistent output format."""

    def test_bulk_water_parser_returns_dataframe_with_t_start_index(self, temp_directory: str) -> None:
        """Test that bulk water parser returns DataFrame with t_start as index."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.non_nem_parsers import envizi_vertical_parser_water_bulk

            filepath = str(Path(temp_directory) / "bulk.csv")
            df = pd.DataFrame(
                {
                    "Serial_No": ["123"],
                    "Date_Time": ["2024-01-01 00:00:00"],
                    "kL": [1.0],
                }
            )
            df.to_csv(filepath, index=False)

            result = envizi_vertical_parser_water_bulk(filepath, "error_log")

            _, result_df = result[0]
            assert result_df.index.name == "t_start"

    def test_comx_parser_returns_dataframe_with_t_start_index(self, temp_directory: str) -> None:
        """Test that ComX parser returns DataFrame with t_start as index."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.non_nem_parsers import green_square_private_wire_schneider_comx_parser

            filepath = str(Path(temp_directory) / "comx.csv")
            content = """Row1,col2,col3,col4,Site
ComX510_Green_Square,data,data,data,Site
Row3,col2,col3,col4,col5
Row4,col2,col3,col4,col5
Row5,col2,col3,col4,col5
Row6,col2,col3,col4,col5
Local Time Stamp,Active energy (kWh),Other,col4,col5
01/01/2024 00:00,1.0,data,col4,col5
"""
            with Path(filepath).open("w") as f:
                f.write(content)

            result = green_square_private_wire_schneider_comx_parser(filepath, "error_log")

            _, result_df = result[0]
            assert result_df.index.name == "t_start"
