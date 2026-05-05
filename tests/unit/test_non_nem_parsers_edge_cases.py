"""Edge case tests for non_nem_parsers.py to improve coverage."""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))


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
