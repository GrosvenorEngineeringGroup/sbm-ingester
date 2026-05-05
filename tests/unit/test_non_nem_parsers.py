"""Unit tests for non_nem_parsers.py module."""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Add tests directory to path
sys.path.insert(0, str(Path(__file__).parent))
from conftest import (
    create_envizi_electricity_csv,
    create_envizi_water_csv,
    create_optima_csv,
)


class TestGetNonNemDf:
    """Tests for get_non_nem_df dispatcher function."""

    def test_tries_parsers_in_order(self, temp_directory: str) -> None:
        """Test that parsers are tried in order until one succeeds."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.non_nem_parsers import get_non_nem_df

            # Create valid Envizi water file
            filepath = str(Path(temp_directory) / "water_data.csv")
            create_envizi_water_csv(filepath, serial_numbers=["12345"])

            result = get_non_nem_df(filepath, "error_log")

            assert isinstance(result, list)
            assert len(result) > 0

    def test_raises_exception_when_all_parsers_fail(self, temp_directory: str) -> None:
        """Test that exception is raised when no parser succeeds."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.non_nem_parsers import get_non_nem_df

            # Create invalid file that no parser can handle
            filepath = str(Path(temp_directory) / "invalid.csv")
            with Path(filepath).open("w") as f:
                f.write("completely,invalid,format\n1,2,3\n")

            with pytest.raises(Exception, match="No Valid Parser Found"):
                get_non_nem_df(filepath, "error_log")

    def test_logs_errors_for_failed_parsers(self, temp_directory: str) -> None:
        """Test that errors are logged for each failed parser."""
        mock_log = MagicMock()
        with patch("shared.non_nem_parsers.logger", mock_log):
            from shared.non_nem_parsers import get_non_nem_df

            # Create valid file for later parser
            filepath = str(Path(temp_directory) / "OptimaGenerationData.csv")
            create_optima_csv(filepath, identifiers=["ID1"])

            get_non_nem_df(filepath, "error_log")

            # Earlier parsers should have logged failures
            assert mock_log.debug.called


class TestGreenSquarePrivateWireSchneiderComXParser:
    """Tests for green_square_private_wire_schneider_comx_parser function."""

    def test_validates_comx_header(self, temp_directory: str) -> None:
        """Test that ComX header is validated."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.non_nem_parsers import green_square_private_wire_schneider_comx_parser

            # Create file without ComX header - match expected CSV structure
            filepath = str(Path(temp_directory) / "not_comx.csv")
            content = """Row1,col2,col3,col4,col5
NotComX510_Green_Square,data,data,data,SiteName
"""
            with Path(filepath).open("w") as f:
                f.write(content)

            with pytest.raises(Exception, match="Not Relevant Parser"):
                green_square_private_wire_schneider_comx_parser(filepath, "error_log")

    def test_converts_wh_to_kwh(self, temp_directory: str) -> None:
        """Test that Wh values are converted to kWh."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.non_nem_parsers import green_square_private_wire_schneider_comx_parser

            # Create valid ComX file with Wh column - must have consistent columns
            filepath = str(Path(temp_directory) / "comx_data.csv")
            content = """Row1,col2,col3,col4,TestSite
ComX510_Green_Square,data,data,data,TestSite
Row3,col2,col3,col4,col5
Row4,col2,col3,col4,col5
Row5,col2,col3,col4,col5
Row6,col2,col3,col4,col5
Local Time Stamp,Active energy (Wh),Other,col4,col5
01/01/2024 00:00,1000,data,col4,col5
01/01/2024 00:30,2000,data,col4,col5
"""
            with Path(filepath).open("w") as f:
                f.write(content)

            result = green_square_private_wire_schneider_comx_parser(filepath, "error_log")

            assert len(result) == 1
            _nmi, df = result[0]

            # Values should be converted from Wh to kWh
            assert "E1_kWh" in df.columns
            # 1000 Wh = 1.0 kWh
            assert df["E1_kWh"].iloc[0] == 1.0


class TestDataFrameOutputFormat:
    """Tests for DataFrame output format consistency across parsers."""

    def test_all_parsers_return_t_start_column(self, temp_directory: str) -> None:
        """Test that all parsers return DataFrames with t_start."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.non_nem_parsers import envizi_vertical_parser_water
            from shared.parsers.envizi.vertical_electricity import envizi_vertical_parser_electricity
            from shared.parsers.optima.interval import interval_parser

            # Test Envizi water
            water_file = str(Path(temp_directory) / "water.csv")
            create_envizi_water_csv(water_file, serial_numbers=["1"])
            result = envizi_vertical_parser_water(water_file, "error")
            _, df = result[0]
            assert df.index.name == "t_start" or "t_start" in df.columns

            # Test Envizi electricity
            elec_file = str(Path(temp_directory) / "elec.csv")
            create_envizi_electricity_csv(elec_file, serial_numbers=["1"])
            result = envizi_vertical_parser_electricity(elec_file, "error")
            _, df = result[0]
            assert df.index.name == "t_start" or "t_start" in df.columns

            # Test Optima generation
            gen_file = str(Path(temp_directory) / "OptimaGenerationData_test.csv")
            create_optima_csv(gen_file, identifiers=["1"])
            result = interval_parser(gen_file, "error")
            _, df = result[0]
            assert df.index.name == "t_start" or "t_start" in df.columns
