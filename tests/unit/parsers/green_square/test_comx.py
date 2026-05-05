"""Unit tests for green_square_private_wire_schneider_comx_parser."""

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent / "src"))


class TestGreenSquarePrivateWireSchneiderComXParser:
    """Tests for green_square_private_wire_schneider_comx_parser function."""

    def test_validates_comx_header(self, temp_directory: str) -> None:
        """Test that ComX header is validated."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.parsers.green_square.comx import green_square_private_wire_schneider_comx_parser

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
            from shared.parsers.green_square.comx import green_square_private_wire_schneider_comx_parser

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


class TestGreenSquareComXParserEdgeCases:
    """Edge case tests for green_square_private_wire_schneider_comx_parser function."""

    def test_handles_kwh_column_directly(self, temp_directory: str) -> None:
        """Test that ComX parser handles Active energy (kWh) column without conversion."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.parsers.green_square.comx import green_square_private_wire_schneider_comx_parser

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
            from shared.parsers.green_square.comx import green_square_private_wire_schneider_comx_parser

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
            from shared.parsers.green_square.comx import green_square_private_wire_schneider_comx_parser

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


class TestParserOutputConsistency:
    """Tests to ensure ComX parser has consistent output format."""

    def test_comx_parser_returns_dataframe_with_t_start_index(self, temp_directory: str) -> None:
        """Test that ComX parser returns DataFrame with t_start as index."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.parsers.green_square.comx import green_square_private_wire_schneider_comx_parser

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
