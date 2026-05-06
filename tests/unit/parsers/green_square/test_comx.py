"""Unit tests for green_square_private_wire_schneider_comx_parser."""

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent / "src"))

from shared.parsers import NotRelevantParser, ParserError, ParserOutcome


def _processed_dfs(result: ParserOutcome):
    assert result.status == "processed"
    assert result.source_row_count > 0
    return result.dfs


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

            with pytest.raises(NotRelevantParser, match="Not Relevant Parser"):
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
            result_dfs = _processed_dfs(result)

            assert len(result_dfs) == 1
            _nmi, df = result_dfs[0]

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
            result_dfs = _processed_dfs(result)

            assert len(result_dfs) == 1
            _, df = result_dfs[0]

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

            with pytest.raises(ParserError, match="Missing Active energy column"):
                green_square_private_wire_schneider_comx_parser(filepath, "error_log")

    @pytest.mark.parametrize("header_site_name", ["", "   ", "123"])
    def test_missing_or_non_string_site_name_raises_parser_error(
        self, temp_directory: str, header_site_name: str
    ) -> None:
        """ComX files with a matching marker still need a usable site name."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.parsers.green_square.comx import green_square_private_wire_schneider_comx_parser

            filepath = str(Path(temp_directory) / "comx_missing_site.csv")
            content = f"""Row1,col2,col3,col4,{header_site_name}
ComX510_Green_Square,data,data,data,{header_site_name}
Row3,col2,col3,col4,col5
Row4,col2,col3,col4,col5
Row5,col2,col3,col4,col5
Row6,col2,col3,col4,col5
Local Time Stamp,Active energy (kWh),Other,col4,col5
01/01/2024 00:00,1.0,data,col4,col5
"""
            with Path(filepath).open("w") as f:
                f.write(content)

            with pytest.raises(ParserError, match="Missing site name in ComX header"):
                green_square_private_wire_schneider_comx_parser(filepath, "error_log")

    def test_returns_processed_empty_when_no_valid_energy_rows(self, temp_directory: str) -> None:
        """Test that ComX parser returns processed_empty when energy rows are blank."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.parsers.green_square.comx import green_square_private_wire_schneider_comx_parser

            filepath = str(Path(temp_directory) / "comx_blank_energy.csv")
            content = """Row1,col2,col3,col4,TestSite
ComX510_Green_Square,data,data,data,TestSite
Row3,col2,col3,col4,col5
Row4,col2,col3,col4,col5
Row5,col2,col3,col4,col5
Row6,col2,col3,col4,col5
Local Time Stamp,Active energy (kWh),Other,col4,col5
01/01/2024 00:00,,data,col4,col5
01/01/2024 00:30,   ,data,col4,col5
"""
            with Path(filepath).open("w") as f:
                f.write(content)

            result = green_square_private_wire_schneider_comx_parser(filepath, "error_log")

            assert result.status == "processed_empty"
            assert result.reason == "all_blank"
            assert result.dfs == []

    def test_skip_counts_for_malformed_energy_value(self, temp_directory: str) -> None:
        """ComX parser skip-counts non-blank malformed energy values
        instead of raising. With every value bad, file becomes processed_empty
        with rows_skipped reflecting unparseable_value."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.parsers.green_square.comx import green_square_private_wire_schneider_comx_parser

            filepath = str(Path(temp_directory) / "comx_bad_energy.csv")
            content = """Row1,col2,col3,col4,TestSite
ComX510_Green_Square,data,data,data,TestSite
Row3,col2,col3,col4,col5
Row4,col2,col3,col4,col5
Row5,col2,col3,col4,col5
Row6,col2,col3,col4,col5
Local Time Stamp,Active energy (kWh),Other,col4,col5
01/01/2024 00:00,not-a-number,data,col4,col5
"""
            with Path(filepath).open("w") as f:
                f.write(content)

            result = green_square_private_wire_schneider_comx_parser(filepath, "error_log")
            assert result.status == "processed_empty"
            assert result.dfs == []
            assert result.skip_reasons["unparseable_value"] == 1
            assert result.reason == "all_blank"

    def test_partial_malformed_energy_with_valid_rows_skip_counts(self, temp_directory: str) -> None:
        """N valid rows + 1 malformed energy → N rows in output, rows_skipped=1."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.parsers.green_square.comx import green_square_private_wire_schneider_comx_parser

            filepath = str(Path(temp_directory) / "comx_partial_bad.csv")
            content = """Row1,col2,col3,col4,TestSite
ComX510_Green_Square,data,data,data,TestSite
Row3,col2,col3,col4,col5
Row4,col2,col3,col4,col5
Row5,col2,col3,col4,col5
Row6,col2,col3,col4,col5
Local Time Stamp,Active energy (kWh),Other,col4,col5
01/01/2024 00:00,1.0,data,col4,col5
01/01/2024 00:30,2.0,data,col4,col5
01/01/2024 01:00,not-a-number,data,col4,col5
"""
            with Path(filepath).open("w") as f:
                f.write(content)

            result = green_square_private_wire_schneider_comx_parser(filepath, "error_log")
            assert result.status == "processed"
            assert result.skip_reasons["unparseable_value"] == 1
            assert result.candidate_row_count == 2
            assert result.rows_skipped == 1

    def test_partial_malformed_timestamp_skip_counts(self, temp_directory: str) -> None:
        """N valid rows + 1 malformed timestamp → N rows in output, rows_skipped=1."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.parsers.green_square.comx import green_square_private_wire_schneider_comx_parser

            filepath = str(Path(temp_directory) / "comx_partial_bad_ts.csv")
            content = """Row1,col2,col3,col4,TestSite
ComX510_Green_Square,data,data,data,TestSite
Row3,col2,col3,col4,col5
Row4,col2,col3,col4,col5
Row5,col2,col3,col4,col5
Row6,col2,col3,col4,col5
Local Time Stamp,Active energy (kWh),Other,col4,col5
01/01/2024 00:00,1.0,data,col4,col5
01/01/2024 00:30,2.0,data,col4,col5
not-a-date,3.0,data,col4,col5
"""
            with Path(filepath).open("w") as f:
                f.write(content)

            result = green_square_private_wire_schneider_comx_parser(filepath, "error_log")
            assert result.status == "processed"
            assert result.skip_reasons["unparseable_timestamp"] == 1
            assert result.candidate_row_count == 2
            assert result.rows_skipped == 1

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
            result_dfs = _processed_dfs(result)

            nmi, _ = result_dfs[0]
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
            result_dfs = _processed_dfs(result)

            _, result_df = result_dfs[0]
            assert result_df.index.name == "t_start"


class TestGreenSquareComXCheapGate:
    """Cheap relevance gate must use BOM-aware encoding."""

    def test_bom_prefixed_marker_passes_gate(self, tmp_path) -> None:
        from shared.parsers.green_square.comx import green_square_private_wire_schneider_comx_parser

        # UTF-8 BOM (\xef\xbb\xbf) prefix on first line. Without utf-8-sig
        # the cheap-sniff would compare a BOM-prefixed first column on the
        # site row and miss; with utf-8-sig the BOM is stripped from the
        # first line only, but our marker is on line 2 anyway.
        path = tmp_path / "bom_comx.csv"
        body = (
            "Row1,col2,col3,col4,TestSite\n"
            "ComX510_Green_Square,data,data,data,TestSite\n"
            "Row3,col2,col3,col4,col5\n"
            "Row4,col2,col3,col4,col5\n"
            "Row5,col2,col3,col4,col5\n"
            "Row6,col2,col3,col4,col5\n"
            "Local Time Stamp,Active energy (kWh),Other,col4,col5\n"
            "01/01/2024 00:00,1.0,data,col4,col5\n"
        )
        path.write_bytes(b"\xef\xbb\xbf" + body.encode("utf-8"))

        result = green_square_private_wire_schneider_comx_parser(str(path), "error_log")
        assert result.status == "processed"
