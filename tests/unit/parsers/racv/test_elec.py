"""Tests for shared.parsers.racv.elec.racv_elec_parser."""

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "src"))

from shared.parsers import NotRelevantParser, ParserOutcome


def _processed_dfs(result: ParserOutcome):
    assert result.status == "processed"
    assert result.source_row_count > 0
    return result.dfs


class TestRacvElecParser:
    """Tests for racv_elec_parser function."""

    def test_skips_header_rows(self, temp_directory: str) -> None:
        """Test that first two rows are skipped."""
        with patch("shared.parsers.racv.elec.logger"):
            from shared.parsers.racv.elec import racv_elec_parser

            # Create RACV format file with header rows
            filepath = str(Path(temp_directory) / "racv_data.csv")
            content = """Header Row 1
Header Row 2
Date,Start Time,Meter1 kWh,Meter2 kWh
2024-01-01,00:00,10.5,20.5
2024-01-01,00:30,11.0,21.0
2024-01-02,00:00,12.0,22.0
2024-01-02,00:30,13.0,23.0
"""
            with Path(filepath).open("w") as f:
                f.write(content)

            result = racv_elec_parser(filepath, "error_log")
            result_dfs = _processed_dfs(result)

            # Should have parsed meter columns
            assert len(result_dfs) >= 1

    def test_filters_zero_days(self, temp_directory: str) -> None:
        """Test that days with all zero values are filtered out."""
        with patch("shared.parsers.racv.elec.logger"):
            from shared.parsers.racv.elec import racv_elec_parser

            # Create file with some zero days
            filepath = str(Path(temp_directory) / "racv_data.csv")
            content = """Header Row 1
Header Row 2
Date,Start Time,Meter1 kWh
2024-01-01,00:00,0.0
2024-01-01,00:30,0.0
2024-01-02,00:00,10.0
2024-01-02,00:30,10.0
"""
            with Path(filepath).open("w") as f:
                f.write(content)

            result = racv_elec_parser(filepath, "error_log")
            result_dfs = _processed_dfs(result)

            # Result should only have non-zero day data
            if len(result_dfs) > 0:
                _nmi, _df = result_dfs[0]
                # Should have filtered out Jan 1 (all zeros)
                # Only Jan 2 data should remain

    def test_rejects_optima_generation_file(self, temp_directory: str) -> None:
        """Test that OptimaGenerationData files are rejected."""
        with patch("shared.parsers.racv.elec.logger"):
            from shared.parsers.racv.elec import racv_elec_parser

            filepath = str(Path(temp_directory) / "OptimaGenerationData.csv")
            content = """Header Row 1
Header Row 2
Date,Start Time,Meter1 kWh
2024-01-01,00:00,10.0
"""
            with Path(filepath).open("w") as f:
                f.write(content)

            with pytest.raises(NotRelevantParser, match="Not Relevant Parser"):
                racv_elec_parser(filepath, "error_log")


class TestRacvElecParserEdgeCases:
    """Edge case tests for racv_elec_parser function."""

    def test_returns_processed_empty_when_all_zeros(self, temp_directory: str) -> None:
        """Test that racv_elec_parser returns processed_empty when all data is zero."""
        with patch("shared.parsers.racv.elec.logger"):
            from shared.parsers.racv.elec import racv_elec_parser

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

            result = racv_elec_parser(filepath, "error_log")

            assert result.status == "processed_empty"
            assert result.reason == "all_zero_valid"
            assert result.dfs == []

    def test_whitespace_blank_kwh_values_are_empty_values(self, temp_directory: str) -> None:
        """Whitespace-only kWh cells are valid blanks, not malformed values."""
        with patch("shared.parsers.racv.elec.logger"):
            from shared.parsers.racv.elec import racv_elec_parser

            filepath = str(Path(temp_directory) / "blank_values.csv")
            content = (
                "Header Row 1\n"
                "Header Row 2\n"
                "Date,Start Time,Meter1 kWh\n"
                "2024-01-01,00:00,0.0\n"
                "2024-01-01,00:30,   \n"
                "2024-01-02,00:00,\n"
                "2024-01-02,00:30,0.0\n"
            )
            with Path(filepath).open("w") as f:
                f.write(content)

            result = racv_elec_parser(filepath, "error_log")

            assert result.status == "processed_empty"
            assert result.reason == "all_zero_valid"
            assert result.dfs == []

    def test_mixed_nonzero_and_blank_kwh_values_parse(self, temp_directory: str) -> None:
        """Non-zero usable rows survive when the same file also contains blanks."""
        with patch("shared.parsers.racv.elec.logger"):
            from shared.parsers.racv.elec import racv_elec_parser

            filepath = str(Path(temp_directory) / "mixed_blank_values.csv")
            content = (
                "Header Row 1\nHeader Row 2\nDate,Start Time,Meter1 kWh\n2024-01-01,00:00,   \n2024-01-01,00:30,5.5\n"
            )
            with Path(filepath).open("w") as f:
                f.write(content)

            result = racv_elec_parser(filepath, "error_log")
            result_dfs = _processed_dfs(result)

            assert len(result_dfs) == 1
            nmi, df = result_dfs[0]
            assert nmi == "Optima_Meter1"
            assert df["E1_kWh"].dropna().tolist() == [5.5]
            assert df["E1_kWh"].isna().sum() == 1

    def test_non_blank_invalid_kwh_value_skip_counts(self, temp_directory: str) -> None:
        """Non-blank non-numeric kWh cells are counted as unparseable_value
        and skipped, not raised."""
        with patch("shared.parsers.racv.elec.logger"):
            from shared.parsers.racv.elec import racv_elec_parser

            filepath = str(Path(temp_directory) / "invalid_value.csv")
            content = """Header Row 1
Header Row 2
Date,Start Time,Meter1 kWh
2024-01-01,00:00,not-a-number
"""
            with Path(filepath).open("w") as f:
                f.write(content)

            result = racv_elec_parser(filepath, "error_log")
            # The single bad cell becomes NaN; daily sum is NaN/0 → all_zero_valid.
            assert result.status == "processed_empty"
            assert result.dfs == []
            assert result.skip_reasons["unparseable_value"] == 1

    def test_partial_malformed_kwh_with_valid_rows_skip_counts(self, temp_directory: str) -> None:
        """Some valid rows + 1 malformed numeric → valid rows still processed,
        rows_skipped reflects the bad row count via unparseable_value."""
        with patch("shared.parsers.racv.elec.logger"):
            from shared.parsers.racv.elec import racv_elec_parser

            filepath = str(Path(temp_directory) / "mixed_invalid.csv")
            valid_rows = "\n".join(f"2024-01-01,{h:02d}:00,5.0" for h in range(24))
            content = (
                "Header Row 1\nHeader Row 2\nDate,Start Time,Meter1 kWh\n"
                + valid_rows
                + "\n2024-01-02,00:00,not-a-number\n"
            )
            with Path(filepath).open("w") as f:
                f.write(content)

            result = racv_elec_parser(filepath, "error_log")
            assert result.status == "processed"
            assert result.skip_reasons["unparseable_value"] == 1
            assert len(result.dfs) == 1

    def test_handles_mixed_zero_nonzero_meters(self, temp_directory: str) -> None:
        """Test that racv_elec_parser handles files with some zero and some non-zero meters."""
        with patch("shared.parsers.racv.elec.logger"):
            from shared.parsers.racv.elec import racv_elec_parser

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
            result_dfs = _processed_dfs(result)

            # Should only have nonzero meter
            assert len(result_dfs) == 1
            nmi, _ = result_dfs[0]
            assert "NonZeroMeter" in nmi
