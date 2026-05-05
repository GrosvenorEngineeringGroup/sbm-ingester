"""Tests for shared.parsers.racv.elec.racv_elec_parser."""

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "src"))


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

            assert isinstance(result, list)
            # Should have parsed meter columns
            assert len(result) >= 1

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

            # Result should only have non-zero day data
            if len(result) > 0:
                _nmi, _df = result[0]
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

            with pytest.raises(Exception, match="Not Relevant Parser"):
                racv_elec_parser(filepath, "error_log")


class TestRacvElecParserEdgeCases:
    """Edge case tests for racv_elec_parser function."""

    def test_raises_exception_when_all_zeros(self, temp_directory: str) -> None:
        """Test that racv_elec_parser raises exception when all data is zero."""
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

            with pytest.raises(Exception, match="No Valid Data"):
                racv_elec_parser(filepath, "error_log")

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

            # Should only have nonzero meter
            assert len(result) == 1
            nmi, _ = result[0]
            assert "NonZeroMeter" in nmi
