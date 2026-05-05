"""Tests for shared.parsers.optima.interval.interval_parser."""

from pathlib import Path
from unittest.mock import patch

from conftest import create_optima_csv


class TestIntervalParser:
    """Tests for interval_parser function."""

    def test_parses_generation_data_correctly(self, temp_directory: str) -> None:
        """Test that generation data is parsed correctly."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.parsers.optima.interval import interval_parser

            filepath = str(Path(temp_directory) / "OptimaGenerationData.csv")
            create_optima_csv(filepath, identifiers=["SOLAR001"], rows_per_id=5)

            result = interval_parser(filepath, "error_log")

            assert isinstance(result, list)
            assert len(result) == 1

            nmi, df = result[0]
            assert nmi == "Optima_SOLAR001"
            assert "B1_kWh" in df.columns  # Generation uses B1
            assert "E1_kWh" in df.columns  # Usage uses E1

    def test_handles_multiple_identifiers(self, temp_directory: str) -> None:
        """Test that multiple identifiers are handled correctly."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.parsers.optima.interval import interval_parser

            filepath = str(Path(temp_directory) / "OptimaGenerationData.csv")
            create_optima_csv(filepath, identifiers=["SOLAR001", "SOLAR002"], rows_per_id=3)

            result = interval_parser(filepath, "error_log")

            assert len(result) == 2
            nmis = [nmi for nmi, df in result]
            assert "Optima_SOLAR001" in nmis
            assert "Optima_SOLAR002" in nmis

    def test_parses_usage_column_as_e1_kwh(self, temp_directory: str) -> None:
        """Test that Usage column is correctly extracted as E1_kWh."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.parsers.optima.interval import interval_parser

            filepath = str(Path(temp_directory) / "OptimaGenerationData.csv")
            create_optima_csv(filepath, identifiers=["METER001"], rows_per_id=5)

            result = interval_parser(filepath, "error_log")

            _nmi, df = result[0]
            assert "E1_kWh" in df.columns
            # Verify values are correctly extracted (Usage = i * 0.5)
            assert df["E1_kWh"].iloc[0] == 0.0  # i=0 -> 0 * 0.5 = 0
            assert df["E1_kWh"].iloc[1] == 0.5  # i=1 -> 1 * 0.5 = 0.5
            assert df["E1_kWh"].iloc[2] == 1.0  # i=2 -> 2 * 0.5 = 1.0

    def test_parses_generation_only_file(self, temp_directory: str) -> None:
        """Test that files with only Generation column work correctly."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.parsers.optima.interval import interval_parser

            filepath = str(Path(temp_directory) / "OptimaGenerationData.csv")
            create_optima_csv(
                filepath, identifiers=["SOLAR001"], rows_per_id=3, include_usage=False, include_generation=True
            )

            result = interval_parser(filepath, "error_log")

            nmi, df = result[0]
            assert nmi == "Optima_SOLAR001"
            assert "B1_kWh" in df.columns
            assert "E1_kWh" not in df.columns  # No Usage column in source

    def test_parses_usage_only_file(self, temp_directory: str) -> None:
        """Test that files with only Usage column work correctly."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.parsers.optima.interval import interval_parser

            filepath = str(Path(temp_directory) / "OptimaGenerationData.csv")
            create_optima_csv(
                filepath, identifiers=["METER001"], rows_per_id=3, include_usage=True, include_generation=False
            )

            result = interval_parser(filepath, "error_log")

            nmi, df = result[0]
            assert nmi == "Optima_METER001"
            assert "E1_kWh" in df.columns
            assert "B1_kWh" not in df.columns  # No Generation column in source
