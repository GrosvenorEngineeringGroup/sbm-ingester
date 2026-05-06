"""Tests for shared.parsers.envizi.vertical_water.envizi_vertical_parser_water."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from conftest import create_envizi_water_csv

from shared.parsers import NotRelevantParser, ParserError, ParserOutcome
from shared.parsers.envizi.vertical_water import envizi_vertical_parser_water


def _processed_dfs(result: ParserOutcome):
    assert result.status == "processed"
    assert result.source_row_count > 0
    return result.dfs


class TestEnviziVerticalParserWater:
    """Tests for envizi_vertical_parser_water function."""

    def test_parses_water_data_correctly(self, temp_directory: str) -> None:
        """Test that water data is parsed correctly."""
        with patch("shared.non_nem_parsers.logger"):
            filepath = str(Path(temp_directory) / "water_data.csv")
            create_envizi_water_csv(filepath, serial_numbers=["12345"], rows_per_meter=5)

            result = envizi_vertical_parser_water(filepath, "error_log")
            result_dfs = _processed_dfs(result)

            assert len(result_dfs) == 1

            nmi, df = result_dfs[0]
            assert nmi == "Envizi_12345"
            assert "t_start" in df.index.name or "t_start" in df.columns
            assert "E1_kL" in df.columns

    def test_handles_multiple_meters(self, temp_directory: str) -> None:
        """Test that multiple meters are handled correctly."""
        with patch("shared.non_nem_parsers.logger"):
            filepath = str(Path(temp_directory) / "water_data.csv")
            create_envizi_water_csv(filepath, serial_numbers=["111", "222", "333"], rows_per_meter=3)

            result = envizi_vertical_parser_water(filepath, "error_log")
            result_dfs = _processed_dfs(result)

            assert len(result_dfs) == 3
            nmis = [nmi for nmi, df in result_dfs]
            assert "Envizi_111" in nmis
            assert "Envizi_222" in nmis
            assert "Envizi_333" in nmis

    def test_rejects_optima_generation_file(self, temp_directory: str) -> None:
        """Test that OptimaGenerationData files are rejected."""
        with patch("shared.non_nem_parsers.logger"):
            filepath = str(Path(temp_directory) / "OptimaGenerationData.csv")
            create_envizi_water_csv(filepath, serial_numbers=["12345"])

            with pytest.raises(NotRelevantParser, match="Not Relevant Parser"):
                envizi_vertical_parser_water(filepath, "error_log")

    def test_logs_warning_for_multiple_units(self, temp_directory: str) -> None:
        """Test that multiple units per meter triggers warning."""
        mock_log = MagicMock()
        with patch("shared.parsers.envizi.vertical_water.logger", mock_log):
            # Create CSV with multiple units for same meter
            filepath = str(Path(temp_directory) / "multi_unit.csv")
            df = pd.DataFrame(
                {
                    "Serial_No": ["12345", "12345"],
                    "Interval_Start": ["2024-01-01T00:00:00", "2024-01-01T01:00:00"],
                    "Interval_End": ["2024-01-01T01:00:00", "2024-01-01T02:00:00"],
                    "Consumption": [1.0, 2.0],
                    "Consumption Unit": ["kL", "L"],  # Different units
                }
            )
            df.to_csv(filepath, index=False)

            envizi_vertical_parser_water(filepath, "error_log")

            # Should log warning about multiple units
            assert mock_log.error.called

    def test_malformed_consumption_after_schema_match_raises_parser_error(self, tmp_path) -> None:
        path = tmp_path / "water.csv"
        path.write_text(
            "Serial_No,Interval_Start,Interval_End,Consumption,Consumption Unit\n"
            "12345,2026-05-01T00:00:00,2026-05-01T00:30:00,not-a-number,kL\n"
        )

        with pytest.raises(ParserError, match="Failed to parse Envizi water Consumption values"):
            envizi_vertical_parser_water(str(path), "error_log")

    def test_blank_only_consumption_values_return_processed_empty(self, tmp_path) -> None:
        path = tmp_path / "water.csv"
        path.write_text(
            "Serial_No,Interval_Start,Interval_End,Consumption,Consumption Unit\n"
            "12345,2026-05-01T00:00:00,2026-05-01T00:30:00,,kL\n"
            "12345,2026-05-01T00:30:00,2026-05-01T01:00:00,   ,kL\n"
        )

        result = envizi_vertical_parser_water(str(path), "error_log")

        assert result.status == "processed_empty"
        assert result.source_row_count == 2
        assert result.reason == "blank_values"
        assert result.dfs == []
