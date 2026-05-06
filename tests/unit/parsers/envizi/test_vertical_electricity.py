"""Tests for shared.parsers.envizi.vertical_electricity.envizi_vertical_parser_electricity."""

from pathlib import Path
from unittest.mock import patch

import pytest
from conftest import create_envizi_electricity_csv

from shared.parsers import NotRelevantParser, ParserError, ParserOutcome
from shared.parsers.envizi.vertical_electricity import envizi_vertical_parser_electricity


def _processed_dfs(result: ParserOutcome):
    assert result.status == "processed"
    assert result.source_row_count > 0
    return result.dfs


class TestEnviziVerticalParserElectricity:
    """Tests for envizi_vertical_parser_electricity function."""

    def test_parses_electricity_data_correctly(self, temp_directory: str) -> None:
        """Test that electricity data is parsed correctly."""
        with patch("shared.non_nem_parsers.logger"):
            filepath = str(Path(temp_directory) / "elec_data.csv")
            create_envizi_electricity_csv(filepath, serial_numbers=["E001"], rows_per_meter=5)

            result = envizi_vertical_parser_electricity(filepath, "error_log")
            result_dfs = _processed_dfs(result)

            assert len(result_dfs) == 1

            nmi, df = result_dfs[0]
            assert nmi == "Envizi_E001"
            assert "E1_kWh" in df.columns

    def test_rejects_optima_generation_file(self, temp_directory: str) -> None:
        """Test that OptimaGenerationData files are rejected."""
        with patch("shared.non_nem_parsers.logger"):
            filepath = str(Path(temp_directory) / "OptimaGenerationData.csv")
            create_envizi_electricity_csv(filepath, serial_numbers=["E001"])

            with pytest.raises(NotRelevantParser, match="Not Relevant Parser"):
                envizi_vertical_parser_electricity(filepath, "error_log")

    def test_missing_required_columns_is_not_relevant(self, tmp_path) -> None:
        path = tmp_path / "bunnings_demand_profile.csv"
        path.write_text('Commodities:,"Electricity"\nNo data found\n')

        with pytest.raises(NotRelevantParser, match="Not an Envizi electricity CSV"):
            envizi_vertical_parser_electricity(str(path), "error_log")

    def test_decode_error_is_not_relevant(self, tmp_path) -> None:
        path = tmp_path / "20260414-RACV-Usage and Spend Report.csv"
        path.write_bytes("Commodities:\n".encode("utf-16-le"))

        with pytest.raises(NotRelevantParser, match="Not readable as an Envizi CSV"):
            envizi_vertical_parser_electricity(str(path), "error_log")

    def test_malformed_kwh_after_schema_match_raises_parser_error(self, tmp_path) -> None:
        path = tmp_path / "electricity.csv"
        path.write_text(
            "Serial_No,Interval_Start,Interval_End,kWh\nE001,2026-05-01T00:00:00,2026-05-01T00:30:00,not-a-number\n"
        )

        with pytest.raises(ParserError, match="Failed to parse Envizi electricity kWh values"):
            envizi_vertical_parser_electricity(str(path), "error_log")

    def test_blank_only_kwh_values_return_processed_empty(self, tmp_path) -> None:
        path = tmp_path / "electricity.csv"
        path.write_text(
            "Serial_No,Interval_Start,Interval_End,kWh\n"
            "E001,2026-05-01T00:00:00,2026-05-01T00:30:00,\n"
            "E001,2026-05-01T00:30:00,2026-05-01T01:00:00,   \n"
        )

        result = envizi_vertical_parser_electricity(str(path), "error_log")

        assert result.status == "processed_empty"
        assert result.source_row_count == 2
        assert result.reason == "blank_values"
        assert result.dfs == []
