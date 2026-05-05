"""Tests for shared.parsers.envizi.vertical_electricity.envizi_vertical_parser_electricity."""

from pathlib import Path
from unittest.mock import patch

import pytest
from conftest import create_envizi_electricity_csv

from shared.parsers.envizi.vertical_electricity import envizi_vertical_parser_electricity


class TestEnviziVerticalParserElectricity:
    """Tests for envizi_vertical_parser_electricity function."""

    def test_parses_electricity_data_correctly(self, temp_directory: str) -> None:
        """Test that electricity data is parsed correctly."""
        with patch("shared.non_nem_parsers.logger"):
            filepath = str(Path(temp_directory) / "elec_data.csv")
            create_envizi_electricity_csv(filepath, serial_numbers=["E001"], rows_per_meter=5)

            result = envizi_vertical_parser_electricity(filepath, "error_log")

            assert isinstance(result, list)
            assert len(result) == 1

            nmi, df = result[0]
            assert nmi == "Envizi_E001"
            assert "E1_kWh" in df.columns

    def test_rejects_optima_generation_file(self, temp_directory: str) -> None:
        """Test that OptimaGenerationData files are rejected."""
        with patch("shared.non_nem_parsers.logger"):
            filepath = str(Path(temp_directory) / "OptimaGenerationData.csv")
            create_envizi_electricity_csv(filepath, serial_numbers=["E001"])

            with pytest.raises(Exception, match="Not Relevant Parser"):
                envizi_vertical_parser_electricity(filepath, "error_log")
