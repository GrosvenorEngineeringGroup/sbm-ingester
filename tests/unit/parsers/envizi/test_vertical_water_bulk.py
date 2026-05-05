"""Tests for shared.parsers.envizi.vertical_water_bulk.envizi_vertical_parser_water_bulk."""

from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from shared.parsers.envizi.vertical_water_bulk import envizi_vertical_parser_water_bulk


class TestEnviziVerticalParserWaterBulk:
    """Tests for envizi_vertical_parser_water_bulk function."""

    def test_parses_bulk_water_data_correctly(self, temp_directory: str) -> None:
        """Test that bulk water data with Date_Time column is parsed correctly."""
        with patch("shared.non_nem_parsers.logger"):
            # Create CSV with Date_Time column (bulk format)
            filepath = str(Path(temp_directory) / "water_bulk.csv")
            df = pd.DataFrame(
                {
                    "Serial_No": ["12345", "12345", "67890"],
                    "Date_Time": ["2024-01-01 00:00:00", "2024-01-01 01:00:00", "2024-01-01 00:00:00"],
                    "kL": [1.5, 2.0, 3.0],
                }
            )
            df.to_csv(filepath, index=False)

            result = envizi_vertical_parser_water_bulk(filepath, "error_log")

            assert isinstance(result, list)
            assert len(result) == 2  # Two unique serial numbers

            nmis = [nmi for nmi, _ in result]
            assert "Envizi_12345" in nmis
            assert "Envizi_67890" in nmis

            # Check column naming
            _, df_result = result[0]
            assert "E1_kL" in df_result.columns

    def test_bulk_water_rejects_optima_generation_file(self, temp_directory: str) -> None:
        """Test that OptimaGenerationData files are rejected by bulk water parser."""
        with patch("shared.non_nem_parsers.logger"):
            # Create file with OptimaGenerationData in name
            filepath = str(Path(temp_directory) / "OptimaGenerationData_water.csv")
            df = pd.DataFrame(
                {
                    "Serial_No": ["12345"],
                    "Date_Time": ["2024-01-01 00:00:00"],
                    "kL": [1.5],
                }
            )
            df.to_csv(filepath, index=False)

            with pytest.raises(Exception, match="Not Relevant Parser"):
                envizi_vertical_parser_water_bulk(filepath, "error_log")

    def test_bulk_water_handles_multiple_meters(self, temp_directory: str) -> None:
        """Test that bulk water parser handles multiple meters correctly."""
        with patch("shared.non_nem_parsers.logger"):
            filepath = str(Path(temp_directory) / "multi_meter_bulk.csv")
            df = pd.DataFrame(
                {
                    "Serial_No": ["111", "222", "333", "111", "222"],
                    "Date_Time": [
                        "2024-01-01 00:00:00",
                        "2024-01-01 00:00:00",
                        "2024-01-01 00:00:00",
                        "2024-01-01 01:00:00",
                        "2024-01-01 01:00:00",
                    ],
                    "kL": [1.0, 2.0, 3.0, 1.5, 2.5],
                }
            )
            df.to_csv(filepath, index=False)

            result = envizi_vertical_parser_water_bulk(filepath, "error_log")

            assert len(result) == 3
            nmis = sorted([nmi for nmi, _ in result])
            assert nmis == ["Envizi_111", "Envizi_222", "Envizi_333"]


class TestParserOutputConsistency:
    """Tests to ensure all parsers have consistent output format."""

    def test_bulk_water_parser_returns_dataframe_with_t_start_index(self, temp_directory: str) -> None:
        """Test that bulk water parser returns DataFrame with t_start as index."""
        with patch("shared.non_nem_parsers.logger"):
            filepath = str(Path(temp_directory) / "bulk.csv")
            df = pd.DataFrame(
                {
                    "Serial_No": ["123"],
                    "Date_Time": ["2024-01-01 00:00:00"],
                    "kL": [1.0],
                }
            )
            df.to_csv(filepath, index=False)

            result = envizi_vertical_parser_water_bulk(filepath, "error_log")

            _, result_df = result[0]
            assert result_df.index.name == "t_start"
