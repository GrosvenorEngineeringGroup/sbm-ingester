"""Edge case tests for non_nem_parsers.py to improve coverage."""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))


class TestGetNonNemDfEdgeCases:
    """Edge case tests for get_non_nem_df dispatcher function."""

    def test_stops_at_first_successful_parser(self, temp_directory: str) -> None:
        """Test that dispatcher stops after first successful parser."""
        mock_log = MagicMock()
        with patch("shared.non_nem_parsers.logger", mock_log):
            from shared.non_nem_parsers import get_non_nem_df

            # Create valid Envizi water file
            filepath = str(Path(temp_directory) / "water.csv")
            df = pd.DataFrame(
                {
                    "Serial_No": ["12345"],
                    "Interval_Start": ["2024-01-01T00:00:00"],
                    "Interval_End": ["2024-01-01T01:00:00"],
                    "Consumption": [1.5],
                    "Consumption Unit": ["kL"],
                }
            )
            df.to_csv(filepath, index=False)

            result = get_non_nem_df(filepath, "error_log")

            # Should successfully parse with first valid parser
            assert len(result) == 1
            assert result[0][0] == "Envizi_12345"

    def test_bulk_water_parser_is_tried(self, temp_directory: str) -> None:
        """Test that bulk water parser is tried in the dispatcher."""
        mock_log = MagicMock()
        with patch("shared.non_nem_parsers.logger", mock_log):
            from shared.non_nem_parsers import get_non_nem_df

            # Create valid bulk water file
            filepath = str(Path(temp_directory) / "bulk_water.csv")
            df = pd.DataFrame(
                {
                    "Serial_No": ["BULK123"],
                    "Date_Time": ["2024-01-01 00:00:00"],
                    "kL": [5.0],
                }
            )
            df.to_csv(filepath, index=False)

            result = get_non_nem_df(filepath, "error_log")

            # Should successfully parse with bulk water parser
            assert len(result) == 1
            assert result[0][0] == "Envizi_BULK123"
