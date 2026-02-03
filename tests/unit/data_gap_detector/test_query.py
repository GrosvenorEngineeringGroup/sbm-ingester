"""Tests for query module."""

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd


class TestQueryBatch:
    """Tests for query_batch function."""

    @patch("src.functions.data_gap_detector.query.wr")
    def test_query_batch_calls_athena(self, mock_wr: MagicMock) -> None:
        """query_batch calls awswrangler with correct parameters."""
        from src.functions.data_gap_detector.query import query_batch

        mock_df = pd.DataFrame(
            {
                "sensorId": ["p:bunnings:abc"],
                "data_date": [date(2024, 1, 1)],
                "record_count": [48],
            }
        )
        mock_wr.athena.read_sql_query.return_value = mock_df

        result = query_batch(
            sensor_ids=["p:bunnings:abc", "p:bunnings:def"],
            start_date="2024-01-01",
            end_date="2024-01-31",
        )

        mock_wr.athena.read_sql_query.assert_called_once()
        call_args = mock_wr.athena.read_sql_query.call_args
        query = call_args[0][0]

        assert "p:bunnings:abc" in query
        assert "p:bunnings:def" in query
        assert "2024-01-01" in query
        assert "2024-01-31" in query
        assert isinstance(result, pd.DataFrame)

    @patch("src.functions.data_gap_detector.query.wr")
    def test_query_batch_handles_empty_result(self, mock_wr: MagicMock) -> None:
        """query_batch returns empty DataFrame when no data."""
        from src.functions.data_gap_detector.query import query_batch

        mock_wr.athena.read_sql_query.return_value = pd.DataFrame()

        result = query_batch(
            sensor_ids=["p:bunnings:abc"],
            start_date="2024-01-01",
            end_date="2024-01-31",
        )

        assert result.empty


class TestQueryAllSensors:
    """Tests for query_all_sensors function."""

    @patch("src.functions.data_gap_detector.query.query_batch")
    def test_query_all_sensors_batches_correctly(self, mock_query_batch: MagicMock) -> None:
        """query_all_sensors splits sensors into batches."""
        from src.functions.data_gap_detector.query import BATCH_SIZE, query_all_sensors

        # Create more sensors than BATCH_SIZE
        sensor_ids = [f"p:bunnings:sensor{i}" for i in range(BATCH_SIZE + 10)]

        mock_query_batch.return_value = pd.DataFrame(
            {
                "sensorId": ["p:bunnings:sensor0"],
                "data_date": [date(2024, 1, 1)],
                "record_count": [48],
            }
        )

        query_all_sensors(
            sensor_ids=sensor_ids,
            start_date="2024-01-01",
            end_date="2024-01-31",
        )

        # Should be called twice (one full batch + one partial)
        assert mock_query_batch.call_count == 2

    @patch("src.functions.data_gap_detector.query.BATCH_SIZE", 1)
    @patch("src.functions.data_gap_detector.query.query_batch")
    def test_query_all_sensors_concatenates_results(self, mock_query_batch: MagicMock) -> None:
        """query_all_sensors concatenates batch results."""
        from src.functions.data_gap_detector.query import query_all_sensors

        mock_query_batch.side_effect = [
            pd.DataFrame(
                {
                    "sensorId": ["p:bunnings:a"],
                    "data_date": [date(2024, 1, 1)],
                    "record_count": [48],
                }
            ),
            pd.DataFrame(
                {
                    "sensorId": ["p:bunnings:b"],
                    "data_date": [date(2024, 1, 2)],
                    "record_count": [48],
                }
            ),
        ]

        result = query_all_sensors(
            sensor_ids=["p:bunnings:a", "p:bunnings:b"],
            start_date="2024-01-01",
            end_date="2024-01-31",
        )

        assert len(result) == 2
        assert "p:bunnings:a" in result["sensorId"].values
        assert "p:bunnings:b" in result["sensorId"].values
