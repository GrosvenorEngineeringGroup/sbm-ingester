"""Tests for detector module."""

from datetime import date

import pandas as pd


class TestAnalyzeSensorGaps:
    """Tests for analyze_sensor_gaps function."""

    def test_no_data_detected(self) -> None:
        """analyze_sensor_gaps detects no_data when sensor has no records."""
        from src.functions.data_gap_detector.detector import analyze_sensor_gaps

        # Empty DataFrame - no data for this sensor
        df = pd.DataFrame(columns=["sensorId", "data_date", "record_count"])

        result = analyze_sensor_gaps(
            sensor_id="p:bunnings:abc123",
            nmi_channel="NMI-E1",
            df=df,
            start_date=None,
            end_date=None,
        )

        assert result["issue_type"] == "no_data"
        assert result["missing_count"] == 0
        assert result["data_start"] == ""
        assert result["data_end"] == ""

    def test_missing_dates_detected(self) -> None:
        """analyze_sensor_gaps detects missing dates in range."""
        from src.functions.data_gap_detector.detector import analyze_sensor_gaps

        # Data for Jan 1, 3, 5 (missing Jan 2, 4)
        df = pd.DataFrame(
            {
                "sensorId": ["p:bunnings:abc123"] * 3,
                "data_date": [date(2024, 1, 1), date(2024, 1, 3), date(2024, 1, 5)],
                "record_count": [48, 48, 48],
            }
        )

        result = analyze_sensor_gaps(
            sensor_id="p:bunnings:abc123",
            nmi_channel="NMI-E1",
            df=df,
            start_date=None,
            end_date=None,
        )

        assert result["issue_type"] == "missing_dates"
        assert result["missing_count"] == 2
        assert "2024-01-02" in result["missing_dates"]
        assert "2024-01-04" in result["missing_dates"]
        assert result["data_start"] == "2024-01-01"
        assert result["data_end"] == "2024-01-05"

    def test_complete_data_returns_none(self) -> None:
        """analyze_sensor_gaps returns None when data is complete."""
        from src.functions.data_gap_detector.detector import analyze_sensor_gaps

        # Complete data for Jan 1-3
        df = pd.DataFrame(
            {
                "sensorId": ["p:bunnings:abc123"] * 3,
                "data_date": [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
                "record_count": [48, 48, 48],
            }
        )

        result = analyze_sensor_gaps(
            sensor_id="p:bunnings:abc123",
            nmi_channel="NMI-E1",
            df=df,
            start_date=None,
            end_date=None,
        )

        assert result is None

    def test_user_specified_date_range(self) -> None:
        """analyze_sensor_gaps uses user-specified date range."""
        from src.functions.data_gap_detector.detector import analyze_sensor_gaps

        # Data only for Jan 2
        df = pd.DataFrame(
            {
                "sensorId": ["p:bunnings:abc123"],
                "data_date": [date(2024, 1, 2)],
                "record_count": [48],
            }
        )

        result = analyze_sensor_gaps(
            sensor_id="p:bunnings:abc123",
            nmi_channel="NMI-E1",
            df=df,
            start_date="2024-01-01",
            end_date="2024-01-03",
        )

        assert result["issue_type"] == "missing_dates"
        assert result["missing_count"] == 2
        assert "2024-01-01" in result["missing_dates"]
        assert "2024-01-03" in result["missing_dates"]
        assert result["total_expected_days"] == 3


class TestChunkList:
    """Tests for chunk_list utility function."""

    def test_chunk_list_even_split(self) -> None:
        """chunk_list splits list evenly."""
        from src.functions.data_gap_detector.detector import chunk_list

        items = [1, 2, 3, 4, 5, 6]
        result = chunk_list(items, 2)

        assert result == [[1, 2], [3, 4], [5, 6]]

    def test_chunk_list_uneven_split(self) -> None:
        """chunk_list handles uneven splits."""
        from src.functions.data_gap_detector.detector import chunk_list

        items = [1, 2, 3, 4, 5]
        result = chunk_list(items, 2)

        assert result == [[1, 2], [3, 4], [5]]

    def test_chunk_list_single_chunk(self) -> None:
        """chunk_list returns single chunk when size >= len."""
        from src.functions.data_gap_detector.detector import chunk_list

        items = [1, 2, 3]
        result = chunk_list(items, 10)

        assert result == [[1, 2, 3]]

    def test_chunk_list_empty(self) -> None:
        """chunk_list handles empty list."""
        from src.functions.data_gap_detector.detector import chunk_list

        result = chunk_list([], 5)

        assert result == []


class TestBuildQuery:
    """Tests for build_query function."""

    def test_build_query_basic(self) -> None:
        """build_query generates correct SQL."""
        from src.functions.data_gap_detector.detector import build_query

        sensor_ids = ["p:bunnings:abc", "p:bunnings:def"]
        query = build_query(sensor_ids, "2024-01-01", "2024-01-31")

        assert "SELECT" in query
        assert "sensorId" in query
        assert "DATE(ts)" in query
        assert "GROUP BY" in query
        assert "'p:bunnings:abc'" in query
        assert "'p:bunnings:def'" in query
        assert "2024-01-01" in query
        assert "2024-01-31" in query
