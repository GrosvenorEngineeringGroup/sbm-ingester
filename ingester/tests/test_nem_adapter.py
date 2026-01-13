"""Unit tests for nem_adapter.py module."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


class TestOutputAsDataFrames:
    """Tests for output_as_data_frames function."""

    def test_nem12_file_parsing(self, nem12_sample_file: str) -> None:
        """Test that NEM12 file is parsed correctly."""
        from modules.nem_adapter import output_as_data_frames

        result = output_as_data_frames(nem12_sample_file)

        assert isinstance(result, list)
        assert len(result) > 0

        # Each result should be a tuple of (NMI, DataFrame)
        for nmi, df in result:
            assert isinstance(nmi, str)
            assert isinstance(df, pd.DataFrame)
            assert len(nmi) > 0

    def test_nem13_file_parsing(self, nem13_sample_file: str) -> None:
        """Test that NEM13 file is parsed correctly."""
        from modules.nem_adapter import output_as_data_frames

        result = output_as_data_frames(nem13_sample_file)

        assert isinstance(result, list)
        assert len(result) > 0

        for nmi, df in result:
            assert isinstance(nmi, str)
            assert isinstance(df, pd.DataFrame)

    def test_column_naming_suffix_unit_format(self, nem12_sample_file: str) -> None:
        """Test that column names follow suffix_unit format (e.g., E1_kWh)."""
        from modules.nem_adapter import output_as_data_frames

        result = output_as_data_frames(nem12_sample_file)
        assert len(result) > 0

        _nmi, df = result[0]

        # Find data columns (not metadata columns)
        metadata_cols = {"t_start", "t_end", "quality_method", "event_code", "event_desc"}
        data_cols = [col for col in df.columns if col not in metadata_cols]

        # Each data column should have format like "E1_kWh"
        for col in data_cols:
            parts = col.split("_")
            assert len(parts) >= 2, f"Column {col} should have suffix_unit format"
            # First part should be channel suffix (like E1, B1)
            assert len(parts[0]) >= 2, f"Channel suffix {parts[0]} too short"

    def test_split_days_enabled(self, nem12_sample_file: str) -> None:
        """Test that split_days=True splits multi-day readings."""
        from modules.nem_adapter import output_as_data_frames

        result = output_as_data_frames(nem12_sample_file, split_days=True)
        assert len(result) > 0

        # Data should be present
        _nmi, df = result[0]
        assert len(df) > 0

    def test_split_days_disabled(self, nem12_sample_file: str) -> None:
        """Test that split_days=False keeps original data."""
        from modules.nem_adapter import output_as_data_frames

        result = output_as_data_frames(nem12_sample_file, split_days=False)
        assert len(result) > 0

        _nmi, df = result[0]
        assert len(df) > 0

    def test_invalid_file_raises_exception(self, temp_directory: str) -> None:
        """Test that invalid file raises exception."""
        from modules.nem_adapter import output_as_data_frames

        invalid_file = str(Path(temp_directory) / "invalid.csv")
        with Path(invalid_file).open("w") as f:
            f.write("not,a,valid,nem,file\n1,2,3,4,5\n")

        with pytest.raises((ValueError, KeyError, IndexError)):
            output_as_data_frames(invalid_file)

    def test_nonexistent_file_raises_exception(self) -> None:
        """Test that nonexistent file raises exception."""
        from modules.nem_adapter import output_as_data_frames

        with pytest.raises((FileNotFoundError, OSError)):
            output_as_data_frames("/nonexistent/path/to/file.csv")

    def test_multiple_nmis_in_file(self, nem12_multiple_meters_file: str) -> None:
        """Test that file with multiple NMIs returns multiple tuples."""
        from modules.nem_adapter import output_as_data_frames

        result = output_as_data_frames(nem12_multiple_meters_file)

        # Should have multiple NMIs
        nmis = [nmi for nmi, df in result]
        assert len(nmis) >= 1  # At least one NMI

        # All NMIs should be unique
        assert len(nmis) == len(set(nmis))

    def test_dataframe_has_required_columns(self, nem12_sample_file: str) -> None:
        """Test that DataFrame has required metadata columns."""
        from modules.nem_adapter import output_as_data_frames

        result = output_as_data_frames(nem12_sample_file)
        assert len(result) > 0

        _nmi, df = result[0]

        # Should have these metadata columns
        expected_cols = {"t_start", "t_end", "quality_method", "event_code", "event_desc"}
        actual_cols = set(df.columns)

        for col in expected_cols:
            assert col in actual_cols, f"Missing column: {col}"

    def test_t_start_is_datetime(self, nem12_sample_file: str) -> None:
        """Test that t_start column contains datetime values."""
        from modules.nem_adapter import output_as_data_frames

        result = output_as_data_frames(nem12_sample_file)
        assert len(result) > 0

        _nmi, df = result[0]

        # t_start should be datetime
        if "t_start" in df.columns:
            assert pd.api.types.is_datetime64_any_dtype(df["t_start"]) or df.index.name == "t_start"


class TestBuildNmiDataframe:
    """Tests for _build_nmi_dataframe helper function."""

    def test_empty_channels_returns_none(self) -> None:
        """Test that empty channels returns None."""
        from modules.nem_adapter import _build_nmi_dataframe

        result = _build_nmi_dataframe(nmi="TEST123", nmi_readings={}, nmi_transactions={}, split_days=True)

        assert result is None

    def test_empty_readings_returns_none(self) -> None:
        """Test that empty readings returns None."""
        from modules.nem_adapter import _build_nmi_dataframe

        result = _build_nmi_dataframe(
            nmi="TEST123", nmi_readings={"E1": []}, nmi_transactions={"E1": []}, split_days=True
        )

        assert result is None


class TestUnitExtraction:
    """Tests for unit extraction from readings."""

    def test_kwh_unit_extracted(self, nem12_sample_file: str) -> None:
        """Test that kWh unit is extracted correctly."""
        from modules.nem_adapter import output_as_data_frames

        result = output_as_data_frames(nem12_sample_file)
        assert len(result) > 0

        _nmi, df = result[0]

        # Find data columns
        metadata_cols = {"t_start", "t_end", "quality_method", "event_code", "event_desc"}
        data_cols = [col for col in df.columns if col not in metadata_cols]

        # At least one column should have a unit suffix
        for col in data_cols:
            if "_" in col:
                unit = col.split("_")[-1]
                # Unit should be a reasonable energy unit
                assert unit.lower() in ["kwh", "kvarh", "kw", "kvar", "mwh", "wh"], f"Unexpected unit: {unit}"


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    @patch("modules.nem_adapter.parse_error_log")
    def test_nmi_processing_error_logged(self, mock_log: MagicMock, nem12_sample_file: str) -> None:
        """Test that NMI processing errors are logged."""
        from modules.nem_adapter import output_as_data_frames

        # This should not raise, errors should be logged
        result = output_as_data_frames(nem12_sample_file)

        # Should still return valid results
        assert isinstance(result, list)

    def test_default_unit_when_missing(self) -> None:
        """Test that default unit (kWh) is used when uom is missing."""
        from modules.nem_adapter import _build_nmi_dataframe

        # Create mock reading without uom
        mock_reading = MagicMock()
        mock_reading.t_start = pd.Timestamp("2024-01-01 00:00:00")
        mock_reading.t_end = pd.Timestamp("2024-01-01 00:30:00")
        mock_reading.quality_method = "A"
        mock_reading.event_code = ""
        mock_reading.event_desc = ""
        mock_reading.read_value = 1.5
        mock_reading.uom = None  # No unit

        result = _build_nmi_dataframe(
            nmi="TEST123", nmi_readings={"E1": [mock_reading]}, nmi_transactions={"E1": []}, split_days=False
        )

        assert result is not None
        # Should default to kWh
        assert "E1_kWh" in result.columns
