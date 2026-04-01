"""Unit tests for nem_adapter.py module."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


class TestOutputAsDataFrames:
    """Tests for output_as_data_frames function."""

    def test_nem12_file_parsing(self, nem12_sample_file: str) -> None:
        """Test that NEM12 file is parsed correctly."""
        from shared.nem_adapter import output_as_data_frames

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
        from shared.nem_adapter import output_as_data_frames

        result = output_as_data_frames(nem13_sample_file)

        assert isinstance(result, list)
        assert len(result) > 0

        for nmi, df in result:
            assert isinstance(nmi, str)
            assert isinstance(df, pd.DataFrame)

    def test_column_naming_suffix_unit_format(self, nem12_sample_file: str) -> None:
        """Test that column names follow suffix_unit format (e.g., E1_kWh)."""
        from shared.nem_adapter import output_as_data_frames

        result = output_as_data_frames(nem12_sample_file)
        assert len(result) > 0

        _nmi, df = result[0]

        # Find data columns (not metadata columns); quality_* columns are also excluded
        metadata_cols = {"t_start", "t_end", "event_code", "event_desc"}
        data_cols = [col for col in df.columns if col not in metadata_cols and not col.startswith("quality_")]

        # Each data column should have format like "E1_kWh"
        for col in data_cols:
            parts = col.split("_")
            assert len(parts) >= 2, f"Column {col} should have suffix_unit format"
            # First part should be channel suffix (like E1, B1)
            assert len(parts[0]) >= 2, f"Channel suffix {parts[0]} too short"

    def test_split_days_enabled(self, nem12_sample_file: str) -> None:
        """Test that split_days=True splits multi-day readings."""
        from shared.nem_adapter import output_as_data_frames

        result = output_as_data_frames(nem12_sample_file, split_days=True)
        assert len(result) > 0

        # Data should be present
        _nmi, df = result[0]
        assert len(df) > 0

    def test_split_days_disabled(self, nem12_sample_file: str) -> None:
        """Test that split_days=False keeps original data."""
        from shared.nem_adapter import output_as_data_frames

        result = output_as_data_frames(nem12_sample_file, split_days=False)
        assert len(result) > 0

        _nmi, df = result[0]
        assert len(df) > 0

    def test_invalid_file_raises_exception(self, temp_directory: str) -> None:
        """Test that invalid file raises exception."""
        from shared.nem_adapter import output_as_data_frames

        invalid_file = str(Path(temp_directory) / "invalid.csv")
        with Path(invalid_file).open("w") as f:
            f.write("not,a,valid,nem,file\n1,2,3,4,5\n")

        with pytest.raises((ValueError, KeyError, IndexError)):
            output_as_data_frames(invalid_file)

    def test_nonexistent_file_raises_exception(self) -> None:
        """Test that nonexistent file raises exception."""
        from shared.nem_adapter import output_as_data_frames

        with pytest.raises((FileNotFoundError, OSError)):
            output_as_data_frames("/nonexistent/path/to/file.csv")

    def test_multiple_nmis_in_file(self, nem12_multiple_meters_file: str) -> None:
        """Test that file with multiple NMIs returns multiple tuples."""
        from shared.nem_adapter import output_as_data_frames

        result = output_as_data_frames(nem12_multiple_meters_file)

        # Should have multiple NMIs
        nmis = [nmi for nmi, df in result]
        assert len(nmis) >= 1  # At least one NMI

        # All NMIs should be unique
        assert len(nmis) == len(set(nmis))

    def test_dataframe_has_required_columns(self, nem12_sample_file: str) -> None:
        """Test that DataFrame has required metadata columns."""
        from shared.nem_adapter import output_as_data_frames

        result = output_as_data_frames(nem12_sample_file)
        assert len(result) > 0

        _nmi, df = result[0]

        # Should have these metadata columns (quality_method replaced by quality_<suffix>)
        expected_cols = {"t_start", "t_end", "event_code", "event_desc"}
        actual_cols = set(df.columns)

        for col in expected_cols:
            assert col in actual_cols, f"Missing column: {col}"

        # At least one per-channel quality column must exist
        quality_cols = [col for col in df.columns if col.startswith("quality_")]
        assert len(quality_cols) >= 1, "Expected at least one quality_<suffix> column"

    def test_t_start_is_datetime(self, nem12_sample_file: str) -> None:
        """Test that t_start column contains datetime values."""
        from shared.nem_adapter import output_as_data_frames

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
        from shared.nem_adapter import _build_nmi_dataframe

        result = _build_nmi_dataframe(nmi="TEST123", nmi_readings={}, nmi_transactions={}, split_days=True)

        assert result is None

    def test_empty_readings_returns_none(self) -> None:
        """Test that empty readings returns None."""
        from shared.nem_adapter import _build_nmi_dataframe

        result = _build_nmi_dataframe(
            nmi="TEST123", nmi_readings={"E1": []}, nmi_transactions={"E1": []}, split_days=True
        )

        assert result is None


class TestUnitExtraction:
    """Tests for unit extraction from readings."""

    def test_kwh_unit_extracted(self, nem12_sample_file: str) -> None:
        """Test that kWh unit is extracted correctly."""
        from shared.nem_adapter import output_as_data_frames

        result = output_as_data_frames(nem12_sample_file)
        assert len(result) > 0

        _nmi, df = result[0]

        # Find data columns (quality_* columns are also excluded)
        metadata_cols = {"t_start", "t_end", "event_code", "event_desc"}
        data_cols = [col for col in df.columns if col not in metadata_cols and not col.startswith("quality_")]

        # At least one column should have a unit suffix
        for col in data_cols:
            if "_" in col:
                unit = col.split("_")[-1]
                # Unit should be a reasonable energy unit
                assert unit.lower() in ["kwh", "kvarh", "kw", "kvar", "mwh", "wh"], f"Unexpected unit: {unit}"


class TestPerChannelQuality:
    """Tests for per-channel quality_<suffix> columns."""

    def test_quality_column_per_channel(self, nem12_sample_file: str) -> None:
        """Each data channel must have a corresponding quality_<suffix> column."""
        from shared.nem_adapter import output_as_data_frames

        result = output_as_data_frames(nem12_sample_file)
        assert len(result) > 0

        _nmi, df = result[0]

        metadata_cols = {"t_start", "t_end", "event_code", "event_desc"}
        data_cols = [col for col in df.columns if col not in metadata_cols and not col.startswith("quality_")]

        for col in data_cols:
            suffix = col.split("_")[0]  # e.g. "E1" from "E1_kWh"
            quality_col = f"quality_{suffix}"
            assert quality_col in df.columns, f"Missing {quality_col} for data column {col}"

    def test_shared_quality_method_column_removed(self, nem12_sample_file: str) -> None:
        """The legacy quality_method column must no longer exist."""
        from shared.nem_adapter import output_as_data_frames

        result = output_as_data_frames(nem12_sample_file)
        assert len(result) > 0

        _nmi, df = result[0]
        assert "quality_method" not in df.columns, "quality_method column should have been removed"

    def test_quality_values_are_strings(self, nem12_sample_file: str) -> None:
        """Quality column values must be non-empty strings."""
        from shared.nem_adapter import output_as_data_frames

        result = output_as_data_frames(nem12_sample_file)
        assert len(result) > 0

        _nmi, df = result[0]

        quality_cols = [col for col in df.columns if col.startswith("quality_")]
        assert len(quality_cols) >= 1

        for q_col in quality_cols:
            for val in df[q_col]:
                assert isinstance(val, str), f"Quality value {val!r} in {q_col} is not a string"
                assert len(val) > 0, f"Quality value in {q_col} is an empty string"

    def test_multi_channel_different_quality(self) -> None:
        """B1 and B2 channels store their own quality independently."""
        from shared.nem_adapter import _build_nmi_dataframe

        def make_reading(ts_offset: int, quality: str) -> MagicMock:
            r = MagicMock()
            r.t_start = pd.Timestamp(f"2024-01-01 0{ts_offset}:00:00")
            r.t_end = pd.Timestamp(f"2024-01-01 0{ts_offset}:30:00")
            r.quality_method = quality
            r.event_code = ""
            r.event_desc = ""
            r.read_value = float(ts_offset)
            r.uom = "kWh"
            return r

        b1_readings = [make_reading(0, "S14"), make_reading(1, "S14")]
        b2_readings = [make_reading(0, "A"), make_reading(1, "A")]

        df = _build_nmi_dataframe(
            nmi="TEST123",
            nmi_readings={"B1": b1_readings, "B2": b2_readings},
            nmi_transactions={"B1": [], "B2": []},
            split_days=False,
        )

        assert df is not None
        assert "quality_B1" in df.columns
        assert "quality_B2" in df.columns
        assert list(df["quality_B1"]) == ["S14", "S14"]
        assert list(df["quality_B2"]) == ["A", "A"]

    def test_nem13_per_channel_quality(self, nem13_sample_file: str) -> None:
        """NEM13 files must also produce per-channel quality_<suffix> columns."""
        from shared.nem_adapter import output_as_data_frames

        result = output_as_data_frames(nem13_sample_file)
        assert len(result) > 0

        _nmi, df = result[0]

        assert "quality_method" not in df.columns, "quality_method should not exist for NEM13 either"
        quality_cols = [col for col in df.columns if col.startswith("quality_")]
        assert len(quality_cols) >= 1, "NEM13 DataFrame must have at least one quality_<suffix> column"


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    @patch("shared.nem_adapter.logger")
    def test_nmi_processing_error_logged(self, mock_log: MagicMock, nem12_sample_file: str) -> None:
        """Test that NMI processing errors are logged."""
        from shared.nem_adapter import output_as_data_frames

        # This should not raise, errors should be logged
        result = output_as_data_frames(nem12_sample_file)

        # Should still return valid results
        assert isinstance(result, list)

    def test_default_unit_when_missing(self) -> None:
        """Test that default unit (kWh) is used when uom is missing."""
        from shared.nem_adapter import _build_nmi_dataframe

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
