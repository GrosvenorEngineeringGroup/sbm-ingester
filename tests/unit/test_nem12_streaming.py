"""Unit tests for NEM12 streaming parser edge cases.

Tests the streaming parser (stream_nem12_file and stream_as_data_frames)
with various edge cases to ensure robust parsing of NEM12 files.
"""

import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest


class TestStreamingParserEdgeCases:
    """Tests for streaming parser edge cases."""

    def test_empty_file_returns_no_data(self, nem12_empty_file: str) -> None:
        """Test that file with only 100/900 records returns no data."""
        from libs.nemreader.streaming import stream_nem12_file

        result = list(stream_nem12_file(nem12_empty_file))
        assert result == []

    def test_no_900_record_still_parses(self, nem12_no_900_file: str) -> None:
        """Test that file without 900 record is still parsed correctly."""
        from libs.nemreader.streaming import stream_nem12_file

        result = list(stream_nem12_file(nem12_no_900_file))

        assert len(result) == 1
        nmi, suffix, uom, readings = result[0]
        assert nmi == "VABD000163"
        assert suffix == "E1"
        assert uom == "kWh"
        assert len(readings) == 48  # 30-minute intervals for one day

    def test_multiple_900_records_powercor_style(self, nem12_multiple_900_file: str) -> None:
        """Test that file with multiple 900 records (Powercor) is parsed correctly."""
        from libs.nemreader.streaming import stream_nem12_file

        result = list(stream_nem12_file(nem12_multiple_900_file))

        # Should have two NMI blocks
        assert len(result) == 2

        nmi1, suffix1, uom1, readings1 = result[0]
        assert nmi1 == "NMI001"
        assert readings1[0].read_value == 1.0

        nmi2, suffix2, uom2, readings2 = result[1]
        assert nmi2 == "NMI002"
        assert readings2[0].read_value == 2.0

    def test_400_event_records_update_quality(self, nem12_with_400_events_file: str) -> None:
        """Test that 400 event records update quality and event fields."""
        from libs.nemreader.streaming import stream_nem12_file

        result = list(stream_nem12_file(nem12_with_400_events_file))

        assert len(result) == 1
        nmi, suffix, uom, readings = result[0]

        # First 300 row has 400 records modifying intervals 1-10 and 20-30
        # Check that intervals 1-10 have the first event applied
        for i in range(10):
            # Note: Multiple 400 records - the last one (20-30) doesn't affect 1-10
            # Only intervals 1-10 affected by first 400 row
            if i < 10:
                assert readings[i].quality_method == "S14"
                assert readings[i].event_code == "51"
                assert readings[i].event_desc == "Power outage"

        # Intervals 20-30 should have second event (from second 400 row)
        for i in range(19, 30):
            assert readings[i].quality_method == "E52"
            assert readings[i].event_code == "52"
            assert readings[i].event_desc == "Meter fault"

        # Second 300 row (day 2) has intervals 15-25 modified
        day2_start = 48  # After first day's 48 intervals
        for i in range(day2_start + 14, day2_start + 25):
            assert readings[i].quality_method == "S14"
            assert readings[i].event_code == "53"
            assert readings[i].event_desc == "Estimated reading"

    def test_15min_interval_parsing(self, nem12_15min_interval_file: str) -> None:
        """Test that 15-minute interval files are parsed correctly."""
        from libs.nemreader.streaming import stream_nem12_file

        result = list(stream_nem12_file(nem12_15min_interval_file))

        assert len(result) == 1
        nmi, suffix, uom, readings = result[0]

        # 24 hours * 4 intervals/hour = 96 intervals
        assert len(readings) == 96
        assert readings[0].read_value == 0.5

    def test_5min_interval_parsing(self, nem12_5min_interval_file: str) -> None:
        """Test that 5-minute interval files are parsed correctly."""
        from libs.nemreader.streaming import stream_nem12_file

        result = list(stream_nem12_file(nem12_5min_interval_file))

        assert len(result) == 1
        nmi, suffix, uom, readings = result[0]

        # 24 hours * 12 intervals/hour = 288 intervals
        assert len(readings) == 288
        assert readings[0].read_value == 0.1

    def test_missing_values_parsed_as_none(self, nem12_missing_values_file: str) -> None:
        """Test that missing interval values are parsed as None."""
        from libs.nemreader.streaming import stream_nem12_file

        result = list(stream_nem12_file(nem12_missing_values_file))

        assert len(result) == 1
        nmi, suffix, uom, readings = result[0]

        # Check that some values are None (missing in file)
        assert readings[1].read_value is None  # Second value is empty
        assert readings[4].read_value is None  # Fifth value is empty
        assert readings[0].read_value == 1.111  # First value is present

    def test_multiday_data_concatenated(self, nem12_multiday_file: str) -> None:
        """Test that multi-day data is concatenated correctly."""
        from libs.nemreader.streaming import stream_nem12_file

        result = list(stream_nem12_file(nem12_multiday_file))

        assert len(result) == 1
        nmi, suffix, uom, readings = result[0]

        # 3 days * 48 intervals = 144
        assert len(readings) == 144

        # Day 1 values should be 1.0
        assert readings[0].read_value == 1.0
        assert readings[47].read_value == 1.0

        # Day 2 values should be 2.0
        assert readings[48].read_value == 2.0
        assert readings[95].read_value == 2.0

        # Day 3 values should be 3.0
        assert readings[96].read_value == 3.0
        assert readings[143].read_value == 3.0

    def test_blank_lines_ignored(self, nem12_blank_lines_file: str) -> None:
        """Test that blank lines in file are ignored."""
        from libs.nemreader.streaming import stream_nem12_file

        result = list(stream_nem12_file(nem12_blank_lines_file))

        assert len(result) == 1
        nmi, suffix, uom, readings = result[0]
        assert nmi == "VABD000163"
        assert len(readings) == 48

    def test_reading_timestamps_correct(self, nem12_sample_file: str) -> None:
        """Test that reading timestamps are calculated correctly."""
        from libs.nemreader.streaming import stream_nem12_file

        result = list(stream_nem12_file(nem12_sample_file))

        assert len(result) > 0
        nmi, suffix, uom, readings = result[0]

        # First reading should start at midnight
        assert readings[0].t_start == datetime(2004, 2, 1, 0, 0, 0)
        assert readings[0].t_end == datetime(2004, 2, 1, 0, 30, 0)

        # Second reading should start at 00:30
        assert readings[1].t_start == datetime(2004, 2, 1, 0, 30, 0)
        assert readings[1].t_end == datetime(2004, 2, 1, 1, 0, 0)

    def test_multiple_channels_same_nmi(self, nem12_sample_file: str) -> None:
        """Test that multiple channels for same NMI are parsed separately."""
        from libs.nemreader.streaming import stream_nem12_file

        result = list(stream_nem12_file(nem12_sample_file))

        # Sample file has E1 and Q1 channels
        assert len(result) == 2

        suffixes = [r[1] for r in result]
        assert "E1" in suffixes
        assert "Q1" in suffixes

    def test_multiple_nmis_yields_separately(self, nem12_multiple_meters_file: str) -> None:
        """Test that multiple NMIs are yielded as separate blocks."""
        from libs.nemreader.streaming import stream_nem12_file

        result = list(stream_nem12_file(nem12_multiple_meters_file))

        # Should have multiple NMI blocks
        nmis = set(r[0] for r in result)
        assert len(nmis) >= 2

    def test_zip_file_support(self, nem12_sample_file: str, temp_directory: str) -> None:
        """Test that ZIP files are parsed correctly."""
        from libs.nemreader.streaming import stream_nem12_file

        # Create ZIP file from sample
        zip_path = Path(temp_directory) / "test.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.write(nem12_sample_file, "nem12_sample.csv")

        result = list(stream_nem12_file(str(zip_path)))

        assert len(result) > 0
        nmi, suffix, uom, readings = result[0]
        assert nmi == "VABD000163"

    def test_split_days_option(self, nem12_sample_file: str) -> None:
        """Test that split_days option works correctly."""
        from libs.nemreader.streaming import stream_nem12_file

        # With split_days=False
        result_no_split = list(stream_nem12_file(nem12_sample_file, split_days=False))
        assert len(result_no_split) > 0

        # With split_days=True
        result_split = list(stream_nem12_file(nem12_sample_file, split_days=True))
        assert len(result_split) > 0


class TestStreamAsDataFramesEdgeCases:
    """Tests for stream_as_data_frames edge cases."""

    def test_empty_file_returns_empty_generator(self, nem12_empty_file: str) -> None:
        """Test that empty file returns no DataFrames."""
        from shared.nem_adapter import stream_as_data_frames

        result = list(stream_as_data_frames(nem12_empty_file))
        assert result == []

    def test_no_900_record_still_returns_dataframe(self, nem12_no_900_file: str) -> None:
        """Test that file without 900 record still returns DataFrame."""
        from shared.nem_adapter import stream_as_data_frames

        result = list(stream_as_data_frames(nem12_no_900_file))

        assert len(result) == 1
        nmi, df = result[0]
        assert nmi == "VABD000163"
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 48

    def test_multiple_900_yields_multiple_dataframes(self, nem12_multiple_900_file: str) -> None:
        """Test that file with multiple 900 records yields multiple DataFrames."""
        from shared.nem_adapter import stream_as_data_frames

        result = list(stream_as_data_frames(nem12_multiple_900_file))

        assert len(result) == 2

        nmis = [nmi for nmi, df in result]
        assert "NMI001" in nmis
        assert "NMI002" in nmis

    def test_dataframe_has_correct_columns(self, nem12_sample_file: str) -> None:
        """Test that DataFrame has correct column structure."""
        from shared.nem_adapter import stream_as_data_frames

        result = list(stream_as_data_frames(nem12_sample_file))

        assert len(result) > 0
        nmi, df = result[0]

        # Required metadata columns
        required_cols = {"t_start", "t_end", "quality_method", "event_code", "event_desc"}
        for col in required_cols:
            assert col in df.columns

    def test_channel_columns_named_correctly(self, nem12_sample_file: str) -> None:
        """Test that channel columns follow suffix_unit format."""
        from shared.nem_adapter import stream_as_data_frames

        result = list(stream_as_data_frames(nem12_sample_file))

        assert len(result) > 0
        nmi, df = result[0]

        # Find data columns
        metadata_cols = {"t_start", "t_end", "quality_method", "event_code", "event_desc"}
        data_cols = [col for col in df.columns if col not in metadata_cols]

        assert len(data_cols) >= 1
        # Should be like "E1_kWh"
        for col in data_cols:
            parts = col.split("_")
            assert len(parts) >= 2, f"Column {col} should have suffix_unit format"

    def test_multiple_channels_merged_into_single_df(self, nem12_sample_file: str) -> None:
        """Test that multiple channels for same NMI are merged into single DataFrame."""
        from shared.nem_adapter import stream_as_data_frames

        result = list(stream_as_data_frames(nem12_sample_file))

        # Should have one NMI with multiple channel columns
        assert len(result) == 1
        nmi, df = result[0]

        # Find data columns
        metadata_cols = {"t_start", "t_end", "quality_method", "event_code", "event_desc"}
        data_cols = [col for col in df.columns if col not in metadata_cols]

        # Sample file has E1 and Q1 channels
        assert len(data_cols) >= 2

    def test_15min_interval_dataframe_size(self, nem12_15min_interval_file: str) -> None:
        """Test that 15-minute interval file produces correct DataFrame size."""
        from shared.nem_adapter import stream_as_data_frames

        result = list(stream_as_data_frames(nem12_15min_interval_file))

        assert len(result) == 1
        nmi, df = result[0]
        assert len(df) == 96  # 24 hours * 4 intervals/hour

    def test_missing_values_in_dataframe(self, nem12_missing_values_file: str) -> None:
        """Test that missing values appear as NaN in DataFrame."""
        from shared.nem_adapter import stream_as_data_frames

        result = list(stream_as_data_frames(nem12_missing_values_file))

        assert len(result) == 1
        nmi, df = result[0]

        # Find the data column
        metadata_cols = {"t_start", "t_end", "quality_method", "event_code", "event_desc"}
        data_cols = [col for col in df.columns if col not in metadata_cols]

        # Should have some NaN values
        assert df[data_cols[0]].isna().any()

    def test_multiday_data_in_dataframe(self, nem12_multiday_file: str) -> None:
        """Test that multi-day data is in single DataFrame."""
        from shared.nem_adapter import stream_as_data_frames

        result = list(stream_as_data_frames(nem12_multiday_file))

        assert len(result) == 1
        nmi, df = result[0]

        # 3 days * 48 intervals = 144
        assert len(df) == 144

    def test_zip_file_support_stream(self, nem12_sample_file: str, temp_directory: str) -> None:
        """Test that ZIP files are supported in stream_as_data_frames."""
        from shared.nem_adapter import stream_as_data_frames

        # Create ZIP file from sample
        zip_path = Path(temp_directory) / "test.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.write(nem12_sample_file, "nem12_sample.csv")

        result = list(stream_as_data_frames(str(zip_path)))

        assert len(result) > 0
        nmi, df = result[0]
        assert nmi == "VABD000163"
        assert isinstance(df, pd.DataFrame)


class TestStreamingVsBatchEquivalence:
    """Tests to verify streaming and batch parsers produce equivalent results."""

    def test_same_data_as_batch_parser(self, nem12_sample_file: str) -> None:
        """Test that streaming parser produces same data as batch parser."""
        from shared.nem_adapter import output_as_data_frames, stream_as_data_frames

        batch_result = output_as_data_frames(nem12_sample_file)
        stream_result = list(stream_as_data_frames(nem12_sample_file))

        # Same number of NMIs
        assert len(batch_result) == len(stream_result)

        # Same NMIs
        batch_nmis = sorted([nmi for nmi, df in batch_result])
        stream_nmis = sorted([nmi for nmi, df in stream_result])
        assert batch_nmis == stream_nmis

    def test_same_column_names(self, nem12_sample_file: str) -> None:
        """Test that streaming and batch parsers produce same column names."""
        from shared.nem_adapter import output_as_data_frames, stream_as_data_frames

        batch_result = output_as_data_frames(nem12_sample_file)
        stream_result = list(stream_as_data_frames(nem12_sample_file))

        for (_batch_nmi, batch_df), (_stream_nmi, stream_df) in zip(
            sorted(batch_result, key=lambda x: x[0]),
            sorted(stream_result, key=lambda x: x[0]),
        ):
            assert set(batch_df.columns) == set(stream_df.columns)

    def test_same_data_values(self, nem12_sample_file: str) -> None:
        """Test that streaming and batch parsers produce same data values."""
        from shared.nem_adapter import output_as_data_frames, stream_as_data_frames

        batch_result = output_as_data_frames(nem12_sample_file)
        stream_result = list(stream_as_data_frames(nem12_sample_file))

        for (batch_nmi, batch_df), (stream_nmi, stream_df) in zip(
            sorted(batch_result, key=lambda x: x[0]),
            sorted(stream_result, key=lambda x: x[0]),
        ):
            assert batch_nmi == stream_nmi

            # Compare data columns
            metadata_cols = {"t_start", "t_end", "quality_method", "event_code", "event_desc"}
            data_cols = [col for col in batch_df.columns if col not in metadata_cols]

            for col in data_cols:
                if col in stream_df.columns:
                    pd.testing.assert_series_equal(
                        batch_df[col].reset_index(drop=True),
                        stream_df[col].reset_index(drop=True),
                        check_names=False,
                    )


class TestMalformedInputHandling:
    """Tests for handling malformed input data."""

    def test_completely_empty_file(self, temp_directory: str) -> None:
        """Test handling of completely empty file."""
        from libs.nemreader.streaming import stream_nem12_file

        empty_file = Path(temp_directory) / "empty.csv"
        empty_file.write_text("")

        result = list(stream_nem12_file(str(empty_file)))
        assert result == []

    def test_file_with_only_garbage(self, temp_directory: str) -> None:
        """Test handling of file with only garbage data."""
        from libs.nemreader.streaming import stream_nem12_file

        garbage_file = Path(temp_directory) / "garbage.csv"
        garbage_file.write_text("not,valid,nem,data\nsome,random,garbage,here\n")

        result = list(stream_nem12_file(str(garbage_file)))
        assert result == []

    def test_malformed_200_row_skipped(self, temp_directory: str) -> None:
        """Test that malformed 200 row is skipped and logged."""
        from libs.nemreader.streaming import stream_nem12_file

        # 200 row with missing required fields
        content = """100,NEM12,200405011135,MDA1,Ret1
200,BADNMI
200,VABD000163,E1Q1,1,E1,N1,METSER123,kWh,30,
300,20040201,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,A,,,20040202120025,
900
"""
        malformed_file = Path(temp_directory) / "malformed_200.csv"
        malformed_file.write_text(content)

        result = list(stream_nem12_file(str(malformed_file)))

        # Should still parse the valid NMI
        assert len(result) >= 1

    def test_malformed_300_row_skipped(self, temp_directory: str) -> None:
        """Test that malformed 300 row is skipped and logged."""
        from libs.nemreader.streaming import stream_nem12_file

        # 300 row with invalid date
        content = """100,NEM12,200405011135,MDA1,Ret1
200,VABD000163,E1Q1,1,E1,N1,METSER123,kWh,30,
300,INVALIDDATE,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,A,,,
300,20040201,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,A,,,
900
"""
        malformed_file = Path(temp_directory) / "malformed_300.csv"
        malformed_file.write_text(content)

        result = list(stream_nem12_file(str(malformed_file)))

        # Should still parse the valid 300 row
        assert len(result) == 1
        nmi, suffix, uom, readings = result[0]
        assert len(readings) == 48  # Only the valid day

    def test_truncated_300_row(self, temp_directory: str) -> None:
        """Test that truncated 300 row (not enough intervals) is skipped."""
        from libs.nemreader.streaming import stream_nem12_file

        # 300 row with only 10 values instead of 48
        content = """100,NEM12,200405011135,MDA1,Ret1
200,VABD000163,E1Q1,1,E1,N1,METSER123,kWh,30,
300,20040201,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,A,,,
300,20040202,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0,A,,,
900
"""
        truncated_file = Path(temp_directory) / "truncated_300.csv"
        truncated_file.write_text(content)

        result = list(stream_nem12_file(str(truncated_file)))

        # Should only parse the complete 300 row
        assert len(result) == 1
        nmi, suffix, uom, readings = result[0]
        # Only the second day should be parsed
        assert len(readings) == 48

    def test_invalid_400_row_interval(self, temp_directory: str) -> None:
        """Test that 400 row with invalid interval range is skipped."""
        from libs.nemreader.streaming import stream_nem12_file

        # 400 row with invalid interval (start > end)
        content = """100,NEM12,200405011135,MDA1,Ret1
200,VABD000163,E1Q1,1,E1,N1,METSER123,kWh,30,
300,20040201,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,A,,,
400,30,10,S14,51,Invalid range
900
"""
        invalid_file = Path(temp_directory) / "invalid_400.csv"
        invalid_file.write_text(content)

        result = list(stream_nem12_file(str(invalid_file)))

        # Should still parse the data, just skip the invalid 400 row
        assert len(result) == 1
        nmi, suffix, uom, readings = result[0]
        # Quality should still be 'A' (not modified by invalid 400)
        assert readings[0].quality_method == "A"


class TestZipFileHandling:
    """Tests for ZIP file handling edge cases."""

    def test_zip_with_multiple_files_raises(self, nem12_sample_file: str, temp_directory: str) -> None:
        """Test that ZIP with multiple files raises ValueError."""
        from libs.nemreader.streaming import stream_nem12_file

        zip_path = Path(temp_directory) / "multi.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.write(nem12_sample_file, "file1.csv")
            zf.write(nem12_sample_file, "file2.csv")

        with pytest.raises(ValueError, match="exactly one file"):
            list(stream_nem12_file(str(zip_path)))

    def test_zip_with_empty_file(self, temp_directory: str) -> None:
        """Test handling of ZIP with empty CSV."""
        from libs.nemreader.streaming import stream_nem12_file

        empty_csv = Path(temp_directory) / "empty.csv"
        empty_csv.write_text("")

        zip_path = Path(temp_directory) / "empty.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.write(empty_csv, "empty.csv")

        result = list(stream_nem12_file(str(zip_path)))
        assert result == []

    def test_corrupted_zip_fallback_to_csv(self, temp_directory: str) -> None:
        """Test that corrupted ZIP falls back to CSV parsing."""
        from libs.nemreader.streaming import stream_nem12_file

        # Write NEM12 content but with .zip extension (BadZipFile triggers CSV fallback)
        fake_zip = Path(temp_directory) / "fake.zip"
        fake_zip.write_text("""100,NEM12,200405011135,MDA1,Ret1
200,TESTNMI,E1,1,E1,N1,METER1,kWh,30,
300,20040201,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,A,,,
900
""")

        # Should fall back to CSV parsing since it's not a valid ZIP
        result = list(stream_nem12_file(str(fake_zip)))
        assert len(result) == 1
        nmi, suffix, uom, readings = result[0]
        assert nmi == "TESTNMI"
