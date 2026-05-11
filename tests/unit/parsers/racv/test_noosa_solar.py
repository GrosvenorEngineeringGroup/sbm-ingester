"""Comprehensive tests for the Noosa Solar parser."""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from shared.parsers import NotRelevantParser, ParserError, ParserOutcome


def _processed_dfs(result: ParserOutcome) -> list[tuple[str, pd.DataFrame]]:
    assert result.status == "processed"
    assert result.source_row_count >= 1
    return result.dataframes


# ==================== Helpers ====================


def _create_noosa_csv(
    filepath: str,
    *,
    columns: dict[str, list[str]] | None = None,
    timestamps: list[str] | None = None,
    include_bom: bool = True,
) -> str:
    """Create a sample RACV_Noosa_Solar CSV file.

    Args:
        filepath: Path to write the CSV file.
        columns: Dict mapping column header (sensor ID) to list of values.
        timestamps: List of timestamp strings.  Defaults to three rows.
        include_bom: Whether to write a UTF-8 BOM (matches real-world files).
    """
    if timestamps is None:
        timestamps = [
            "31-Mar-26 8:00 AM AEST",
            "31-Mar-26 8:30 AM AEST",
            "31-Mar-26 9:00 AM AEST",
        ]

    if columns is None:
        columns = {
            "p:racv:r:abc123-energy": ["1.5", "2.0", "3.5"],
        }

    # Build a DataFrame and use pandas to_csv to handle quoting correctly
    data: dict[str, list[str]] = {"timestamp": timestamps}
    for col_name, col_values in columns.items():
        data[col_name] = col_values

    df = pd.DataFrame(data)
    df.to_csv(filepath, index=False, encoding="utf-8-sig" if include_bom else "utf-8")
    return filepath


# ==================== Tests ====================


class TestParseNumericColumns:
    """Tests for numeric kWh value parsing."""

    def test_parse_numeric_columns(self, tmp_path: Path) -> None:
        """Numeric kWh values parsed correctly, column name is E1_kWh."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        _create_noosa_csv(
            filepath,
            columns={"p:racv:r:sensor1": ["10.5", "20.0", "30.5"]},
        )

        with patch("shared.parsers.racv.noosa_solar.logger"):
            from shared.parsers.racv.noosa_solar import noosa_solar_parser

            result = _processed_dfs(noosa_solar_parser(filepath))

        assert len(result) == 1
        sensor_id, df = result[0]
        assert sensor_id == "p:racv:r:sensor1"
        assert "E1_kWh" in df.columns
        assert list(df["E1_kWh"]) == [10.5, 20.0, 30.5]

    def test_malformed_numeric_value_skip_counts(self, tmp_path: Path) -> None:
        """Numeric-looking columns skip-count non-empty malformed values."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        _create_noosa_csv(
            filepath,
            columns={"p:racv:r:sensor1": ["1.0", "bad", "2.0"]},
        )

        with patch("shared.parsers.racv.noosa_solar.logger"):
            from shared.parsers.racv.noosa_solar import noosa_solar_parser

            result = noosa_solar_parser(filepath)
            assert result.status == "processed"
            assert result.skip_reasons["unparseable_value"] == 1
            # Two valid rows survive in the output DataFrame.
            assert len(result.dataframes) == 1
            _sensor, df = result.dataframes[0]
            assert len(df) == 2

    def test_partial_malformed_timestamp_with_valid_rows_skip_counts(self, tmp_path: Path) -> None:
        """N valid rows + 1 malformed timestamp → N rows in output, rows_skipped=1."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        _create_noosa_csv(
            filepath,
            timestamps=[
                "31-Mar-26 8:00 AM AEST",
                "31-Mar-26 8:30 AM AEST",
                "not-a-date AEST",
            ],
            columns={"p:racv:r:sensor1": ["1.0", "2.0", "3.0"]},
        )

        with patch("shared.parsers.racv.noosa_solar.logger"):
            from shared.parsers.racv.noosa_solar import noosa_solar_parser

            result = noosa_solar_parser(filepath)
            assert result.status == "processed"
            assert result.skip_reasons["unparseable_timestamp"] == 1
            assert len(result.dataframes) == 1
            _sensor, df = result.dataframes[0]
            assert len(df) == 2


class TestParseStatusColumns:
    """Tests for Fronius status string mapping."""

    def test_parse_status_columns(self, tmp_path: Path) -> None:
        """Status strings mapped to codes via FRONIUS_MODE_MAP, column name is E1_mode."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        _create_noosa_csv(
            filepath,
            columns={
                "p:racv:r:status1": [
                    "Normal Operation",
                    "Standby",
                    "Error Exists",
                ],
            },
        )

        with patch("shared.parsers.racv.noosa_solar.logger"):
            from shared.parsers.racv.noosa_solar import noosa_solar_parser

            result = _processed_dfs(noosa_solar_parser(filepath))

        assert len(result) == 1
        sensor_id, df = result[0]
        assert sensor_id == "p:racv:r:status1"
        assert "E1_mode" in df.columns
        assert list(df["E1_mode"]) == [4.0, 8.0, 7.0]


class TestRejectsNonMatchingFile:
    """Tests for filename validation."""

    def test_rejects_non_matching_file(self, tmp_path: Path) -> None:
        """Non-RACV_Noosa_Solar files raise exception."""
        filepath = str(tmp_path / "some_other_file.csv")
        _create_noosa_csv(filepath)

        with patch("shared.parsers.racv.noosa_solar.logger"):
            from shared.parsers.racv.noosa_solar import noosa_solar_parser

            with pytest.raises(NotRelevantParser, match="Not a Noosa Solar file"):
                noosa_solar_parser(filepath)


class TestTimestampParsing:
    """Tests for timestamp format handling."""

    def test_timestamp_parsing(self, tmp_path: Path) -> None:
        """'31-Mar-26 8:00 AM AEST' parsed correctly."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        _create_noosa_csv(
            filepath,
            timestamps=["31-Mar-26 8:00 AM AEST"],
            columns={"p:racv:r:s1": ["5.0"]},
        )

        with patch("shared.parsers.racv.noosa_solar.logger"):
            from shared.parsers.racv.noosa_solar import noosa_solar_parser

            result = _processed_dfs(noosa_solar_parser(filepath))

        _, df = result[0]
        ts = df.index[0]
        assert ts == pd.Timestamp("2026-03-31 08:00:00")


class TestAestOnlyContract:
    """Noosa Resort is in QLD (no DST). Only AEST timezone suffix is allowed in source data."""

    def test_aest_only_file_parses_normally(self, tmp_path: Path) -> None:
        """Happy path: all AEST rows parse without raising."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        _create_noosa_csv(
            filepath,
            timestamps=[
                "31-Mar-26 8:00 AM AEST",
                "31-Mar-26 8:30 AM AEST",
                "31-Mar-26 9:00 AM AEST",
            ],
            columns={"p:racv:r:s1": ["5.0", "5.5", "6.0"]},
        )

        with patch("shared.parsers.racv.noosa_solar.logger"):
            from shared.parsers.racv.noosa_solar import noosa_solar_parser

            result = _processed_dfs(noosa_solar_parser(filepath))

        _, df = result[0]
        # Timestamps are persisted as naive AEST on the t_start index.
        assert df.index[0] == pd.Timestamp("2026-03-31 08:00:00")

    def test_aedt_in_source_raises_parser_error(self, tmp_path: Path) -> None:
        """AEDT in Noosa source = contract violation → ParserError, not silent conversion."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        _create_noosa_csv(
            filepath,
            timestamps=[
                "31-Mar-26 8:00 AM AEST",
                "31-Mar-26 8:30 AM AEDT",  # rogue AEDT row
            ],
            columns={"p:racv:r:s1": ["5.0", "5.5"]},
        )

        with patch("shared.parsers.racv.noosa_solar.logger"):
            from shared.parsers.racv.noosa_solar import noosa_solar_parser

            with pytest.raises(ParserError, match=r"non-AEST timezone"):
                noosa_solar_parser(filepath)

    def test_utc_or_other_suffix_raises_parser_error(self, tmp_path: Path) -> None:
        """Any non-AEST suffix raises (defensive against tool changes)."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        _create_noosa_csv(
            filepath,
            timestamps=[
                "31-Mar-26 8:00 AM AEST",
                "31-Mar-26 8:30 AM UTC",
            ],
            columns={"p:racv:r:s1": ["5.0", "5.5"]},
        )

        with patch("shared.parsers.racv.noosa_solar.logger"):
            from shared.parsers.racv.noosa_solar import noosa_solar_parser

            with pytest.raises(ParserError, match=r"non-AEST timezone"):
                noosa_solar_parser(filepath)


class TestNanValuesDropped:
    """Tests for NaN value filtering."""

    def test_nan_values_dropped(self, tmp_path: Path) -> None:
        """NaN and empty values filtered out."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        _create_noosa_csv(
            filepath,
            timestamps=[
                "31-Mar-26 8:00 AM AEST",
                "31-Mar-26 8:30 AM AEST",
                "31-Mar-26 9:00 AM AEST",
                "31-Mar-26 9:30 AM AEST",
            ],
            columns={"p:racv:r:s1": ["1.0", "nan", "3.0", "nan"]},
        )

        with patch("shared.parsers.racv.noosa_solar.logger"):
            from shared.parsers.racv.noosa_solar import noosa_solar_parser

            result = _processed_dfs(noosa_solar_parser(filepath))

        _, df = result[0]
        assert len(df) == 2
        assert list(df["E1_kWh"]) == [1.0, 3.0]


class TestMixedEmptyAndNanValues:
    """Tests for both empty cells and nan strings."""

    def test_mixed_empty_and_nan_values(self, tmp_path: Path) -> None:
        """Both empty cells (,,) and 'nan' strings are handled identically (dropped)."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        # Write manually to include truly empty cells
        content = (
            "timestamp,p:racv:r:s1\n"
            "31-Mar-26 8:00 AM AEST,1.0\n"
            "31-Mar-26 8:30 AM AEST,\n"
            "31-Mar-26 9:00 AM AEST,nan\n"
            "31-Mar-26 9:30 AM AEST,5.0\n"
        )
        Path(filepath).write_text(content, encoding="utf-8-sig")

        with patch("shared.parsers.racv.noosa_solar.logger"):
            from shared.parsers.racv.noosa_solar import noosa_solar_parser

            result = _processed_dfs(noosa_solar_parser(filepath))

        _, df = result[0]
        # Both empty and nan rows should be dropped
        assert len(df) == 2
        assert list(df["E1_kWh"]) == [1.0, 5.0]


class TestUnknownStatusWarning:
    """Tests for unknown Fronius status strings."""

    def test_unknown_status_warning(self, tmp_path: Path) -> None:
        """Unknown status strings logged as warning and dropped."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        _create_noosa_csv(
            filepath,
            columns={
                "p:racv:r:status1": [
                    "Normal Operation",
                    "TotallyUnknownStatus",
                    "Error Exists",
                ],
            },
        )

        mock_log = MagicMock()
        with patch("shared.parsers.racv.noosa_solar.logger", mock_log):
            from shared.parsers.racv.noosa_solar import noosa_solar_parser

            result = _processed_dfs(noosa_solar_parser(filepath))

        # Unknown status should be dropped (mapped to NaN then dropped)
        _, df = result[0]
        assert len(df) == 2
        assert list(df["E1_mode"]) == [4.0, 7.0]

        # Should have logged a warning about unknown values
        mock_log.warning.assert_called_once()
        call_kwargs = mock_log.warning.call_args
        assert "Unknown Fronius mode" in call_kwargs[0][0]


class TestAllColumnsReturnTStart:
    """Tests for DataFrame index format."""

    def test_all_columns_return_t_start(self, tmp_path: Path) -> None:
        """All returned DataFrames have t_start as index."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        _create_noosa_csv(
            filepath,
            columns={
                "p:racv:r:energy1": ["1.0", "2.0", "3.0"],
                "p:racv:r:status1": [
                    "Normal Operation",
                    "Standby",
                    "Error Exists",
                ],
            },
        )

        with patch("shared.parsers.racv.noosa_solar.logger"):
            from shared.parsers.racv.noosa_solar import noosa_solar_parser

            result = _processed_dfs(noosa_solar_parser(filepath))

        assert len(result) == 2
        for _, df in result:
            assert df.index.name == "t_start"


class TestSensorIdFormat:
    """Tests for sensor ID prefix format."""

    def test_sensor_id_format(self, tmp_path: Path) -> None:
        """All returned identifiers start with 'p:racv:r:'."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        _create_noosa_csv(
            filepath,
            columns={
                "p:racv:r:abc-energy": ["1.0", "2.0", "3.0"],
                "p:racv:r:def-status": [
                    "Normal Operation",
                    "Standby",
                    "Error Exists",
                ],
            },
        )

        with patch("shared.parsers.racv.noosa_solar.logger"):
            from shared.parsers.racv.noosa_solar import noosa_solar_parser

            result = _processed_dfs(noosa_solar_parser(filepath))

        for sensor_id, _ in result:
            assert sensor_id.startswith("p:racv:r:")


class TestStripParenthesizedSuffix:
    """Tests for stripping parenthesized suffixes from sensor IDs."""

    def test_strip_kwhr_suffix(self, tmp_path: Path) -> None:
        """Sensor ID with '(kW-hr)' suffix is stripped to plain ID."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        _create_noosa_csv(
            filepath,
            columns={"p:racv:r:abc-123 (kW-hr)": ["10.0", "20.0", "30.0"]},
        )

        with patch("shared.parsers.racv.noosa_solar.logger"):
            from shared.parsers.racv.noosa_solar import noosa_solar_parser

            result = _processed_dfs(noosa_solar_parser(filepath))

        assert len(result) == 1
        sensor_id, _ = result[0]
        assert sensor_id == "p:racv:r:abc-123"
        assert "(kW-hr)" not in sensor_id

    def test_mixed_suffixed_and_plain_columns(self, tmp_path: Path) -> None:
        """File with both suffixed and plain headers produces consistent IDs."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        _create_noosa_csv(
            filepath,
            columns={
                "p:racv:r:sensor-a (kW-hr)": ["1.0", "2.0", "3.0"],
                "p:racv:r:sensor-b": ["4.0", "5.0", "6.0"],
            },
        )

        with patch("shared.parsers.racv.noosa_solar.logger"):
            from shared.parsers.racv.noosa_solar import noosa_solar_parser

            result = _processed_dfs(noosa_solar_parser(filepath))

        ids = [sid for sid, _ in result]
        assert "p:racv:r:sensor-a" in ids
        assert "p:racv:r:sensor-b" in ids
        assert all("(" not in sid for sid in ids)

    def test_strip_arbitrary_parenthesized_suffix(self, tmp_path: Path) -> None:
        """Any parenthesized suffix is stripped, not just (kW-hr)."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        _create_noosa_csv(
            filepath,
            columns={"p:racv:r:xyz (some unit)": ["Normal Operation", "Standby", "Off"]},
        )

        with patch("shared.parsers.racv.noosa_solar.logger"):
            from shared.parsers.racv.noosa_solar import noosa_solar_parser

            result = _processed_dfs(noosa_solar_parser(filepath))

        sensor_id, _ = result[0]
        assert sensor_id == "p:racv:r:xyz"


class TestEmptyFile:
    """Tests for empty file handling."""

    def test_empty_file(self, tmp_path: Path) -> None:
        """File with only headers returns processed_empty."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        content = "timestamp,p:racv:r:s1\n"
        Path(filepath).write_text(content, encoding="utf-8-sig")

        with patch("shared.parsers.racv.noosa_solar.logger"):
            from shared.parsers.racv.noosa_solar import noosa_solar_parser

            result = noosa_solar_parser(filepath)

        assert result.status == "processed_empty"
        assert result.reason == "all_blank"
        assert result.dataframes == []


class TestMissingTimestampColumn:
    """Tests for missing timestamp column validation."""

    def test_missing_timestamp_column(self, tmp_path: Path) -> None:
        """File with wrong first column name raises exception."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        content = "wrong_header,p:racv:r:s1\n31-Mar-26 8:00 AM AEST,1.0\n"
        Path(filepath).write_text(content, encoding="utf-8-sig")

        with patch("shared.parsers.racv.noosa_solar.logger"):
            from shared.parsers.racv.noosa_solar import noosa_solar_parser

            with pytest.raises(ParserError, match="Missing timestamp column"):
                noosa_solar_parser(filepath)


class TestAllNanColumnSkipped:
    """Tests for columns with all NaN values."""

    def test_all_nan_column_skipped(self, tmp_path: Path) -> None:
        """Column where every value is NaN is excluded from results."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        _create_noosa_csv(
            filepath,
            columns={
                "p:racv:r:good": ["1.0", "2.0", "3.0"],
                "p:racv:r:bad": ["nan", "nan", "nan"],
            },
        )

        with patch("shared.parsers.racv.noosa_solar.logger"):
            from shared.parsers.racv.noosa_solar import noosa_solar_parser

            result = _processed_dfs(noosa_solar_parser(filepath))

        # Only the good column should be in results
        sensor_ids = [sid for sid, _ in result]
        assert "p:racv:r:good" in sensor_ids
        assert "p:racv:r:bad" not in sensor_ids


class TestAllZeroColumnPreserved:
    """Tests for columns with all zero values."""

    def test_all_zero_column_preserved(self, tmp_path: Path) -> None:
        """Column with all 0 values is correctly classified as numeric and preserved."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        _create_noosa_csv(
            filepath,
            columns={"p:racv:r:zeros": ["0", "0", "0"]},
        )

        with patch("shared.parsers.racv.noosa_solar.logger"):
            from shared.parsers.racv.noosa_solar import noosa_solar_parser

            result = _processed_dfs(noosa_solar_parser(filepath))

        assert len(result) == 1
        sensor_id, df = result[0]
        assert sensor_id == "p:racv:r:zeros"
        assert "E1_kWh" in df.columns
        assert list(df["E1_kWh"]) == [0.0, 0.0, 0.0]


class TestMultipleStatusValues:
    """Tests for multiple different status strings in one column."""

    def test_multiple_status_values(self, tmp_path: Path) -> None:
        """Column with mixed status strings all mapped correctly."""
        filepath = str(tmp_path / "RACV_Noosa_Solar.csv")
        statuses = [
            "Off",
            "In Operation, No Feed In",
            "Run Up Phase",
            "Normal Operation",
            "Power Reduction",
            "Switch Off Phase",
            "Error Exists",
            "Standby",
            "No Fronius Solar Net Comm",
            "No Comm with Inverter",
            "Overcurrent detected in Fronius Solar Net",
            "Inverter Update being Performed",
            "AFCI Event",
        ]
        # Generate 13 valid 12-hour timestamps (1:00 AM through 1:00 PM)
        timestamps = []
        for i in range(13):
            hour = i + 1  # 1..13
            if hour <= 11:
                timestamps.append(f"31-Mar-26 {hour}:00 AM AEST")
            elif hour == 12:
                timestamps.append("31-Mar-26 12:00 PM AEST")
            else:
                timestamps.append(f"31-Mar-26 {hour - 12}:00 PM AEST")

        _create_noosa_csv(
            filepath,
            timestamps=timestamps,
            columns={"p:racv:r:all_statuses": statuses},
        )

        with patch("shared.parsers.racv.noosa_solar.logger"):
            from shared.parsers.racv.noosa_solar import noosa_solar_parser

            result = _processed_dfs(noosa_solar_parser(filepath))

        assert len(result) == 1
        _, df = result[0]
        assert "E1_mode" in df.columns
        expected_codes = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0]
        assert list(df["E1_mode"]) == expected_codes


class TestDispatcherIntegration:
    """Tests for get_non_nem_df routing to the Noosa Solar parser."""

    def test_dispatcher_integration(self, tmp_path: Path) -> None:
        """get_non_nem_df() correctly routes RACV_Noosa_Solar files to the Noosa parser."""
        filepath = str(tmp_path / "RACV_Noosa_Solar_20260331.csv")
        _create_noosa_csv(
            filepath,
            columns={"p:racv:r:s1": ["10.0", "20.0", "30.0"]},
        )

        with patch("shared.parsers.racv.noosa_solar.logger"), patch("shared.parsers.dispatcher.logger"):
            from shared.parsers.dispatcher import get_non_nem_df

            result = get_non_nem_df(filepath)

        assert len(result) == 1
        sensor_id, df = result[0]
        assert sensor_id == "p:racv:r:s1"
        assert "E1_kWh" in df.columns
