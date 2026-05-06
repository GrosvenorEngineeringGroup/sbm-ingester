"""Tests for shared.parsers.optima.interval.interval_parser."""

from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from conftest import create_optima_csv

from shared.parsers import NotRelevantParser, ParserError, ParserOutcome


def _processed_dfs(result: ParserOutcome) -> list[tuple[str, pd.DataFrame]]:
    assert result.status == "processed"
    assert result.source_row_count > 0
    return result.dfs


class TestIntervalParser:
    """Tests for interval_parser function."""

    def test_returns_processed_empty_for_no_data_sentinel(self, temp_directory: str) -> None:
        """BidEnergy returns 148-byte 'No data is available' CSV when site has no data
        for the requested range. Parser must return processed_empty."""
        from pathlib import Path

        from shared.parsers.optima.interval import interval_parser

        sentinel_csv = (
            b"BuyerShortName,Country,Commodity,Identifier,IdentifierType,DistributorId,"
            b"Date,Start Time,Usage,Generation,DemandKva,Reactive\r\n"
            b"No data is available\r\n"
        )
        filepath = Path(temp_directory) / "empty.csv"
        filepath.write_bytes(sentinel_csv)

        result = interval_parser(str(filepath), "error_log")

        assert result.status == "processed_empty"
        assert result.reason == "no_data_sentinel"
        assert result.dfs == []

    def test_pseudo_no_data_sentinel_with_malformed_usage_skip_counts(self, tmp_path) -> None:
        """Pseudo-sentinel row with malformed Usage and blank date is skipped.

        Both the bad timestamp (blank Date) and unparseable Usage value get
        counted in skip_reasons. File becomes processed_empty, not parse-err.
        """
        from shared.parsers.optima.interval import interval_parser

        filepath = tmp_path / "interval.csv"
        filepath.write_text(
            "BuyerShortName,Country,Commodity,Identifier,IdentifierType,DistributorId,"
            "Date,Start Time,Usage,Generation,DemandKva,Reactive\n"
            "No data is available,,,,,,,,not-a-number,,,\n"
        )

        result = interval_parser(str(filepath), "error_log")
        assert result.status == "processed_empty"
        assert result.dfs == []
        assert result.rows_skipped == 1
        assert result.skip_reasons["unparseable_value"] == 1
        assert result.skip_reasons["unparseable_timestamp"] == 1

    def test_missing_required_columns_is_not_relevant(self, tmp_path) -> None:
        from shared.parsers.optima.interval import interval_parser

        filepath = tmp_path / "not_interval.csv"
        filepath.write_text("Identifier,Usage\nMETER001,1.0\n")

        with pytest.raises(NotRelevantParser, match="Not an Optima interval CSV"):
            interval_parser(str(filepath), "error_log")

    def test_invalid_timestamp_after_schema_match_skip_counts(self, tmp_path) -> None:
        """Single bad-timestamp row is skipped, not raised. File stays
        processed_empty with rows_skipped recorded."""
        from shared.parsers.optima.interval import interval_parser

        filepath = tmp_path / "interval.csv"
        filepath.write_text("Date,Start Time,Identifier,Usage\nnot-a-date,00:00,METER001,1.0\n")

        result = interval_parser(str(filepath), "error_log")
        assert result.status == "processed_empty"
        assert result.dfs == []
        assert result.rows_skipped == 1
        assert result.skip_reasons["unparseable_timestamp"] == 1
        assert result.reason == "all_skipped"

    def test_missing_value_channel_after_schema_match_raises_parser_error(self, tmp_path) -> None:
        from shared.parsers.optima.interval import interval_parser

        filepath = tmp_path / "interval.csv"
        filepath.write_text("Date,Start Time,Identifier,DemandKva\n2026-05-01,00:00,METER001,3.0\n")

        with pytest.raises(ParserError, match="Missing interval value column"):
            interval_parser(str(filepath), "error_log")

    def test_header_only_value_file_returns_processed_empty(self, tmp_path) -> None:
        from shared.parsers.optima.interval import interval_parser

        filepath = tmp_path / "interval.csv"
        filepath.write_text("Date,Start Time,Identifier,Usage\n")

        result = interval_parser(str(filepath), "error_log")

        assert result.status == "processed_empty"
        assert result.source_row_count == 0
        assert result.reason == "all_blank"
        assert result.dfs == []

    def test_header_only_generation_file_returns_processed_empty(self, tmp_path) -> None:
        from shared.parsers.optima.interval import interval_parser

        filepath = tmp_path / "interval.csv"
        filepath.write_text("Date,Start Time,Identifier,Generation\n")

        result = interval_parser(str(filepath), "error_log")

        assert result.status == "processed_empty"
        assert result.source_row_count == 0
        assert result.reason == "all_blank"
        assert result.dfs == []

    def test_blank_date_with_malformed_usage_skip_counts(self, tmp_path) -> None:
        """Row with blank Date AND malformed Usage is double-skipped."""
        from shared.parsers.optima.interval import interval_parser

        filepath = tmp_path / "interval.csv"
        filepath.write_text("Date,Start Time,Identifier,Usage\n,00:00,METER001,not-a-number\n")

        result = interval_parser(str(filepath), "error_log")
        assert result.status == "processed_empty"
        assert result.rows_skipped == 1
        assert result.skip_reasons["unparseable_value"] == 1
        assert result.skip_reasons["unparseable_timestamp"] == 1

    def test_blank_date_with_blank_usage_skip_counts(self, tmp_path) -> None:
        """Row with blank Date and blank Usage is skipped on timestamp."""
        from shared.parsers.optima.interval import interval_parser

        filepath = tmp_path / "interval.csv"
        filepath.write_text("Date,Start Time,Identifier,Usage\n,00:00,METER001,\n")

        result = interval_parser(str(filepath), "error_log")
        assert result.status == "processed_empty"
        assert result.rows_skipped == 1
        assert result.skip_reasons["unparseable_timestamp"] == 1

    def test_blank_only_value_rows_return_processed_empty(self, tmp_path) -> None:
        from shared.parsers.optima.interval import interval_parser

        filepath = tmp_path / "interval.csv"
        filepath.write_text(
            "Date,Start Time,Identifier,Usage,Generation\n"
            "2026-05-01,00:00,METER001,,   \n"
            "2026-05-01,00:30,METER001,   ,\n"
        )

        result = interval_parser(str(filepath), "error_log")

        assert result.status == "processed_empty"
        assert result.source_row_count == 2
        assert result.reason == "all_blank"
        assert result.dfs == []

    def test_malformed_usage_after_schema_match_skip_counts(self, tmp_path) -> None:
        """Single malformed Usage value is skipped (counted), not raised."""
        from shared.parsers.optima.interval import interval_parser

        filepath = tmp_path / "interval.csv"
        filepath.write_text("Date,Start Time,Identifier,Usage\n2026-05-01,00:00,METER001,not-a-number\n")

        result = interval_parser(str(filepath), "error_log")
        assert result.status == "processed_empty"
        assert result.dfs == []
        assert result.rows_skipped == 1
        assert result.skip_reasons["unparseable_value"] == 1

    def test_malformed_generation_after_schema_match_skip_counts(self, tmp_path) -> None:
        """Single malformed Generation value is skipped (counted), not raised."""
        from shared.parsers.optima.interval import interval_parser

        filepath = tmp_path / "interval.csv"
        filepath.write_text("Date,Start Time,Identifier,Generation\n2026-05-01,00:00,METER001,not-a-number\n")

        result = interval_parser(str(filepath), "error_log")
        assert result.status == "processed_empty"
        assert result.dfs == []
        assert result.rows_skipped == 1
        assert result.skip_reasons["unparseable_value"] == 1

    def test_partial_bad_rows_in_otherwise_valid_file_skip_counts(self, tmp_path) -> None:
        """N valid rows + 1 malformed numeric → N rows in output, rows_skipped=1."""
        from shared.parsers.optima.interval import interval_parser

        filepath = tmp_path / "interval.csv"
        # Use 30-min intervals across two days so all timestamps are distinct & valid.
        good_rows = []
        for day in (1, 2, 3, 4, 5):
            for h in range(24):
                for m in (0, 30):
                    good_rows.append(f"2026-05-0{day},{h:02d}:{m:02d},METER001,{(h + m) * 0.5}")
        good_rows = good_rows[:99]
        filepath.write_text(
            "Date,Start Time,Identifier,Usage\n" + "\n".join(good_rows) + "\n2026-05-06,00:00,METER001,not-a-number\n"
        )

        result = interval_parser(str(filepath), "error_log")
        assert result.status == "processed"
        assert result.source_row_count == 100
        assert result.candidate_row_count == 99
        assert result.rows_skipped == 1
        assert result.skip_reasons["unparseable_value"] == 1
        assert len(result.dfs) == 1
        _nmi, df = result.dfs[0]
        assert len(df) == 99

    def test_partial_bad_timestamp_in_otherwise_valid_file_skip_counts(self, tmp_path) -> None:
        """N valid rows + 1 malformed timestamp → N rows, rows_skipped=1."""
        from shared.parsers.optima.interval import interval_parser

        filepath = tmp_path / "interval.csv"
        good_rows = []
        for day in (1, 2, 3, 4, 5):
            for h in range(24):
                for m in (0, 30):
                    good_rows.append(f"2026-05-0{day},{h:02d}:{m:02d},METER001,{(h + m) * 0.5}")
        good_rows = good_rows[:99]
        filepath.write_text(
            "Date,Start Time,Identifier,Usage\n" + "\n".join(good_rows) + "\nnot-a-date,00:00,METER001,1.0\n"
        )

        result = interval_parser(str(filepath), "error_log")
        assert result.status == "processed"
        assert result.source_row_count == 100
        assert result.candidate_row_count == 99
        assert result.rows_skipped == 1
        assert result.skip_reasons["unparseable_timestamp"] == 1
        assert len(result.dfs) == 1
        _nmi, df = result.dfs[0]
        assert len(df) == 99

    def test_parses_generation_data_correctly(self, temp_directory: str) -> None:
        """Test that generation data is parsed correctly."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.parsers.optima.interval import interval_parser

            filepath = str(Path(temp_directory) / "OptimaGenerationData.csv")
            create_optima_csv(filepath, identifiers=["SOLAR001"], rows_per_id=5)

            result = interval_parser(filepath, "error_log")
            result_dfs = _processed_dfs(result)

            assert len(result_dfs) == 1

            nmi, df = result_dfs[0]
            assert nmi == "Optima_SOLAR001"
            assert "B1_kWh" in df.columns  # Generation uses B1
            assert "E1_kWh" in df.columns  # Usage uses E1

    def test_handles_multiple_identifiers(self, temp_directory: str) -> None:
        """Test that multiple identifiers are handled correctly."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.parsers.optima.interval import interval_parser

            filepath = str(Path(temp_directory) / "OptimaGenerationData.csv")
            create_optima_csv(filepath, identifiers=["SOLAR001", "SOLAR002"], rows_per_id=3)

            result = interval_parser(filepath, "error_log")
            result_dfs = _processed_dfs(result)

            assert len(result_dfs) == 2
            nmis = [nmi for nmi, df in result_dfs]
            assert "Optima_SOLAR001" in nmis
            assert "Optima_SOLAR002" in nmis

    def test_parses_usage_column_as_e1_kwh(self, temp_directory: str) -> None:
        """Test that Usage column is correctly extracted as E1_kWh."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.parsers.optima.interval import interval_parser

            filepath = str(Path(temp_directory) / "OptimaGenerationData.csv")
            create_optima_csv(filepath, identifiers=["METER001"], rows_per_id=5)

            result = interval_parser(filepath, "error_log")
            result_dfs = _processed_dfs(result)

            _nmi, df = result_dfs[0]
            assert "E1_kWh" in df.columns
            # Verify values are correctly extracted (Usage = i * 0.5)
            assert df["E1_kWh"].iloc[0] == 0.0  # i=0 -> 0 * 0.5 = 0
            assert df["E1_kWh"].iloc[1] == 0.5  # i=1 -> 1 * 0.5 = 0.5
            assert df["E1_kWh"].iloc[2] == 1.0  # i=2 -> 2 * 0.5 = 1.0

    def test_parses_generation_only_file(self, temp_directory: str) -> None:
        """Test that files with only Generation column work correctly."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.parsers.optima.interval import interval_parser

            filepath = str(Path(temp_directory) / "OptimaGenerationData.csv")
            create_optima_csv(
                filepath, identifiers=["SOLAR001"], rows_per_id=3, include_usage=False, include_generation=True
            )

            result = interval_parser(filepath, "error_log")
            result_dfs = _processed_dfs(result)

            nmi, df = result_dfs[0]
            assert nmi == "Optima_SOLAR001"
            assert "B1_kWh" in df.columns
            assert "E1_kWh" not in df.columns  # No Usage column in source

    def test_parses_usage_only_file(self, temp_directory: str) -> None:
        """Test that files with only Usage column work correctly."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.parsers.optima.interval import interval_parser

            filepath = str(Path(temp_directory) / "OptimaGenerationData.csv")
            create_optima_csv(
                filepath, identifiers=["METER001"], rows_per_id=3, include_usage=True, include_generation=False
            )

            result = interval_parser(filepath, "error_log")
            result_dfs = _processed_dfs(result)

            nmi, df = result_dfs[0]
            assert nmi == "Optima_METER001"
            assert "E1_kWh" in df.columns
            assert "B1_kWh" not in df.columns  # No Generation column in source

    def test_interval_parser_persists_both_usage_and_generation(self, tmp_path) -> None:
        """Both Usage→E1_kWh and Generation→B1_kWh must be produced when present.

        Regression guard: if a future change to interval_parser drops one of the
        Usage or Generation channels, this test breaks.
        """
        with patch("shared.non_nem_parsers.logger"):
            from shared.parsers.optima.interval import interval_parser

            csv_path = tmp_path / "Bunnings-AU-Electricity-TEST-NMI-ENERGYAP.csv"
            csv_path.write_text(
                "BuyerShortName,Country,Commodity,Identifier,IdentifierType,DistributorId,"
                "Date,Start Time,Usage,Generation,DemandKva,Reactive\n"
                '"Bunnings","AU","Electricity","TEST","NMI","ENERGYAP",01 May 2026,00:00,1.5,0.8,3.0,0.0\n'
                '"Bunnings","AU","Electricity","TEST","NMI","ENERGYAP",01 May 2026,00:30,1.7,0.9,3.4,0.0\n'
            )
            result = interval_parser(str(csv_path), str(tmp_path / "err.log"))
            result_dfs = _processed_dfs(result)
            assert len(result_dfs) == 1
            nmi_key, df = result_dfs[0]
            assert nmi_key == "Optima_TEST"
            assert "E1_kWh" in df.columns
            assert "B1_kWh" in df.columns
            assert df["E1_kWh"].sum() == pytest.approx(3.2)  # 1.5 + 1.7
            assert df["B1_kWh"].sum() == pytest.approx(1.7)  # 0.8 + 0.9 — Generation persists


class TestIntervalParserOnRealFixtures:
    """Regression tests using verbatim BidEnergy responses from committed fixtures.

    These fixtures lock in real-world quirks that synthetic data would miss:
    CRLF line endings, double-quoted columns, NZ alphanumeric ICP identifiers,
    and the empty-data sentinel CSV.
    """

    FIXTURE_DIR = Path(__file__).parent.parent.parent / "fixtures" / "optima_interval"

    def test_au_single_day_parses_to_48_intervals(self) -> None:
        from shared.parsers.optima.interval import interval_parser

        path = str(self.FIXTURE_DIR / "interval_au_single_day.csv")
        result = interval_parser(path, "error_log")
        result_dfs = _processed_dfs(result)

        assert len(result_dfs) == 1
        sensor_id, df = result_dfs[0]
        assert sensor_id == "Optima_2002105104"
        assert list(df.columns) == ["E1_kWh", "B1_kWh"]
        assert len(df) == 48  # 30-min intervals x 24 h
        assert df.index.min() == pd.Timestamp("2025-05-01 00:00:00")

    def test_nz_icp_alphanumeric_identifier(self) -> None:
        """NZ uses alphanumeric ICP; parser must not assume numeric NMI."""
        from shared.parsers.optima.interval import interval_parser

        path = str(self.FIXTURE_DIR / "interval_nz_single_day.csv")
        result = interval_parser(path, "error_log")
        result_dfs = _processed_dfs(result)

        assert len(result_dfs) == 1
        sensor_id, df = result_dfs[0]
        assert sensor_id == "Optima_0000010008MQCB6"
        assert len(df) == 48

    def test_au_four_months_spans_distinct_months(self) -> None:
        """5856 rows spanning Apr to Jul. Catches any future date-format regression."""
        from shared.parsers.optima.interval import interval_parser

        path = str(self.FIXTURE_DIR / "interval_au_4month.csv")
        result = interval_parser(path, "error_log")
        result_dfs = _processed_dfs(result)

        assert len(result_dfs) == 1
        sensor_id, df = result_dfs[0]
        assert sensor_id == "Optima_2002105104"
        assert len(df) > 5000
        assert sorted(df.index.month.unique().tolist()) == [4, 5, 6, 7]

    def test_empty_data_fixture_returns_processed_empty(self) -> None:
        from shared.parsers.optima.interval import interval_parser

        path = str(self.FIXTURE_DIR / "interval_empty.csv")
        result = interval_parser(path, "error_log")

        assert result.status == "processed_empty"
        assert result.reason == "no_data_sentinel"
        assert result.dfs == []


class TestIntervalParserCheapGate:
    """Cheap relevance gate must run before any pd.read_csv full parse."""

    def test_bom_prefixed_header_passes_gate(self, tmp_path) -> None:
        from shared.parsers.optima.interval import interval_parser

        # UTF-8 BOM (\xef\xbb\xbf) prefixed file with a single valid row.
        filepath = tmp_path / "bom_interval.csv"
        filepath.write_bytes(b"\xef\xbb\xbfDate,Start Time,Identifier,Usage\n2026-05-01,00:00,METER001,1.0\n")

        result = interval_parser(str(filepath), "error_log")
        assert result.status == "processed"
        assert result.candidate_row_count == 1

    def test_cheap_gate_does_not_invoke_pd_read_csv(self, tmp_path) -> None:
        from shared.parsers import NotRelevantParser
        from shared.parsers.optima import interval as interval_mod

        # First-line content that does NOT look like an interval CSV.
        filepath = tmp_path / "wrong.csv"
        filepath.write_text("foo,bar,baz\n1,2,3\n")

        with patch.object(interval_mod.pd, "read_csv", side_effect=RuntimeError("must not be called")):
            with pytest.raises(NotRelevantParser):
                interval_mod.interval_parser(str(filepath), "error_log")

    def test_full_parse_failure_after_gate_raises_parser_error(self, tmp_path) -> None:
        from shared.parsers.optima.interval import interval_parser

        # First line matches the gate, but body is corrupt: mismatched
        # column counts will raise pandas.errors.ParserError on full parse.
        filepath = tmp_path / "corrupt.csv"
        filepath.write_bytes(b'Date,Start Time,Identifier,Usage\n2026-05-01,00:00,"unterminated,1.0\n')

        with pytest.raises(ParserError, match="Failed to read Optima interval CSV"):
            interval_parser(str(filepath), "error_log")
