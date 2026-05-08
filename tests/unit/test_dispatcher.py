"""Tests for the get_non_nem_df dispatcher in shared.non_nem_parsers."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from conftest import (
    create_envizi_electricity_csv,
    create_envizi_water_csv,
    create_optima_csv,
)


class TestGetNonNemDf:
    """Tests for get_non_nem_df dispatcher function."""

    def test_tries_parsers_in_order(self, temp_directory: str) -> None:
        """Test that parsers are tried in order until one succeeds."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.non_nem_parsers import get_non_nem_df

            # Create valid Envizi water file
            filepath = str(Path(temp_directory) / "water_data.csv")
            create_envizi_water_csv(filepath, serial_numbers=["12345"])

            result = get_non_nem_df(filepath, "error_log")

            assert isinstance(result, list)
            assert len(result) > 0

    def test_raises_exception_when_all_parsers_fail(self, temp_directory: str) -> None:
        """Test that exception is raised when no parser succeeds."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.non_nem_parsers import get_non_nem_df

            # Create invalid file that no parser can handle
            filepath = str(Path(temp_directory) / "invalid.csv")
            with Path(filepath).open("w") as f:
                f.write("completely,invalid,format\n1,2,3\n")

            with pytest.raises(Exception, match="No Valid Parser Found"):
                get_non_nem_df(filepath, "error_log")

    def test_logs_errors_for_failed_parsers(self, temp_directory: str) -> None:
        """Test that errors are logged for each failed parser."""
        mock_log = MagicMock()
        with patch("shared.non_nem_parsers.logger", mock_log):
            from shared.non_nem_parsers import get_non_nem_df

            # Create valid file for later parser
            filepath = str(Path(temp_directory) / "OptimaGenerationData.csv")
            create_optima_csv(filepath, identifiers=["ID1"])

            get_non_nem_df(filepath, "error_log")

            # Earlier parsers should have logged failures
            assert mock_log.debug.called


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


class TestDataFrameOutputFormat:
    """Tests for DataFrame output format consistency across parsers."""

    def test_all_parsers_return_t_start_column(self, temp_directory: str) -> None:
        """Test that all parsers return DataFrames with t_start."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.parsers.envizi.vertical_electricity import envizi_vertical_parser_electricity
            from shared.parsers.envizi.vertical_water import envizi_vertical_parser_water
            from shared.parsers.optima.interval import interval_parser

            # Test Envizi water
            water_file = str(Path(temp_directory) / "water.csv")
            create_envizi_water_csv(water_file, serial_numbers=["1"])
            result = envizi_vertical_parser_water(water_file, "error")
            assert result.status == "processed"
            _, df = result.dataframes[0]
            assert df.index.name == "t_start" or "t_start" in df.columns

            # Test Envizi electricity
            elec_file = str(Path(temp_directory) / "elec.csv")
            create_envizi_electricity_csv(elec_file, serial_numbers=["1"])
            result = envizi_vertical_parser_electricity(elec_file, "error")
            assert result.status == "processed"
            _, df = result.dataframes[0]
            assert df.index.name == "t_start" or "t_start" in df.columns

            # Test Optima generation
            gen_file = str(Path(temp_directory) / "OptimaGenerationData_test.csv")
            create_optima_csv(gen_file, identifiers=["1"])
            result = interval_parser(gen_file, "error")
            assert result.status == "processed"
            _, df = result.dataframes[0]
            assert df.index.name == "t_start" or "t_start" in df.columns


class TestOutcomeDispatcher:
    def test_wraps_legacy_parser_result_as_processed_outcome(self, tmp_path, monkeypatch) -> None:
        from shared.non_nem_parsers import get_non_nem_outcome

        def parser(file_name: str, error_file_path: str):
            df = pd.DataFrame({"t_start": ["2026-01-01 00:00:00"], "E1_kWh": [1.0]})
            return [("NMI1", df)]

        monkeypatch.setattr("shared.non_nem_parsers.PARSERS", [parser])

        result = get_non_nem_outcome(str(tmp_path / "file.csv"), "error_log")

        assert result.status == "processed"
        assert len(result.dataframes) == 1

    def test_legacy_get_non_nem_df_still_returns_raw_dfs(self, tmp_path, monkeypatch) -> None:
        from shared.non_nem_parsers import get_non_nem_df

        def parser(file_name: str, error_file_path: str):
            df = pd.DataFrame({"t_start": ["2026-01-01 00:00:00"], "E1_kWh": [1.0]})
            return [("NMI1", df)]

        monkeypatch.setattr("shared.non_nem_parsers.PARSERS", [parser])

        result = get_non_nem_df(str(tmp_path / "file.csv"), "error_log")

        assert isinstance(result, list)
        assert result[0][0] == "NMI1"

    def test_not_relevant_parser_continues_to_next_parser(self, tmp_path, monkeypatch) -> None:
        from shared.non_nem_parsers import get_non_nem_outcome
        from shared.parsers import NotRelevantParser, ParserOutcome

        def first_parser(file_name: str, error_file_path: str):
            raise NotRelevantParser("not mine")

        def second_parser(file_name: str, error_file_path: str):
            return ParserOutcome(status="processed_empty", reason="matched")

        monkeypatch.setattr("shared.non_nem_parsers.PARSERS", [first_parser, second_parser])

        result = get_non_nem_outcome(str(tmp_path / "file.csv"), "error_log")

        assert result.status == "processed_empty"
        assert result.reason == "matched"

    def test_parser_error_stops_dispatch(self, tmp_path, monkeypatch) -> None:
        from shared.non_nem_parsers import get_non_nem_outcome
        from shared.parsers import NotRelevantParser, ParserError

        def first_parser(file_name: str, error_file_path: str):
            raise ParserError("matched but malformed")

        def second_parser(file_name: str, error_file_path: str):
            raise NotRelevantParser("should not run")

        monkeypatch.setattr("shared.non_nem_parsers.PARSERS", [first_parser, second_parser])

        with pytest.raises(ParserError, match="matched but malformed"):
            get_non_nem_outcome(str(tmp_path / "file.csv"), "error_log")

    def test_processing_error_stops_dispatch(self, tmp_path, monkeypatch) -> None:
        from shared.non_nem_parsers import get_non_nem_outcome
        from shared.parsers import NotRelevantParser, ProcessingError

        def first_parser(file_name: str, error_file_path: str):
            raise ProcessingError("s3 write failed")

        def second_parser(file_name: str, error_file_path: str):
            raise NotRelevantParser("should not run")

        monkeypatch.setattr("shared.non_nem_parsers.PARSERS", [first_parser, second_parser])

        with pytest.raises(ProcessingError, match="s3 write failed"):
            get_non_nem_outcome(str(tmp_path / "file.csv"), "error_log")

    def test_unexpected_parser_exception_becomes_parser_error(self, tmp_path, monkeypatch) -> None:
        from shared.non_nem_parsers import get_non_nem_outcome
        from shared.parsers import ParserError

        def parser(file_name: str, error_file_path: str):
            raise RuntimeError("unexpected")

        monkeypatch.setattr("shared.non_nem_parsers.PARSERS", [parser])

        with pytest.raises(ParserError, match="Unexpected parser failure"):
            get_non_nem_outcome(str(tmp_path / "file.csv"), "error_log")

    def test_envizi_schema_miss_does_not_block_later_parser(self, tmp_path, monkeypatch) -> None:
        from shared.non_nem_parsers import get_non_nem_outcome
        from shared.parsers import NotRelevantParser, ParserOutcome

        def first_parser(file_name: str, error_file_path: str) -> ParserOutcome:
            raise NotRelevantParser("schema miss")

        def second_parser(file_name: str, error_file_path: str) -> ParserOutcome:
            return ParserOutcome(status="processed_empty", reason="later_parser_matched")

        monkeypatch.setattr("shared.non_nem_parsers.PARSERS", [first_parser, second_parser])

        result = get_non_nem_outcome(str(tmp_path / "file.csv"), "error_log")

        assert result.status == "processed_empty"
        assert result.reason == "later_parser_matched"

    def test_real_dispatcher_routes_optima_interval_after_early_schema_misses(self, tmp_path) -> None:
        from shared.non_nem_parsers import get_non_nem_outcome

        filepath = tmp_path / "OptimaIntervalData.csv"
        create_optima_csv(str(filepath), identifiers=["4001260599"], rows_per_id=1)

        result = get_non_nem_outcome(str(filepath), "error_log")

        assert result.status == "processed"
        assert len(result.dataframes) == 1
        assert result.dataframes[0][0] == "Optima_4001260599"
