"""Tests for shared.parsers.optima.interval.interval_parser."""

from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from conftest import create_optima_csv


class TestIntervalParser:
    """Tests for interval_parser function."""

    def test_returns_empty_list_for_no_data_sentinel(self, temp_directory: str) -> None:
        """BidEnergy returns 148-byte 'No data is available' CSV when site has no data
        for the requested range. Parser must return [] (not raise UFuncTypeError)."""
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

        assert result == []

    def test_parses_generation_data_correctly(self, temp_directory: str) -> None:
        """Test that generation data is parsed correctly."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.parsers.optima.interval import interval_parser

            filepath = str(Path(temp_directory) / "OptimaGenerationData.csv")
            create_optima_csv(filepath, identifiers=["SOLAR001"], rows_per_id=5)

            result = interval_parser(filepath, "error_log")

            assert isinstance(result, list)
            assert len(result) == 1

            nmi, df = result[0]
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

            assert len(result) == 2
            nmis = [nmi for nmi, df in result]
            assert "Optima_SOLAR001" in nmis
            assert "Optima_SOLAR002" in nmis

    def test_parses_usage_column_as_e1_kwh(self, temp_directory: str) -> None:
        """Test that Usage column is correctly extracted as E1_kWh."""
        with patch("shared.non_nem_parsers.logger"):
            from shared.parsers.optima.interval import interval_parser

            filepath = str(Path(temp_directory) / "OptimaGenerationData.csv")
            create_optima_csv(filepath, identifiers=["METER001"], rows_per_id=5)

            result = interval_parser(filepath, "error_log")

            _nmi, df = result[0]
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

            nmi, df = result[0]
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

            nmi, df = result[0]
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
            assert len(result) == 1
            nmi_key, df = result[0]
            assert nmi_key == "Optima_TEST"
            assert "E1_kWh" in df.columns
            assert "B1_kWh" in df.columns
            assert df["E1_kWh"].sum() == pytest.approx(3.2)  # 1.5 + 1.7
            assert df["B1_kWh"].sum() == pytest.approx(1.7)  # 0.8 + 0.9 — Generation persists


class TestIntervalParserOnRealFixtures:
    """Regression tests using verbatim BidEnergy responses (committed at 86ab1bf).

    These fixtures lock in real-world quirks that synthetic data would miss:
    CRLF line endings, double-quoted columns, NZ alphanumeric ICP identifiers,
    and the empty-data sentinel CSV.
    """

    FIXTURE_DIR = Path(__file__).parent.parent.parent / "fixtures" / "optima_interval"

    def test_au_single_day_parses_to_48_intervals(self) -> None:
        from shared.parsers.optima.interval import interval_parser

        path = str(self.FIXTURE_DIR / "interval_au_single_day.csv")
        result = interval_parser(path, "error_log")

        assert len(result) == 1
        sensor_id, df = result[0]
        assert sensor_id == "Optima_2002105104"
        assert list(df.columns) == ["E1_kWh", "B1_kWh"]
        assert len(df) == 48  # 30-min intervals x 24 h
        assert df.index.min() == pd.Timestamp("2025-05-01 00:00:00")

    def test_nz_icp_alphanumeric_identifier(self) -> None:
        """NZ uses alphanumeric ICP; parser must not assume numeric NMI."""
        from shared.parsers.optima.interval import interval_parser

        path = str(self.FIXTURE_DIR / "interval_nz_single_day.csv")
        result = interval_parser(path, "error_log")

        assert len(result) == 1
        sensor_id, df = result[0]
        assert sensor_id == "Optima_0000010008MQCB6"
        assert len(df) == 48

    def test_au_four_months_spans_distinct_months(self) -> None:
        """5856 rows spanning Apr to Jul. Catches any future date-format regression."""
        from shared.parsers.optima.interval import interval_parser

        path = str(self.FIXTURE_DIR / "interval_au_4month.csv")
        result = interval_parser(path, "error_log")

        sensor_id, df = result[0]
        assert sensor_id == "Optima_2002105104"
        assert len(df) > 5000
        assert sorted(df.index.month.unique().tolist()) == [4, 5, 6, 7]

    def test_empty_data_fixture_returns_empty_list(self) -> None:
        from shared.parsers.optima.interval import interval_parser

        path = str(self.FIXTURE_DIR / "interval_empty.csv")
        result = interval_parser(path, "error_log")

        assert result == []
