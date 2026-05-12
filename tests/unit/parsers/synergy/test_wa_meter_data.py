"""Tests for the Synergy WA meter data parser."""

from __future__ import annotations

from pathlib import Path

import pytest

from shared.parsers import NotRelevantParser, ParserOutcome
from shared.parsers.synergy.wa_meter_data import synergy_wa_meter_data_parser

FIXTURE_DIR = Path(__file__).parent.parent.parent / "fixtures" / "synergy"


class TestSynergyWaMeterDataParser:
    def test_sentinel_fixture_returns_processed_empty(self, tmp_path: Path) -> None:
        """The committed fixture (56-byte 'No data found' sentinel), when delivered
        with a real Synergy WA filename, returns processed_empty."""
        prod_path = tmp_path / "Meter_Data_WA (AU)_Electricity_1778517074_2026051202315309.csv"
        prod_path.write_bytes((FIXTURE_DIR / "wa_no_data_found.csv").read_bytes())

        outcome = synergy_wa_meter_data_parser(str(prod_path))

        assert isinstance(outcome, ParserOutcome)
        assert outcome.status == "processed_empty"
        assert outcome.reason == "no_data_sentinel"

    def test_rejects_files_without_synergy_wa_prefix(self, tmp_path: Path) -> None:
        """Any filename not starting with the Synergy WA prefix is NotRelevantParser."""
        f = tmp_path / "interval_au_single_day.csv"
        f.write_text("Date,Start Time,Identifier\n")

        with pytest.raises(NotRelevantParser, match="Not a Synergy WA"):
            synergy_wa_meter_data_parser(str(f))

    def test_falls_through_on_header_drift(self, tmp_path: Path) -> None:
        """A future format with a different header falls through to NotRelevantParser.

        Routes to newIrrevFiles/ rather than newParseErr/, so format drift surfaces
        as accumulation in newIrrevFiles/ instead of false-positive parse errors.
        """
        f = tmp_path / "Meter_Data_WA (AU)_Electricity_1778999999_2026051300000000.csv"
        f.write_text("Date,NMI,Usage\n2026-05-13,12345,1.23\n")

        with pytest.raises(NotRelevantParser, match="drifted"):
            synergy_wa_meter_data_parser(str(f))
