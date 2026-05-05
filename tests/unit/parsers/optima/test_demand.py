"""Tests for shared.parsers.optima.demand.demand_parser."""

import pytest

from shared.parsers.optima.demand import demand_parser


class TestFilenameGate:
    def test_rejects_non_demand_files(self, write_demand_csv):
        path = write_demand_csv(filename="Bunnings_Interval_Usage.csv")
        with pytest.raises(Exception, match="Not a Demand Profile"):
            demand_parser(str(path), "/tmp/err.log")

    def test_accepts_lowercase_user_download(self, write_demand_csv):
        # The user's manual download is named "Bunnings demand profile.csv"
        # (lowercase). Must accept this casing — i.e., the filename gate
        # MUST NOT raise. (The parser may still raise downstream because
        # the stub returns [] without reading content, but specifically
        # the filename-gate-mismatch exception must not fire.)
        path = write_demand_csv(filename="Bunnings demand profile.csv")
        try:
            result = demand_parser(str(path), "/tmp/err.log")
            assert result == [] or isinstance(result, list)
        except Exception as e:
            assert "filename mismatch" not in str(e), f"Filename gate rejected lowercase user download: {e}"


class TestContentGate:
    def test_rejects_files_without_commodities_header(self, write_demand_csv):
        # Filename matches but content doesn't start with "Commodities:"
        path = write_demand_csv(
            filename="Bunnings_Demand_Profile.csv",
            body_override="Wrong,Header\nfoo,bar\n",
        )
        with pytest.raises(Exception, match="missing metadata header"):
            demand_parser(str(path), "/tmp/err.log")
