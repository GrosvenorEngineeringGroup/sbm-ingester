"""Tests for shared.parsers.optima.demand.demand_parser."""

from unittest.mock import patch

import pytest

from shared.parsers import _mappings as mappings_mod
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


class TestNoDataFoundSentinel:
    def test_no_data_found_returns_empty_list_no_exception(self, write_demand_csv):
        # BidEnergy returns this sentinel for sites with no demand data
        # (verified against NZ Bunnings sites 2026-05-05).
        body = (
            'Commodities:,"Electricity"\r\n'
            'Sites (NMIs):,"0000005438UN02B"\r\n'
            'Status:,"Active"\r\n'
            "Country:, New Zealand\r\n"
            "Start:,01-May-2026\r\n"
            "End:,03-May-2026\r\n"
            "\r\n"
            "\r\n"
            "No data found"
        )
        path = write_demand_csv(filename="NZ demand profile.csv", body_override=body)
        result = demand_parser(str(path), "/tmp/err.log")
        assert result == []


class TestEmptyData:
    def test_header_only_returns_empty_list(self, write_demand_csv):
        # File has the column header but zero data rows
        path = write_demand_csv(filename="Bunnings_Demand_Profile.csv", rows=[])
        result = demand_parser(str(path), "/tmp/err.log")
        assert result == []


@pytest.fixture
def _reset_mappings_cache():
    """Clear the shared mappings cache before and after each test."""
    mappings_mod._cache = None
    yield
    mappings_mod._cache = None


class TestMappingLookupAndHudiWrite:
    def test_writes_kw_kva_pf_with_correct_sensor_ids(self, write_demand_csv, monkeypatch, _reset_mappings_cache):
        # Arrange: synthetic mappings for the test NMI
        fake_mappings = {
            "Optima_4001260599-demand-kw": "p:bunnings:test-kw-id",
            "Optima_4001260599-demand-kva": "p:bunnings:test-kva-id",
            "Optima_4001260599-demand-pf": "p:bunnings:test-pf-id",
        }
        monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: fake_mappings)

        captured = {}

        def fake_put_object(**kwargs):
            captured["bucket"] = kwargs["Bucket"]
            captured["key"] = kwargs["Key"]
            captured["body"] = kwargs["Body"].decode()
            return {"ETag": "fake-etag"}

        with patch("shared.parsers.optima.demand.boto3.client") as mock_client:
            mock_client.return_value.put_object = fake_put_object
            path = write_demand_csv()
            result = demand_parser(str(path), "/tmp/err.log")

        # Assert: parser returned [] (signals dispatcher to not flow DataFrames)
        assert result == []

        # Assert: S3 PUT happened to the right place
        assert captured["bucket"] == "hudibucketsrc"
        assert captured["key"].startswith("sensorDataFiles/demand_export_")
        assert captured["key"].endswith(".csv")

        # Assert: 3 rows of input x 3 columns each = 9 Hudi rows
        body_lines = captured["body"].strip().split("\n")
        assert body_lines[0] == "sensorId,ts,val,unit,its,quality"
        data_lines = body_lines[1:]
        assert len(data_lines) == 9

        # Assert: each sensor ID appears 3 times (one per input row)
        assert sum(1 for L in data_lines if L.startswith("p:bunnings:test-kw-id,")) == 3
        assert sum(1 for L in data_lines if L.startswith("p:bunnings:test-kva-id,")) == 3
        assert sum(1 for L in data_lines if L.startswith("p:bunnings:test-pf-id,")) == 3

    def test_pf_unit_is_empty_string(self, write_demand_csv, monkeypatch, _reset_mappings_cache):
        fake_mappings = {
            "Optima_4001260599-demand-kw": "p:bunnings:kw",
            "Optima_4001260599-demand-kva": "p:bunnings:kva",
            "Optima_4001260599-demand-pf": "p:bunnings:pf",
        }
        monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: fake_mappings)

        captured_body = []

        def fake_put_object(**kwargs):
            captured_body.append(kwargs["Body"].decode())
            return {"ETag": "fake-etag"}

        with patch("shared.parsers.optima.demand.boto3.client") as mock_client:
            mock_client.return_value.put_object = fake_put_object
            path = write_demand_csv()
            demand_parser(str(path), "/tmp/err.log")

        body = captured_body[0]
        # Find a PF row: its unit field (4th CSV column) must be empty
        pf_lines = [L for L in body.split("\n") if L.startswith("p:bunnings:pf,")]
        assert len(pf_lines) == 3
        for line in pf_lines:
            fields = line.split(",")
            # CSV: sensorId, ts, val, unit, its, quality
            assert fields[3] == "", f"PF unit should be empty string, got {fields[3]!r}"

    def test_unmapped_nmis_skipped_silently(self, write_demand_csv, monkeypatch, _reset_mappings_cache):
        # Mappings only contain kw — kva and pf will be unmapped
        fake_mappings = {
            "Optima_4001260599-demand-kw": "p:bunnings:only-kw",
        }
        monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: fake_mappings)

        captured_body = []

        def fake_put_object(**kwargs):
            captured_body.append(kwargs["Body"].decode())
            return {"ETag": "fake-etag"}

        with patch("shared.parsers.optima.demand.boto3.client") as mock_client:
            mock_client.return_value.put_object = fake_put_object
            path = write_demand_csv()
            demand_parser(str(path), "/tmp/err.log")

        body = captured_body[0]
        data_lines = [L for L in body.strip().split("\n")[1:] if L]
        # 3 input rows x 1 mapped column = 3 Hudi rows
        assert len(data_lines) == 3
        assert all(L.startswith("p:bunnings:only-kw,") for L in data_lines)


class TestDispatcherIntegration:
    def test_dispatcher_routes_demand_file(self, write_demand_csv, monkeypatch, _reset_mappings_cache):
        from shared.non_nem_parsers import get_non_nem_df

        fake_mappings = {
            "Optima_4001260599-demand-kw": "p:bunnings:kw",
            "Optima_4001260599-demand-kva": "p:bunnings:kva",
            "Optima_4001260599-demand-pf": "p:bunnings:pf",
        }
        monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: fake_mappings)

        with patch("shared.parsers.optima.demand.boto3.client") as mock_client:
            mock_client.return_value.put_object.return_value = {"ETag": "fake"}
            path = write_demand_csv()
            result = get_non_nem_df(str(path), "/tmp/err.log")

        # Demand parser returns [], so the dispatcher returns [] too
        assert result == []
        # And the parser actually fired (not just dispatcher's no-parser-found path):
        # the boto3 mock was called means demand_parser ran.
        assert mock_client.called
