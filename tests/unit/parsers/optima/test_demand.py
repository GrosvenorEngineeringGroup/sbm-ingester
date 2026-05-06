"""Tests for shared.parsers.optima.demand.demand_parser."""

from unittest.mock import patch

import pytest

from shared.parsers import NotRelevantParser, ParserError
from shared.parsers import _mappings as mappings_mod
from shared.parsers.optima.demand import demand_parser


class TestFilenameGate:
    def test_rejects_non_demand_files(self, write_demand_csv):
        path = write_demand_csv(filename="Bunnings_Interval_Usage.csv")
        with pytest.raises(NotRelevantParser, match="Not a Demand Profile"):
            demand_parser(str(path), "/tmp/err.log")

    def test_accepts_lowercase_user_download(self, write_demand_csv, monkeypatch, _reset_mappings_cache):
        # The user's manual download is named "Bunnings demand profile.csv"
        # (lowercase). Must accept this casing — i.e., the filename gate
        # MUST NOT raise. (The parser may still raise downstream because
        # the stub returns [] without reading content, but specifically
        # the filename-gate-mismatch exception must not fire.)
        monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: {})

        path = write_demand_csv(filename="Bunnings demand profile.csv")
        result = demand_parser(str(path), "/tmp/err.log")

        assert result.status == "unmapped"


class TestContentGate:
    def test_rejects_files_without_commodities_header(self, write_demand_csv):
        # Filename matches but content doesn't start with "Commodities:"
        path = write_demand_csv(
            filename="Bunnings_Demand_Profile.csv",
            body_override="Wrong,Header\nfoo,bar\n",
        )
        with pytest.raises(NotRelevantParser, match="missing metadata header"):
            demand_parser(str(path), "/tmp/err.log")


class TestNoDataFoundSentinel:
    def test_no_data_found_returns_processed_empty(self, write_demand_csv):
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

        assert result.status == "processed_empty"
        assert result.reason == "no_data_sentinel"
        assert result.rows_written == 0


class TestEmptyData:
    def test_header_only_returns_processed_empty(self, write_demand_csv):
        # File has the column header but zero data rows
        path = write_demand_csv(filename="Bunnings_Demand_Profile.csv", rows=[])
        result = demand_parser(str(path), "/tmp/err.log")

        assert result.status == "processed_empty"
        assert result.source_row_count == 0
        assert result.reason == "blank_values"
        assert result.rows_written == 0

    def test_blank_value_rows_return_processed_empty_with_source_count(self, write_demand_csv, monkeypatch):
        monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: {})
        rows = [("4001260599", "01-Feb-2026 00:00:00", "5.2400", "", "", "")]
        path = write_demand_csv(rows=rows)

        result = demand_parser(str(path), "/tmp/err.log")

        assert result.status == "processed_empty"
        assert result.source_row_count == 1
        assert result.reason == "blank_values"
        assert result.rows_written == 0


class TestHeaderValidation:
    def test_missing_expected_demand_column_raises_parser_error(
        self, write_demand_csv, monkeypatch, _reset_mappings_cache
    ):
        fake_mappings = {
            "Optima_4001260599-demand-kw": "p:bunnings:kw",
            "Optima_4001260599-demand-kva": "p:bunnings:kva",
            "Optima_4001260599-demand-pf": "p:bunnings:pf",
        }
        monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: fake_mappings)
        body = "\n".join(
            [
                'Commodities:,"Electricity"',
                'Sites (NMIs):,"4001260599"',
                'Status:,"Active"',
                "Country:, Australia",
                "Start:,01-Feb-2026",
                "End:,30-Apr-2026",
                "",
                "",
                "Business Unit,Identifier,Identifier Type,ReadingDateTime,E,kW,kVA,Power Factor,Site Name",
                "Bunnings Australia,4001260599,NMI,01-Feb-2026 00:00:00,5.24,10.48,10.48,1.0000,BUN AUS Forbes",
            ]
        )
        path = write_demand_csv(body_override=body)

        with patch("shared.parsers.optima.demand.boto3.client") as mock_client:
            with pytest.raises(ParserError, match="Missing demand columns"):
                demand_parser(str(path), "/tmp/err.log")

        mock_client.return_value.put_object.assert_not_called()


class TestRowShapeValidation:
    def test_truncated_row_raises_parser_error_without_upload(
        self, write_demand_csv, monkeypatch, _reset_mappings_cache
    ):
        fake_mappings = {
            "Optima_4001260599-demand-kw": "p:bunnings:kw",
        }
        monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: fake_mappings)
        body = "\n".join(
            [
                'Commodities:,"Electricity"',
                'Sites (NMIs):,"4001260599"',
                'Status:,"Active"',
                "Country:, Australia",
                "Start:,01-Feb-2026",
                "End:,30-Apr-2026",
                "",
                "",
                "Business Unit,Identifier,Identifier Type,ReadingDateTime,E,kW,kVa,Power Factor,Site Name",
                "Bunnings Australia,4001260599,NMI,01-Feb-2026 00:00:00,5.2400,10.4800",
            ]
        )
        path = write_demand_csv(body_override=body)

        with patch("shared.parsers.optima.demand.boto3.client") as mock_client:
            with pytest.raises(ParserError, match="Malformed demand row"):
                demand_parser(str(path), "/tmp/err.log")

        mock_client.return_value.put_object.assert_not_called()


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

        assert result.status == "processed"
        assert result.source_row_count == 3
        assert result.candidate_row_count == 9
        assert result.rows_written == 9
        assert result.unmapped_count == 0

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
            result = demand_parser(str(path), "/tmp/err.log")

        assert result.status == "processed"
        assert result.rows_written == 9
        assert result.unmapped_count == 0

        body = captured_body[0]
        # Find a PF row: its unit field (4th CSV column) must be empty
        pf_lines = [L for L in body.split("\n") if L.startswith("p:bunnings:pf,")]
        assert len(pf_lines) == 3
        for line in pf_lines:
            fields = line.split(",")
            # CSV: sensorId, ts, val, unit, its, quality
            assert fields[3] == "", f"PF unit should be empty string, got {fields[3]!r}"

    def test_preserves_source_value_precision(self, write_demand_csv, monkeypatch, _reset_mappings_cache):
        # Source CSV uses 4-decimal precision (e.g. "0.8800", "1.0000").
        # Hudi rows should preserve the raw string representation rather
        # than collapsing via float() (which would give "0.88", "1.0").
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
            # Use values that would lose precision via float() round-trip
            rows = [("4001260599", "01-Feb-2026 00:00:00", "5.2400", "10.4800", "10.4800", "1.0000")]
            path = write_demand_csv(rows=rows)
            result = demand_parser(str(path), "/tmp/err.log")

        assert result.status == "processed"
        assert result.source_row_count == 1
        assert result.candidate_row_count == 3
        assert result.rows_written == 3
        assert result.unmapped_count == 0

        body = captured_body[0]
        # 1 input row x 3 mapped columns = 3 Hudi rows
        data_lines = [L for L in body.strip().split("\n")[1:] if L]
        assert len(data_lines) == 3

        # kW row: val field (index 2) should be "10.4800" not "10.48"
        kw_line = next(L for L in data_lines if L.startswith("p:bunnings:kw,"))
        assert kw_line.split(",")[2] == "10.4800", f"kW precision lost: {kw_line!r}"

        # kVa row: val field should be "10.4800"
        kva_line = next(L for L in data_lines if L.startswith("p:bunnings:kva,"))
        assert kva_line.split(",")[2] == "10.4800", f"kVa precision lost: {kva_line!r}"

        # PF row: val field should be "1.0000"
        pf_line = next(L for L in data_lines if L.startswith("p:bunnings:pf,"))
        assert pf_line.split(",")[2] == "1.0000", f"PF precision lost: {pf_line!r}"

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
            result = demand_parser(str(path), "/tmp/err.log")

        assert result.status == "processed"
        assert result.source_row_count == 3
        assert result.candidate_row_count == 9
        assert result.rows_written == 3
        assert result.unmapped_count == 6

        body = captured_body[0]
        data_lines = [L for L in body.strip().split("\n")[1:] if L]
        # 3 input rows x 1 mapped column = 3 Hudi rows
        assert len(data_lines) == 3
        assert all(L.startswith("p:bunnings:only-kw,") for L in data_lines)

    def test_all_valid_candidates_unmapped_returns_unmapped(self, write_demand_csv, monkeypatch, _reset_mappings_cache):
        monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: {})

        with patch("shared.parsers.optima.demand.boto3.client") as mock_client:
            path = write_demand_csv()
            result = demand_parser(str(path), "/tmp/err.log")

        assert result.status == "unmapped"
        assert result.source_row_count == 3
        assert result.candidate_row_count == 9
        assert result.rows_written == 0
        assert result.unmapped_count == 9
        mock_client.return_value.put_object.assert_not_called()

    def test_mixed_bad_timestamp_and_unmapped_candidates_raise_parser_error(
        self, write_demand_csv, monkeypatch, _reset_mappings_cache
    ):
        from shared.parsers import ParserError

        monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: {})
        rows = [
            ("4001260599", "01-Feb-2026 00:00:00", "5.2400", "10.4800", "10.4800", "1.0000"),
            ("4001260599", "bad-date", "5.2400", "10.4800", "10.4800", "1.0000"),
        ]

        with patch("shared.parsers.optima.demand.boto3.client") as mock_client:
            path = write_demand_csv(rows=rows)

            with pytest.raises(ParserError, match="No valid demand candidates"):
                demand_parser(str(path), "/tmp/err.log")

        mock_client.return_value.put_object.assert_not_called()

    def test_mixed_mapped_row_and_bad_timestamp_raise_parser_error_without_upload(
        self, write_demand_csv, monkeypatch, _reset_mappings_cache
    ):
        fake_mappings = {
            "Optima_4001260599-demand-kw": "p:bunnings:kw",
            "Optima_4001260599-demand-kva": "p:bunnings:kva",
            "Optima_4001260599-demand-pf": "p:bunnings:pf",
        }
        monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: fake_mappings)
        rows = [
            ("4001260599", "01-Feb-2026 00:00:00", "5.2400", "10.4800", "10.4800", "1.0000"),
            ("4001260599", "bad-date", "5.2400", "10.4800", "10.4800", "1.0000"),
        ]

        with patch("shared.parsers.optima.demand.boto3.client") as mock_client:
            path = write_demand_csv(rows=rows)

            with pytest.raises(ParserError, match="No valid demand candidates"):
                demand_parser(str(path), "/tmp/err.log")

        mock_client.return_value.put_object.assert_not_called()

    def test_all_bad_timestamps_raise_parser_error(self, write_demand_csv, monkeypatch, _reset_mappings_cache):
        from shared.parsers import ParserError

        monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: {})
        rows = [("4001260599", "bad-date", "5.2400", "10.4800", "10.4800", "1.0000")]
        path = write_demand_csv(rows=rows)

        with pytest.raises(ParserError, match="No valid demand candidates"):
            demand_parser(str(path), "/tmp/err.log")

    def test_all_non_numeric_values_raise_parser_error(self, write_demand_csv, monkeypatch, _reset_mappings_cache):
        from shared.parsers import ParserError

        monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: {})
        rows = [("4001260599", "01-Feb-2026 00:00:00", "5.2400", "bad-kw", "bad-kva", "bad-pf")]
        path = write_demand_csv(rows=rows)

        with pytest.raises(ParserError, match="No valid demand candidates"):
            demand_parser(str(path), "/tmp/err.log")

    def test_put_object_failure_raises_processing_error(self, write_demand_csv, monkeypatch, _reset_mappings_cache):
        from shared.parsers import ProcessingError

        fake_mappings = {
            "Optima_4001260599-demand-kw": "p:bunnings:kw",
            "Optima_4001260599-demand-kva": "p:bunnings:kva",
            "Optima_4001260599-demand-pf": "p:bunnings:pf",
        }
        monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: fake_mappings)

        with patch("shared.parsers.optima.demand.boto3.client") as mock_client:
            mock_client.return_value.put_object.side_effect = RuntimeError("boom")
            path = write_demand_csv()

            with pytest.raises(ProcessingError, match="Failed to write demand Hudi CSV"):
                demand_parser(str(path), "/tmp/err.log")


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
