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
            demand_parser(str(path))

    def test_accepts_lowercase_user_download(self, write_demand_csv, monkeypatch, _reset_mappings_cache):
        # The user's manual download is named "Bunnings demand profile.csv"
        # (lowercase). Must accept this casing — i.e., the filename gate
        # MUST NOT raise. (The parser may still raise downstream because
        # the parser returns ParserOutcome without reading content, but specifically
        # the filename-gate-mismatch exception must not fire.)
        monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: {})

        path = write_demand_csv(filename="Bunnings demand profile.csv")
        result = demand_parser(str(path))

        assert result.status == "unmapped"


class TestContentGate:
    def test_rejects_files_without_commodities_header(self, write_demand_csv):
        # Filename matches but content doesn't start with "Commodities:"
        path = write_demand_csv(
            filename="Bunnings_Demand_Profile.csv",
            body_override="Wrong,Header\nfoo,bar\n",
        )
        with pytest.raises(NotRelevantParser, match="missing metadata header"):
            demand_parser(str(path))

    def test_bom_prefixed_metadata_header_passes_gate(self, tmp_path, monkeypatch, _reset_mappings_cache):
        # UTF-8 BOM (\xef\xbb\xbf) before "Commodities:" must still match the
        # content sniff. ``utf-8-sig`` strips the BOM transparently.
        monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: {})

        path = tmp_path / "Bunnings_Demand_Profile.csv"
        body = (
            'Commodities:,"Electricity"\n'
            'Sites (NMIs):,"4001260599"\n'
            'Status:,"Active"\n'
            "Country:, Australia\n"
            "Start:,01-Feb-2026\n"
            "End:,30-Apr-2026\n"
            "\n"
            "\n"
            "Business Unit,Identifier,Identifier Type,ReadingDateTime,E,kW,kVa,Power Factor,Site Name\n"
            "Bunnings Australia,4001260599,NMI,01-Feb-2026 00:00:00,5.24,10.48,10.48,1.0000,BUN AUS Forbes\n"
        )
        path.write_bytes(b"\xef\xbb\xbf" + body.encode("utf-8"))

        # Gate must accept (mappings empty → unmapped, but no NotRelevantParser raise).
        result = demand_parser(str(path))
        assert result.status == "unmapped"


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
        result = demand_parser(str(path))

        assert result.status == "processed_empty"
        assert result.reason == "no_data_sentinel"
        assert result.rows_written == 0


class TestEmptyData:
    def test_header_only_returns_processed_empty(self, write_demand_csv):
        # File has the column header but zero data rows
        path = write_demand_csv(filename="Bunnings_Demand_Profile.csv", rows=[])
        result = demand_parser(str(path))

        assert result.status == "processed_empty"
        assert result.source_row_count == 0
        assert result.reason == "all_blank"
        assert result.rows_written == 0

    def test_blank_value_rows_return_processed_empty_with_source_count(self, write_demand_csv, monkeypatch):
        monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: {})
        rows = [("4001260599", "01-Feb-2026 00:00:00", "5.2400", "", "", "")]
        path = write_demand_csv(rows=rows)

        result = demand_parser(str(path))

        assert result.status == "processed_empty"
        assert result.source_row_count == 1
        assert result.reason == "all_blank"
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
                demand_parser(str(path))

        mock_client.return_value.put_object.assert_not_called()


class TestRowShapeValidation:
    def test_truncated_row_is_skipped_without_upload(self, write_demand_csv, monkeypatch, _reset_mappings_cache):
        # The malformed row should be skipped and counted; with no other valid
        # rows in this file the parser returns processed_empty(all_skipped)
        # rather than raising.
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
            result = demand_parser(str(path))

        assert result.status == "processed_empty"
        assert result.reason == "all_skipped"
        assert result.rows_written == 0
        assert result.rows_skipped == 1
        assert result.skip_reasons["row_shape_mismatch"] == 1
        mock_client.return_value.put_object.assert_not_called()

    def test_truncated_row_skipped_and_other_rows_written(self, write_demand_csv, monkeypatch, _reset_mappings_cache):
        # File with one well-formed row followed by a truncated row.
        # The good row writes; the bad row is skipped.
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
                "Business Unit,Identifier,Identifier Type,ReadingDateTime,E,kW,kVa,Power Factor,Site Name",
                "Bunnings Australia,4001260599,NMI,01-Feb-2026 00:00:00,5.2400,10.4800,10.4800,1.0000,BUN AUS Forbes",
                "Bunnings Australia,4001260599,NMI,01-Feb-2026 00:30:00,5.2400,10.4800",
            ]
        )
        path = write_demand_csv(body_override=body)

        with patch("shared.parsers.optima.demand.boto3.client") as mock_client:
            mock_client.return_value.put_object.return_value = {"ETag": "fake"}
            result = demand_parser(str(path))

        assert result.status == "processed"
        assert result.rows_written == 3  # 1 good row x 3 channels
        assert result.rows_skipped == 1
        assert result.skip_reasons["row_shape_mismatch"] == 1
        mock_client.return_value.put_object.assert_called_once()


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
            result = demand_parser(str(path))

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
            result = demand_parser(str(path))

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

    def test_quality_cell_is_empty_for_demand_rows(self, write_demand_csv, monkeypatch, _reset_mappings_cache):
        """Demand exports do not carry vendor quality codes — quality cell
        must be empty (zero-length) so Athena reads it as NULL. Spec line 570.
        """
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
            result = demand_parser(str(path))

        assert result.status == "processed"
        body = captured_body[0]
        data_lines = [L for L in body.strip().split("\n")[1:] if L]
        assert len(data_lines) == 9

        # Every data line must end with ``,`` (zero characters between the
        # final comma and the line terminator) — never the literal ``""``.
        for line in data_lines:
            fields = line.split(",")
            assert len(fields) == 6
            assert fields[5] == "", f"quality must be empty cell, got {fields[5]!r}"
            assert line.endswith(",")  # raw bytes: trailing empty cell

        # Sanity: parsing back with csv.reader yields zero-length string
        import csv
        import io

        rows = list(csv.reader(io.StringIO(body)))
        for row in rows[1:]:  # skip header
            assert row[-1] == ""

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
            result = demand_parser(str(path))

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
            result = demand_parser(str(path))

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
            result = demand_parser(str(path))

        assert result.status == "unmapped"
        assert result.source_row_count == 3
        assert result.candidate_row_count == 9
        assert result.rows_written == 0
        assert result.unmapped_count == 9
        mock_client.return_value.put_object.assert_not_called()

    def test_mixed_bad_timestamp_and_unmapped_candidates_returns_unmapped(
        self, write_demand_csv, monkeypatch, _reset_mappings_cache
    ):
        # One good row → 3 unmapped candidates; one bad-date row → skipped.
        # No mappings, so the file as a whole reports unmapped (the typical
        # disposition signal) while still surfacing the skipped row in
        # rows_skipped/skip_reasons.
        monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: {})
        rows = [
            ("4001260599", "01-Feb-2026 00:00:00", "5.2400", "10.4800", "10.4800", "1.0000"),
            ("4001260599", "bad-date", "5.2400", "10.4800", "10.4800", "1.0000"),
        ]

        with patch("shared.parsers.optima.demand.boto3.client") as mock_client:
            path = write_demand_csv(rows=rows)
            result = demand_parser(str(path))

        assert result.status == "unmapped"
        assert result.candidate_row_count == 3
        assert result.unmapped_count == 3
        assert result.rows_written == 0
        assert result.rows_skipped == 1
        assert result.skip_reasons["unparseable_timestamp"] == 1
        mock_client.return_value.put_object.assert_not_called()

    def test_mixed_mapped_row_and_bad_timestamp_writes_good_row(
        self, write_demand_csv, monkeypatch, _reset_mappings_cache
    ):
        # The well-formed row maps to 3 sensors; the bad-date row is skipped.
        # File goes to processed; the bad row is reported via rows_skipped.
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
            mock_client.return_value.put_object.return_value = {"ETag": "fake"}
            path = write_demand_csv(rows=rows)
            result = demand_parser(str(path))

        assert result.status == "processed"
        assert result.rows_written == 3
        assert result.rows_skipped == 1
        assert result.skip_reasons["unparseable_timestamp"] == 1
        mock_client.return_value.put_object.assert_called_once()

    def test_all_bad_timestamps_returns_processed_empty_all_skipped(
        self, write_demand_csv, monkeypatch, _reset_mappings_cache
    ):
        monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: {})
        rows = [("4001260599", "bad-date", "5.2400", "10.4800", "10.4800", "1.0000")]
        path = write_demand_csv(rows=rows)

        with patch("shared.parsers.optima.demand.boto3.client") as mock_client:
            result = demand_parser(str(path))

        assert result.status == "processed_empty"
        assert result.reason == "all_skipped"
        assert result.rows_written == 0
        assert result.rows_skipped == 1
        assert result.skip_reasons["unparseable_timestamp"] == 1
        mock_client.return_value.put_object.assert_not_called()

    def test_all_non_numeric_values_returns_processed_empty_all_skipped(
        self, write_demand_csv, monkeypatch, _reset_mappings_cache
    ):
        # Every value column is non-numeric, so no candidate is built and no
        # Hudi row is written. Each unparseable cell counts in skip_reasons,
        # and the row itself is reported as one rows_skipped entry.
        monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: {})
        rows = [("4001260599", "01-Feb-2026 00:00:00", "5.2400", "bad-kw", "bad-kva", "bad-pf")]
        path = write_demand_csv(rows=rows)

        with patch("shared.parsers.optima.demand.boto3.client") as mock_client:
            result = demand_parser(str(path))

        assert result.status == "processed_empty"
        assert result.reason == "all_skipped"
        assert result.rows_written == 0
        assert result.rows_skipped == 1
        # 3 unparseable value cells (kW, kVa, Power Factor) on the same row
        assert result.skip_reasons["unparseable_value"] == 3
        mock_client.return_value.put_object.assert_not_called()

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
                demand_parser(str(path))


class TestPartialRowFailures:
    """Cover the post-Task-11 skip-and-count semantics for row-level errors."""

    def test_one_bad_date_among_five_rows_writes_others(self, write_demand_csv, monkeypatch, _reset_mappings_cache):
        # 5 input rows, 1 with an unparseable Date and 4 valid.
        # Each valid row x 3 channels (kw/kva/pf) = 12 Hudi rows.
        fake_mappings = {
            "Optima_4001260599-demand-kw": "p:bunnings:kw",
            "Optima_4001260599-demand-kva": "p:bunnings:kva",
            "Optima_4001260599-demand-pf": "p:bunnings:pf",
        }
        monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: fake_mappings)
        rows = [
            ("4001260599", "01-Feb-2026 00:00:00", "5.2400", "10.4800", "10.4800", "1.0000"),
            ("4001260599", "01-Feb-2026 00:30:00", "5.2400", "10.4800", "10.4800", "1.0000"),
            ("4001260599", "bad-date", "5.2400", "10.4800", "10.4800", "1.0000"),
            ("4001260599", "01-Feb-2026 01:00:00", "5.2400", "10.4800", "10.4800", "1.0000"),
            ("4001260599", "01-Feb-2026 01:30:00", "5.2400", "10.4800", "10.4800", "1.0000"),
        ]

        with patch("shared.parsers.optima.demand.boto3.client") as mock_client:
            mock_client.return_value.put_object.return_value = {"ETag": "fake"}
            path = write_demand_csv(rows=rows)
            result = demand_parser(str(path))

        assert result.status == "processed"
        assert result.source_row_count == 5
        assert result.candidate_row_count == 12
        assert result.rows_written == 12
        assert result.rows_skipped == 1
        assert result.skip_reasons["unparseable_timestamp"] == 1

    def test_one_unparseable_kw_value_writes_other_rows(self, write_demand_csv, monkeypatch, _reset_mappings_cache):
        # Row 1 has unparseable kW (kva and pf still valid → 2 Hudi rows).
        # Row 2 is fully valid (3 Hudi rows). Row 1 is NOT counted as
        # rows_skipped because it produced output rows; only the failed
        # cell is reflected in skip_reasons.
        fake_mappings = {
            "Optima_4001260599-demand-kw": "p:bunnings:kw",
            "Optima_4001260599-demand-kva": "p:bunnings:kva",
            "Optima_4001260599-demand-pf": "p:bunnings:pf",
        }
        monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: fake_mappings)
        rows = [
            ("4001260599", "01-Feb-2026 00:00:00", "5.2400", "bad-kw", "10.4800", "1.0000"),
            ("4001260599", "01-Feb-2026 00:30:00", "5.2400", "10.4800", "10.4800", "1.0000"),
        ]

        with patch("shared.parsers.optima.demand.boto3.client") as mock_client:
            mock_client.return_value.put_object.return_value = {"ETag": "fake"}
            path = write_demand_csv(rows=rows)
            result = demand_parser(str(path))

        assert result.status == "processed"
        assert result.rows_written == 5  # row1: 2, row2: 3
        assert result.rows_skipped == 0
        assert result.skip_reasons["unparseable_value"] == 1


class TestDispatcherIntegration:
    def test_dispatcher_routes_demand_file(self, write_demand_csv, monkeypatch, _reset_mappings_cache):
        from shared.non_nem_parsers import get_non_nem_outcome

        fake_mappings = {
            "Optima_4001260599-demand-kw": "p:bunnings:kw",
            "Optima_4001260599-demand-kva": "p:bunnings:kva",
            "Optima_4001260599-demand-pf": "p:bunnings:pf",
        }
        monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: fake_mappings)

        with patch("shared.parsers.optima.demand.boto3.client") as mock_client:
            mock_client.return_value.put_object.return_value = {"ETag": "fake"}
            path = write_demand_csv()
            result = get_non_nem_outcome(str(path))

        assert result.status == "processed"
        assert result.source_row_count == 3
        assert result.candidate_row_count == 9
        assert result.rows_written == 9
        assert result.unmapped_count == 0
        assert mock_client.called


class TestUnmappedIdentifiersPopulated:
    """Demand parser surfaces unmapped (kind, value) pairs on the outcome."""

    def test_partial_mapping_populates_unmapped_identifiers(self, write_demand_csv, monkeypatch, _reset_mappings_cache):
        # Only ``kw`` mapped — ``kva`` and ``pf`` should appear in
        # unmapped_identifiers with kind ``nmi`` and the full mapping
        # lookup key as the value (so dashboards can reproduce the miss).
        fake_mappings = {
            "Optima_4001260599-demand-kw": "p:bunnings:only-kw",
        }
        monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: fake_mappings)

        with patch("shared.parsers.optima.demand.boto3.client") as mock_client:
            mock_client.return_value.put_object.return_value = {"ETag": "fake"}
            path = write_demand_csv()
            result = demand_parser(str(path))

        assert result.status == "processed"
        assert result.unmapped_count == 6
        # Two distinct lookup keys for kva and pf (one per row dedups).
        values = {value for _kind, value in result.unmapped_identifiers}
        assert "Optima_4001260599-demand-kva" in values
        assert "Optima_4001260599-demand-pf" in values
        # Kind canonical for Optima parsers per spec identifier-kind table.
        assert all(kind == "nmi" for kind, _ in result.unmapped_identifiers)

    def test_all_unmapped_outcome_carries_identifiers(self, write_demand_csv, monkeypatch, _reset_mappings_cache):
        # Empty mapping → status=unmapped — identifiers must still flow.
        monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: {})

        with patch("shared.parsers.optima.demand.boto3.client") as mock_client:
            mock_client.return_value.put_object.return_value = {"ETag": "fake"}
            path = write_demand_csv()
            result = demand_parser(str(path))

        assert result.status == "unmapped"
        assert len(result.unmapped_identifiers) >= 1
        assert all(kind == "nmi" for kind, _ in result.unmapped_identifiers)
