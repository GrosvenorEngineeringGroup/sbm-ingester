"""Tests for ParserOutcome.derive_final ladder and new parse_failed status."""

from __future__ import annotations

from shared.parsers.outcome import ParserOutcome


class TestDeriveFinal:
    def test_rule1_rows_written_yields_processed(self) -> None:
        seed = ParserOutcome(status="processed", rows_written=0)
        final = seed.derive_final(
            rows_written=5,
            candidate_row_count=5,
            unmapped_count=0,
            unsupported_suffixes=frozenset(),
            rows_skipped=0,
        )
        assert final.status == "processed"
        assert final.reason is None
        assert final.rows_written == 5

    def test_rule2_all_unmapped_yields_unmapped(self) -> None:
        seed = ParserOutcome(status="processed")
        final = seed.derive_final(
            rows_written=0,
            candidate_row_count=10,
            unmapped_count=10,
            unsupported_suffixes=frozenset(),
            rows_skipped=0,
        )
        assert final.status == "unmapped"
        assert final.reason is None
        assert final.candidate_row_count == 10
        assert final.unmapped_count == 10

    def test_rule3_unknown_suffix_yields_processed_empty_all_unknown_suffix(self) -> None:
        seed = ParserOutcome(status="processed")
        final = seed.derive_final(
            rows_written=0,
            candidate_row_count=0,
            unmapped_count=0,
            unsupported_suffixes=frozenset({"X9"}),
            rows_skipped=0,
        )
        assert final.status == "processed_empty"
        assert final.reason == "all_unknown_suffix"

    def test_rule4_all_skipped_yields_processed_empty_all_skipped(self) -> None:
        seed = ParserOutcome(status="processed")
        final = seed.derive_final(
            rows_written=0,
            candidate_row_count=0,
            unmapped_count=0,
            unsupported_suffixes=frozenset(),
            rows_skipped=3,
        )
        assert final.status == "processed_empty"
        assert final.reason == "all_skipped"

    def test_rule5_inherits_seed_reason(self) -> None:
        seed = ParserOutcome(status="processed", reason="zero_rows")
        final = seed.derive_final(
            rows_written=0,
            candidate_row_count=0,
            unmapped_count=0,
            unsupported_suffixes=frozenset(),
            rows_skipped=0,
        )
        assert final.status == "processed_empty"
        assert final.reason == "zero_rows"

    def test_rule5_no_seed_reason_yields_none(self) -> None:
        seed = ParserOutcome(status="processed")
        final = seed.derive_final(
            rows_written=0,
            candidate_row_count=0,
            unmapped_count=0,
            unsupported_suffixes=frozenset(),
            rows_skipped=0,
        )
        assert final.status == "processed_empty"
        assert final.reason is None


class TestParseFailedStatus:
    def test_parse_failed_is_valid_status(self) -> None:
        outcome = ParserOutcome(
            status="parse_failed",
            reason="parser_error",
            source_row_count=0,
        )
        assert outcome.status == "parse_failed"
        assert outcome.reason == "parser_error"

    def test_parse_failed_with_processing_error_reason(self) -> None:
        outcome = ParserOutcome(
            status="parse_failed",
            reason="processing_error",
            source_row_count=0,
        )
        assert outcome.status == "parse_failed"
        assert outcome.reason == "processing_error"


class TestDataframesField:
    def test_field_name_is_dataframes(self) -> None:
        outcome = ParserOutcome(status="processed", dataframes=[("NMI1", None)])  # type: ignore[arg-type]
        assert outcome.dataframes == [("NMI1", None)]
        assert not hasattr(outcome, "dfs"), "Old field name 'dfs' must be removed"
