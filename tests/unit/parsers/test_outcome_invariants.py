"""Tests for the assert_parser_outcome_invariants helper.

Covers each cross-field invariant branch (passing + failing case).
Real-parser sanity checks are kept in
``optima/test_outcome_invariants_optima.py`` because they depend on
optima-only fixtures.
"""

from __future__ import annotations

from collections import Counter

import pytest

from shared.parsers import ParserOutcome, SkipReason
from tests.helpers.outcome_invariants import assert_parser_outcome_invariants


class TestProcessedInvariant:
    def test_processed_with_rows_written_passes(self) -> None:
        outcome = ParserOutcome(status="processed", rows_written=5)
        assert_parser_outcome_invariants(outcome)

    def test_processed_with_zero_rows_fails(self) -> None:
        outcome = ParserOutcome(status="processed", rows_written=0)
        with pytest.raises(AssertionError, match="rows_written >= 1"):
            assert_parser_outcome_invariants(outcome)


class TestProcessedEmptyInvariant:
    def test_processed_empty_zero_rows_passes(self) -> None:
        outcome = ParserOutcome(status="processed_empty", reason="zero_rows")
        assert_parser_outcome_invariants(outcome)

    def test_processed_empty_with_rows_written_fails(self) -> None:
        outcome = ParserOutcome(status="processed_empty", rows_written=1)
        with pytest.raises(AssertionError, match="rows_written == 0"):
            assert_parser_outcome_invariants(outcome)

    def test_processed_empty_with_unmapped_fails(self) -> None:
        outcome = ParserOutcome(status="processed_empty", unmapped_count=2)
        with pytest.raises(AssertionError, match="unmapped_count == 0"):
            assert_parser_outcome_invariants(outcome)


class TestUnmappedInvariant:
    def test_unmapped_all_candidates_unmapped_passes(self) -> None:
        outcome = ParserOutcome(
            status="unmapped",
            candidate_row_count=4,
            unmapped_count=4,
        )
        assert_parser_outcome_invariants(outcome)

    def test_unmapped_zero_candidates_fails(self) -> None:
        outcome = ParserOutcome(
            status="unmapped",
            candidate_row_count=0,
            unmapped_count=0,
        )
        with pytest.raises(AssertionError, match="candidate_row_count > 0"):
            assert_parser_outcome_invariants(outcome)

    def test_unmapped_partial_unmapped_fails(self) -> None:
        outcome = ParserOutcome(
            status="unmapped",
            candidate_row_count=4,
            unmapped_count=2,
        )
        with pytest.raises(AssertionError, match="unmapped_count == candidate_row_count"):
            assert_parser_outcome_invariants(outcome)

    def test_unmapped_with_rows_written_fails(self) -> None:
        outcome = ParserOutcome(
            status="unmapped",
            candidate_row_count=4,
            unmapped_count=4,
            rows_written=1,
        )
        with pytest.raises(AssertionError, match="rows_written == 0"):
            assert_parser_outcome_invariants(outcome)


class TestProcessedExternalInvariant:
    def test_processed_external_zero_dfs_passes(self) -> None:
        outcome = ParserOutcome(
            status="processed_external",
            reason="external_gegoptimareports",
        )
        assert_parser_outcome_invariants(outcome)

    def test_processed_external_with_rows_written_fails(self) -> None:
        outcome = ParserOutcome(
            status="processed_external",
            rows_written=2,
        )
        with pytest.raises(AssertionError, match="rows_written == 0"):
            assert_parser_outcome_invariants(outcome)


class TestSkipReasonsInvariant:
    def test_rows_skipped_zero_with_no_skip_reasons_passes(self) -> None:
        outcome = ParserOutcome(status="processed", rows_written=1, rows_skipped=0)
        assert_parser_outcome_invariants(outcome)

    def test_rows_skipped_le_sum_skip_reasons_passes(self) -> None:
        # Cell-level skip counts can exceed row count; rows_skipped <= sum.
        skip_reasons: Counter[SkipReason] = Counter({"unparseable_value": 3, "blank_value": 2})
        outcome = ParserOutcome(
            status="processed",
            rows_written=10,
            rows_skipped=2,
            skip_reasons=skip_reasons,
        )
        assert_parser_outcome_invariants(outcome)

    def test_rows_skipped_equal_sum_skip_reasons_passes(self) -> None:
        skip_reasons: Counter[SkipReason] = Counter({"unparseable_value": 3})
        outcome = ParserOutcome(
            status="processed",
            rows_written=10,
            rows_skipped=3,
            skip_reasons=skip_reasons,
        )
        assert_parser_outcome_invariants(outcome)

    def test_rows_skipped_gt_sum_skip_reasons_fails(self) -> None:
        skip_reasons: Counter[SkipReason] = Counter({"unparseable_value": 1})
        outcome = ParserOutcome(
            status="processed",
            rows_written=10,
            rows_skipped=5,
            skip_reasons=skip_reasons,
        )
        with pytest.raises(AssertionError, match="rows_skipped"):
            assert_parser_outcome_invariants(outcome)
