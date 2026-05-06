"""Tests for parser outcome contract."""

from collections import Counter

import pytest

from shared.parsers import (
    NotRelevantParser,
    ParserError,
    ParserOutcome,
    ParserReason,
    ProcessingError,
    SkipReason,
)


def test_processed_empty_outcome_defaults_to_no_rows() -> None:
    outcome = ParserOutcome(status="processed_empty", reason="no_data_sentinel")

    assert outcome.status == "processed_empty"
    assert outcome.dfs == []
    assert outcome.source_row_count == 0
    assert outcome.candidate_row_count == 0
    assert outcome.rows_written == 0
    assert outcome.unmapped_count == 0
    assert outcome.reason == "no_data_sentinel"


def test_unmapped_outcome_records_candidate_and_unmapped_counts() -> None:
    outcome = ParserOutcome(
        status="unmapped",
        source_row_count=3,
        candidate_row_count=9,
        unmapped_count=9,
    )

    assert outcome.status == "unmapped"
    assert outcome.source_row_count == 3
    assert outcome.candidate_row_count == 9
    assert outcome.unmapped_count == 9


@pytest.mark.parametrize("exc_type", [NotRelevantParser, ParserError, ProcessingError])
def test_parser_exceptions_preserve_message(exc_type: type[Exception]) -> None:
    with pytest.raises(exc_type, match="specific failure"):
        raise exc_type("specific failure")


def test_parser_outcome_observability_fields_default_empty() -> None:
    outcome = ParserOutcome(status="processed_empty", reason="zero_rows")

    assert outcome.unmapped_identifiers == ()
    assert outcome.unsupported_suffixes == frozenset()
    assert outcome.rows_skipped == 0
    assert outcome.skip_reasons == Counter()


def test_parser_outcome_with_skip_reasons_round_trips() -> None:
    skip_reasons: Counter[SkipReason] = Counter({"unparseable_value": 3, "blank_value": 2})
    outcome = ParserOutcome(
        status="processed_empty",
        reason="all_skipped",
        rows_skipped=5,
        skip_reasons=skip_reasons,
    )

    assert outcome.status == "processed_empty"
    assert outcome.reason == "all_skipped"
    assert outcome.rows_skipped == 5
    assert outcome.skip_reasons["unparseable_value"] == 3
    assert outcome.skip_reasons["blank_value"] == 2
    assert sum(outcome.skip_reasons.values()) == outcome.rows_skipped


def test_parser_outcome_with_unmapped_identifiers_and_unsupported_suffixes() -> None:
    outcome = ParserOutcome(
        status="unmapped",
        candidate_row_count=4,
        unmapped_count=4,
        unmapped_identifiers=(("nmi", "1234567890"), ("nmi", "0987654321")),
        unsupported_suffixes=frozenset({"foo", "bar"}),
    )

    assert outcome.unmapped_identifiers == (("nmi", "1234567890"), ("nmi", "0987654321"))
    assert outcome.unsupported_suffixes == frozenset({"foo", "bar"})


def test_parser_reason_and_skip_reason_types_importable() -> None:
    """Ensure the closed enums are importable from `shared.parsers`."""
    # Use them to confirm they exist; Literal types accept string values.
    reason: ParserReason = "idempotency_skip"
    skip: SkipReason = "unparseable_value"
    assert reason == "idempotency_skip"
    assert skip == "unparseable_value"
