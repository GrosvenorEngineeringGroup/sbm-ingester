"""Tests for parser outcome contract."""

import pytest

from shared.parsers import (
    NotRelevantParser,
    ParserError,
    ParserOutcome,
    ProcessingError,
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
