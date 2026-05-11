"""Tests for _emit_per_file_metrics emission of all branches."""

from __future__ import annotations

from collections import Counter
from unittest.mock import patch

from functions.file_processor.pipeline import _emit_per_file_metrics
from shared.parsers.outcome import ParserOutcome


def _captured_metric_names(call_args_list) -> list[str]:
    names: list[str] = []
    for call in call_args_list:
        name = call.kwargs.get("name")
        if name is None and call.args:
            name = call.args[0]
        if name is not None:
            names.append(name)
    return names


class TestStatusMetrics:
    def test_processed_emits_valid_processed_files(self) -> None:
        outcome = ParserOutcome(status="processed", rows_written=1)
        with patch("functions.file_processor.pipeline.metrics.add_metric") as add:
            _emit_per_file_metrics(outcome, {})
        assert "ValidProcessedFiles" in _captured_metric_names(add.call_args_list)

    def test_processed_external_emits_valid_processed_files(self) -> None:
        outcome = ParserOutcome(status="processed_external", reason="external_gegoptimareports")
        with patch("functions.file_processor.pipeline.metrics.add_metric") as add:
            _emit_per_file_metrics(outcome, {})
        assert "ValidProcessedFiles" in _captured_metric_names(add.call_args_list)

    def test_processed_empty_emits_processed_empty_files(self) -> None:
        outcome = ParserOutcome(status="processed_empty", reason="zero_rows")
        with patch("functions.file_processor.pipeline.metrics.add_metric") as add:
            _emit_per_file_metrics(outcome, {})
        assert "ProcessedEmptyFiles" in _captured_metric_names(add.call_args_list)

    def test_unmapped_emits_irrelevant_files(self) -> None:
        outcome = ParserOutcome(status="unmapped", candidate_row_count=10, unmapped_count=10)
        with patch("functions.file_processor.pipeline.metrics.add_metric") as add:
            _emit_per_file_metrics(outcome, {"candidate_row_count": 10, "unmapped_count": 10})
        assert "IrrelevantFiles" in _captured_metric_names(add.call_args_list)

    def test_parse_failed_emits_parse_error_files(self) -> None:
        outcome = ParserOutcome(status="parse_failed", reason="parser_error")
        with patch("functions.file_processor.pipeline.metrics.add_metric") as add:
            _emit_per_file_metrics(outcome, {})
        assert "ParseErrorFiles" in _captured_metric_names(add.call_args_list)


class TestPartialRecognitionMetrics:
    def test_partial_mapped_ratio_emitted_when_candidates_exist(self) -> None:
        outcome = ParserOutcome(status="processed", rows_written=8, candidate_row_count=10, unmapped_count=2)
        accumulators = {"candidate_row_count": 10, "unmapped_count": 2, "rows_skipped": 0}
        with patch("functions.file_processor.pipeline.metrics.add_metric") as add:
            _emit_per_file_metrics(outcome, accumulators)
        names = _captured_metric_names(add.call_args_list)
        assert "PartialMappedRatio" in names

    def test_rows_skipped_ratio_emitted_when_source_rows_known(self) -> None:
        outcome = ParserOutcome(status="processed_empty", reason="all_skipped", source_row_count=10, rows_skipped=10)
        accumulators = {"candidate_row_count": 0, "unmapped_count": 0, "rows_skipped": 10}
        with patch("functions.file_processor.pipeline.metrics.add_metric") as add:
            _emit_per_file_metrics(outcome, accumulators)
        assert "RowsSkippedRatio" in _captured_metric_names(add.call_args_list)

    def test_malformed_value_count_emits_zero_when_no_skip_reasons(self) -> None:
        outcome = ParserOutcome(status="processed", rows_written=1)
        with patch("functions.file_processor.pipeline.metrics.add_metric") as add:
            _emit_per_file_metrics(outcome, {"skip_counter": Counter()})
        # MalformedValueCount is always emitted (with 0 when no unparseable values)
        assert "MalformedValueCount" in _captured_metric_names(add.call_args_list)

    def test_unsupported_suffixes_found_emitted_when_set_non_empty(self) -> None:
        outcome = ParserOutcome(status="processed_empty", reason="all_unknown_suffix")
        with patch("functions.file_processor.pipeline.metrics.add_metric") as add:
            _emit_per_file_metrics(outcome, {"unsupported_suffixes": {"X9"}})
        assert "UnsupportedSuffixesFound" in _captured_metric_names(add.call_args_list)

    def test_unmapped_identifier_kind_emitted_per_kind(self) -> None:
        outcome = ParserOutcome(status="unmapped")
        accumulators = {"unmapped_identifiers": {("nem12_nmi", "ABC-E1"), ("p_id", "p:bunnings:xxx")}}
        with patch("functions.file_processor.pipeline.metrics.add_metric") as add:
            _emit_per_file_metrics(outcome, accumulators)
        names = _captured_metric_names(add.call_args_list)
        assert "UnmappedIdentifierKind_nem12_nmi" in names
        assert "UnmappedIdentifierKind_p_id" in names
