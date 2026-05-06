"""Test-only assertion helper for ParserOutcome cross-field invariants.

This helper enforces the spec's cross-field invariants on a ParserOutcome
during dev/CI without crashing production. Production code MUST NOT depend
on this module — the invariants are intentionally not enforced via
``__post_init__`` to avoid latent runtime crash risk on unmet expectations.

Spec reference: docs/superpowers/specs/2026-05-06-parser-outcome-semantics-design.md
(Cross-field invariants section).

Invariants:

- status="processed"          → rows_written >= 1
- status="processed_empty"    → rows_written == 0 AND unmapped_count == 0
- status="unmapped"           → rows_written == 0 AND
                                candidate_row_count > 0 AND
                                unmapped_count == candidate_row_count
- status="processed_external" → rows_written == 0 AND dfs == []
- rows_skipped <= sum(skip_reasons.values())
  (cell-level skip counts can exceed row count when a row contributes
  multiple value-column skips — see Tasks 10/11/16.)

Exception: ``reason="idempotency_skip"`` bypasses all invariants. The
file_processor synthesizes this outcome for the duplicate-skip case
(currently dead code per Task 12 amend, but reserved).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from shared.parsers import ParserOutcome


def assert_parser_outcome_invariants(outcome: ParserOutcome) -> None:
    """Assert spec cross-field invariants hold on ``outcome``.

    Raises ``AssertionError`` if any invariant is violated. Test-only —
    do not call from production code.
    """
    if outcome.reason == "idempotency_skip":
        return

    if outcome.status == "processed":
        assert outcome.rows_written >= 1, f"status='processed' requires rows_written >= 1, got {outcome.rows_written}"
    elif outcome.status == "processed_empty":
        assert outcome.rows_written == 0, (
            f"status='processed_empty' requires rows_written == 0, got {outcome.rows_written}"
        )
        assert outcome.unmapped_count == 0, (
            f"status='processed_empty' requires unmapped_count == 0, got {outcome.unmapped_count}"
        )
    elif outcome.status == "unmapped":
        assert outcome.rows_written == 0, f"status='unmapped' requires rows_written == 0, got {outcome.rows_written}"
        assert outcome.candidate_row_count > 0, (
            f"status='unmapped' requires candidate_row_count > 0, got {outcome.candidate_row_count}"
        )
        assert outcome.unmapped_count == outcome.candidate_row_count, (
            f"status='unmapped' requires unmapped_count == candidate_row_count "
            f"({outcome.candidate_row_count}), got {outcome.unmapped_count}"
        )
    elif outcome.status == "processed_external":
        assert outcome.rows_written == 0, (
            f"status='processed_external' requires rows_written == 0, got {outcome.rows_written}"
        )
        assert list(outcome.dfs) == [], (
            f"status='processed_external' requires dfs == [], got {len(list(outcome.dfs))} dfs"
        )

    if outcome.skip_reasons:
        skip_total = sum(outcome.skip_reasons.values())
        assert outcome.rows_skipped <= skip_total, (
            f"rows_skipped ({outcome.rows_skipped}) must be <= sum(skip_reasons) ({skip_total})"
        )
