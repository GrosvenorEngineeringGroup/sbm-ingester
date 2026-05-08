"""Parser outcome contract used by file disposition logic."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field, replace
from typing import Literal

import pandas as pd

ParserStatus = Literal[
    "processed",
    "processed_empty",
    "unmapped",
    "processed_external",
    "parse_failed",
]

ParserReason = Literal[
    "no_data_sentinel",
    "zero_rows",
    "all_blank",
    "all_zero_valid",
    "all_unknown_suffix",
    "all_skipped",
    "external_gegoptimareports",
    "parser_error",
    "processing_error",
]

SkipReason = Literal[
    "unparseable_value",
    "blank_value",
    "unparseable_timestamp",
    "row_anchor_failure",
    "row_shape_mismatch",
]

ParserResult = list[tuple[str, pd.DataFrame]]


@dataclass(frozen=True)
class ParserOutcome:
    status: ParserStatus
    dataframes: ParserResult = field(default_factory=list)
    source_row_count: int = 0
    candidate_row_count: int = 0
    rows_written: int = 0
    unmapped_count: int = 0
    reason: ParserReason | None = None
    unmapped_identifiers: tuple[tuple[str, str], ...] = ()
    unsupported_suffixes: frozenset[str] = field(default_factory=frozenset)
    rows_skipped: int = 0
    # Counter[SkipReason] is a static-typing constraint only; Counter does not
    # validate keys at runtime. Tests must assert key membership against the
    # SkipReason Literal values.
    skip_reasons: Counter[SkipReason] = field(default_factory=Counter)

    def derive_final(
        self,
        *,
        rows_written: int,
        candidate_row_count: int,
        unmapped_count: int,
        unsupported_suffixes: frozenset[str],
        rows_skipped: int,
    ) -> ParserOutcome:
        """Return a new outcome with final (status, reason) per spec ladder.

        Ladder (in order):
          1. rows_written > 0                                   -> processed
          2. candidate_row_count > 0 and unmapped_count == candidate_row_count
                                                               -> unmapped
          3. candidate_row_count == 0 and unsupported_suffixes -> processed_empty(all_unknown_suffix)
          4. rows_skipped > 0 and rows_written == 0 and candidate_row_count == 0
                                                               -> processed_empty(all_skipped)
          5. else                                              -> processed_empty(self.reason)

        ``derive_final`` never produces ``parse_failed``; that status only
        arises from caught ``ParserError`` in ``ingest_file``'s exception
        handler.
        """
        new_status: ParserStatus
        new_reason: ParserReason | None

        if rows_written > 0:
            new_status, new_reason = ("processed", None)
        elif candidate_row_count > 0 and unmapped_count == candidate_row_count:
            new_status, new_reason = ("unmapped", None)
        elif candidate_row_count == 0 and unsupported_suffixes:
            new_status, new_reason = ("processed_empty", "all_unknown_suffix")
        elif rows_skipped > 0 and rows_written == 0 and candidate_row_count == 0:
            new_status, new_reason = ("processed_empty", "all_skipped")
        else:
            new_status, new_reason = ("processed_empty", self.reason)

        return replace(
            self,
            status=new_status,
            reason=new_reason,
            rows_written=rows_written,
            candidate_row_count=candidate_row_count,
            unmapped_count=unmapped_count,
            unsupported_suffixes=unsupported_suffixes,
            rows_skipped=rows_skipped,
        )


class NotRelevantParser(Exception):
    """Raised when a parser does not apply to the file."""


class ParserError(Exception):
    """Raised when a matching file cannot be parsed."""


class ProcessingError(Exception):
    """Raised when parsed data cannot be written or otherwise handled."""
