"""Parser outcome contract used by file disposition logic."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Literal

import pandas as pd

ParserStatus = Literal[
    "processed",
    "processed_empty",
    "unmapped",
    "processed_external",
]

ParserReason = Literal[
    "no_data_sentinel",
    "zero_rows",
    "all_blank",
    "all_zero_valid",
    "all_unknown_suffix",
    "all_skipped",
    "external_gegoptimareports",
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
    dfs: ParserResult = field(default_factory=list)
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


class NotRelevantParser(Exception):
    """Raised when a parser does not apply to the file."""


class ParserError(Exception):
    """Raised when a matching file cannot be parsed."""


class ProcessingError(Exception):
    """Raised when parsed data cannot be written or otherwise handled."""
