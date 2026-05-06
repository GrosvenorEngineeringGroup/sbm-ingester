"""Non-NEM file parser contracts."""

from __future__ import annotations

from shared.parsers.outcome import (
    NotRelevantParser,
    ParserError,
    ParserOutcome,
    ParserReason,
    ParserResult,
    ParserStatus,
    ProcessingError,
    SkipReason,
)

__all__ = [
    "NotRelevantParser",
    "ParserError",
    "ParserOutcome",
    "ParserReason",
    "ParserResult",
    "ParserStatus",
    "ProcessingError",
    "SkipReason",
]
