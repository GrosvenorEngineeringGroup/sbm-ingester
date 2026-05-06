"""Non-NEM file parser contracts."""

from __future__ import annotations

from shared.parsers.outcome import (
    NotRelevantParser,
    ParserError,
    ParserOutcome,
    ParserResult,
    ParserStatus,
    ProcessingError,
)

__all__ = [
    "NotRelevantParser",
    "ParserError",
    "ParserOutcome",
    "ParserResult",
    "ParserStatus",
    "ProcessingError",
]
