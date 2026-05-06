"""Shared numeric coercion helper for permissive row-level parsing.

Per the parser outcome contract, parsers must skip-and-count row-level
data quality issues rather than raising. This helper implements the
canonical pattern: ``pd.to_numeric(errors='coerce')`` with separate counts
for explicit blanks vs unparseable non-empty cells.
"""

from __future__ import annotations

import pandas as pd


def coerce_numeric_column(series: pd.Series) -> tuple[pd.Series, int, int]:
    """Coerce ``series`` to numeric, returning ``(coerced, unparseable, blank)``.

    A cell is considered "blank" if it is NaN/None or its string-stripped
    form is empty. A cell is "unparseable" if it is non-blank but
    ``pd.to_numeric`` coerces it to NaN.

    The returned ``coerced`` series has NaN where the original was blank
    or unparseable. Callers decide how to attribute the counts (e.g. only
    ``unparseable`` for wide-format sparse parsers, or both for tall-format
    parsers).
    """
    coerced = pd.to_numeric(series, errors="coerce")
    is_nan_after = coerced.isna()
    if not bool(is_nan_after.any()):
        return coerced, 0, 0

    original_str = series.astype(str).str.strip()
    is_blank_orig = series.isna() | (original_str == "")
    blank_count = int((is_nan_after & is_blank_orig).sum())
    unparseable_count = int((is_nan_after & ~is_blank_orig).sum())
    return coerced, unparseable_count, blank_count
