"""Tests for the shared numeric coercion helper."""

from __future__ import annotations

import pandas as pd

from shared.parsers._coerce import coerce_numeric_column


def test_all_numeric_returns_zero_counts() -> None:
    series = pd.Series(["1", "2.5", "3"])
    coerced, unparseable, blank = coerce_numeric_column(series)

    assert unparseable == 0
    assert blank == 0
    assert coerced.tolist() == [1.0, 2.5, 3.0]


def test_blank_cells_counted_as_blank() -> None:
    series = pd.Series(["1", "", "  ", None])
    coerced, unparseable, blank = coerce_numeric_column(series)

    assert unparseable == 0
    assert blank == 3
    assert coerced.iloc[0] == 1.0
    assert coerced.iloc[1:].isna().all()


def test_unparseable_cells_counted_separately() -> None:
    series = pd.Series(["1", "abc", "2.5"])
    coerced, unparseable, blank = coerce_numeric_column(series)

    assert unparseable == 1
    assert blank == 0
    assert coerced.iloc[0] == 1.0
    assert pd.isna(coerced.iloc[1])
    assert coerced.iloc[2] == 2.5


def test_mixed_blank_and_unparseable() -> None:
    series = pd.Series(["1", "", "abc", None, "2"])
    coerced, unparseable, blank = coerce_numeric_column(series)

    assert unparseable == 1
    assert blank == 2
    assert coerced.iloc[0] == 1.0
    assert coerced.iloc[4] == 2.0


def test_empty_series_returns_zero_counts() -> None:
    series = pd.Series([], dtype=object)
    coerced, unparseable, blank = coerce_numeric_column(series)

    assert unparseable == 0
    assert blank == 0
    assert len(coerced) == 0


def test_already_numeric_dtype_passthrough() -> None:
    series = pd.Series([1.0, 2.0, float("nan")])
    coerced, unparseable, blank = coerce_numeric_column(series)

    # NaN in a numeric series is reported as blank (its string-stripped
    # form is "nan" technically, but isna() catches it first).
    assert unparseable == 0
    assert blank == 1
    assert coerced.iloc[0] == 1.0
    assert coerced.iloc[1] == 2.0
