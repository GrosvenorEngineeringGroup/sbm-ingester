"""Tests for row-level skip handling + skip_reasons aggregation.

Per the parser-outcome contract, row-level data quality issues never raise.
Bad rows are skipped silently with the disqualifying reason recorded in a
``skip_counter`` (mutated in place by ``extract_valid_readings``). The
pipeline's per-file accumulators then surface those reasons via the
``ParserOutcome.skip_reasons`` field and the audit sidecar.
"""

from __future__ import annotations

from collections import Counter
from typing import Any

import numpy as np
import pandas as pd
import pytest

from functions.file_processor.pipeline import (
    DataFrameCandidate,
    extract_valid_readings,
)
from shared.parsers import ParserError


class TestExtractValidReadingsSkipCounters:
    """``extract_valid_readings`` must record skip reasons without raising."""

    def test_unparseable_timestamp_skipped_and_counted(self) -> None:
        timestamps: list[Any] = [pd.Timestamp("2024-01-01") + pd.Timedelta(minutes=30 * i) for i in range(100)]
        timestamps[17] = "definitely-not-a-timestamp"
        df = pd.DataFrame({"t_start": timestamps, "E1_kWh": [float(i) for i in range(100)]})
        skip_counter: Counter = Counter()

        candidates = extract_valid_readings(df, "E1_kWh", df["t_start"], None, skip_counter)

        assert len(candidates) == 99
        assert skip_counter["unparseable_timestamp"] == 1
        assert skip_counter["unparseable_value"] == 0
        assert skip_counter["blank_value"] == 0

    def test_unparseable_value_skipped_and_counted(self) -> None:
        timestamps = [pd.Timestamp("2024-01-01") + pd.Timedelta(minutes=30 * i) for i in range(100)]
        values: list[Any] = [float(i) for i in range(100)]
        values[5] = "abc"
        df = pd.DataFrame({"t_start": timestamps, "E1_kWh": values})
        skip_counter: Counter = Counter()

        candidates = extract_valid_readings(df, "E1_kWh", df["t_start"], None, skip_counter)

        assert len(candidates) == 99
        assert skip_counter["unparseable_value"] == 1
        assert skip_counter["unparseable_timestamp"] == 0
        assert skip_counter["blank_value"] == 0

    def test_blank_string_value_counted_as_blank(self) -> None:
        df = pd.DataFrame({"t_start": [pd.Timestamp("2024-01-01")], "E1_kWh": [""]})
        skip_counter: Counter = Counter()

        candidates = extract_valid_readings(df, "E1_kWh", df["t_start"], None, skip_counter)

        assert candidates == []
        assert skip_counter["blank_value"] == 1
        assert skip_counter["unparseable_value"] == 0

    def test_whitespace_value_counted_as_blank(self) -> None:
        df = pd.DataFrame({"t_start": [pd.Timestamp("2024-01-01")], "E1_kWh": ["   "]})
        skip_counter: Counter = Counter()

        candidates = extract_valid_readings(df, "E1_kWh", df["t_start"], None, skip_counter)

        assert candidates == []
        assert skip_counter["blank_value"] == 1

    def test_nan_value_counted_as_blank(self) -> None:
        df = pd.DataFrame({"t_start": [pd.Timestamp("2024-01-01")], "E1_kWh": [np.nan]})
        skip_counter: Counter = Counter()

        candidates = extract_valid_readings(df, "E1_kWh", df["t_start"], None, skip_counter)

        assert candidates == []
        assert skip_counter["blank_value"] == 1

    def test_does_not_raise_on_any_row_level_issue(self) -> None:
        df = pd.DataFrame(
            {
                "t_start": ["bad-ts", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")],
                "E1_kWh": ["x", "y", ""],
            }
        )

        # Must not raise even though every row is bad.
        candidates = extract_valid_readings(df, "E1_kWh", df["t_start"])

        assert candidates == []

    def test_skip_counter_optional(self) -> None:
        df = pd.DataFrame({"t_start": [pd.Timestamp("2024-01-01")], "E1_kWh": [1.0]})
        # No skip_counter argument; must not error.
        candidates = extract_valid_readings(df, "E1_kWh", df["t_start"])

        assert len(candidates) == 1
        assert isinstance(candidates[0], DataFrameCandidate)

    def test_skipped_samples_populated_when_sink_supplied(self) -> None:
        df = pd.DataFrame(
            {
                "t_start": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")],
                "E1_kWh": ["bad", 1.0],
            }
        )
        samples: list[dict[str, Any]] = []

        extract_valid_readings(df, "E1_kWh", df["t_start"], samples_sink=samples)

        assert len(samples) == 1
        assert samples[0]["reason"] == "unparseable_value"
        assert samples[0]["column"] == "E1_kWh"
        assert samples[0]["value"] == "bad"


class TestMissingTStartIsParserError:
    """Parser output missing the ``t_start`` column is a structural error."""

    def test_missing_t_start_raises_parser_error_when_processed(self) -> None:
        """A DataFrame without ``t_start`` and without an index named ``t_start``
        must trigger a ``ParserError`` inside ``_process_dataframes``.
        """
        from functions.file_processor.csv_writer import HudiSourceCsvWriter
        from functions.file_processor.pipeline import _process_dataframes
        from shared.parsers import ParserOutcome

        df_bad = pd.DataFrame({"foo": [1, 2], "E1_kWh": [1.0, 2.0]})
        outcome = ParserOutcome(status="processed", dataframes=[("NMI1", df_bad)])

        # Spy writer — we never expect to write a row before the error fires.
        class _SpyWriter:
            row_count = 0
            batch_timestamp = "spy"

            def write_row(self, *_a, **_kw):
                self.row_count += 1

            def flush(self):
                pass

        spy = _SpyWriter()
        with pytest.raises(ParserError):
            _process_dataframes(outcome, spy, nem12_mappings={})  # type: ignore[arg-type]
        assert spy.row_count == 0

        # Confirm HudiSourceCsvWriter import still resolves (smoke test on imports).
        assert HudiSourceCsvWriter is not None
