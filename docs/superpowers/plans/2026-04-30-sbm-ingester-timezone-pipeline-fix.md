# SBM Ingester Timezone Pipeline Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop new SBM ingester data from losing source timezone context before it is written to Hudi.

**Architecture:** Each parser attaches the correct timezone semantics at the source boundary. The file writer accepts only timezone-aware timestamps, converts them to UTC, and writes ISO 8601 strings with explicit offset. Glue reads that explicit format and writes Hudi `ts` as a canonical UTC instant.

**Tech Stack:** Python 3.13, pandas, nemreader fork, pytest, AWS Glue/Spark CSV reader, Hudi COPY_ON_WRITE, Athena.

---

## Scope

This plan fixes the forward pipeline only. Historical Hudi data migration/backfill is intentionally out of scope and should be handled in a separate runbook.

Timezone source contracts to implement:

- NEM12 / NEM13 / Optima-style market data: fixed AEST (`UTC+10`, no DST).
- RACV / `Meter_Data_* (AU)_Electricity` exports: fixed market/AEST-style timestamps.
- Green Square ComX: local timestamp plus per-row `UTC Offset (minutes)`.
- Noosa Solar: timestamp suffix such as `AEST` / `AEDT`; missing suffix defaults to AEST with warning.
- Envizi vertical parsers: source convention unverified, so do not silently localize; writer must fail fast if these paths still emit naive timestamps.
- Bunnings billing: not DST-sensitive, but it writes directly to `sensorDataFiles`, so its timestamp string must match the new Glue/Hudi format.

Important shared-prefix constraint:

- `s3://hudibucketsrc/sensorDataFiles/` is not exclusively written by `sbm-ingester`. Before changing Glue to expect ISO offset timestamps, every active producer for this prefix must either emit the new format or Glue must explicitly support both old and new formats during the cutover.
- Known additional writers observed in the repo/docs include target/profile generators, RACV SkySpark push scripts, Fiskil, and weather sync. These may not all be active, but they must be inventoried before production cutover.

Important downstream-consumer constraint:

- After this change, Hudi `ts` represents a UTC instant. Any API, report, or SkySpark sync that accepts local wall-clock date strings must convert the requested local range to UTC before querying Hudi, compute `its` partition filters from that UTC range, and convert returned `ts` back to the display/site timezone before presenting or writing to SkySpark.
- If downstream consumers continue to query `ts between TIMESTAMP '<local wall-clock>'` and then parse returned `ts` as local time, Hudi will be correct but user-visible data will still be shifted.

## Files

- Create: `src/shared/timezone_utils.py`
- Modify: `src/libs/nemreader/streaming.py`
- Modify: `src/libs/nemreader/nem_reader.py`
- Modify: `src/shared/non_nem_parsers.py`
- Modify: `src/shared/noosa_solar_parser.py`
- Modify: `src/shared/billing_parser.py`
- Modify: `src/functions/file_processor/app.py`
- Modify: `src/glue/hudi_import/script.py`
- Test: `tests/unit/test_timezone_utils.py`
- Test: `tests/unit/test_nem12_streaming.py`
- Test: `tests/unit/test_non_nem_parsers.py`
- Test: `tests/unit/test_non_nem_parsers_edge_cases.py`
- Test: `tests/unit/test_noosa_solar_parser.py`
- Test: `tests/unit/test_billing_parser.py`
- Test: `tests/unit/test_batch_s3_writes.py`
- Test: `tests/unit/test_glue_hudi_import.py`
- Test: `tests/unit/test_timezone_real_samples.py` (new)
- Fixture directory: `tests/unit/fixtures/timezone/` (new)

---

### Task 0: Add Realistic Timezone Fixtures

**Files:**
- Create: `tests/unit/fixtures/timezone/nem12_sched_dst_start.csv`
- Create: `tests/unit/fixtures/timezone/nem12_grosv_dst_end.csv`
- Create: `tests/unit/fixtures/timezone/optima_bunnings_dst_end.csv`
- Create: `tests/unit/fixtures/timezone/racv_meter_data_dst_start.csv`
- Create: `tests/unit/fixtures/timezone/racv_meter_data_dst_end.csv`
- Create: `tests/unit/fixtures/timezone/comx_dst_end.csv`
- Create: `tests/unit/fixtures/timezone/noosa_aest.csv`
- Create: `tests/unit/test_timezone_real_samples.py`

These fixtures are real source-file excerpts captured during investigation. Keep enough rows/headers for the real parsers to run; do not replace them with synthetic mini-CSV formats that skip parser branches.

- [ ] **Step 1: Create fixture directory**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
mkdir -p tests/unit/fixtures/timezone
```

- [ ] **Step 2: Copy real small fixtures directly**

```bash
cp /tmp/parser_samples/sched_oct.csv tests/unit/fixtures/timezone/nem12_sched_dst_start.csv
cp /tmp/parser_samples/grosv_apr5.csv tests/unit/fixtures/timezone/nem12_grosv_dst_end.csv
cp /tmp/parser_samples/optbun.csv tests/unit/fixtures/timezone/optima_bunnings_dst_end.csv
cp /tmp/parser_samples/comx_apr5.csv tests/unit/fixtures/timezone/comx_dst_end.csv
cp /tmp/parser_samples/noosa.csv tests/unit/fixtures/timezone/noosa_aest.csv
```

Expected:
- `nem12_sched_dst_start.csv` is a real `NEM12` file for `2025-10-05` with 48 half-hour intervals.
- `nem12_grosv_dst_end.csv` is a real `NEM12` file for `2026-04-05` with 96 quarter-hour intervals.
- `optima_bunnings_dst_end.csv` is a real Optima-style CSV for `2026-04-05` with 48 half-hour rows.
- `comx_dst_end.csv` is a real ComX file with `Historical Intervals=100`, duplicated `02:00-02:45`, and per-row `UTC Offset (minutes)`.
- `noosa_aest.csv` is a real Noosa Solar file with `AEST` timestamp suffixes.

- [ ] **Step 3: Create RACV/Meter_Data excerpts from real files**

Use real files but keep only one DST day plus the original first two metadata rows.

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
python - <<'PY'
from pathlib import Path

def extract_meter_data_day(src: str, dst: str, date_value: str) -> None:
    src_path = Path(src)
    lines = src_path.read_text().splitlines()
    header = lines[:3]
    day_rows = [line for line in lines[3:] if line.startswith(date_value + ",")]
    if len(day_rows) != 48:
        raise SystemExit(f"{src} {date_value}: expected 48 rows, got {len(day_rows)}")
    Path(dst).write_text("\n".join(header + day_rows) + "\n")

extract_meter_data_day(
    "/tmp/parser_samples/racv_vic.csv",
    "tests/unit/fixtures/timezone/racv_meter_data_dst_start.csv",
    "05-Oct-2025",
)
extract_meter_data_day(
    "/tmp/parser_samples/racv_apr3.csv",
    "tests/unit/fixtures/timezone/racv_meter_data_dst_end.csv",
    "05-Apr-2026",
)
PY
```

- [ ] **Step 4: Add real-sample test skeleton**

Create `tests/unit/test_timezone_real_samples.py`:

```python
from pathlib import Path

import pandas as pd

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "timezone"


def _first_frame(result):
    assert result
    return result[0][1]


def test_real_nem12_dst_start_is_fixed_aest() -> None:
    from shared.nem_adapter import stream_as_data_frames
    from shared.timezone_utils import format_ts_for_hudi

    frames = list(stream_as_data_frames(str(FIXTURE_DIR / "nem12_sched_dst_start.csv")))
    df = _first_frame(frames)

    assert len(df) == 48
    assert df.index[0].utcoffset().total_seconds() == 10 * 3600
    assert pd.Timestamp("2025-10-05 02:00:00", tz=df.index.tz) in df.index
    assert format_ts_for_hudi(pd.Timestamp("2025-10-05 02:00:00", tz=df.index.tz)) == "2025-10-04T16:00:00+00:00"


def test_real_nem12_dst_end_is_fixed_aest_not_25_hour_day() -> None:
    from shared.nem_adapter import stream_as_data_frames
    from shared.timezone_utils import format_ts_for_hudi

    frames = list(stream_as_data_frames(str(FIXTURE_DIR / "nem12_grosv_dst_end.csv")))
    df = _first_frame(frames)

    assert len(df) == 96
    assert df.index[0].utcoffset().total_seconds() == 10 * 3600
    assert pd.Timestamp("2026-04-05 02:00:00", tz=df.index.tz) in df.index
    assert format_ts_for_hudi(pd.Timestamp("2026-04-05 02:00:00", tz=df.index.tz)) == "2026-04-04T16:00:00+00:00"


def test_real_optima_dst_end_is_fixed_aest() -> None:
    from shared.non_nem_parsers import optima_parser
    from shared.timezone_utils import format_ts_for_hudi

    result = optima_parser(str(FIXTURE_DIR / "optima_bunnings_dst_end.csv"), "error_log")
    df = _first_frame(result)

    assert len(df) == 48
    assert df.index[0].utcoffset().total_seconds() == 10 * 3600
    assert pd.Timestamp("2026-04-05 02:00:00", tz=df.index.tz) in df.index
    assert format_ts_for_hudi(pd.Timestamp("2026-04-05 02:00:00", tz=df.index.tz)) == "2026-04-04T16:00:00+00:00"


def test_real_racv_meter_data_dst_start_is_fixed_aest() -> None:
    from shared.non_nem_parsers import racv_elec_parser
    from shared.timezone_utils import format_ts_for_hudi

    result = racv_elec_parser(str(FIXTURE_DIR / "racv_meter_data_dst_start.csv"), "error_log")
    df = _first_frame(result)

    assert len(df) == 48
    assert df.index[0].utcoffset().total_seconds() == 10 * 3600
    assert pd.Timestamp("2025-10-05 02:00:00", tz=df.index.tz) in df.index
    assert format_ts_for_hudi(pd.Timestamp("2025-10-05 02:00:00", tz=df.index.tz)) == "2025-10-04T16:00:00+00:00"


def test_real_comx_dst_end_uses_utc_offset_to_disambiguate_fold() -> None:
    from shared.non_nem_parsers import green_square_private_wire_schneider_comx_parser

    result = green_square_private_wire_schneider_comx_parser(str(FIXTURE_DIR / "comx_dst_end.csv"), "error_log")
    df = _first_frame(result)

    assert len(df) == 100
    assert df.index.is_unique
    assert df.index[0].tzinfo is not None
    assert pd.Timestamp("2026-04-04 15:00:00", tz="UTC") in df.index
    assert pd.Timestamp("2026-04-04 16:00:00", tz="UTC") in df.index


def test_real_noosa_aest_suffix_is_preserved() -> None:
    from shared.noosa_solar_parser import noosa_solar_parser
    from shared.timezone_utils import format_ts_for_hudi

    result = noosa_solar_parser(str(FIXTURE_DIR / "noosa_aest.csv"), "error_log")
    df = _first_frame(result)

    assert df.index[0].utcoffset().total_seconds() == 10 * 3600
    assert format_ts_for_hudi(df.index[0]) == "2026-03-30T22:00:00+00:00"
```

- [ ] **Step 5: Run real-sample tests and verify they fail before implementation**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
uv run pytest tests/unit/test_timezone_real_samples.py -q
```

Expected: tests fail because parser timestamps are currently naive or ComX duplicates are not disambiguated.

- [ ] **Step 6: Commit fixtures and failing tests**

```bash
git add tests/unit/fixtures/timezone tests/unit/test_timezone_real_samples.py
git commit -m "test: add real timezone source fixtures"
```

---

### Task 1: Add Shared Timezone Utilities

**Files:**
- Create: `src/shared/timezone_utils.py`
- Create: `tests/unit/test_timezone_utils.py`

- [ ] **Step 1: Write failing tests**

Create `tests/unit/test_timezone_utils.py`:

```python
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest


def test_localize_fixed_aest_sets_plus_10_offset() -> None:
    from shared.timezone_utils import localize_fixed_aest

    result = localize_fixed_aest(pd.Series(["2026-03-31 06:00:00"]))

    assert result.iloc[0].utcoffset() == timedelta(hours=10)


def test_timestamp_from_local_and_offset_disambiguates_dst_end() -> None:
    from shared.timezone_utils import timestamp_from_local_and_offset

    result = timestamp_from_local_and_offset(
        pd.Series(["2026-04-05 02:00:00", "2026-04-05 02:00:00"]),
        pd.Series([660, 600]),
    )

    assert result.iloc[0].tzinfo is not None
    assert result.iloc[1].tzinfo is not None
    assert result.iloc[0] != result.iloc[1]
    assert result.iloc[1] - result.iloc[0] == pd.Timedelta(hours=1)


def test_parse_noosa_timestamp_preserves_aest_suffix() -> None:
    from shared.timezone_utils import parse_noosa_timestamp

    result = parse_noosa_timestamp(pd.Series(["31-Mar-26 8:00 AM AEST"]))

    assert result.iloc[0].utcoffset() == timedelta(hours=10)


def test_parse_noosa_timestamp_preserves_aedt_suffix() -> None:
    from shared.timezone_utils import parse_noosa_timestamp

    result = parse_noosa_timestamp(pd.Series(["31-Mar-26 8:00 AM AEDT"]))

    assert result.iloc[0].utcoffset() == timedelta(hours=11)


def test_parse_noosa_timestamp_missing_suffix_defaults_to_aest(caplog) -> None:
    from shared.timezone_utils import parse_noosa_timestamp

    result = parse_noosa_timestamp(pd.Series(["31-Mar-26 8:00 AM"]))

    assert result.iloc[0].utcoffset() == timedelta(hours=10)
    assert "missing tz suffix" in caplog.text.lower()


def test_format_ts_for_hudi_converts_to_utc_iso_offset() -> None:
    from shared.timezone_utils import AEST, format_ts_for_hudi

    result = format_ts_for_hudi(datetime(2026, 3, 31, 6, 0, tzinfo=AEST))

    assert result == "2026-03-30T20:00:00+00:00"


def test_format_ts_for_hudi_rejects_naive_timestamp() -> None:
    from shared.timezone_utils import format_ts_for_hudi

    with pytest.raises(ValueError, match="naive"):
        format_ts_for_hudi(datetime(2026, 3, 31, 6, 0))


def test_format_ts_for_hudi_accepts_pandas_timestamp() -> None:
    from shared.timezone_utils import AEST, format_ts_for_hudi

    result = format_ts_for_hudi(pd.Timestamp("2026-04-05 02:00:00", tz=AEST))

    assert result == "2026-04-04T16:00:00+00:00"
```

- [ ] **Step 2: Run failing tests**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
uv run pytest tests/unit/test_timezone_utils.py -q
```

Expected: fails with `ModuleNotFoundError: No module named 'shared.timezone_utils'`.

- [ ] **Step 3: Implement `timezone_utils.py`**

Create `src/shared/timezone_utils.py`:

```python
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

AEST = timezone(timedelta(hours=10))
AEDT = timezone(timedelta(hours=11))
UTC = timezone.utc

TZ_SUFFIX_OFFSETS = {
    "AEST": AEST,
    "AEDT": AEDT,
}


def localize_fixed_aest(values: pd.Series) -> pd.Series:
    """Attach fixed AEST (+10:00, no DST) to source-local timestamps."""
    parsed = pd.to_datetime(values)
    if parsed.dt.tz is not None:
        return parsed.dt.tz_convert(AEST)
    return parsed.dt.tz_localize(AEST)


def timestamp_from_local_and_offset(local_ts: pd.Series, offset_minutes: pd.Series) -> pd.Series:
    """Convert local wall-clock timestamps plus UTC offset minutes to UTC timestamps."""
    parsed_local = pd.to_datetime(local_ts, dayfirst=True)
    offsets = pd.to_timedelta(pd.to_numeric(offset_minutes), unit="m")
    utc_naive = parsed_local - offsets
    return utc_naive.dt.tz_localize(UTC)


def parse_noosa_timestamp(values: pd.Series) -> pd.Series:
    """Parse Noosa timestamp strings and preserve AEST/AEDT suffix semantics."""
    suffix = values.str.extract(r"\s+([A-Z]{3,4})$")[0]
    missing_count = int(suffix.isna().sum())
    if missing_count:
        logger.warning("Noosa timestamps missing tz suffix; defaulting to AEST", extra={"row_count": missing_count})
        suffix = suffix.fillna("AEST")

    unknown = sorted(set(suffix.dropna()) - set(TZ_SUFFIX_OFFSETS))
    if unknown:
        raise ValueError(f"Unsupported Noosa timezone suffix: {unknown}")

    stripped = values.str.replace(r"\s+[A-Z]{3,4}$", "", regex=True)
    parsed = pd.to_datetime(stripped, format="%d-%b-%y %I:%M %p")
    aware = [ts.tz_localize(TZ_SUFFIX_OFFSETS[tz]) for ts, tz in zip(parsed, suffix, strict=True)]
    return pd.Series(aware, index=values.index)


def format_ts_for_hudi(ts: Any) -> str:
    """Format a timezone-aware timestamp as UTC ISO 8601 with explicit offset."""
    if isinstance(ts, pd.Timestamp):
        if ts.tzinfo is None or ts.utcoffset() is None:
            raise ValueError(f"format_ts_for_hudi received naive timestamp: {ts!r}")
        return ts.to_pydatetime().astimezone(UTC).isoformat(timespec="seconds")

    if not isinstance(ts, datetime) or ts.tzinfo is None or ts.utcoffset() is None:
        raise ValueError(f"format_ts_for_hudi received naive timestamp: {ts!r}")

    return ts.astimezone(UTC).isoformat(timespec="seconds")
```

- [ ] **Step 4: Verify tests**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
uv run pytest tests/unit/test_timezone_utils.py -q
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/shared/timezone_utils.py tests/unit/test_timezone_utils.py
git commit -m "feat: add timezone helpers for ingester timestamps"
```

---

### Task 2: Fix NEM Parser Timezone Semantics

**Files:**
- Modify: `src/libs/nemreader/streaming.py`
- Modify: `src/libs/nemreader/nem_reader.py`
- Test: `tests/unit/test_nem12_streaming.py`

- [ ] **Step 1: Add failing NEM timezone tests**

Add to `tests/unit/test_nem12_streaming.py`:

```python
from datetime import timedelta


def test_streaming_nem12_timestamps_are_fixed_aest(nem12_sample_file: str) -> None:
    from libs.nemreader.streaming import stream_nem12_file

    _nmi, _suffix, _details, readings = next(stream_nem12_file(nem12_sample_file))

    assert readings[0].t_start.tzinfo is not None
    assert readings[0].t_end.tzinfo is not None
    assert readings[0].t_start.utcoffset() == timedelta(hours=10)
    assert readings[0].t_end - readings[0].t_start == timedelta(minutes=30)
```

- [ ] **Step 2: Run failing test**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
uv run pytest tests/unit/test_nem12_streaming.py -q -k "timestamps_are_fixed_aest"
```

Expected: fails because NEM datetimes are currently naive.

- [ ] **Step 3: Update NEM parser functions**

In `src/libs/nemreader/streaming.py`, import `AEST`:

```python
from shared.timezone_utils import AEST
```

Update `_parse_datetime`:

```python
return datetime.strptime(record, format_strings[len(record)]).replace(tzinfo=AEST)
```

In `src/libs/nemreader/nem_reader.py`, import `AEST` and update `parse_datetime`:

```python
timestamp = datetime.strptime(record.strip(), format_strings[len(record.strip())])
return timestamp.replace(tzinfo=AEST)
```

- [ ] **Step 4: Update existing naive datetime assertions**

Find existing tests that compare NEM timestamps to naive datetimes:

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
rg -n "datetime\([0-9]+, *[0-9]+, *[0-9]+" tests/unit/test_nem12_streaming.py tests/unit/test_nem_adapter.py tests/unit/test_nem12_real_file_equivalence.py
```

For expected NEM timestamps, add `tzinfo=timezone(timedelta(hours=10))` or compare `utcoffset() == timedelta(hours=10)`.

- [ ] **Step 5: Verify NEM tests**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
uv run pytest tests/unit/test_nem12_streaming.py tests/unit/test_nem_adapter.py -q
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add src/libs/nemreader/streaming.py src/libs/nemreader/nem_reader.py tests/unit/test_nem12_streaming.py tests/unit/test_nem_adapter.py
git commit -m "fix: preserve fixed AEST semantics in NEM parsers"
```

---

### Task 3: Fix Non-NEM Parser Timezone Semantics

**Files:**
- Modify: `src/shared/non_nem_parsers.py`
- Test: `tests/unit/test_non_nem_parsers.py`
- Test: `tests/unit/test_non_nem_parsers_edge_cases.py`

- [ ] **Step 1: Add failing tests for Optima/RACV fixed AEST**

Add tests that parse existing fixture files for `optima_parser` and `racv_elec_parser` and assert:

```python
assert df.index.tz is not None
assert df.index[0].utcoffset().total_seconds() == 10 * 3600
```

Use the existing fixtures in `tests/unit/conftest.py`; do not create new source formats.

- [ ] **Step 2: Add failing test for ComX duplicate 02:00 disambiguation**

Create a temp ComX CSV in `tests/unit/test_non_nem_parsers_edge_cases.py` containing these two rows under the normal ComX header:

```csv
0,660,05-04-2026 02:00:00,1
0,600,05-04-2026 02:00:00,2
```

Assert the output index has two distinct UTC timestamps:

```python
assert result_df.index[0] != result_df.index[1]
assert result_df.index[0].tzinfo is not None
assert result_df.index[1] - result_df.index[0] == pd.Timedelta(hours=1)
```

- [ ] **Step 3: Update imports**

In `src/shared/non_nem_parsers.py`, add:

```python
from shared.timezone_utils import localize_fixed_aest, timestamp_from_local_and_offset
```

- [ ] **Step 4: Localize RACV and Optima parser timestamps**

Replace:

```python
raw_df["Interval_Start"] = pd.to_datetime(raw_df["Date"] + " " + raw_df["Start Time"])
```

with:

```python
raw_df["Interval_Start"] = localize_fixed_aest(raw_df["Date"] + " " + raw_df["Start Time"])
```

Do this in both `racv_elec_parser` and `optima_parser`.

- [ ] **Step 5: Use ComX `UTC Offset (minutes)`**

In `green_square_private_wire_schneider_comx_parser`, replace:

```python
raw_df["Local Time Stamp"] = pd.to_datetime(raw_df["Local Time Stamp"], dayfirst=True)
```

with:

```python
if "UTC Offset (minutes)" not in raw_df.columns:
    raise Exception("Missing UTC Offset (minutes) column in ComX file.")

raw_df["Local Time Stamp"] = timestamp_from_local_and_offset(
    raw_df["Local Time Stamp"],
    raw_df["UTC Offset (minutes)"],
)
```

Keep the output column name as `t_start`.

- [ ] **Step 6: Leave Envizi unlocalized**

Do not add AEST localization to `envizi_vertical_parser_*`. Add this comment immediately before the Envizi parser section:

```python
# Envizi vertical source timezone convention is not verified. These parsers
# intentionally return whatever the source encodes; DirectCSVWriter will reject
# naive timestamps so this cold path cannot silently write ambiguous data.
```

- [ ] **Step 7: Verify non-NEM tests**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
uv run pytest tests/unit/test_non_nem_parsers.py tests/unit/test_non_nem_parsers_edge_cases.py -q
```

Expected: pass.

- [ ] **Step 8: Commit**

```bash
git add src/shared/non_nem_parsers.py tests/unit/test_non_nem_parsers.py tests/unit/test_non_nem_parsers_edge_cases.py
git commit -m "fix: preserve timezone semantics in non-NEM parsers"
```

---

### Task 4: Fix Noosa Solar Timezone Parsing

**Files:**
- Modify: `src/shared/noosa_solar_parser.py`
- Test: `tests/unit/test_noosa_solar_parser.py`

- [ ] **Step 1: Add failing tests**

Add tests that parse Noosa rows with `AEST`, `AEDT`, and missing suffix. For each output DataFrame:

```python
assert df.index[0].tzinfo is not None
```

For missing suffix, assert parsing succeeds and logs a warning.

- [ ] **Step 2: Update parser**

In `src/shared/noosa_solar_parser.py`, import:

```python
from shared.timezone_utils import parse_noosa_timestamp
```

Replace:

```python
df["timestamp"] = df["timestamp"].str.replace(r"\s+[A-Z]{3,4}$", "", regex=True)
df["timestamp"] = pd.to_datetime(df["timestamp"], format="%d-%b-%y %I:%M %p")
```

with:

```python
df["timestamp"] = parse_noosa_timestamp(df["timestamp"])
```

Keep the existing `unexpected_tz` warning if desired, but do not strip the suffix before parsing.

- [ ] **Step 3: Verify Noosa tests**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
uv run pytest tests/unit/test_noosa_solar_parser.py tests/unit/test_timezone_utils.py -q
```

Expected: pass.

- [ ] **Step 4: Commit**

```bash
git add src/shared/noosa_solar_parser.py tests/unit/test_noosa_solar_parser.py
git commit -m "fix: preserve Noosa Solar timezone suffix"
```

---

### Task 5: Enforce UTC ISO Output In File Writer

**Files:**
- Modify: `src/functions/file_processor/app.py`
- Test: `tests/unit/test_batch_s3_writes.py`

- [ ] **Step 1: Add failing writer tests**

Add tests for `DirectCSVWriter.write_row`:

```python
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pytest


def test_direct_csv_writer_outputs_utc_iso_timestamp() -> None:
    from functions.file_processor.app import DirectCSVWriter
    from shared.timezone_utils import AEST

    with ThreadPoolExecutor(max_workers=1) as executor:
        writer = DirectCSVWriter("batch", executor)
        writer.write_row("p:test:r:1", datetime(2026, 3, 31, 6, 0, tzinfo=AEST), 1.0, "kwh", "A")

    assert "2026-03-30T20:00:00+00:00" in writer.buffer.getvalue()


def test_direct_csv_writer_rejects_naive_timestamp() -> None:
    from functions.file_processor.app import DirectCSVWriter

    with ThreadPoolExecutor(max_workers=1) as executor:
        writer = DirectCSVWriter("batch", executor)
        with pytest.raises(ValueError, match="naive"):
            writer.write_row("p:test:r:1", datetime(2026, 3, 31, 6, 0), 1.0, "kwh")
```

- [ ] **Step 2: Update writer**

In `src/functions/file_processor/app.py`, import:

```python
from shared.timezone_utils import format_ts_for_hudi
```

Remove `TS_FORMAT`. Replace `write_row` with:

```python
def write_row(self, sensor_id: str, ts: Any, val: float, unit: str, quality: str = "") -> None:
    """Write a single row to the buffer."""
    ts_str = format_ts_for_hudi(ts)
    self.buffer.write(f"{sensor_id},{ts_str},{val},{unit},{ts_str},{quality}\n")
    self.row_count += 1
```

- [ ] **Step 3: Update old tests**

Search:

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
rg -n "2024-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:|TS_FORMAT|strftime" tests/unit/test_batch_s3_writes.py tests/unit
```

Change tests that expect `YYYY-MM-DD HH:mm:ss` from `DirectCSVWriter` to expect ISO UTC strings.

- [ ] **Step 4: Verify writer tests**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
uv run pytest tests/unit/test_batch_s3_writes.py -q
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add src/functions/file_processor/app.py tests/unit/test_batch_s3_writes.py
git commit -m "fix: write Hudi source timestamps as UTC ISO strings"
```

---

### Task 6: Update Direct Billing Output Format

**Files:**
- Modify: `src/shared/billing_parser.py`
- Test: `tests/unit/test_billing_parser.py`

- [ ] **Step 1: Add failing billing timestamp test**

Add:

```python
def test_billing_date_to_ts_outputs_hudi_iso_utc() -> None:
    from shared.billing_parser import _billing_date_to_ts

    assert _billing_date_to_ts("Mar 2026") == "2026-02-28T14:00:00+00:00"
```

- [ ] **Step 2: Update billing formatter**

In `src/shared/billing_parser.py`, import:

```python
from shared.timezone_utils import AEST, format_ts_for_hudi
```

Replace `_billing_date_to_ts` with:

```python
def _billing_date_to_ts(date_str: str) -> str | None:
    """Convert 'Mmm YYYY' to ISO 8601 UTC anchored to fixed AEST month start."""
    if not date_str or not date_str.strip():
        return None
    try:
        dt = datetime.strptime(date_str.strip(), "%b %Y")
    except ValueError:
        return None
    return format_ts_for_hudi(dt.replace(tzinfo=AEST))
```

- [ ] **Step 3: Verify billing tests**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
uv run pytest tests/unit/test_billing_parser.py -q
```

Expected: pass.

- [ ] **Step 4: Commit**

```bash
git add src/shared/billing_parser.py tests/unit/test_billing_parser.py
git commit -m "fix: align billing Hudi CSV timestamps with UTC ISO contract"
```

---

### Task 7: Update Glue/Hudi Timestamp Parsing

**Files:**
- Modify: `src/glue/hudi_import/script.py`
- Test: `tests/unit/test_glue_hudi_import.py`

- [ ] **Step 1: Add failing Glue config tests**

Update `tests/unit/test_glue_hudi_import.py` assertions:

```python
assert config["hoodie.deltastreamer.keygen.timebased.input.dateformat"] == "yyyy-MM-dd'T'HH:mm:ssXXX"
assert config["hoodie.deltastreamer.keygen.timebased.timezone"] == "UTC"
```

Add a test for `read_csv_batch` options if the test harness mocks Spark reader options:

```python
assert reader.options.assert_any_call(header=True, delimiter=",", timestampFormat="yyyy-MM-dd'T'HH:mm:ssXXX")
```

- [ ] **Step 2: Update Hudi keygen input format**

In `src/glue/hudi_import/script.py`, replace:

```python
"hoodie.deltastreamer.keygen.timebased.input.dateformat": "yyyy-MM-dd H:mm:ss",
```

with:

```python
"hoodie.deltastreamer.keygen.timebased.input.dateformat": "yyyy-MM-dd'T'HH:mm:ssXXX",
```

- [ ] **Step 3: Force Spark session timezone to UTC**

In `src/glue/hudi_import/script.py`, extend the `SparkSession.builder` chain:

```python
.config("spark.sql.session.timeZone", "UTC")
```

Reason: Spark `TimestampType` stores instants but renders/parses them using the session timezone. The job should not depend on Glue's environment default.

- [ ] **Step 4: Update Spark CSV reader timestamp format**

Replace:

```python
.options(header=True, delimiter=",")
```

with:

```python
.options(header=True, delimiter=",", timestampFormat="yyyy-MM-dd'T'HH:mm:ssXXX")
```

- [ ] **Step 5: Verify Glue tests**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
uv run pytest tests/unit/test_glue_hudi_import.py -q
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add src/glue/hudi_import/script.py tests/unit/test_glue_hudi_import.py
git commit -m "fix: parse Hudi source timestamps with explicit UTC offset"
```

---

### Task 7A: Validate Shared Hudi Staging Prefix Compatibility

**Files:**
- No default code changes in `sbm-ingester`.
- Possible follow-up changes depend on which external producers are confirmed active.

This task is a production-cutover gate. `DataImportIntoLake` consumes the shared prefix `s3://hudibucketsrc/sensorDataFiles/`, not only CSVs produced by `sbm-files-ingester`. If Glue is changed to only parse `yyyy-MM-dd'T'HH:mm:ssXXX`, any still-active producer writing `YYYY-MM-DD HH:mm:ss` can fail the whole batch or write null timestamps.

- [ ] **Step 1: Inventory all active writers to `sensorDataFiles/`**

Run:

```bash
cd /Users/zeyu/Desktop/GEG/sbm
rg -n "sensorDataFiles/|sensorDataFiles\\+|hudibucketsrc" \
  sbm-ingester/src \
  sbm-backend/verdeos-sbm-lambda \
  verdeos-infra-docs/src/aws/functions \
  verdeos-infra-docs/src/skyspark \
  -g '*.py' -g '*.axon'
```

Known writers to verify:

- `sbm-ingester/src/functions/file_processor/app.py`
- `sbm-ingester/src/shared/billing_parser.py`
- `sbm-backend/verdeos-sbm-lambda/generateSiteElecProfile/src/lambda_function.py`
- `sbm-backend/verdeos-sbm-lambda/generateSiteUtilityProfile/src/lambda_function.py`
- `verdeos-infra-docs/src/skyspark/racv/functions/pushDataToAWSDataLakeScript/script.axon`
- `verdeos-infra-docs/src/aws/functions/fiskilAPIFunctions/lambda_function.py`
- `verdeos-infra-docs/src/aws/functions/syncWeatherStationData/lambda_function.py`

- [ ] **Step 2: Choose one compatibility strategy before production cutover**

Use one of these strategies:

```text
Strategy A: update or pause every active producer so all new files use ISO offset UTC before deploying the Glue change.

Strategy B: make Glue accept both formats during the transition:
  - new ISO offset UTC: yyyy-MM-dd'T'HH:mm:ssXXX
  - legacy naive string: yyyy-MM-dd HH:mm:ss
```

Strategy B is operationally safer if any external writer cannot be updated in the same deployment window. It prevents new Glue from breaking on legacy-format CSVs, but it does not fix timezone semantics for those external legacy producers. Those producers still need source-specific timezone review later.

- [ ] **Step 3: Dry-run one CSV from every active producer**

Before production cutover, collect or generate one small CSV from each active writer and run the Glue parsing path against all of them together.

Expected:

- no file produces null `ts`;
- no file produces null/invalid partition `its`;
- Hudi partition path remains valid, e.g. `its=2026`;
- ISO UTC files from the fixed SBM ingester parse to the expected UTC instants.

- [ ] **Step 4: Record the compatibility decision**

Add the selected strategy and verified writer list to the deployment ticket/release notes. Do not proceed with Task 10 until this is done.

---

### Task 7B: Validate Downstream UTC Consumer Contract

**Files:**
- Review/update outside `sbm-ingester` as needed:
  - `sbm-backend/verdeos-sbm-lambda/requestAthenaData/src/lambda_function.py`
  - `sbm-backend/verdeos-sbm-lambda/generateSiteConsumptionsForView/src/lambda_function.py`
  - `sbm-backend/verdeos-sbm-lambda/generateSiteConsumptionsForBulkExportAndEmail/src/lambda_function.py`
  - `sbm-backend/verdeos-sbm-lambda/generateSiteElecProfile/src/lambda_function.py`
  - `sbm-backend/verdeos-sbm-lambda/generateSiteUtilityProfile/src/lambda_function.py`
  - any active `precomputeMeterAndTargetReadingsAndStoreToDynamoDB` deployment
  - SkySpark `gegGems2DataSync` functions.

This task is required for end-to-end user-visible correctness. It may be implemented in a separate repo/deployment, but it must be completed or explicitly accepted before claiming that SkySpark/GEMS/SBM displays are correct after the UTC Hudi change.

- [ ] **Step 1: Convert local query ranges to UTC before Hudi queries**

Current consumers commonly build SQL like:

```sql
ts between TIMESTAMP '<local_start>' and TIMESTAMP '<local_end>'
```

For new UTC Hudi data, this must become:

```text
local_start + site/report timezone -> UTC start
local_end + site/report timezone -> UTC end
itsRange = years covered by UTC start/end
```

- [ ] **Step 2: Convert returned UTC timestamps to the display/site timezone**

For APIs that return rows to UI or SkySpark, treat Athena `ts` as UTC and convert before formatting if the existing caller contract expects local wall-clock strings.

Example expected contract for existing SkySpark sync compatibility:

```text
Hudi ts:     2026-03-30 20:00:00 UTC
API returns: 2026-03-31 07:00:00
SkySpark parses with point tz Australia/Sydney -> 2026-03-31T07:00:00+11:00
```

- [ ] **Step 3: Update or version the SkySpark sync contract**

Current SkySpark sync sends local strings and appends `pointTz` when parsing returned `ts`. Either:

```text
Option A: keep API response as local wall-clock string for backward compatibility, after UTC conversion inside the API.
Option B: return explicit UTC/offset timestamps and update SkySpark to parse them as absolute instants.
```

Do not mix these contracts per project.

- [ ] **Step 4: Add an end-to-end DST validation**

Use a known DST-period point and assert:

```text
source fixed AEST 2026-03-31 06:00
Hudi UTC       2026-03-30 20:00:00
API/SkySpark   2026-03-31 07:00:00 Australia/Sydney
```

Expected: BMS/SkySpark wall-clock alignment is correct for new data.

---

### Task 8: Real-Sample Pipeline Regression

**Files:**
- Test: `tests/unit/test_timezone_real_samples.py`

- [ ] **Step 1: Run the real-sample timezone tests**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
uv run pytest tests/unit/test_timezone_real_samples.py -q
```

Expected: pass. This is the main guard that the implemented logic matches real source-file shapes, not just synthetic mini-CSV cases.

- [ ] **Step 2: Check no real-sample parser output reaches the writer as naive**

Add or keep assertions in `tests/unit/test_timezone_real_samples.py` that call `format_ts_for_hudi` on at least one timestamp from each source type:

```python
from shared.timezone_utils import format_ts_for_hudi

formatted = format_ts_for_hudi(df.index[0])
assert formatted.endswith("+00:00")
```

Expected: every confirmed source type can be formatted by the writer without raising `ValueError`.

- [ ] **Step 3: Commit real-sample test updates**

```bash
git add tests/unit/test_timezone_real_samples.py tests/unit/fixtures/timezone
git commit -m "test: verify timezone pipeline against real source samples"
```

---

### Task 9: Full Regression

**Files:**
- All modified files.

- [ ] **Step 1: Run focused test suite**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
uv run pytest \
  tests/unit/test_timezone_utils.py \
  tests/unit/test_nem12_streaming.py \
  tests/unit/test_nem_adapter.py \
  tests/unit/test_non_nem_parsers.py \
  tests/unit/test_non_nem_parsers_edge_cases.py \
  tests/unit/test_noosa_solar_parser.py \
  tests/unit/test_billing_parser.py \
  tests/unit/test_batch_s3_writes.py \
  tests/unit/test_glue_hudi_import.py \
  tests/unit/test_timezone_real_samples.py \
  -q
```

Expected: pass.

- [ ] **Step 2: Run full ingester test suite**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
uv run pytest -q
```

Expected: pass.

- [ ] **Step 3: Run lint**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
uv run ruff check .
```

Expected: pass.

- [ ] **Step 4: Inspect changed files**

```bash
cd /Users/zeyu/Desktop/GEG/sbm
git diff -- sbm-ingester/src sbm-ingester/tests
```

Expected:
- no parser emits naive timestamps for confirmed source types;
- `DirectCSVWriter` rejects naive timestamps;
- Glue expects ISO offset timestamps;
- historical migration is not included.

- [ ] **Step 5: Final commit if needed**

```bash
git status --short
git add sbm-ingester/src sbm-ingester/tests
git commit -m "fix: preserve timezone context through Hudi ingest pipeline"
```

---

### Task 10: Deployment Validation For New Data

**Files:**
- No code changes.

- [ ] **Step 1: Drain old-format Hudi source CSV files before cutover**

Before deploying either the writer or Glue change, confirm the old-format queue is empty:

```bash
aws s3 ls s3://hudibucketsrc/sensorDataFiles/ --region ap-southeast-2
```

Expected: no pending CSV files. If files exist, run the current production Glue job first and wait for it to archive them to `sensorDataFilesArchived/`.

Reason: new Glue expects ISO offset timestamps unless Task 7A selects a dual-format Glue strategy. Old writer output is `YYYY-MM-DD HH:mm:ss`. Mixing old and new CSV formats in the same `sensorDataFiles/` batch can produce null timestamps or failed parsing.

- [ ] **Step 2: Deploy ingester Lambda and Glue changes as one cutover**

Use the deployment flow currently used for `sbm-files-ingester` and `DataImportIntoLake`. Do not run a historical redrive in this task.

Deploy `DirectCSVWriter`/parser changes and Glue timestamp parsing changes together, after Task 7A confirms the shared prefix is safe. Do not leave either combination live:

```text
old writer + new Glue = old CSV may not parse
new writer + old Glue = new CSV may not parse
```

- [ ] **Step 3: Process one known DST-period test file**

Pick a small archived sample that covers DST, copy it into a test input prefix, and let the fixed pipeline write a new `sensorDataFiles` CSV.

Expected CSV timestamp shape:

```text
2026-03-30T20:00:00+00:00
```

- [ ] **Step 4: Run Glue on only the new test CSV**

Use `--MAX_FILES` or a temporary prefix if supported by the environment.

Expected: Glue succeeds and archives the file.

- [ ] **Step 5: Query Athena for the inserted sensor/time**

```sql
SELECT sensorid, ts, val, unit
FROM default.sensordata_default
WHERE sensorid = '<test_sensor_id>'
ORDER BY ts DESC
LIMIT 10;
```

Expected: new rows are stored as the UTC instant corresponding to the source timestamp. Do not compare against old historical rows in this task.

- [ ] **Step 6: Record validation result**

Add a short note to the deployment ticket or release notes:

```text
Timezone pipeline validation passed: parser emitted aware timestamp, writer emitted UTC ISO offset, Glue parsed timestamp, and Athena shows expected UTC instant.
```

---

## Out Of Scope

- Historical Hudi data migration or deletion of old rows.
- Re-ingesting archived source files.
- Changing Hudi schema or record key.
- Historical SkySpark/GEMS reindexing. Forward SkySpark/API sync contract validation is covered by Task 7B if end-to-end display correctness is required.
- Guessing Envizi timezone semantics without source samples.

## Rollback

If Glue cannot parse the new ISO offset format in production, roll back the Lambda writer and Glue script together. Do not roll back only one side: old writer + new Glue or new writer + old Glue will break CSV parsing.

## Implementation Order

1. Task 0: real source fixtures and failing real-sample tests.
2. Task 1: helper module.
3. Task 2: NEM parser timezone.
4. Task 3: non-NEM parser timezone.
5. Task 4: Noosa parser.
6. Task 5: file writer.
7. Task 6: direct billing output.
8. Task 7: Glue/Hudi parser config.
9. Task 7A: shared Hudi staging prefix compatibility gate.
10. Task 7B: downstream UTC consumer contract validation.
11. Task 8: real-sample pipeline regression.
12. Task 9: full regression.
13. Task 10: deploy and validate new data only.
