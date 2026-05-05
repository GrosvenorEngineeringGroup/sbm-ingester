# NEM12 Timezone Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Correct SBM ingester timestamp handling so every Hudi `ts` value is a canonical UTC instant derived from the source timezone, then safely backfill affected DST-period data.

**Architecture:** Define one timestamp contract end-to-end — `ts` is written as ISO 8601 with explicit offset (e.g. `2026-03-30T20:00:00+00:00`), Glue/Spark parses it with `timestampFormat="yyyy-MM-dd'T'HH:mm:ssXXX"`, Hudi keygen uses the same input format. All parser paths must produce timezone-aware timestamps before reaching the writer; the writer fails fast on naive timestamps so no path silently falls back to the Lambda's system timezone (UTC), which would shift AEST data by 10 hours.

**Tech Stack:** Python 3.13, pandas, nemreader fork, pytest, AWS Glue/Spark CSV ingestion, Hudi (COPY_ON_WRITE), Athena.

---

## Scope And Evidence

Confirmed fixed-AEST sources (verified across DST start 2025-10-05 and DST end 2026-04-05 in S3 archive):
- `NEM12MDFF_*`, `5MINNEM12MDFF_*`, lowercase `nem12#...`, `NEM12#...`, `NEM12#SCHED_*`, `NEM12#GROSV_EG*`: DST days keep 48/96/288 intervals.
- Multiple FromParticipant codes verified: `MDYMDP`, `CNTURION`, `POWMEMDP`, `TCAUSTM`, `ACTIVMDP`. Any AEMO-compliant NEM12 file follows the same rule.
- `optima_bunnings_NMI#OPTIMA_*` and `optima_racv_NMI#OPTIMA_*` (CSV variants, `BuyerShortName,...,Date,Start Time,...` format): Apr 5 has 48 half-hour timestamps including 02:00.
- `OptimaGenerationData(...)` (CSV): Oct 5 has 48 half-hour timestamps including 02:00.
- `Meter_Data_NSW/VIC/TAS/QLD (AU)_Electricity_*` (wide-format CSV via `racv_elec_parser`): Oct 5 / Apr 5 have 48 half-hour timestamps, no duplicate or missing rows.

Confirmed local-time-with-explicit-offset source:
- Green Square ComX files (`Aquatic Centre`, `Baby Health`, `Childcare`, `Community Hall`, `Community Centre & Park`, `Community Theatre`, `Council Street Lighting`, `Creative Centre`, `GIC Building`, `Park`, `Power Quality meter Incoming Supply`, `Pulse Meter_Custom`, `Storm water take off pump`).
- Header row: `Error,UTC Offset (minutes),Local Time Stamp,...`. Oct 5 (DST start) skips `02:00-02:59` and switches offset `600 → 660` at `03:00`. Apr 5 (DST end) duplicates `02:00-02:45` four times each — first occurrence with offset `660` (AEDT before fall-back), second occurrence with offset `600` (AEST after fall-back).

Confirmed AEST-tag source:
- `RACV_Noosa_Solar_*`: timestamp string format `dd-Mmm-yy h:MM AM/PM AEST`. Queensland does not observe DST so all rows carry `AEST` year-round.

Out of scope for timestamp backfill:
- `RACV-Usage and Spend Report` (UTF-16 monthly billing, no interval data — does not enter Hudi).
- `Bunnings-Usage and Spend Report` is monthly billing data, but its parser **does** write to `hudibucketsrc/sensorDataFiles/`. The timestamps it writes are already month-aligned (`YYYY-MM-01 00:00:00`) and DST-irrelevant in value, but the format must still match the new contract or Glue will fail to parse the CSV.

Out of scope entirely:
- `envizi_vertical_parser_water`, `envizi_vertical_parser_water_bulk`, `envizi_vertical_parser_electricity`. The S3 archive (12 months sampled) contains zero files matching the `Interval_Start, Interval_End, Serial_No` columns these parsers expect. They may be dead code or a cold path. We will **not silently localize** these parsers because the source timezone convention is unverified. Instead, the writer will fail fast on naive timestamps so the path surfaces explicitly the next time it runs.

## Storage Contract Decision (Locked)

```text
Hudi `ts` column contract:
  - Type: TimestampType (epoch microseconds internally)
  - CSV string format on disk: ISO 8601 with offset, e.g. "2026-03-30T20:00:00+00:00"
  - All writers convert to UTC before formatting via datetime.astimezone(timezone.utc).isoformat(timespec="seconds")
  - Spark/Hudi reads with timestampFormat="yyyy-MM-dd'T'HH:mm:ssXXX"
  - Glue keygen partition path uses the same input.dateformat
  - `its` partition column carries the same string as `ts` so it round-trips through CustomKeyGenerator
```

This is a deliberate change from the current naive contract. Downstream consumers of the Hudi table (notably SkySpark `gegGems2DataSync`) must be coordinated separately. Task 0 confirms downstream behavior; if SkySpark hardcodes a naive-Sydney assumption, Task 0 escalates to coordinate the cutover before any writer/Glue change ships.

## Files

- Modify: `src/libs/nemreader/streaming.py` — attach fixed-AEST timezone to NEM date/datetime parsing.
- Modify: `src/libs/nemreader/nem_reader.py` — keep batch parser consistent with streaming parser.
- Create: `src/shared/timezone_utils.py` — shared TZ helpers and the `format_ts_for_hudi` writer helper.
- Modify: `src/shared/non_nem_parsers.py` — fixed-AEST localization for Optima/RACV; explicit-offset for Green Square ComX.
- Modify: `src/shared/billing_parser.py` — emit ISO 8601 offset format consistent with the writer contract.
- Modify: `src/shared/noosa_solar_parser.py` — parse source timezone suffix instead of stripping it.
- Modify: `src/functions/file_processor/app.py` — `DirectCSVWriter.write_row` formats with `format_ts_for_hudi`; raises on naive `ts`.
- Modify: `src/glue/hudi_import/script.py` — Spark CSV reader gets `timestampFormat`; Hudi keygen `input.dateformat` updated.
- Test: `tests/unit/test_timezone_utils.py` (new).
- Test: `tests/unit/test_nem12_streaming.py`.
- Test: `tests/unit/test_nem_adapter.py`.
- Test: `tests/unit/test_non_nem_parsers.py`.
- Test: `tests/unit/test_non_nem_parsers_edge_cases.py`.
- Test: `tests/unit/test_billing_csv_to_hudi.py`.
- Test: `tests/unit/test_noosa_solar_parser.py`.
- Test: `tests/unit/test_batch_s3_writes.py`.
- Test: `tests/unit/test_glue_hudi_import.py`.
- Create: `docs/runbooks/2026-04-30-nem12-timezone-backfill.md` (operational runbook for Task 9).

---

### Task 0: Confirm Downstream Contract And Lock Plan

**Files:**
- Read: `src/functions/file_processor/app.py:332-348` (current writer format).
- Read: `src/glue/hudi_import/script.py:280-355` (current Hudi config and CSV reader).
- Search: `gegGems2DataSync`, `sensordata_default` consumers.

- [ ] **Step 1: Search for downstream consumers of the Hudi `ts` column**

```bash
rg -n "sensordata_default|sensorDataFiles|gegGems2DataSync" /Users/zeyu/Desktop/GEG
```

Expected: identify whether any consumer reads `ts` as a naive Sydney wall-clock string.

- [ ] **Step 2: Query SkySpark MCP to confirm current display behavior**

Pick one Mirvac NMI (`p:mirvac:r:2555158c-cd196e71`, House MSB 2). Run an Athena query for one row at `2026-03-31 06:00` and a SkySpark `hisRead` for the same point/time. If the values match byte-for-byte (already established earlier in investigation), SkySpark is reading `ts` as naive and applying `tz=Sydney`. Document the finding in the runbook draft.

Expected: confirmed prior finding; SkySpark currently reads `ts` as naive Sydney local. Cutover to UTC instants requires coordinating SkySpark sync changes before this plan's Task 7 ships.

- [ ] **Step 3: Decide cutover sequencing**

Two options:

```text
Option A (preferred, single switch):
  1. Apply Tasks 1–6 (parsers + helpers + writer + Glue) on a feature branch.
  2. Coordinate with SkySpark team to update gegGems2DataSync to read UTC instants.
  3. Deploy parser/Glue change and SkySpark sync in the same maintenance window.
  4. Run Task 9 backfill after cutover.

Option B (compatibility shim, longer-lived):
  Have writer emit Sydney-local wall-clock derived from the parsed-aware timestamp,
  for example "2026-03-31 06:00:00+11:00" (parser produces real AEST=06:00, writer
  converts to AEDT=07:00 before write). Glue reads timestampFormat with offset.
  SkySpark sees the same wall-clock it does today.
  Drawback: still mixes wall-clock and instant semantics; DST-end fall-back row
  pairs are still indistinguishable in the wall-clock string alone.

Default: Option A. Only fall back to Option B if SkySpark coordination cannot
happen within 2 weeks.
```

Pick Option A and proceed. If Option B is required, add a sub-task to convert aware AEST timestamps to Sydney wall-clock with offset in `format_ts_for_hudi`.

- [ ] **Step 4: Commit decision to runbook**

Create `docs/runbooks/2026-04-30-nem12-timezone-backfill.md` with the chosen contract, escalation contacts (Anova for SkySpark), and the cutover order. Commit:

```bash
git add docs/runbooks/2026-04-30-nem12-timezone-backfill.md docs/superpowers/plans/2026-04-30-nem12-timezone-fix.md
git commit -m "docs: lock NEM12 timezone fix contract and cutover plan"
```

---

### Task 1: Add Shared Timezone Helpers

**Files:**
- Create: `src/shared/timezone_utils.py`.
- Test: `tests/unit/test_timezone_utils.py` (new).

- [ ] **Step 1: Create the failing test file**

```python
# tests/unit/test_timezone_utils.py
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest


def test_aest_constant_is_fixed_plus_10() -> None:
    from shared.timezone_utils import AEST

    assert AEST.utcoffset(None) == timedelta(hours=10)


def test_localize_fixed_aest_sets_plus_10_offset() -> None:
    from shared.timezone_utils import localize_fixed_aest

    series = pd.to_datetime(pd.Series(["2026-03-31 06:00:00", "2026-04-05 02:00:00"]))
    result = localize_fixed_aest(series)

    assert result.iloc[0].utcoffset().total_seconds() == 10 * 3600
    assert result.iloc[1].utcoffset().total_seconds() == 10 * 3600


def test_timestamp_from_local_and_offset_disambiguates_dst_end() -> None:
    from shared.timezone_utils import timestamp_from_local_and_offset

    local = pd.to_datetime(pd.Series(["2026-04-05 02:00:00", "2026-04-05 02:00:00"]))
    offsets = pd.Series([660, 600])

    result = timestamp_from_local_and_offset(local, offsets)

    assert result.iloc[0].utcoffset() == timedelta(0)
    assert result.iloc[1].utcoffset() == timedelta(0)
    assert result.iloc[0] != result.iloc[1]
    assert (result.iloc[1] - result.iloc[0]) == pd.Timedelta(hours=1)


def test_parse_noosa_timestamp_aest_suffix() -> None:
    from shared.timezone_utils import parse_noosa_timestamp

    result = parse_noosa_timestamp(pd.Series(["31-Mar-26 8:00 AM AEST"]))

    ts = result.iloc[0]
    assert ts.utcoffset().total_seconds() == 10 * 3600
    assert ts.strftime("%Y-%m-%d %H:%M:%S") == "2026-03-31 08:00:00"


def test_parse_noosa_timestamp_aedt_suffix() -> None:
    from shared.timezone_utils import parse_noosa_timestamp

    result = parse_noosa_timestamp(pd.Series(["31-Mar-26 8:00 AM AEDT"]))

    assert result.iloc[0].utcoffset().total_seconds() == 11 * 3600


def test_parse_noosa_timestamp_missing_suffix_warns_and_falls_back_to_aest(caplog) -> None:
    from shared.timezone_utils import parse_noosa_timestamp

    with caplog.at_level("WARNING"):
        result = parse_noosa_timestamp(pd.Series(["31-Mar-26 8:00 AM"]))

    assert result.iloc[0].utcoffset().total_seconds() == 10 * 3600
    assert any("missing tz suffix" in record.message.lower() for record in caplog.records)


def test_format_ts_for_hudi_emits_iso_offset_for_aest() -> None:
    from shared.timezone_utils import AEST, format_ts_for_hudi

    ts = datetime(2026, 3, 31, 6, 0, tzinfo=AEST)

    assert format_ts_for_hudi(ts) == "2026-03-30T20:00:00+00:00"


def test_format_ts_for_hudi_emits_iso_offset_for_utc() -> None:
    from shared.timezone_utils import format_ts_for_hudi

    ts = datetime(2026, 3, 30, 20, 0, tzinfo=timezone.utc)

    assert format_ts_for_hudi(ts) == "2026-03-30T20:00:00+00:00"


def test_format_ts_for_hudi_raises_on_naive() -> None:
    from shared.timezone_utils import format_ts_for_hudi

    ts = datetime(2026, 3, 31, 6, 0)

    with pytest.raises(ValueError, match="naive"):
        format_ts_for_hudi(ts)


def test_format_ts_for_hudi_accepts_pandas_timestamp() -> None:
    from shared.timezone_utils import AEST, format_ts_for_hudi

    ts = pd.Timestamp("2026-03-31 06:00", tz=AEST)

    assert format_ts_for_hudi(ts) == "2026-03-30T20:00:00+00:00"
```

- [ ] **Step 2: Run the failing tests**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
uv run pytest tests/unit/test_timezone_utils.py -q
```

Expected: every test fails with `ModuleNotFoundError: No module named 'shared.timezone_utils'`.

- [ ] **Step 3: Create the helper module**

```python
# src/shared/timezone_utils.py
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import pandas as pd

log = logging.getLogger(__name__)

AEST = timezone(timedelta(hours=10))
AEDT = timezone(timedelta(hours=11))

TZ_SUFFIX_OFFSETS = {
    "AEST": AEST,
    "AEDT": AEDT,
}


def localize_fixed_aest(series: pd.Series) -> pd.Series:
    """Attach fixed +10:00 (AEST, no DST) to a naive datetime Series.

    Use for sources that follow AEMO/NEM time semantics: NEM12, OptimaGenerationData,
    optima_*_NMI#OPTIMA_* CSVs, Meter_Data_*(AU)_Electricity wide-format CSVs.
    """
    parsed = pd.to_datetime(series)
    if parsed.dt.tz is not None:
        return parsed.dt.tz_convert(AEST)
    return parsed.dt.tz_localize(AEST)


def timestamp_from_local_and_offset(
    local_ts: pd.Series, offset_minutes: pd.Series
) -> pd.Series:
    """Convert a local-clock Series + per-row UTC-offset (minutes) into UTC-aware timestamps.

    Use for Schneider ComX files where each row carries a `UTC Offset (minutes)`
    column (600 = AEST, 660 = AEDT). DST-end fall-back rows share the same wall
    clock but different offsets; the returned UTC instants are distinct.
    """
    parsed_local = pd.to_datetime(local_ts, dayfirst=True)
    offsets_td = pd.to_timedelta(pd.to_numeric(offset_minutes), unit="m")
    utc_naive = parsed_local - offsets_td
    return utc_naive.dt.tz_localize(timezone.utc)


def parse_noosa_timestamp(series: pd.Series) -> pd.Series:
    """Parse Fronius/Noosa timestamps of form `31-Mar-26 8:00 AM AEST`.

    Preserves the explicit AEST/AEDT suffix as the returned tzinfo. Rows with a
    missing suffix log a warning and fall back to AEST (Queensland default). Rows
    with an unsupported suffix raise.
    """
    suffix = series.str.extract(r"\s+([A-Z]{3,4})$")[0]

    missing_count = int(suffix.isna().sum())
    if missing_count:
        log.warning(
            "Noosa timestamps missing tz suffix; defaulting to AEST",
            extra={"row_count": missing_count},
        )
        suffix = suffix.fillna("AEST")

    unknown = sorted(set(suffix.dropna()) - set(TZ_SUFFIX_OFFSETS))
    if unknown:
        raise ValueError(f"Unsupported Noosa timezone suffix: {unknown}")

    stripped = series.str.replace(r"\s+[A-Z]{3,4}$", "", regex=True)
    parsed = pd.to_datetime(stripped, format="%d-%b-%y %I:%M %p")

    aware = [
        ts.tz_localize(TZ_SUFFIX_OFFSETS[tz_name])
        for ts, tz_name in zip(parsed, suffix, strict=True)
    ]
    return pd.Series(aware, index=series.index)


def format_ts_for_hudi(ts) -> str:
    """Format a tz-aware datetime/Timestamp as ISO 8601 UTC with a `+00:00` offset.

    Output shape: `2026-03-30T20:00:00+00:00`, designed to round-trip through
    Spark `timestampFormat="yyyy-MM-dd'T'HH:mm:ssXXX"`.

    Raises on naive timestamps. The Lambda runtime is UTC; silently treating a
    naive AEST timestamp as UTC would shift data by 10 hours, so all parser
    paths must produce aware timestamps before reaching the writer.
    """
    if not hasattr(ts, "tzinfo") or ts.tzinfo is None:
        raise ValueError(
            f"format_ts_for_hudi received naive timestamp: {ts!r}. "
            "Parser must attach tzinfo before writing."
        )
    if isinstance(ts, pd.Timestamp):
        ts = ts.to_pydatetime()
    utc = ts.astimezone(timezone.utc)
    return utc.isoformat(timespec="seconds")
```

- [ ] **Step 4: Run the helper tests**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
uv run pytest tests/unit/test_timezone_utils.py -q
```

Expected: all 10 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/shared/timezone_utils.py tests/unit/test_timezone_utils.py
git commit -m "feat: add shared timezone helpers for NEM12/ComX/Noosa"
```

---

### Task 2: Update CSV Writer To Enforce Aware Timestamps

**Files:**
- Modify: `src/functions/file_processor/app.py:324-373` (`DirectCSVWriter`).
- Test: `tests/unit/test_batch_s3_writes.py` (existing) plus new file-processor unit tests.

This task lands the writer change before any parser change. After this task, every existing parser path either still works (because parsers that produce aware timestamps would write correct values — but none do yet) or fails fast with a clear error. We will not deploy this commit alone; it ships together with Tasks 3–6 in the same release.

- [ ] **Step 1: Write failing writer tests**

Append to `tests/unit/test_batch_s3_writes.py` (or create `tests/unit/test_direct_csv_writer.py` if the existing module is awkward):

```python
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest


def test_direct_csv_writer_emits_iso_offset_for_aware_aest() -> None:
    from functions.file_processor.app import DirectCSVWriter

    with ThreadPoolExecutor(max_workers=1) as executor:
        writer = DirectCSVWriter("batch", executor)
        writer.write_row(
            "p:mirvac:r:abc",
            datetime(2026, 3, 31, 6, 0, tzinfo=timezone(timedelta(hours=10))),
            1.5,
            "kwh",
            "A",
        )

    rendered = writer.buffer.getvalue()
    assert "2026-03-30T20:00:00+00:00" in rendered
    assert "p:mirvac:r:abc,2026-03-30T20:00:00+00:00,1.5,kwh,2026-03-30T20:00:00+00:00,A" in rendered


def test_direct_csv_writer_accepts_pandas_timestamp() -> None:
    from functions.file_processor.app import DirectCSVWriter
    from shared.timezone_utils import AEST

    with ThreadPoolExecutor(max_workers=1) as executor:
        writer = DirectCSVWriter("batch", executor)
        writer.write_row(
            "p:mirvac:r:abc",
            pd.Timestamp("2026-04-05 02:00", tz=AEST),
            2.0,
            "kwh",
        )

    assert "2026-04-04T16:00:00+00:00" in writer.buffer.getvalue()


def test_direct_csv_writer_rejects_naive_timestamp() -> None:
    from functions.file_processor.app import DirectCSVWriter

    with ThreadPoolExecutor(max_workers=1) as executor:
        writer = DirectCSVWriter("batch", executor)

        with pytest.raises(ValueError, match="naive"):
            writer.write_row(
                "p:mirvac:r:abc",
                datetime(2026, 3, 31, 6, 0),  # naive — must fail fast
                1.0,
                "kwh",
            )
```

- [ ] **Step 2: Run failing tests**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
uv run pytest tests/unit/test_batch_s3_writes.py -q -k "direct_csv_writer"
```

Expected: tests fail because `DirectCSVWriter.write_row` currently emits naive `%Y-%m-%d %H:%M:%S`.

- [ ] **Step 3: Update `DirectCSVWriter`**

Locate `src/functions/file_processor/app.py:324-348` and replace `TS_FORMAT` and `write_row`:

```python
from shared.timezone_utils import format_ts_for_hudi


class DirectCSVWriter:
    """Memory-efficient CSV writer that bypasses pandas DataFrame.

    Writes rows directly to a string buffer, then uploads to S3 in parallel.
    Eliminates DataFrame construction, concat, and to_csv overhead.

    Timestamp contract: every `ts` passed in MUST be timezone-aware. The
    writer formats it as ISO 8601 UTC (`2026-03-30T20:00:00+00:00`) so that
    Glue/Spark can parse it deterministically with
    `timestampFormat="yyyy-MM-dd'T'HH:mm:ssXXX"`. Naive timestamps raise
    immediately rather than silently being interpreted as the Lambda's
    system timezone (UTC), which would shift AEST data by 10 hours.
    """

    CSV_HEADER = "sensorId,ts,val,unit,its,quality\n"

    def __init__(self, batch_timestamp: str, executor: ThreadPoolExecutor) -> None:
        self.batch_timestamp = batch_timestamp
        self.executor = executor
        self.buffer = io.StringIO()
        self.buffer.write(self.CSV_HEADER)
        self.row_count = 0
        self.futures: list = []

    def write_row(self, sensor_id: str, ts: Any, val: float, unit: str, quality: str = "") -> None:
        """Write a single row to the buffer.

        Raises ValueError if ts is naive.
        """
        ts_str = format_ts_for_hudi(ts)
        self.buffer.write(f"{sensor_id},{ts_str},{val},{unit},{ts_str},{quality}\n")
        self.row_count += 1
```

Delete the old `TS_FORMAT` constant and the `ts.strftime(...)` fallback path. Keep `flush()` and `wait_for_uploads()` unchanged.

- [ ] **Step 4: Run writer tests**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
uv run pytest tests/unit/test_batch_s3_writes.py -q
```

Expected: new tests pass; pre-existing tests fail because they construct naive datetimes. We fix those in the next step.

- [ ] **Step 5: Update existing batch_s3 tests to use aware timestamps**

Search and update:

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
rg -n "datetime\([0-9]+, *[0-9]+, *[0-9]+(?:, *[0-9]+){0,3}\)" tests/unit/test_batch_s3_writes.py
```

For each match, add `tzinfo=timezone(timedelta(hours=10))`. If a test deliberately constructs a naive datetime and asserts naive output, change the assertion to expect `ValueError` instead.

Re-run:

```bash
uv run pytest tests/unit/test_batch_s3_writes.py -q
```

Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add src/functions/file_processor/app.py tests/unit/test_batch_s3_writes.py
git commit -m "feat: DirectCSVWriter emits ISO 8601 UTC, fails on naive ts"
```

---

### Task 3: Update Billing Parser To Match Writer Contract

**Files:**
- Modify: `src/shared/billing_parser.py:63-74,128-184` (`_billing_date_to_ts`, `_process_rows_and_write`).
- Test: `tests/unit/test_billing_csv_to_hudi.py`.

The Bunnings billing parser writes directly to `hudibucketsrc/sensorDataFiles/`, bypassing `DirectCSVWriter`. Once Task 2 lands, Glue will be reconfigured (Task 6) to expect ISO 8601 with offset. Billing rows must match.

- [ ] **Step 1: Write failing test**

Add to `tests/unit/test_billing_csv_to_hudi.py`:

```python
def test_billing_date_to_ts_emits_iso_offset_utc() -> None:
    from shared.billing_parser import _billing_date_to_ts

    assert _billing_date_to_ts("Mar 2026") == "2026-02-28T14:00:00+00:00"


def test_billing_date_to_ts_returns_none_for_blank() -> None:
    from shared.billing_parser import _billing_date_to_ts

    assert _billing_date_to_ts("") is None
    assert _billing_date_to_ts("   ") is None


def test_billing_date_to_ts_returns_none_for_unparseable() -> None:
    from shared.billing_parser import _billing_date_to_ts

    assert _billing_date_to_ts("not a date") is None
```

The expected value `2026-02-28T14:00:00+00:00` is the UTC equivalent of `Mar 2026 00:00 AEST` (i.e. `2026-03-01T00:00:00+10:00` → `2026-02-28T14:00:00+00:00`). Billing periods are denominated in market days, so anchor each month to AEST midnight then convert to UTC.

- [ ] **Step 2: Run failing tests**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
uv run pytest tests/unit/test_billing_csv_to_hudi.py -q -k "_billing_date_to_ts"
```

Expected: fail because current implementation returns `"2026-03-01 00:00:00"`.

- [ ] **Step 3: Update `_billing_date_to_ts`**

Replace the function in `src/shared/billing_parser.py`:

```python
from shared.timezone_utils import AEST, format_ts_for_hudi


def _billing_date_to_ts(date_str: str) -> str | None:
    """Convert 'Mmm YYYY' (e.g. 'Mar 2026') to ISO 8601 UTC string anchored to AEST midnight.

    Returns None if the string does not parse; callers skip such rows.
    The result format matches the Hudi writer contract used by DirectCSVWriter.
    """
    if not date_str or not date_str.strip():
        return None
    try:
        dt = datetime.strptime(date_str.strip(), "%b %Y")
    except ValueError:
        return None
    aware = dt.replace(tzinfo=AEST)
    return format_ts_for_hudi(aware)
```

Update the imports at the top of `billing_parser.py`:

```python
from datetime import UTC, datetime
```

becomes

```python
from datetime import UTC, datetime
```

(no change there) and add the new import:

```python
from shared.timezone_utils import AEST, format_ts_for_hudi
```

- [ ] **Step 4: Update existing billing assertions**

Search:

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
rg -n "2026-[0-9]{2}-01 00:00:00|YYYY-MM-01" tests/unit/test_billing_csv_to_hudi.py
```

For every assertion that expects the old naive format, update to the new ISO offset string. For example:

```python
# before
assert ts == "2026-03-01 00:00:00"
# after
assert ts == "2026-02-28T14:00:00+00:00"
```

- [ ] **Step 5: Run billing tests**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
uv run pytest tests/unit/test_billing_csv_to_hudi.py -q
```

Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add src/shared/billing_parser.py tests/unit/test_billing_csv_to_hudi.py
git commit -m "feat: billing parser emits ISO 8601 UTC ts to match writer"
```

---

### Task 4: Update Glue Spark/Hudi To Read New Format

**Files:**
- Modify: `src/glue/hudi_import/script.py:280-355` (`build_hudi_config`, `read_csv_batch`).
- Test: `tests/unit/test_glue_hudi_import.py`.

- [ ] **Step 1: Write failing test for Hudi config and CSV reader options**

Append to `tests/unit/test_glue_hudi_import.py`:

```python
def test_hudi_config_uses_iso_offset_dateformat() -> None:
    from glue.hudi_import.script import build_hudi_config

    config = build_hudi_config(
        hudi_db_name="default",
        hudi_table_name="sensordata",
        hudi_init_sort_option="GLOBAL_SORT",
    )

    assert config["hoodie.deltastreamer.keygen.timebased.input.dateformat"] == "yyyy-MM-dd'T'HH:mm:ssXXX"
    assert config["hoodie.deltastreamer.keygen.timebased.timezone"] == "UTC"


def test_read_csv_batch_uses_iso_offset_timestampformat(monkeypatch) -> None:
    from glue.hudi_import import script

    captured: dict = {}

    class FakeReader:
        def schema(self, schema):
            captured["schema"] = schema
            return self

        def format(self, fmt):
            captured["format"] = fmt
            return self

        def options(self, **kwargs):
            captured["options"] = kwargs
            return self

        def load(self, uris):
            captured["uris"] = uris
            return FakeDF()

    class FakeDF:
        def withColumn(self, *args, **kwargs):
            return self

    monkeypatch.setattr(script, "spark", type("S", (), {"read": FakeReader()})())

    script.read_csv_batch(["s3://x/y.csv"], schema=script.get_schema())

    assert captured["options"]["timestampFormat"] == "yyyy-MM-dd'T'HH:mm:ssXXX"
    assert captured["options"]["header"] is True
```

If the existing test module already imports `script` differently or stubs Spark with a fixture, follow that convention instead of `monkeypatch.setattr`.

- [ ] **Step 2: Run failing tests**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
uv run pytest tests/unit/test_glue_hudi_import.py -q -k "iso_offset"
```

Expected: fail.

- [ ] **Step 3: Update `build_hudi_config`**

In `src/glue/hudi_import/script.py:280-321`, change two lines:

```python
"hoodie.deltastreamer.keygen.timebased.input.dateformat": "yyyy-MM-dd'T'HH:mm:ssXXX",
```

(was `"yyyy-MM-dd H:mm:ss"`). Leave `"hoodie.deltastreamer.keygen.timebased.timezone": "UTC"` and `output.dateformat: "yyyy"` untouched.

- [ ] **Step 4: Update `read_csv_batch`**

In `src/glue/hudi_import/script.py:338-355`:

```python
def read_csv_batch(file_uris: list[str], schema: StructType) -> DataFrame:
    """Read a batch of CSV files into a DataFrame.

    Timestamp columns must be ISO 8601 with offset, e.g. `2026-03-30T20:00:00+00:00`,
    matching the writer contract in `DirectCSVWriter` / `billing_parser`.
    """
    return (
        spark.read.schema(schema)
        .format("csv")
        .options(
            header=True,
            delimiter=",",
            timestampFormat="yyyy-MM-dd'T'HH:mm:ssXXX",
        )
        .load(file_uris)
        .withColumn("ats", current_timestamp())
    )
```

- [ ] **Step 5: Run Glue tests**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
uv run pytest tests/unit/test_glue_hudi_import.py -q
```

Expected: all pass. Update any pre-existing assertions that hardcoded the old `"yyyy-MM-dd H:mm:ss"` value.

- [ ] **Step 6: Commit**

```bash
git add src/glue/hudi_import/script.py tests/unit/test_glue_hudi_import.py
git commit -m "feat: Glue/Hudi parses ISO 8601 UTC timestamp format"
```

---

### Task 5: Fix NEM12 Streaming And Batch Parsers

**Files:**
- Modify: `src/libs/nemreader/streaming.py:357-372`.
- Modify: `src/libs/nemreader/nem_reader.py:640-654`.
- Test: `tests/unit/test_nem12_streaming.py`.
- Test: `tests/unit/test_nem_adapter.py`.

NEM12 is fixed AEST per AEMO MDFF spec. Every interval date/datetime must be aware before flowing through the file processor — otherwise Task 2's writer raises.

NEM12 does **not** have DST end fall-back duplicates; the file always contains 48/96/288 monotonically increasing intervals. Tests covering DST behavior should assert "single 02:00 with +10:00 offset" — they should NOT assert duplicate `02:00` rows (that is ComX-specific behavior, covered in Task 6).

- [ ] **Step 1: Write failing tests in `test_nem12_streaming.py`**

```python
from datetime import datetime, timedelta, timezone


def test_streaming_reading_timestamps_are_fixed_aest(self, nem12_sample_file: str) -> None:
    from libs.nemreader.streaming import stream_nem12_file

    [(_, _, _, readings)] = list(stream_nem12_file(nem12_sample_file))

    expected_offset = timedelta(hours=10)
    assert readings[0].t_start.tzinfo is not None
    assert readings[0].t_start.utcoffset() == expected_offset
    assert readings[0].t_end.utcoffset() == expected_offset
    assert readings[0].t_end - readings[0].t_start == timedelta(minutes=30)


def test_dst_end_day_has_no_duplicate_intervals(tmp_path) -> None:
    """NEM12 is fixed AEST; Apr 5 has 48 monotonic 30-min intervals, no repeat."""
    from libs.nemreader.streaming import stream_nem12_file

    nem12_path = tmp_path / "dst_end.csv"
    intervals = ",".join(["1.0"] * 48)
    nem12_path.write_text(
        "100,NEM12,202604060000,MDYMDP,GROUP,,,,,\n"
        "200,4103639192,E1Q1,,E1,,219076864,kWh,30,\n"
        f"300,20260405,{intervals},A,,,,\n"
        "900\n"
    )

    [(_, _, _, readings)] = list(stream_nem12_file(str(nem12_path)))

    assert len(readings) == 48
    starts = [r.t_start for r in readings]
    assert starts == sorted(starts)
    assert all(r.t_start.utcoffset() == timedelta(hours=10) for r in readings)
    assert len({r.t_start for r in readings}) == 48
```

- [ ] **Step 2: Run failing tests**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
uv run pytest tests/unit/test_nem12_streaming.py -q -k "fixed_aest or dst_end_day_has_no_duplicate"
```

Expected: fail because timestamps are naive.

- [ ] **Step 3: Update `streaming._parse_datetime`**

In `src/libs/nemreader/streaming.py`, top of file:

```python
from shared.timezone_utils import AEST
```

Replace lines 357-372:

```python
def _parse_datetime(record: str | None) -> datetime | None:
    """Parse NEM datetime string (Date8, DateTime12, DateTime14) as fixed AEST.

    Per AEMO MDFF NEM12/NEM13 spec, all NEM12 timestamps are AEST (UTC+10) year-round
    with no DST adjustment.
    """
    if not record:
        return None

    record = record.strip()
    format_strings = {
        8: "%Y%m%d",
        12: "%Y%m%d%H%M",
        14: "%Y%m%d%H%M%S",
    }

    try:
        return datetime.strptime(record, format_strings[len(record)]).replace(tzinfo=AEST)
    except (ValueError, KeyError):
        return None
```

- [ ] **Step 4: Update `nem_reader.parse_datetime`**

In `src/libs/nemreader/nem_reader.py`, top of file:

```python
from shared.timezone_utils import AEST
```

Replace lines 640-654:

```python
def parse_datetime(record: str) -> datetime | None:
    """Parse a datetime string into a python datetime object (fixed AEST per AEMO spec)."""
    format_strings = {8: "%Y%m%d", 12: "%Y%m%d%H%M", 14: "%Y%m%d%H%M%S"}

    if record == "" or record is None:
        return None

    try:
        timestamp = datetime.strptime(record.strip(), format_strings[len(record.strip())])
    except (ValueError, KeyError):
        log.debug(f"Malformed date '{record}' ")
        return None

    return timestamp.replace(tzinfo=AEST)
```

- [ ] **Step 5: Update existing naive-datetime assertions**

Search:

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
rg -n "datetime\([0-9]+, *[0-9]+, *[0-9]+(?:, *[0-9]+){0,3}\)" tests/unit/test_nem12_streaming.py tests/unit/test_nem_adapter.py
```

For each match in tests that compare against parsed NEM12 timestamps, append `tzinfo=timezone(timedelta(hours=10))`. Example:

```python
# before
assert readings[0].t_start == datetime(2004, 2, 1, 0, 0, 0)
# after
assert readings[0].t_start == datetime(2004, 2, 1, 0, 0, 0, tzinfo=timezone(timedelta(hours=10)))
```

- [ ] **Step 6: Run NEM12 tests**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
uv run pytest tests/unit/test_nem12_streaming.py tests/unit/test_nem_adapter.py -q
```

Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add src/libs/nemreader/streaming.py src/libs/nemreader/nem_reader.py tests/unit/test_nem12_streaming.py tests/unit/test_nem_adapter.py
git commit -m "feat: NEM12 parser produces tz-aware AEST timestamps"
```

---

### Task 6: Fix Optima/RACV And ComX Parsers

**Files:**
- Modify: `src/shared/non_nem_parsers.py:103-184`.
- Test: `tests/unit/test_non_nem_parsers.py`.
- Test: `tests/unit/test_non_nem_parsers_edge_cases.py`.

Optima/RACV/Meter_Data parsers all use the `Date + Start Time` pattern and follow fixed AEST. ComX uses the explicit `UTC Offset (minutes)` column.

- [ ] **Step 1: Write failing tests**

Append to `tests/unit/test_non_nem_parsers.py`:

```python
from datetime import timedelta
from pathlib import Path

import pandas as pd


def test_optima_parser_localizes_to_fixed_aest(temp_directory: str) -> None:
    from shared.non_nem_parsers import optima_parser

    filepath = str(Path(temp_directory) / "OptimaGenerationData.csv")
    pd.DataFrame(
        {
            "BuyerShortName": ["RACV"],
            "Country": ["AU"],
            "Commodity": ["Electricity"],
            "Identifier": ["8000282622"],
            "IdentifierType": ["NMI"],
            "DistributorId": ["AURORAP"],
            "Date": ["05-Oct-2025"],
            "Start Time": ["02:00"],
            "Usage": [1.0],
            "Generation": [0.0],
        }
    ).to_csv(filepath, index=False)

    [(_, df)] = optima_parser(filepath, "error")

    assert df.index[0].utcoffset() == timedelta(hours=10)


def test_racv_elec_parser_localizes_to_fixed_aest(temp_directory: str) -> None:
    from shared.non_nem_parsers import racv_elec_parser

    filepath = str(Path(temp_directory) / "racv_data.csv")
    Path(filepath).write_text(
        "Header Row 1\n"
        "Header Row 2\n"
        "Date,Start Time,Meter1 (kWh)\n"
        "05-Oct-2025,02:00,10.0\n"
    )

    [(_, df)] = racv_elec_parser(filepath, "error")

    assert df.index[0].utcoffset() == timedelta(hours=10)


def test_comx_uses_utc_offset_to_disambiguate_dst_end(temp_directory: str) -> None:
    from shared.non_nem_parsers import green_square_private_wire_schneider_comx_parser

    filepath = str(Path(temp_directory) / "comx_dst_end.csv")
    Path(filepath).write_text(
        "Gateway Name,Gateway SN,Gateway IP Address,Gateway MAC Address,Device Name,Device Local ID,Device Type ID,Device Type Name,Logging Interval,Historical Intervals\n"
        "ComX510_Green_Square,DN20350SE000008,192.168.1.5,00:80:67:FA:AD:5C,Storm water take off pump,6,10000,Compact NSX E,15,100\n"
        "\n"
        ",,,Topic ID1\n"
        ",,,1601\n"
        "\n"
        "Error,UTC Offset (minutes),Local Time Stamp,Active energy (kWh)\n"
        "0,660,05-04-2026 02:00:00,1.0\n"
        "0,600,05-04-2026 02:00:00,2.0\n"
    )

    [(_, df)] = green_square_private_wire_schneider_comx_parser(filepath, "error")

    assert len(df.index) == 2
    assert df.index[0] != df.index[1]
    assert df.index[0].utcoffset() == timedelta(0)
    assert (df.index[1] - df.index[0]) == pd.Timedelta(hours=1)
```

- [ ] **Step 2: Run failing tests**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
uv run pytest tests/unit/test_non_nem_parsers.py -q -k "fixed_aest or comx_uses_utc_offset"
```

Expected: fail (timestamps naive; ComX rows collide).

- [ ] **Step 3: Localize Optima and RACV parsers to fixed AEST**

In `src/shared/non_nem_parsers.py`, add at the top:

```python
from shared.timezone_utils import localize_fixed_aest, timestamp_from_local_and_offset
```

Update the relevant lines:

```python
# racv_elec_parser, replace line 111
raw_df["Interval_Start"] = localize_fixed_aest(
    pd.to_datetime(raw_df["Date"] + " " + raw_df["Start Time"])
)

# optima_parser, replace line 133
raw_df["Interval_Start"] = localize_fixed_aest(
    pd.to_datetime(raw_df["Date"] + " " + raw_df["Start Time"])
)
```

Do **not** modify `envizi_vertical_parser_water`, `envizi_vertical_parser_water_bulk`, `envizi_vertical_parser_electricity`. They are unverified and out of scope. The writer fail-fast in Task 2 will surface them on the next real run.

- [ ] **Step 4: Replace ComX local-time parse with offset-aware parse**

In `green_square_private_wire_schneider_comx_parser` (lines 158-184), replace:

```python
raw_df["Local Time Stamp"] = pd.to_datetime(raw_df["Local Time Stamp"], dayfirst=True)
```

with:

```python
raw_df["Local Time Stamp"] = timestamp_from_local_and_offset(
    raw_df["Local Time Stamp"],
    raw_df["UTC Offset (minutes)"],
)
```

The downstream rename to `t_start` and `set_index("t_start")` stays unchanged. Two distinct rows on Apr 5 02:00 (offsets 660 and 600) now become two distinct UTC instants in the DataFrame index.

- [ ] **Step 5: Run non-NEM parser tests**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
uv run pytest tests/unit/test_non_nem_parsers.py tests/unit/test_non_nem_parsers_edge_cases.py -q
```

Expected: all pass. Pre-existing tests that asserted naive `Interval_Start` values must be updated to expect aware AEST values.

- [ ] **Step 6: Commit**

```bash
git add src/shared/non_nem_parsers.py tests/unit/test_non_nem_parsers.py tests/unit/test_non_nem_parsers_edge_cases.py
git commit -m "feat: Optima/RACV use fixed AEST; ComX uses explicit UTC offset"
```

---

### Task 7: Fix Noosa Solar Suffix Handling

**Files:**
- Modify: `src/shared/noosa_solar_parser.py:40-50`.
- Test: `tests/unit/test_noosa_solar_parser.py`.

- [ ] **Step 1: Update Noosa tests to expect aware AEST timestamps**

In `tests/unit/test_noosa_solar_parser.py`, find `TestTimestampParsing.test_timestamp_parsing` and assert:

```python
ts = df.index[0]
assert ts.utcoffset().total_seconds() == 10 * 3600
assert ts.strftime("%Y-%m-%d %H:%M:%S") == "2026-03-31 08:00:00"
```

Find `TestTimezoneWarning.test_timezone_warning` and update so the AEDT row produces a `+11:00` aware timestamp:

```python
[(_, df)] = result
assert df.index[0].utcoffset().total_seconds() == 11 * 3600
```

- [ ] **Step 2: Run failing tests**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
uv run pytest tests/unit/test_noosa_solar_parser.py -q -k "test_timestamp_parsing or test_timezone_warning"
```

Expected: fail.

- [ ] **Step 3: Update parser to use `parse_noosa_timestamp`**

In `src/shared/noosa_solar_parser.py`, replace lines 40-50:

```python
from shared.timezone_utils import parse_noosa_timestamp


# inside noosa_solar_parser(...)
tz_values = (
    df["timestamp"].dropna().str.extract(r"\s+([A-Z]{3,4})$")[0].dropna().unique()
)
unexpected_tz = [tz for tz in tz_values if tz != "AEST"]
if len(unexpected_tz) > 0:
    logger.warning(
        "Unexpected timezone in Noosa Solar file",
        extra={"timezones": unexpected_tz},
    )

df["timestamp"] = parse_noosa_timestamp(df["timestamp"])
```

The helper preserves `AEST`/`AEDT` per row; rows missing a suffix log a warning and fall back to AEST (Queensland default).

- [ ] **Step 4: Run Noosa tests**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
uv run pytest tests/unit/test_noosa_solar_parser.py -q
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/shared/noosa_solar_parser.py tests/unit/test_noosa_solar_parser.py
git commit -m "feat: Noosa Solar preserves AEST/AEDT suffix as tzinfo"
```

---

### Task 8: Full Regression And Sample Validation

**Files:**
- No source edits unless tests expose failures.

- [ ] **Step 1: Bulk sweep for any remaining naive datetime assertions**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
rg -n "datetime\([0-9]+, *[0-9]+, *[0-9]+(?:, *[0-9]+){0,3}\)" tests/unit | rg -v "tzinfo"
```

Review every match. If the assertion compares against a parsed source timestamp, it must include `tzinfo=`. If the test explicitly constructs a naive value to feed the writer, the test should expect a `ValueError` from `format_ts_for_hudi`.

- [ ] **Step 2: Run targeted unit tests**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
uv run pytest \
  tests/unit/test_timezone_utils.py \
  tests/unit/test_nem12_streaming.py \
  tests/unit/test_nem_adapter.py \
  tests/unit/test_non_nem_parsers.py \
  tests/unit/test_non_nem_parsers_edge_cases.py \
  tests/unit/test_noosa_solar_parser.py \
  tests/unit/test_billing_csv_to_hudi.py \
  tests/unit/test_batch_s3_writes.py \
  tests/unit/test_glue_hudi_import.py \
  -q
```

Expected: all pass.

- [ ] **Step 3: Run full test suite**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
uv run pytest -q
```

Expected: all 525+ tests pass.

- [ ] **Step 4: Run lint**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
uv run ruff check .
```

Expected: clean.

- [ ] **Step 5: Validate against real S3 source samples**

Pre-stage representative samples (the investigation already downloaded several to `/tmp/parser_samples/`):

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
PYTHONPATH=src uv run python - <<'PY'
from datetime import timedelta
from pathlib import Path

from shared.nem_adapter import stream_as_data_frames
from shared.non_nem_parsers import get_non_nem_df

samples = [
    ("/tmp/parser_samples/comx_apr5.csv", "comx-dst-end"),
    ("/tmp/nem12_dst.csv", "nem12-mar30-mirvac"),
    ("/tmp/parser_samples/optbun.csv", "optima-bunnings-apr5"),
    ("/tmp/parser_samples/racv_apr3.csv", "racv-elec-apr5"),
]

for path_str, label in samples:
    path = Path(path_str)
    if not path.exists():
        print(f"SKIP {label}: file not present")
        continue
    try:
        result = stream_as_data_frames(str(path))
        result = list(result)
    except Exception:
        result = get_non_nem_df(str(path), "/tmp/error.csv")
    name, df = result[0]
    first = df.index[0]
    print(f"{label}: name={name} first_ts={first} tz={first.tzinfo}")
    assert first.tzinfo is not None, f"{label} produced naive timestamp"
PY
```

Expected: every sample prints a tz-aware timestamp; the assertion never fires.

- [ ] **Step 6: Commit any test fixes uncovered above**

```bash
git add tests/
git commit -m "test: align legacy assertions with tz-aware contract"
```

---

### Task 9: Backfill Strategy And Runbook

**Files:**
- Modify: `docs/runbooks/2026-04-30-nem12-timezone-backfill.md` (started in Task 0).

This task produces the operational plan; it does not execute the backfill. Execution happens after the code change is deployed and SkySpark sync coordination is complete.

- [ ] **Step 1: Document why simple upsert does not heal old rows**

Add to the runbook:

```markdown
## Why Reprocessing Alone Is Insufficient

Hudi `recordkey.field = "sensorId, ts"`. After this fix:

- Old (broken) row in Hudi: `("p:mirvac:r:abc", "2026-03-31 06:00:00")` — a naive
  string that Hudi/Spark interpreted as Sydney local. During DST, this row
  represents the BMS event that physically happened at 7am AEDT.
- New (correct) row produced by reprocessing the same source NEM12 line:
  `("p:mirvac:r:abc", "2026-03-30T20:00:00+00:00")` — a UTC instant.

These are different record keys. Hudi upsert WILL NOT merge them. The new row
is inserted; the broken row remains. Without explicit cleanup, every
reprocessed DST row creates a duplicate.

Conclusion: backfill is a two-phase operation — delete old rows in the
affected windows, then reprocess source files.
```

- [ ] **Step 2: Define the affected window per source class**

```markdown
## Affected Windows

For the AU DST-aware sources, the broken rows are those where `ts` falls inside
an Australian DST period:

- 2024–2025 DST: 2024-10-06 03:00 AEDT → 2025-04-06 03:00 AEDT.
- 2025–2026 DST: 2025-10-05 03:00 AEDT → 2026-04-05 03:00 AEDT.
- Earlier DST seasons since each NMI was first ingested.

Per-source backfill scope:

- NEM12 + Optima/RACV CSV: every NMI ingested via the SBM pipeline.
- ComX (Green Square): every sensor in the affected files; both DST start
  (lost-hour) and DST end (duplicated 02:00) cases need reprocessing.
- Noosa Solar: Queensland — no DST shift in the data, but rows are still naive
  in Hudi today and must be reprocessed to gain `+10:00` offset.
- Bunnings billing: monthly anchor timestamps shift from naive
  `YYYY-MM-01 00:00:00` to a UTC anchor like `2026-02-28T14:00:00+00:00`.
  This is a key change for every billing row. Reprocess all of them.
```

- [ ] **Step 3: Choose a deletion mechanism**

```markdown
## Deletion Mechanism

Hudi 0.14+ on Glue supports `DELETE` operations against COPY_ON_WRITE tables.
Two viable approaches:

1. Predicate delete via Spark SQL:
       DELETE FROM sensordata_default
       WHERE its < '2026-04-05T03:00:00+00:00'  -- end of last DST period
         AND ts NOT LIKE '%T%'                  -- old naive format only
   The `NOT LIKE '%T%'` guard ensures we only target rows written before the
   cutover. Validate the count with a SELECT first.

2. Partition-level rebuild: use Glue to overwrite the affected `its=` year
   partitions. Faster for full-year reprocessing but riskier if any post-cutover
   rows already landed in the same partition.

Default: predicate delete with the `T` guard. Run on a non-prod copy of the
table first.
```

- [ ] **Step 4: Document the reprocessing pipeline**

```markdown
## Reprocessing Source Files

1. List affected archived source files under `s3://sbm-file-ingester/newP/archived/`
   for the DST windows.
2. For each batch (sized to fit Lambda concurrency / Glue runtime):
   a. Copy file from `newP/archived/<W>/` → `newTBP/` to re-trigger ingestion
      via the existing SQS notification path.
   b. Delete the corresponding row in `sbm-ingester-idempotency` DynamoDB so
      `idempotent_function` does not skip the file (the table has TTL but a
      recent reprocess might still be cached).
   c. Wait for `sbm-files-ingester` to drain, confirm the file moved to
      `newP/`, and confirm new CSV rows landed in `hudibucketsrc/sensorDataFiles/`.
3. Trigger `DataImportIntoLake` Glue job manually to flush all reprocessed CSVs
   into Hudi.
4. After Glue completes, run the predicate delete from Step 3 to remove the
   superseded naive rows.
5. Spot-check: re-run the Mirvac 123 Pitt St GEMS-vs-BMS comparison around
   2026-03-31 morning. The 6am surge should now appear at 7am AEDT.
```

- [ ] **Step 5: Document explicit non-actions**

```markdown
## What We Will Not Do

- We will not run a blanket `UPDATE ... SET ts = ts + INTERVAL '1' HOUR` on
  the Hudi table. DST start days lost an hour, not gained one; ComX rows are
  already correct in real time; billing rows do not need a wall-clock shift.
- We will not deprecate the existing `sensorid + ts` record key.
- We will not delete archived source files from S3 during backfill — the
  archive remains the source of truth.
```

- [ ] **Step 6: Commit runbook**

```bash
git add docs/runbooks/2026-04-30-nem12-timezone-backfill.md
git commit -m "docs: backfill runbook for NEM12 timezone fix"
```

---

## Self-Review

- **Spec coverage:** Each correction the user identified maps to a task:
  - Backfill cannot rely on upsert → Task 9 Steps 1, 3, 4 (delete + reprocess).
  - Billing parser must change format → Task 3 (full task).
  - `%z` vs `XXX` mismatch → `format_ts_for_hudi` (Task 1) + Glue config (Task 4) both use `XXX` and `isoformat(timespec="seconds")`.
  - Glue CSV `timestampFormat` must be added → Task 4 Step 4.
  - Envizi must be aware OR fail-fast → Task 2 Step 3 (writer raises) + Task 6 Step 3 (Envizi explicitly out of scope).
  - NEM12 has no DST end repeat → Task 5 Step 1 explicitly tests "48 monotonic, no duplicate".

- **Execution order:** helpers → writer → billing → Glue → NEM12 → Optima/RACV/ComX → Noosa → regression → backfill. Writer contract lands before any parser change so each parser commit is independently verifiable end-to-end.

- **Cutover risk:** Task 0 Step 3 explicitly addresses SkySpark coordination. If Option B (compatibility shim) is required, the only file affected is `format_ts_for_hudi` in Task 1 (one-file change) and the corresponding Glue/billing format in Tasks 3–4.

- **Out-of-scope clarity:** Envizi vertical parsers and `RACV-Usage and Spend Report` (UTF-16 monthly billing routed to a separate S3 bucket, not Hudi) are explicitly excluded with reasoning.

- **Type consistency check:** `format_ts_for_hudi` accepts `datetime` and `pd.Timestamp`; both Task 2 and Task 3 (billing) use it; Task 1 tests both inputs.
