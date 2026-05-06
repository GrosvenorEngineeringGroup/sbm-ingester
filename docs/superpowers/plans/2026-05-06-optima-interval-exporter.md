# Optima Interval Exporter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a new `optima-interval-exporter` Lambda that downloads ZIP-wrapped interval CSVs from BidEnergy's `POST /BuyerReport/exportdailyusagecsv` endpoint and uploads them to S3, while disabling the old NEM12 schedules and fixing the existing `interval_parser` to gracefully handle the empty-data sentinel.

**Architecture:** New Lambda mirrors `optima-demand-exporter` (same package layout, same shared `optima_shared/` modules, same packaging into `optima_exporter.zip`). The existing `interval_parser` (already wired into the dispatcher and `file_processor` channel-mapping) needs only a 4-line fix to short-circuit on the 148-byte "No data is available" sentinel CSV that BidEnergy returns when a site has no data for the requested range. Cutover swaps the two daily 14:00 Sydney schedules (nem12 → interval); NEM12 Lambda code/IAM/log group/alarm remain intact for fallback use.

**Tech Stack:** Python 3.13, uv (deps), `requests` (HTTP), `pandas` (CSV→DataFrame), `boto3` (S3 + DynamoDB), `aws_lambda_powertools.Logger`, `pytest` + `responses` + `moto` (testing), Terraform (infra), GitHub Actions (deploy).

**Spec reference:** `docs/superpowers/specs/2026-05-06-optima-interval-exporter-design.md` (commit `86ab1bf`).

**Verified truth (do not re-verify during implementation):**
- BidEnergy CSV uses CRLF line endings, no BOM, double-quoted strings.
- AU `IdentifierType=NMI` (numeric), NZ `IdentifierType=ICP` (alphanumeric); `interval_parser` reads `Identifier` column directly.
- Empty-data response is a ZIP wrapping a 148-byte CSV with literal `No data is available\r\n` body. **Truly empty (EOCD-only) ZIPs were never observed** in 8 production samples.
- Pandas auto-inference of `DD MMM YYYY` works correctly across all 12 months — no `format=` argument needed.
- Neptune mappings 100% present for 64/64 NZ Bunnings sites (no NZ-cutover risk).

---

## File Structure

**New files:**
```
src/functions/optima_exporter/interval_exporter/
├── __init__.py            # empty package marker
├── app.py                 # Lambda handler — pass-through to processor.process_export
├── downloader.py          # POST /exportdailyusagecsv, ZIP magic validation, ZIP→CSV extraction
├── processor.py           # per-site orchestration (ThreadPoolExecutor, 20 workers)
└── uploader.py            # S3 PUT (verbatim copy from demand_exporter, only logger renamed)

tests/unit/optima_exporter/interval_exporter/
├── __init__.py            # empty
├── test_app.py            # 2 tests — handler routing
├── test_downloader.py     # ~20 tests — POST contract, ZIP magic, error paths
├── test_processor.py      # ~12 tests — orchestration, partial failure, 207 status
└── test_uploader.py       # 4 tests — copied from demand_exporter
```

**Modified files:**
```
src/shared/parsers/optima/interval.py         # +4 lines: empty-data sentinel short-circuit
tests/unit/parsers/optima/test_interval.py    # add TestIntervalParserOnRealFixtures (4 tests)
.github/workflows/main.yml                    # 1 cp -r line + 1 update-function-code block
terraform/optima_exporter.tf                  # remove 5 moved blocks; add 5 new resources;
                                              # comment-out 2 nem12 schedules; +1 ARN to scheduler IAM
sbm-ingester/CLAUDE.md                        # update Lambda table + CI/CD policy whitelist
```

**Already in place (no changes needed):**
- `tests/unit/fixtures/optima_interval/` (4 real CSV fixtures — committed in `86ab1bf`)
- `src/shared/non_nem_parsers.py` — `interval_parser` already imported + registered
- `src/functions/file_processor/app.py:457-475` — channel mapping already handles `E1_kWh`/`B1_kWh`
- `src/functions/optima_exporter/optima_shared/{auth,config,dynamodb}.py` — reused unchanged

---

## Task Sequencing & Commit Strategy

The 13 tasks below are ordered so each commit can stand on its own. Tasks 1-2 ship the parser fix + fixture tests as a first PR-able chunk (the parser bug crashes ~25% of daily site files today, so this is independently valuable). Tasks 3-8 build the new Lambda with TDD. Tasks 9-11 do infra wiring (Task 10 split into 10a/10b around the `terraform apply` user gate). Task 12 pushes + verifies.

**Branch:** main (per user's standing workflow — push triggers GitHub Actions deploy).

**Push gate:** Tasks 9, 10a, 10b, 11 each create commits but **NONE** of them push. The single `git push` happens in Task 12, after all four prerequisites are confirmed in place. This avoids deploying a Lambda whose function name is missing from the IAM whitelist (would 403) or whose code references a function that doesn't yet exist (would 404).

---

### Task 1: Fix `interval_parser` empty-data sentinel crash

**Files:**
- Modify: `src/shared/parsers/optima/interval.py`
- Test: `tests/unit/parsers/optima/test_interval.py`

**Why first:** The bug crashes ~25% of daily site responses today (NZ sites with no data return the sentinel CSV). Fix is small, isolated, and independently shippable.

- [ ] **Step 1: Write the failing test**

Append to `tests/unit/parsers/optima/test_interval.py`:

```python
def test_returns_empty_list_for_no_data_sentinel(self, temp_directory: str) -> None:
    """BidEnergy returns 148-byte 'No data is available' CSV when site has no data
    for the requested range. Parser must return [] (not raise UFuncTypeError)."""
    from pathlib import Path

    from shared.parsers.optima.interval import interval_parser

    sentinel_csv = (
        b"BuyerShortName,Country,Commodity,Identifier,IdentifierType,DistributorId,"
        b"Date,Start Time,Usage,Generation,DemandKva,Reactive\r\n"
        b"No data is available\r\n"
    )
    filepath = Path(temp_directory) / "empty.csv"
    filepath.write_bytes(sentinel_csv)

    result = interval_parser(str(filepath), "error_log")

    assert result == []
```

(Place inside the existing `class TestIntervalParser` — find the class definition with `grep -n "class TestIntervalParser" tests/unit/parsers/optima/test_interval.py`. The `temp_directory` fixture comes from existing `conftest.py`.)

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/unit/parsers/optima/test_interval.py::TestIntervalParser::test_returns_empty_list_for_no_data_sentinel -v
```

Expected: FAIL with `UFuncTypeError: ufunc 'add' did not contain a loop with signature matching types (dtype('float64'), dtype('<U1')) -> None`

- [ ] **Step 3: Apply the parser fix**

In `src/shared/parsers/optima/interval.py`, replace the body of `interval_parser` so it reads:

```python
def interval_parser(file_name: str, error_file_path: str) -> ParserResult:
    raw_df = pd.read_csv(file_name)

    # BidEnergy returns a 148-byte sentinel CSV when a site has no data for the
    # requested range. Pandas reads "No data is available" as a single row with
    # NaN-typed Date/Start Time columns, which would crash the str+str datetime
    # concat below with UFuncTypeError. Detect and short-circuit to [].
    if len(raw_df) == 1 and raw_df["Date"].isna().all():
        logger.info("interval_no_data_sentinel", extra={"file": file_name})
        return []

    raw_df["Interval_Start"] = pd.to_datetime(raw_df["Date"] + " " + raw_df["Start Time"])
    raw_df["Identifier"] = raw_df["Identifier"].astype(str)

    dfs: ParserResult = []
    for name in sorted(raw_df["Identifier"].unique()):
        base_df = raw_df.loc[raw_df["Identifier"] == name].copy()

        # Build output DataFrame with t_start as index
        output_df = base_df[["Interval_Start"]].copy()
        output_df = output_df.rename(columns={"Interval_Start": "t_start"})

        # Add Usage column as E1_kWh if present
        if "Usage" in raw_df.columns:
            output_df["E1_kWh"] = base_df["Usage"].values

        # Add Generation column as B1_kWh if present
        if "Generation" in raw_df.columns:
            output_df["B1_kWh"] = base_df["Generation"].values

        output_df = output_df.set_index("t_start")
        dfs.append((f"Optima_{name}", output_df))

    return dfs
```

Only the 5-line `if len(raw_df) == 1 and raw_df["Date"].isna().all(): ...` block is new; the rest is unchanged. Do not add `format=` to `pd.to_datetime` — verified unnecessary.

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/unit/parsers/optima/test_interval.py -v
```

Expected: All tests pass (existing tests untouched + new sentinel test green).

- [ ] **Step 5: Run full parser test suite to confirm no regression**

```bash
uv run pytest tests/unit/parsers/ -v
```

Expected: All pass.

- [ ] **Step 6: Commit**

```bash
git add src/shared/parsers/optima/interval.py tests/unit/parsers/optima/test_interval.py
git commit -m "fix: handle BidEnergy 'No data is available' sentinel in interval_parser

Parser previously crashed with UFuncTypeError when BidEnergy returned the
148-byte 'No data is available' CSV (observed in ~25% of daily site
responses). Detect and short-circuit to []."
```

---

### Task 2: Add fixture-driven regression tests for `interval_parser`

**Files:**
- Modify: `tests/unit/parsers/optima/test_interval.py`

**Why:** Lock in correctness against 4 real BidEnergy samples already committed under `tests/unit/fixtures/optima_interval/`. Catches future regressions in date parsing, NZ ICP handling, multi-month spans, and the empty sentinel.

- [ ] **Step 1: Add the fixture-driven test class**

Append to the end of `tests/unit/parsers/optima/test_interval.py`:

```python
class TestIntervalParserOnRealFixtures:
    """Regression tests using verbatim BidEnergy responses (committed at 86ab1bf).

    These fixtures lock in real-world quirks that synthetic data would miss:
    CRLF line endings, double-quoted columns, NZ alphanumeric ICP identifiers,
    and the empty-data sentinel CSV.
    """

    FIXTURE_DIR = (
        Path(__file__).parent.parent.parent
        / "fixtures"
        / "optima_interval"
    )

    def test_au_single_day_parses_to_48_intervals(self) -> None:
        from shared.parsers.optima.interval import interval_parser

        path = str(self.FIXTURE_DIR / "interval_au_single_day.csv")
        result = interval_parser(path, "error_log")

        assert len(result) == 1
        sensor_id, df = result[0]
        assert sensor_id == "Optima_2002105104"
        assert list(df.columns) == ["E1_kWh", "B1_kWh"]
        assert len(df) == 48  # 30-min intervals × 24 h
        assert df.index.min() == pd.Timestamp("2025-05-01 00:00:00")

    def test_nz_icp_alphanumeric_identifier(self) -> None:
        """NZ uses alphanumeric ICP — parser must not assume numeric NMI."""
        from shared.parsers.optima.interval import interval_parser

        path = str(self.FIXTURE_DIR / "interval_nz_single_day.csv")
        result = interval_parser(path, "error_log")

        assert len(result) == 1
        sensor_id, df = result[0]
        assert sensor_id == "Optima_0000010008MQCB6"
        assert len(df) == 48

    def test_au_four_months_spans_distinct_months(self) -> None:
        """5856 rows spanning Apr→Jul. Catches any future date-format regression."""
        from shared.parsers.optima.interval import interval_parser

        path = str(self.FIXTURE_DIR / "interval_au_4month.csv")
        result = interval_parser(path, "error_log")

        sensor_id, df = result[0]
        assert sensor_id == "Optima_2002105104"
        assert len(df) > 5000
        assert sorted(df.index.month.unique().tolist()) == [4, 5, 6, 7]

    def test_empty_data_fixture_returns_empty_list(self) -> None:
        from shared.parsers.optima.interval import interval_parser

        path = str(self.FIXTURE_DIR / "interval_empty.csv")
        result = interval_parser(path, "error_log")

        assert result == []
```

Imports needed at top of file (add only if missing — check first with `head -20 tests/unit/parsers/optima/test_interval.py`):

```python
from pathlib import Path

import pandas as pd
```

- [ ] **Step 2: Run the new tests**

```bash
uv run pytest tests/unit/parsers/optima/test_interval.py::TestIntervalParserOnRealFixtures -v
```

Expected: All 4 tests pass.

- [ ] **Step 3: Run full parser suite for regression check**

```bash
uv run pytest tests/unit/parsers/ -v
```

Expected: All pass.

- [ ] **Step 4: Commit**

```bash
git add tests/unit/parsers/optima/test_interval.py
git commit -m "test: add fixture-driven regression tests for interval_parser

Lock in correctness against 4 real BidEnergy samples (AU NMI, NZ ICP,
4-month span, empty-data sentinel) committed at 86ab1bf."
```

---

### Task 3: Create `interval_exporter` package skeleton + `uploader.py`

**Files:**
- Create: `src/functions/optima_exporter/interval_exporter/__init__.py`
- Create: `src/functions/optima_exporter/interval_exporter/uploader.py`
- Create: `tests/unit/optima_exporter/interval_exporter/__init__.py`
- Create: `tests/unit/optima_exporter/interval_exporter/test_uploader.py`

**Why:** Uploader is a verbatim copy from `demand_exporter` (only logger service name differs). Trivial to ship first; gives the new package a foundation.

- [ ] **Step 1: Create empty package markers**

```bash
mkdir -p src/functions/optima_exporter/interval_exporter tests/unit/optima_exporter/interval_exporter
touch src/functions/optima_exporter/interval_exporter/__init__.py
touch tests/unit/optima_exporter/interval_exporter/__init__.py
```

- [ ] **Step 2: Write the failing uploader tests**

Create `tests/unit/optima_exporter/interval_exporter/test_uploader.py`:

```python
"""Unit tests for interval_exporter/uploader.py — verbatim copy of demand_exporter tests."""

from unittest.mock import MagicMock, patch


class TestUploadToS3:
    @patch("interval_exporter.uploader.get_s3_client")
    def test_uploads_csv_with_correct_key(self, mock_get_client: MagicMock) -> None:
        from interval_exporter.uploader import upload_to_s3

        mock_s3 = MagicMock()
        mock_get_client.return_value = mock_s3

        result = upload_to_s3(b"col1,col2\n1,2\n", "test.csv")

        assert result is True
        mock_s3.put_object.assert_called_once()
        call_kwargs = mock_s3.put_object.call_args.kwargs
        assert call_kwargs["Bucket"] == "sbm-file-ingester"
        assert call_kwargs["Key"] == "newTBP/test.csv"
        assert call_kwargs["Body"] == b"col1,col2\n1,2\n"
        assert call_kwargs["ContentType"] == "text/csv"

    @patch("interval_exporter.uploader.get_s3_client")
    def test_returns_false_on_s3_error(self, mock_get_client: MagicMock) -> None:
        from interval_exporter.uploader import upload_to_s3

        mock_s3 = MagicMock()
        mock_s3.put_object.side_effect = Exception("S3 unreachable")
        mock_get_client.return_value = mock_s3

        result = upload_to_s3(b"x", "fail.csv")

        assert result is False

    @patch("interval_exporter.uploader.get_s3_client")
    def test_custom_bucket_and_prefix_override(self, mock_get_client: MagicMock) -> None:
        from interval_exporter.uploader import upload_to_s3

        mock_s3 = MagicMock()
        mock_get_client.return_value = mock_s3

        upload_to_s3(b"x", "f.csv", bucket="alt-bucket", prefix="alt/")

        call_kwargs = mock_s3.put_object.call_args.kwargs
        assert call_kwargs["Bucket"] == "alt-bucket"
        assert call_kwargs["Key"] == "alt/f.csv"

    def test_get_s3_client_singleton_reuses_instance(self) -> None:
        import interval_exporter.uploader as uploader_mod

        # Reset cached client
        uploader_mod._s3_client = None
        c1 = uploader_mod.get_s3_client()
        c2 = uploader_mod.get_s3_client()
        assert c1 is c2
```

- [ ] **Step 3: Run tests to verify they fail with ModuleNotFoundError**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_uploader.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'interval_exporter.uploader'`

- [ ] **Step 4: Implement uploader.py (verbatim from demand, logger renamed)**

Create `src/functions/optima_exporter/interval_exporter/uploader.py`:

```python
"""S3 upload utilities for interval CSV export."""

from typing import Any

import boto3
from aws_lambda_powertools import Logger
from optima_shared.config import S3_UPLOAD_BUCKET, S3_UPLOAD_PREFIX

logger = Logger(service="optima-interval-exporter")

# S3 client (lazy initialization)
_s3_client = None


def get_s3_client() -> Any:
    """Get S3 client with lazy initialization."""
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3", region_name="ap-southeast-2")
    return _s3_client


def upload_to_s3(
    file_content: bytes,
    filename: str,
    bucket: str | None = None,
    prefix: str | None = None,
) -> bool:
    """
    Upload CSV file to S3 for ingestion pipeline.

    Args:
        file_content: CSV file content as bytes
        filename: Filename for S3 object
        bucket: S3 bucket name (default: S3_UPLOAD_BUCKET)
        prefix: S3 prefix/folder (default: S3_UPLOAD_PREFIX)

    Returns:
        True if upload successful, False otherwise
    """
    bucket = bucket or S3_UPLOAD_BUCKET
    prefix = prefix or S3_UPLOAD_PREFIX
    s3_key = f"{prefix}{filename}"

    logger.info(
        "Uploading CSV to S3",
        extra={
            "bucket": bucket,
            "key": s3_key,
            "size_bytes": len(file_content),
            "file_name": filename,
        },
    )

    try:
        s3 = get_s3_client()
        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=file_content,
            ContentType="text/csv",
        )
        logger.info(
            "CSV uploaded successfully to S3",
            extra={"bucket": bucket, "key": s3_key, "file_name": filename},
        )
        return True

    except Exception as e:
        logger.error(
            "S3 upload failed",
            exc_info=True,
            extra={
                "error": str(e),
                "bucket": bucket,
                "key": s3_key,
                "file_name": filename,
            },
        )
        return False
```

- [ ] **Step 5: Run uploader tests to confirm pass**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_uploader.py -v
```

Expected: 4 tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/functions/optima_exporter/interval_exporter/__init__.py \
        src/functions/optima_exporter/interval_exporter/uploader.py \
        tests/unit/optima_exporter/interval_exporter/__init__.py \
        tests/unit/optima_exporter/interval_exporter/test_uploader.py
git commit -m "feat: scaffold interval_exporter package with uploader

Mirrors demand_exporter/uploader.py verbatim (only logger service renamed)."
```

---

### Task 4: Build `downloader.py` — `format_date_for_url`

**Files:**
- Modify: `src/functions/optima_exporter/interval_exporter/downloader.py` (create on first sub-step)
- Modify: `tests/unit/optima_exporter/interval_exporter/test_downloader.py` (create on first sub-step)

**Why:** Smallest standalone helper. Build it first so subsequent download tests can use it.

- [ ] **Step 1: Write the failing test**

Create `tests/unit/optima_exporter/interval_exporter/test_downloader.py`:

```python
"""Unit tests for interval_exporter/downloader.py module.

Tests POST /BuyerReport/exportdailyusagecsv contract, ZIP magic byte validation,
ZIP→CSV extraction, and graceful handling of every observed error mode.
"""

import io
import re
import zipfile
from urllib.parse import parse_qs

import pytest
import responses


class TestFormatDateForUrl:
    def test_formats_date_correctly(self) -> None:
        from interval_exporter.downloader import format_date_for_url

        assert format_date_for_url("2026-04-29") == "29 Apr 2026"

    def test_handles_different_months(self) -> None:
        from interval_exporter.downloader import format_date_for_url

        assert format_date_for_url("2026-12-01") == "01 Dec 2026"
        assert format_date_for_url("2026-09-30") == "30 Sep 2026"

    def test_handles_leap_year(self) -> None:
        from interval_exporter.downloader import format_date_for_url

        assert format_date_for_url("2024-02-29") == "29 Feb 2024"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_downloader.py::TestFormatDateForUrl -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'interval_exporter.downloader'`

- [ ] **Step 3: Create the minimal downloader.py with format_date_for_url**

Create `src/functions/optima_exporter/interval_exporter/downloader.py`:

```python
"""CSV download utilities for interval CSV export.

Endpoint: POST /BuyerReport/exportdailyusagecsv
Returns: application/zip wrapping a single CSV (or the 148-byte
"No data is available" sentinel CSV when a site has no data).
"""

from datetime import datetime

from aws_lambda_powertools import Logger

logger = Logger(service="optima-interval-exporter")


def format_date_for_url(date_str: str) -> str:
    """Convert ISO date format to BidEnergy URL format.

    Args:
        date_str: Date in ISO format (YYYY-MM-DD)

    Returns:
        Date formatted for URL (e.g., "29 Apr 2026")

    Note:
        %b is locale-dependent. AWS Lambda Python 3.13 uses C.UTF-8 where %b
        matches "Apr", "Jun", etc.; CI runners are the same. Non-English dev
        locales would produce different output and break local testing.
    """
    return datetime.strptime(date_str, "%Y-%m-%d").strftime("%d %b %Y")
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_downloader.py::TestFormatDateForUrl -v
```

Expected: 3 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/functions/optima_exporter/interval_exporter/downloader.py \
        tests/unit/optima_exporter/interval_exporter/test_downloader.py
git commit -m "feat: add format_date_for_url helper to interval_exporter downloader"
```

---

### Task 5: Build `downloader.py` — `extract_first_csv`

**Files:**
- Modify: `src/functions/optima_exporter/interval_exporter/downloader.py`
- Modify: `tests/unit/optima_exporter/interval_exporter/test_downloader.py`

**Why:** Pure function — given ZIP bytes, return inner CSV bytes. No HTTP. Builds the ZIP-handling primitive that `download_interval_zip` will use.

- [ ] **Step 1: Append failing tests**

Append to `tests/unit/optima_exporter/interval_exporter/test_downloader.py`:

```python
def _make_zip_with_csv(csv_bytes: bytes, filename: str = "report.csv") -> bytes:
    """Helper: produce in-memory ZIP wrapping a single CSV."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(filename, csv_bytes)
    return buf.getvalue()


SAMPLE_CSV = b'BuyerShortName,Country\r\n"Bunnings","AU"\r\n'
SAMPLE_SENTINEL = (
    b"BuyerShortName,Country,Commodity,Identifier,IdentifierType,DistributorId,"
    b"Date,Start Time,Usage,Generation,DemandKva,Reactive\r\n"
    b"No data is available\r\n"
)


class TestExtractFirstCsv:
    def test_returns_inner_csv_bytes_verbatim(self) -> None:
        from interval_exporter.downloader import extract_first_csv

        zip_bytes = _make_zip_with_csv(SAMPLE_CSV)
        assert extract_first_csv(zip_bytes) == SAMPLE_CSV

    def test_returns_no_data_sentinel_unchanged(self) -> None:
        """The 148-byte sentinel CSV is returned verbatim — no synthesis."""
        from interval_exporter.downloader import extract_first_csv

        zip_bytes = _make_zip_with_csv(SAMPLE_SENTINEL)
        assert extract_first_csv(zip_bytes) == SAMPLE_SENTINEL

    def test_raises_on_invalid_zip(self) -> None:
        from interval_exporter.downloader import extract_first_csv

        with pytest.raises(zipfile.BadZipFile):
            extract_first_csv(b"not a zip")

    def test_raises_on_empty_zip(self) -> None:
        """Defensive — never observed in 8/8 production samples."""
        from interval_exporter.downloader import extract_first_csv

        empty_zip = io.BytesIO()
        with zipfile.ZipFile(empty_zip, "w", zipfile.ZIP_DEFLATED):
            pass

        with pytest.raises(ValueError, match="empty"):
            extract_first_csv(empty_zip.getvalue())
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_downloader.py::TestExtractFirstCsv -v
```

Expected: FAIL with `ImportError: cannot import name 'extract_first_csv' from 'interval_exporter.downloader'`

- [ ] **Step 3: Implement extract_first_csv**

Append to `src/functions/optima_exporter/interval_exporter/downloader.py`:

```python
import io
import zipfile


def extract_first_csv(zip_bytes: bytes) -> bytes:
    """Open the ZIP and return the bytes of the single inner CSV verbatim.

    No synthesis, no special casing. The 148-byte "No data is available"
    sentinel CSV is returned as-is for audit retention; the parser detects
    and handles the sentinel downstream.

    Raises:
        zipfile.BadZipFile: input is not a valid ZIP.
        ValueError: ZIP contains zero entries (defensive — never observed
            in 8/8 production samples).
    """
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = zf.namelist()
        if not names:
            raise ValueError("ZIP contains no entries (empty archive)")
        return zf.read(names[0])
```

(Place imports at the top of the file together with the existing `from datetime import datetime` import.)

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_downloader.py::TestExtractFirstCsv -v
```

Expected: 4 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/functions/optima_exporter/interval_exporter/downloader.py \
        tests/unit/optima_exporter/interval_exporter/test_downloader.py
git commit -m "feat: add extract_first_csv ZIP→CSV extraction helper"
```

---

### Task 6: Build `downloader.py` — `download_interval_zip` happy path + error paths

**Files:**
- Modify: `src/functions/optima_exporter/interval_exporter/downloader.py`
- Modify: `tests/unit/optima_exporter/interval_exporter/test_downloader.py`

**Why:** This is the main download function — POST contract + body validation + every observed failure mode. Bundles all `download_interval_zip` tests in one task to keep related assertions together.

- [ ] **Step 1: Append failing tests**

Append to `tests/unit/optima_exporter/interval_exporter/test_downloader.py`:

```python
ZIP_HAPPY_BODY = _make_zip_with_csv(SAMPLE_CSV)


class TestDownloadIntervalZipHappyPath:
    @responses.activate
    def test_returns_zip_bytes_and_filename_on_success(self) -> None:
        from interval_exporter.downloader import download_interval_zip

        responses.add(
            responses.POST,
            "https://app.bidenergy.com/BuyerReport/exportdailyusagecsv",
            status=200,
            body=ZIP_HAPPY_BODY,
            content_type="application/zip",
        )

        result = download_interval_zip(
            cookies=".ASPXAUTH=tok",
            site_id_str="abc-uuid",
            start_date="2026-04-29",
            end_date="2026-04-29",
            project="bunnings",
            nmi="Optima_2002105104",
        )

        assert result is not None
        zip_bytes, filename = result
        assert zip_bytes == ZIP_HAPPY_BODY
        assert re.match(
            r"^optima_bunnings_interval_NMI#OPTIMA_2002105104_2026-04-29_2026-04-29_\d{14}\.csv$",
            filename,
        )

    @responses.activate
    def test_request_uses_correct_url_method_and_form_body(self) -> None:
        from interval_exporter.downloader import download_interval_zip

        responses.add(
            responses.POST,
            "https://app.bidenergy.com/BuyerReport/exportdailyusagecsv",
            status=200,
            body=ZIP_HAPPY_BODY,
            content_type="application/zip",
        )

        download_interval_zip(
            cookies=".ASPXAUTH=tok",
            site_id_str="abc-uuid",
            start_date="2026-04-29",
            end_date="2026-04-30",
            project="bunnings",
            nmi="Optima_X",
        )

        assert len(responses.calls) == 1
        request = responses.calls[0].request
        assert request.method == "POST"
        assert request.url == "https://app.bidenergy.com/BuyerReport/exportdailyusagecsv"
        assert "application/x-www-form-urlencoded" in request.headers.get("Content-Type", "")
        assert request.headers.get("Cookie") == ".ASPXAUTH=tok"

        body_params = parse_qs(request.body)
        assert body_params["siteId"] == ["abc-uuid"]
        assert body_params["start"] == ["29 Apr 2026"]
        assert body_params["end"] == ["30 Apr 2026"]
        # Confirm there is NO `nmi` field in the form body (kept as Python arg only)
        assert "nmi" not in body_params

    @responses.activate
    def test_sentinel_zip_is_returned_unchanged(self) -> None:
        """When BidEnergy returns the 148-byte 'No data is available' CSV,
        the downloader passes the bytes through; the parser handles it later."""
        from interval_exporter.downloader import download_interval_zip

        sentinel_zip = _make_zip_with_csv(SAMPLE_SENTINEL)
        responses.add(
            responses.POST,
            "https://app.bidenergy.com/BuyerReport/exportdailyusagecsv",
            status=200,
            body=sentinel_zip,
            content_type="application/zip",
        )

        result = download_interval_zip(
            cookies="c",
            site_id_str="s",
            start_date="2026-01-01",
            end_date="2026-01-01",
            project="bunnings",
            nmi="Optima_X",
        )

        assert result is not None
        zip_bytes, _ = result
        assert zip_bytes == sentinel_zip


class TestDownloadIntervalZipErrorPaths:
    @responses.activate
    def test_returns_none_on_html_response(self) -> None:
        """POST/Content-Type misconfig → BidEnergy returns HTML error page."""
        from interval_exporter.downloader import download_interval_zip

        responses.add(
            responses.POST,
            "https://app.bidenergy.com/BuyerReport/exportdailyusagecsv",
            status=200,
            body=b"<!doctype html><html>error page</html>",
            content_type="text/html",
        )

        result = download_interval_zip(
            cookies="c", site_id_str="s", start_date="2026-01-01",
            end_date="2026-01-01", project="bunnings", nmi="Optima_X",
        )
        assert result is None

    @responses.activate
    def test_returns_none_on_non_zip_body(self) -> None:
        """Body not starting with PK magic → reject."""
        from interval_exporter.downloader import download_interval_zip

        responses.add(
            responses.POST,
            "https://app.bidenergy.com/BuyerReport/exportdailyusagecsv",
            status=200,
            body=b"not a zip body",
            content_type="application/zip",  # claims zip but isn't
        )

        result = download_interval_zip(
            cookies="c", site_id_str="s", start_date="2026-01-01",
            end_date="2026-01-01", project="bunnings", nmi="Optima_X",
        )
        assert result is None

    @responses.activate
    def test_returns_none_on_401(self) -> None:
        from interval_exporter.downloader import download_interval_zip

        responses.add(
            responses.POST,
            "https://app.bidenergy.com/BuyerReport/exportdailyusagecsv",
            status=401, body=b"",
        )
        result = download_interval_zip(
            cookies="c", site_id_str="s", start_date="2026-01-01",
            end_date="2026-01-01", project="bunnings", nmi="Optima_X",
        )
        assert result is None

    @responses.activate
    def test_returns_none_on_403(self) -> None:
        from interval_exporter.downloader import download_interval_zip

        responses.add(
            responses.POST,
            "https://app.bidenergy.com/BuyerReport/exportdailyusagecsv",
            status=403, body=b"",
        )
        result = download_interval_zip(
            cookies="c", site_id_str="s", start_date="2026-01-01",
            end_date="2026-01-01", project="bunnings", nmi="Optima_X",
        )
        assert result is None

    @responses.activate
    def test_returns_none_on_404(self) -> None:
        from interval_exporter.downloader import download_interval_zip

        responses.add(
            responses.POST,
            "https://app.bidenergy.com/BuyerReport/exportdailyusagecsv",
            status=404, body=b"",
        )
        result = download_interval_zip(
            cookies="c", site_id_str="s", start_date="2026-01-01",
            end_date="2026-01-01", project="bunnings", nmi="Optima_X",
        )
        assert result is None

    @responses.activate
    def test_returns_none_on_500(self) -> None:
        from interval_exporter.downloader import download_interval_zip

        responses.add(
            responses.POST,
            "https://app.bidenergy.com/BuyerReport/exportdailyusagecsv",
            status=500, body=b"",
        )
        result = download_interval_zip(
            cookies="c", site_id_str="s", start_date="2026-01-01",
            end_date="2026-01-01", project="bunnings", nmi="Optima_X",
        )
        assert result is None

    @responses.activate
    def test_returns_none_on_timeout(self) -> None:
        import requests as _req
        from unittest.mock import patch
        from interval_exporter.downloader import download_interval_zip

        with patch("interval_exporter.downloader.requests.post", side_effect=_req.Timeout()):
            result = download_interval_zip(
                cookies="c", site_id_str="s", start_date="2026-01-01",
                end_date="2026-01-01", project="bunnings", nmi="Optima_X",
            )
        assert result is None

    @responses.activate
    def test_returns_none_on_connection_error(self) -> None:
        import requests as _req
        from unittest.mock import patch
        from interval_exporter.downloader import download_interval_zip

        with patch(
            "interval_exporter.downloader.requests.post",
            side_effect=_req.ConnectionError("boom"),
        ):
            result = download_interval_zip(
                cookies="c", site_id_str="s", start_date="2026-01-01",
                end_date="2026-01-01", project="bunnings", nmi="Optima_X",
            )
        assert result is None
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_downloader.py -v
```

Expected: FAIL with `ImportError: cannot import name 'download_interval_zip' from 'interval_exporter.downloader'`

- [ ] **Step 3: Implement download_interval_zip**

Append to `src/functions/optima_exporter/interval_exporter/downloader.py`:

```python
import requests
from optima_shared.config import BIDENERGY_BASE_URL


def download_interval_zip(
    cookies: str,
    site_id_str: str,
    start_date: str,
    end_date: str,
    project: str,
    nmi: str,
) -> tuple[bytes, str] | None:
    """POST /BuyerReport/exportdailyusagecsv and return raw ZIP bytes + filename.

    Args:
        cookies: Authentication cookie string.
        site_id_str: Site identifier GUID.
        start_date: Start date in ISO format (YYYY-MM-DD).
        end_date: End date in ISO format (YYYY-MM-DD).
        project: Project name (used for filename only).
        nmi: NMI identifier (used for filename only — never sent in URL/body).

    Returns:
        Tuple of (raw ZIP bytes, suggested filename), or None on failure.
        The "No data is available" sentinel ZIP is returned successfully (the
        parser detects and handles the sentinel CSV downstream).
    """
    export_url = f"{BIDENERGY_BASE_URL}/BuyerReport/exportdailyusagecsv"

    body = {
        "siteId": site_id_str,
        "start": format_date_for_url(start_date),
        "end": format_date_for_url(end_date),
    }

    logger.info(
        "Downloading interval ZIP",
        extra={
            "site_id": site_id_str,
            "start_date": start_date,
            "end_date": end_date,
        },
    )

    try:
        response = requests.post(
            export_url,
            data=body,  # auto-sets Content-Type: application/x-www-form-urlencoded
            headers={"Cookie": cookies},
            timeout=300,
        )
    except requests.Timeout:
        logger.error(
            "Interval ZIP download failed: request timeout",
            extra={"project": project, "nmi": nmi, "site_id": site_id_str, "timeout_seconds": 300},
        )
        return None
    except requests.ConnectionError as e:
        logger.error(
            "Interval ZIP download failed: connection error",
            extra={"project": project, "nmi": nmi, "error": str(e)},
        )
        return None
    except requests.RequestException as e:
        logger.error(
            "Interval ZIP download failed: request error",
            exc_info=True,
            extra={"project": project, "nmi": nmi, "error": str(e)},
        )
        return None

    if response.status_code == 200:
        content_type = response.headers.get("Content-Type", "").lower()
        starts_with_pk = response.content[:2] == b"PK"

        if "html" in content_type or not starts_with_pk:
            logger.error(
                "Interval ZIP download failed: response is not a ZIP",
                extra={
                    "project": project,
                    "nmi": nmi,
                    "site_id": site_id_str,
                    "content_type": content_type,
                    "first_bytes_hex": response.content[:8].hex(),
                },
            )
            return None

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        filename = (
            f"optima_{project.lower()}_interval_NMI#{nmi.upper()}_"
            f"{start_date}_{end_date}_{timestamp}.csv"
        )

        logger.info(
            "Interval ZIP download successful",
            extra={
                "project": project,
                "nmi": nmi,
                "csv_filename": filename,
                "size_bytes": len(response.content),
            },
        )
        return response.content, filename

    if response.status_code in (401, 403):
        logger.error(
            "Interval ZIP download failed: authentication/authorization error",
            extra={"project": project, "nmi": nmi, "status_code": response.status_code},
        )
    elif response.status_code == 404:
        logger.error(
            "Interval ZIP download failed: site not found",
            extra={"project": project, "nmi": nmi, "site_id": site_id_str, "status_code": 404},
        )
    else:
        logger.error(
            "Interval ZIP download failed: unexpected response",
            extra={
                "project": project,
                "nmi": nmi,
                "site_id": site_id_str,
                "status_code": response.status_code,
                "response_preview": response.text[:500] if response.text else "empty",
            },
        )
    return None
```

(`requests` and `optima_shared.config.BIDENERGY_BASE_URL` are added to the imports at the top of the file.)

- [ ] **Step 4: Run all downloader tests to verify pass**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_downloader.py -v
```

Expected: All ~17 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/functions/optima_exporter/interval_exporter/downloader.py \
        tests/unit/optima_exporter/interval_exporter/test_downloader.py
git commit -m "feat: implement download_interval_zip with POST contract + ZIP magic validation"
```

---

### Task 7: Build `processor.py` — orchestration with `ThreadPoolExecutor`

**Files:**
- Create: `src/functions/optima_exporter/interval_exporter/processor.py`
- Create: `tests/unit/optima_exporter/interval_exporter/test_processor.py`

**Why:** Processor orchestrates per-site download → upload across 20 worker threads. Mirrors `demand_exporter/processor.py` exactly except for: the 2-step download→extract flow, and `result["empty_data"]` instead of `result["no_data"]`.

- [ ] **Step 1: Write failing tests**

Create `tests/unit/optima_exporter/interval_exporter/test_processor.py`:

```python
"""Unit tests for interval_exporter/processor.py."""

from unittest.mock import MagicMock, patch


SENTINEL_CSV_BYTES = (
    b"BuyerShortName,Country,Commodity,Identifier,IdentifierType,DistributorId,"
    b"Date,Start Time,Usage,Generation,DemandKva,Reactive\r\n"
    b"No data is available\r\n"
)


class TestGetDateRange:
    @patch("interval_exporter.processor.OPTIMA_DAYS_BACK", 1)
    def test_returns_yesterday_for_days_back_1(self) -> None:
        from interval_exporter.processor import get_date_range

        start, end = get_date_range()
        # Both dates equal yesterday in UTC (single-day window)
        assert start == end

    @patch("interval_exporter.processor.OPTIMA_DAYS_BACK", 7)
    def test_returns_7_day_window(self) -> None:
        from datetime import date, timedelta
        from interval_exporter.processor import get_date_range

        start, end = get_date_range()
        s = date.fromisoformat(start)
        e = date.fromisoformat(end)
        assert (e - s).days == 6  # 7-day window inclusive


class TestProcessSite:
    @patch("interval_exporter.processor.upload_to_s3")
    @patch("interval_exporter.processor.extract_first_csv")
    @patch("interval_exporter.processor.download_interval_zip")
    def test_happy_path_returns_success_with_filename(
        self, mock_dl: MagicMock, mock_extract: MagicMock, mock_upload: MagicMock,
    ) -> None:
        from interval_exporter.processor import process_site

        mock_dl.return_value = (b"<zipbytes>", "out.csv")
        mock_extract.return_value = b"BuyerShortName,Country\r\nx,AU\r\n"
        mock_upload.return_value = True

        result = process_site(
            cookies="c", nmi="Optima_X", site_id_str="sid",
            start_date="2026-04-29", end_date="2026-04-29", project="bunnings",
        )

        assert result["success"] is True
        assert result["filename"] == "out.csv"
        assert result["s3_key"] == "newTBP/out.csv"
        assert result["empty_data"] is False
        assert result["error"] is None

    @patch("interval_exporter.processor.upload_to_s3")
    @patch("interval_exporter.processor.extract_first_csv")
    @patch("interval_exporter.processor.download_interval_zip")
    def test_no_data_sentinel_marks_empty_data_true(
        self, mock_dl: MagicMock, mock_extract: MagicMock, mock_upload: MagicMock,
    ) -> None:
        from interval_exporter.processor import process_site

        mock_dl.return_value = (b"<zipbytes>", "out.csv")
        mock_extract.return_value = SENTINEL_CSV_BYTES
        mock_upload.return_value = True

        result = process_site(
            cookies="c", nmi="Optima_X", site_id_str="sid",
            start_date="2026-04-29", end_date="2026-04-29", project="bunnings",
        )

        assert result["success"] is True
        assert result["empty_data"] is True

    @patch("interval_exporter.processor.download_interval_zip", return_value=None)
    def test_download_failure_sets_error(self, mock_dl: MagicMock) -> None:
        from interval_exporter.processor import process_site

        result = process_site(
            cookies="c", nmi="Optima_X", site_id_str="sid",
            start_date="2026-04-29", end_date="2026-04-29", project="bunnings",
        )

        assert result["success"] is False
        assert result["error"] == "Failed to download ZIP"

    @patch("interval_exporter.processor.extract_first_csv", side_effect=ValueError("bad"))
    @patch("interval_exporter.processor.download_interval_zip")
    def test_extract_failure_sets_error(
        self, mock_dl: MagicMock, mock_extract: MagicMock,
    ) -> None:
        from interval_exporter.processor import process_site

        mock_dl.return_value = (b"<zip>", "f.csv")
        result = process_site(
            cookies="c", nmi="Optima_X", site_id_str="sid",
            start_date="2026-04-29", end_date="2026-04-29", project="bunnings",
        )

        assert result["success"] is False
        assert result["error"] == "Failed to extract CSV from ZIP"

    @patch("interval_exporter.processor.upload_to_s3", return_value=False)
    @patch("interval_exporter.processor.extract_first_csv")
    @patch("interval_exporter.processor.download_interval_zip")
    def test_s3_upload_failure_sets_error(
        self, mock_dl: MagicMock, mock_extract: MagicMock, mock_upload: MagicMock,
    ) -> None:
        from interval_exporter.processor import process_site

        mock_dl.return_value = (b"<zip>", "f.csv")
        mock_extract.return_value = b"data"

        result = process_site(
            cookies="c", nmi="Optima_X", site_id_str="sid",
            start_date="2026-04-29", end_date="2026-04-29", project="bunnings",
        )

        assert result["success"] is False
        assert result["error"] == "Failed to upload to S3"


class TestProcessExport:
    @patch("interval_exporter.processor.process_site")
    @patch("interval_exporter.processor.login_bidenergy", return_value="cookie")
    @patch("interval_exporter.processor.get_sites_for_project")
    @patch("interval_exporter.processor.get_project_config", return_value={
        "username": "u", "password": "p", "client_id": "BidEnergy"
    })
    def test_happy_path_returns_200(
        self, mock_cfg: MagicMock, mock_sites: MagicMock,
        mock_login: MagicMock, mock_process: MagicMock,
    ) -> None:
        from interval_exporter.processor import process_export

        mock_sites.return_value = [
            {"nmi": "Optima_A", "siteIdStr": "id-a", "country": "AU"},
            {"nmi": "Optima_B", "siteIdStr": "id-b", "country": "NZ"},
        ]
        mock_process.return_value = {
            "nmi": "Optima_A", "site_id": "id-a", "success": True,
            "filename": "f.csv", "s3_key": "newTBP/f.csv",
            "empty_data": False, "error": None,
        }

        result = process_export(project="bunnings", start_date="2026-04-29", end_date="2026-04-29")

        assert result["statusCode"] == 200
        assert result["body"]["success_count"] == 2
        assert result["body"]["error_count"] == 0
        assert result["body"]["empty_data_count"] == 0

    @patch("interval_exporter.processor.process_site")
    @patch("interval_exporter.processor.login_bidenergy", return_value="cookie")
    @patch("interval_exporter.processor.get_sites_for_project")
    @patch("interval_exporter.processor.get_project_config", return_value={"username": "u", "password": "p", "client_id": "BidEnergy"})
    def test_partial_failure_returns_207(
        self, mock_cfg: MagicMock, mock_sites: MagicMock,
        mock_login: MagicMock, mock_process: MagicMock,
    ) -> None:
        from interval_exporter.processor import process_export

        mock_sites.return_value = [
            {"nmi": "Optima_A", "siteIdStr": "id-a", "country": "AU"},
            {"nmi": "Optima_B", "siteIdStr": "id-b", "country": "AU"},
        ]
        mock_process.side_effect = [
            {"nmi": "Optima_A", "site_id": "id-a", "success": True, "filename": "a.csv", "s3_key": "newTBP/a.csv", "empty_data": False, "error": None},
            {"nmi": "Optima_B", "site_id": "id-b", "success": False, "error": "boom"},
        ]

        result = process_export(project="bunnings", start_date="2026-04-29", end_date="2026-04-29")

        assert result["statusCode"] == 207
        assert result["body"]["success_count"] == 1
        assert result["body"]["error_count"] == 1

    @patch("interval_exporter.processor.get_project_config", return_value=None)
    def test_missing_project_credentials_returns_400(self, mock_cfg: MagicMock) -> None:
        from interval_exporter.processor import process_export

        result = process_export(project="unknown")

        assert result["statusCode"] == 400
        assert "No credentials" in result["body"]

    @patch("interval_exporter.processor.get_sites_for_project", return_value=[])
    @patch("interval_exporter.processor.get_project_config", return_value={"username": "u", "password": "p", "client_id": "x"})
    def test_no_sites_returns_404(self, mock_cfg: MagicMock, mock_sites: MagicMock) -> None:
        from interval_exporter.processor import process_export

        result = process_export(project="bunnings")

        assert result["statusCode"] == 404
        assert "No sites" in result["body"]

    @patch("interval_exporter.processor.login_bidenergy", return_value=None)
    @patch("interval_exporter.processor.get_sites_for_project")
    @patch("interval_exporter.processor.get_project_config", return_value={"username": "u", "password": "p", "client_id": "x"})
    def test_login_failure_returns_401(
        self, mock_cfg: MagicMock, mock_sites: MagicMock, mock_login: MagicMock,
    ) -> None:
        from interval_exporter.processor import process_export

        mock_sites.return_value = [{"nmi": "Optima_A", "siteIdStr": "x", "country": "AU"}]

        result = process_export(project="bunnings")

        assert result["statusCode"] == 401
        assert "authenticate" in result["body"]

    def test_inverted_dates_returns_400(self) -> None:
        from interval_exporter.processor import process_export

        result = process_export(
            project="bunnings", start_date="2026-04-30", end_date="2026-04-01",
        )

        assert result["statusCode"] == 400
        assert "Invalid range" in result["body"]

    @patch("interval_exporter.processor.process_site")
    @patch("interval_exporter.processor.login_bidenergy", return_value="cookie")
    @patch("interval_exporter.processor.get_site_by_nmi")
    @patch("interval_exporter.processor.get_project_config", return_value={"username": "u", "password": "p", "client_id": "x"})
    def test_single_nmi_mode_uses_get_site_by_nmi(
        self, mock_cfg: MagicMock, mock_get_one: MagicMock,
        mock_login: MagicMock, mock_process: MagicMock,
    ) -> None:
        from interval_exporter.processor import process_export

        mock_get_one.return_value = {"nmi": "Optima_X", "siteIdStr": "id-x", "country": "AU"}
        mock_process.return_value = {
            "nmi": "Optima_X", "site_id": "id-x", "success": True,
            "filename": "f.csv", "s3_key": "newTBP/f.csv",
            "empty_data": False, "error": None,
        }

        result = process_export(
            project="bunnings", nmi="Optima_X",
            start_date="2026-04-29", end_date="2026-04-29",
        )

        assert result["statusCode"] == 200
        mock_get_one.assert_called_once_with("bunnings", "Optima_X")

    @patch("interval_exporter.processor.get_site_by_nmi", return_value=None)
    @patch("interval_exporter.processor.get_project_config", return_value={"username": "u", "password": "p", "client_id": "x"})
    def test_single_nmi_not_found_returns_404(
        self, mock_cfg: MagicMock, mock_get_one: MagicMock,
    ) -> None:
        from interval_exporter.processor import process_export

        result = process_export(project="bunnings", nmi="Optima_MISSING")

        assert result["statusCode"] == 404
        assert "not found" in result["body"]
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_processor.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'interval_exporter.processor'`

- [ ] **Step 3: Implement processor.py**

Create `src/functions/optima_exporter/interval_exporter/processor.py`:

```python
"""Processing logic for interval CSV export."""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, date, datetime, timedelta
from typing import Any

from aws_lambda_powertools import Logger
from optima_shared.auth import login_bidenergy
from optima_shared.config import (
    MAX_WORKERS,
    OPTIMA_DAYS_BACK,
    S3_UPLOAD_PREFIX,
    get_project_config,
)
from optima_shared.dynamodb import get_site_by_nmi, get_sites_for_project

from interval_exporter.downloader import download_interval_zip, extract_first_csv
from interval_exporter.uploader import upload_to_s3

logger = Logger(service="optima-interval-exporter")

# 148-byte sentinel marker — present in the body of every empty-data CSV from BidEnergy.
_NO_DATA_MARKER = b"No data is available"


def get_date_range() -> tuple[str, str]:
    """Calculate date range from OPTIMA_DAYS_BACK.

    Returns (start, end) in ISO format. End is yesterday (UTC); start is
    OPTIMA_DAYS_BACK - 1 days before that (so days_back=1 → single-day window).
    """
    today = datetime.now(UTC).date()
    end_date = today - timedelta(days=1)
    start_date = end_date - timedelta(days=OPTIMA_DAYS_BACK - 1)
    logger.info(
        "Calculated date range",
        extra={
            "start_date": str(start_date),
            "end_date": str(end_date),
            "days_back": OPTIMA_DAYS_BACK,
        },
    )
    return start_date.isoformat(), end_date.isoformat()


def process_site(
    cookies: str,
    nmi: str,
    site_id_str: str,
    start_date: str,
    end_date: str,
    project: str,
) -> dict[str, Any]:
    """Process a single site: download ZIP, extract CSV, upload to S3.

    Note: no `country` parameter — the BidEnergy POST body for this endpoint
    is only siteId/start/end, and the parser reads the Identifier column
    directly (AU NMI / NZ ICP both work). See spec "Differences from
    demand_exporter" table.

    The "No data is available" sentinel CSV is uploaded for audit retention;
    result["empty_data"] flag lets callers count them separately.
    """
    result: dict[str, Any] = {
        "nmi": nmi,
        "site_id": site_id_str,
        "success": False,
        "error": None,
    }

    download_result = download_interval_zip(
        cookies, site_id_str, start_date, end_date, project, nmi,
    )
    if download_result is None:
        result["error"] = "Failed to download ZIP"
        return result

    zip_bytes, filename = download_result

    try:
        csv_content = extract_first_csv(zip_bytes)
    except Exception as e:
        logger.error(
            "Failed to extract CSV from ZIP",
            exc_info=True,
            extra={"project": project, "nmi": nmi, "error": str(e)},
        )
        result["error"] = "Failed to extract CSV from ZIP"
        return result

    if not upload_to_s3(csv_content, filename):
        result["error"] = "Failed to upload to S3"
        return result

    result["success"] = True
    result["filename"] = filename
    result["s3_key"] = f"{S3_UPLOAD_PREFIX}{filename}"
    result["empty_data"] = _NO_DATA_MARKER in csv_content
    return result


def process_export(
    project: str,
    nmi: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    """Process interval export for a project.

    Returns Response dict with statusCode and body.
    200 = all OK; 207 = partial failure; 4xx = early reject (no retry by EventBridge).
    """
    project = project.lower()

    if start_date and end_date and date.fromisoformat(start_date) > date.fromisoformat(end_date):
        logger.warning(
            "Export rejected: startDate after endDate",
            extra={"project": project, "start_date": start_date, "end_date": end_date},
        )
        return {
            "statusCode": 400,
            "body": f"Invalid range: startDate ({start_date}) > endDate ({end_date})",
        }

    logger.info(
        "Starting interval export",
        extra={"project": project, "nmi": nmi, "start_date": start_date, "end_date": end_date},
    )

    config = get_project_config(project)
    if not config:
        logger.error("Export rejected: no credentials for project", extra={"project": project})
        return {
            "statusCode": 400,
            "body": f"No credentials configured for project: {project}",
        }

    if nmi:
        site = get_site_by_nmi(project, nmi)
        if not site:
            logger.warning("Export rejected: NMI not found", extra={"project": project, "nmi": nmi})
            return {
                "statusCode": 404,
                "body": f"NMI {nmi} not found for project {project}",
            }
        sites = [site]
    else:
        sites = get_sites_for_project(project)
        if not sites:
            logger.warning("Export rejected: no sites found", extra={"project": project})
            return {
                "statusCode": 404,
                "body": f"No sites found for project {project}",
            }

    if not start_date and not end_date:
        start_date, end_date = get_date_range()
    else:
        today = datetime.now(UTC).date()
        if not end_date:
            end_date = (today - timedelta(days=1)).isoformat()
        if not start_date:
            end_d = date.fromisoformat(end_date)
            start_date = (end_d - timedelta(days=OPTIMA_DAYS_BACK - 1)).isoformat()

    if date.fromisoformat(start_date) > date.fromisoformat(end_date):
        logger.warning(
            "Export rejected: resolved startDate after endDate",
            extra={"project": project, "start_date": start_date, "end_date": end_date},
        )
        return {
            "statusCode": 400,
            "body": f"Invalid range after resolution: startDate ({start_date}) > endDate ({end_date})",
        }

    logger.info(
        "Authenticating with BidEnergy",
        extra={"project": project, "site_count": len(sites)},
    )
    cookies = login_bidenergy(config["username"], config["password"], config["client_id"])
    if cookies is None:
        logger.error("Export failed: authentication failed", extra={"project": project})
        return {
            "statusCode": 401,
            "body": "Failed to authenticate with BidEnergy",
        }

    results: list[dict[str, Any]] = []
    logger.info(
        "Processing sites in parallel",
        extra={"project": project, "site_count": len(sites), "max_workers": MAX_WORKERS},
    )

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_site = {
            executor.submit(
                process_site,
                cookies=cookies,
                nmi=site["nmi"],
                site_id_str=site["siteIdStr"],
                start_date=start_date,
                end_date=end_date,
                project=project,
            ): site
            for site in sites
        }

        for future in as_completed(future_to_site):
            site = future_to_site[future]
            try:
                result = future.result()
                result["project"] = project
                results.append(result)
            except Exception as e:
                logger.error(
                    "Site processing failed with exception",
                    exc_info=True,
                    extra={"project": project, "nmi": site["nmi"], "error": str(e)},
                )
                results.append({
                    "nmi": site["nmi"],
                    "site_id": site["siteIdStr"],
                    "project": project,
                    "success": False,
                    "error": f"Thread execution failed: {e}",
                })

    success_count = sum(1 for r in results if r.get("success"))
    error_count = sum(1 for r in results if not r.get("success"))
    empty_data_count = sum(1 for r in results if r.get("empty_data"))

    logger.info(
        "Interval export completed",
        extra={
            "project": project,
            "total_sites": len(sites),
            "success_count": success_count,
            "error_count": error_count,
            "empty_data_count": empty_data_count,
        },
    )

    return {
        "statusCode": 200 if error_count == 0 else 207,
        "body": {
            "message": f"Processed {len(sites)} site(s) for {project}",
            "project": project,
            "date_range": {"start": start_date, "end": end_date},
            "success_count": success_count,
            "error_count": error_count,
            "empty_data_count": empty_data_count,
            "results": results,
        },
    }
```

- [ ] **Step 4: Run processor tests to verify pass**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_processor.py -v
```

Expected: All ~12 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/functions/optima_exporter/interval_exporter/processor.py \
        tests/unit/optima_exporter/interval_exporter/test_processor.py
git commit -m "feat: add interval_exporter processor with parallel site orchestration"
```

---

### Task 8: Build `app.py` — Lambda handler

**Files:**
- Create: `src/functions/optima_exporter/interval_exporter/app.py`
- Create: `tests/unit/optima_exporter/interval_exporter/test_app.py`

- [ ] **Step 1: Write failing tests**

Create `tests/unit/optima_exporter/interval_exporter/test_app.py`:

```python
"""Unit tests for interval_exporter/app.py Lambda handler."""

from unittest.mock import MagicMock, patch


class TestLambdaHandler:
    @patch("interval_exporter.app.process_export")
    def test_handler_invokes_process_export_with_event_args(
        self, mock_process: MagicMock,
    ) -> None:
        from interval_exporter.app import lambda_handler

        mock_process.return_value = {"statusCode": 200, "body": {}}
        event = {
            "project": "bunnings",
            "nmi": "Optima_X",
            "startDate": "2026-04-29",
            "endDate": "2026-04-29",
        }

        result = lambda_handler(event, MagicMock())

        assert result["statusCode"] == 200
        mock_process.assert_called_once_with(
            project="bunnings",
            nmi="Optima_X",
            start_date="2026-04-29",
            end_date="2026-04-29",
        )

    def test_handler_rejects_event_missing_project_with_400(self) -> None:
        from interval_exporter.app import lambda_handler

        result = lambda_handler({}, MagicMock())

        assert result["statusCode"] == 400
        assert "project" in result["body"]
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_app.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'interval_exporter.app'`

- [ ] **Step 3: Implement app.py**

Create `src/functions/optima_exporter/interval_exporter/app.py`:

```python
"""
Optima Interval Exporter Lambda

Exports BidEnergy interval CSVs by POSTing to /BuyerReport/exportdailyusagecsv
and uploading the inner CSV to S3 for the existing interval_parser to consume.

Event parameters:
    project: Project name ("bunnings" or "racv") - required
    nmi: NMI identifier - optional (single-NMI export mode if provided)
    startDate: Start date in ISO format (YYYY-MM-DD) - optional
    endDate: End date in ISO format (YYYY-MM-DD) - optional
"""

from typing import Any

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

from interval_exporter.processor import process_export

logger = Logger(service="optima-interval-exporter")


@logger.inject_lambda_context
def lambda_handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:
    """Lambda handler for interval CSV export.

    Returns:
        Response dict from process_export (statusCode 200/207/400/401/404).
    """
    project = event.get("project")

    if not project:
        logger.warning("Export rejected: missing project parameter")
        return {
            "statusCode": 400,
            "body": "Missing required parameter: project",
        }

    logger.info(
        "Lambda invoked",
        extra={
            "project": project,
            "nmi": event.get("nmi"),
            "start_date": event.get("startDate"),
            "end_date": event.get("endDate"),
        },
    )

    result = process_export(
        project=project,
        nmi=event.get("nmi"),
        start_date=event.get("startDate"),
        end_date=event.get("endDate"),
    )

    body = result.get("body", {})
    if isinstance(body, dict):
        logger.info(
            "Export completed",
            extra={
                "success_count": body.get("success_count", 0),
                "error_count": body.get("error_count", 0),
                "empty_data_count": body.get("empty_data_count", 0),
            },
        )

    return result
```

- [ ] **Step 4: Run app tests + full interval_exporter suite**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/ -v
```

Expected: All tests pass (app + downloader + processor + uploader).

- [ ] **Step 5: Run full test suite to confirm no regression**

```bash
uv run pytest --cov=src/functions/optima_exporter/interval_exporter --cov-report=term-missing
```

Expected: All pre-existing tests pass; new module coverage ≥ 90%.

- [ ] **Step 6: Commit**

```bash
git add src/functions/optima_exporter/interval_exporter/app.py \
        tests/unit/optima_exporter/interval_exporter/test_app.py
git commit -m "feat: add interval_exporter Lambda handler"
```

---

### Task 9: Update GitHub Actions workflow to build + deploy interval_exporter

**Files:**
- Modify: `.github/workflows/main.yml` — Build step (locate via `grep -n "Build Optima Exporter Lambda" .github/workflows/main.yml`)
- Modify: `.github/workflows/main.yml` — Deploy step (locate via `grep -n "Upload Optima Exporter & Refresh" .github/workflows/main.yml`)

> ⛔ **SEQUENCING RULE — READ BEFORE STARTING.** This task creates a commit only. **DO NOT `git push` in this task.** The push happens in Task 12, after Task 10 (terraform apply) and Task 11 (CI/CD policy v10) are both complete. If you push the commit from this task before Task 10+11, GitHub Actions will fail with `ResourceNotFoundException: Function not found: optima-interval-exporter` (Lambda doesn't exist yet) or `AccessDeniedException: lambda:UpdateFunctionCode` (whitelist doesn't include the new Lambda yet). The commit + push are intentionally split across tasks.

- [ ] **Step 1: Add interval_exporter to the build step**

In `.github/workflows/main.yml`, find the block beginning `- name: Build Optima Exporter Lambda`. After the line `cp -r src/functions/optima_exporter/demand_exporter build/optima_exporter/`, add:

```yaml
          cp -r src/functions/optima_exporter/interval_exporter build/optima_exporter/
```

So the block reads:

```yaml
      - name: Build Optima Exporter Lambda
        if: steps.changes.outputs.optima_exporter == 'true'
        run: |
          mkdir -p build/optima_exporter
          cp -r build/deps/* build/optima_exporter/
          cp -r src/functions/optima_exporter/optima_shared build/optima_exporter/
          cp -r src/functions/optima_exporter/nem12_exporter build/optima_exporter/
          cp -r src/functions/optima_exporter/billing_exporter build/optima_exporter/
          cp -r src/functions/optima_exporter/demand_exporter build/optima_exporter/
          cp -r src/functions/optima_exporter/interval_exporter build/optima_exporter/
          cd build/optima_exporter && zip -r ../../optima_exporter.zip . && cd ../..
```

- [ ] **Step 2: Add the 4th `update-function-code` block to the deploy step**

In the same file, find the block beginning `- name: Upload Optima Exporter & Refresh`. Append a 4th `aws lambda update-function-code` block after the existing `optima-demand-exporter` block:

```yaml
          aws lambda update-function-code \
            --function-name optima-interval-exporter \
            --s3-bucket gega-code-deployment-bucket \
            --s3-key sbm-files-ingester/optima_exporter.zip \
            --publish
```

So the full deploy block reads:

```yaml
      - name: Upload Optima Exporter & Refresh
        if: steps.changes.outputs.optima_exporter == 'true'
        run: |
          aws s3 cp optima_exporter.zip s3://gega-code-deployment-bucket/sbm-files-ingester/optima_exporter.zip
          aws lambda update-function-code \
            --function-name optima-nem12-exporter \
            --s3-bucket gega-code-deployment-bucket \
            --s3-key sbm-files-ingester/optima_exporter.zip \
            --publish
          aws lambda update-function-code \
            --function-name optima-billing-exporter \
            --s3-bucket gega-code-deployment-bucket \
            --s3-key sbm-files-ingester/optima_exporter.zip \
            --publish
          aws lambda update-function-code \
            --function-name optima-demand-exporter \
            --s3-bucket gega-code-deployment-bucket \
            --s3-key sbm-files-ingester/optima_exporter.zip \
            --publish
          aws lambda update-function-code \
            --function-name optima-interval-exporter \
            --s3-bucket gega-code-deployment-bucket \
            --s3-key sbm-files-ingester/optima_exporter.zip \
            --publish
```

- [ ] **Step 3: Lint workflow YAML**

```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/main.yml'))" && echo OK
```

Expected: `OK`

- [ ] **Step 4: Commit (DO NOT PUSH)**

```bash
git add .github/workflows/main.yml
# DO NOT git push — push happens in Task 12 after Task 10 + Task 11.
git commit -m "ci: build and deploy optima-interval-exporter via GitHub Actions"
```

After committing, verify there are no uncommitted changes (`git status` clean) and that the commit is on `main` but unpushed (`git log origin/main..HEAD --oneline` should show this commit). Then STOP — Task 10 is next.

---

### Task 10a: Edit Terraform + validate + plan (no apply)

**Files:**
- Modify: `terraform/optima_exporter.tf`

**Why split:** Subagents cannot interactively prompt the user. This task does all the editing + `terraform plan` review and stops at the gate. The actual `terraform apply` happens in Task 10b after the orchestrator/user explicitly approves the plan output.

**Pre-flight:** Ensure CI/CD policy v10 work (Task 11) is queued — if it's not done before push (Task 12), GitHub Actions will fail with `AccessDeniedException`.

- [ ] **Step 1: Remove the 5 stale `moved` blocks**

First locate them by anchor (line numbers in this plan are advisory — find actual line range with grep before editing):

```bash
grep -n "Terraform state moves\|^moved {" terraform/optima_exporter.tf
```

Expected: 1 heading line + 5 `moved {` opening lines. Delete the heading comment block plus all 5 `moved { ... }` blocks (~30 lines total). The content to remove looks exactly like:

```hcl
# ================================
# Terraform state moves (rename interval_exporter → nem12_exporter)
# ================================
moved {
  from = aws_cloudwatch_log_group.optima_interval_exporter
  to   = aws_cloudwatch_log_group.optima_nem12_exporter
}

moved {
  from = aws_lambda_function.optima_interval_exporter
  to   = aws_lambda_function.optima_nem12_exporter
}

moved {
  from = aws_scheduler_schedule.optima_bunnings_interval
  to   = aws_scheduler_schedule.optima_bunnings_nem12
}

moved {
  from = aws_scheduler_schedule.optima_racv_interval
  to   = aws_scheduler_schedule.optima_racv_nem12
}

moved {
  from = aws_cloudwatch_metric_alarm.optima_interval_errors
  to   = aws_cloudwatch_metric_alarm.optima_nem12_errors
}
```

- [ ] **Step 2: Comment out the 2 NEM12 schedule resources**

First locate by anchor:

```bash
grep -n 'aws_scheduler_schedule" "optima_bunnings_nem12"\|aws_scheduler_schedule" "optima_racv_nem12"' terraform/optima_exporter.tf
```

Expected: 2 hits — the opening lines of `optima_bunnings_nem12` and `optima_racv_nem12` schedule resources (NOT the `..._weekly` variants which are already commented out). Find the comment line `# Bunnings NEM12 - Daily 2:00 PM Sydney` just above the first hit; that marks the start of the block to replace. The end is the closing `}` of `optima_racv_nem12`. Replace that whole range with this commented-out version:

```hcl
# === DISABLED 2026-05-06 ===
# Replaced by optima-interval-exporter (uses POST /BuyerReport/exportdailyusagecsv).
# The optima-nem12-exporter Lambda function, log group, and alarm are intentionally
# kept for manual invoke / backup / debug. To re-enable: uncomment these two
# resource blocks + run `terraform apply`.
#
# # Bunnings NEM12 - Daily 2:00 PM Sydney
# resource "aws_scheduler_schedule" "optima_bunnings_nem12" {
#   name       = "optima-bunnings-nem12-daily"
#   group_name = "default"
#
#   flexible_time_window {
#     mode = "OFF"
#   }
#
#   schedule_expression          = "cron(0 14 * * ? *)"
#   schedule_expression_timezone = "Australia/Sydney"
#
#   target {
#     arn      = aws_lambda_function.optima_nem12_exporter.arn
#     role_arn = aws_iam_role.optima_scheduler_role.arn
#     input    = jsonencode({ project = "bunnings" })
#   }
# }
#
# # RACV NEM12 - Daily 2:00 PM Sydney
# resource "aws_scheduler_schedule" "optima_racv_nem12" {
#   name       = "optima-racv-nem12-daily"
#   group_name = "default"
#
#   flexible_time_window {
#     mode = "OFF"
#   }
#
#   schedule_expression          = "cron(0 14 * * ? *)"
#   schedule_expression_timezone = "Australia/Sydney"
#
#   target {
#     arn      = aws_lambda_function.optima_nem12_exporter.arn
#     role_arn = aws_iam_role.optima_scheduler_role.arn
#     input    = jsonencode({ project = "racv" })
#   }
# }
```

- [ ] **Step 3: Update the scheduler invoke-lambda IAM policy**

Locate by anchor:

```bash
grep -n 'aws_iam_role_policy" "optima_scheduler_invoke_lambda"' terraform/optima_exporter.tf
```

Find the `aws_iam_role_policy "optima_scheduler_invoke_lambda"` block and add the 4th ARN to the `Resource` list. Replace the existing block with:

```hcl
resource "aws_iam_role_policy" "optima_scheduler_invoke_lambda" {
  name = "sbm-optima-scheduler-invoke-lambda"
  role = aws_iam_role.optima_scheduler_role.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = "lambda:InvokeFunction"
      Resource = [
        aws_lambda_function.optima_nem12_exporter.arn,
        aws_lambda_function.optima_billing_exporter.arn,
        aws_lambda_function.optima_demand_exporter.arn,
        aws_lambda_function.optima_interval_exporter.arn,
      ]
    }]
  })
}
```

- [ ] **Step 4: Add the 5 new resources for `optima_interval_exporter`**

Append at the end of `terraform/optima_exporter.tf` (after the last resource and before any closing markers):

```hcl
# ================================
# Lambda 4: Interval Exporter (NEW primary interval data source)
# ================================

resource "aws_cloudwatch_log_group" "optima_interval_exporter" {
  name              = "/aws/lambda/optima-interval-exporter"
  retention_in_days = var.log_retention_days

  tags = local.common_tags
}

resource "aws_lambda_function" "optima_interval_exporter" {
  function_name = "optima-interval-exporter"
  description   = "Exports Optima interval CSVs (POST exportdailyusagecsv) to S3 — primary source"
  role          = data.aws_iam_role.ingester_role.arn
  handler       = "interval_exporter.app.lambda_handler"
  runtime       = "python3.13"
  timeout       = 900
  memory_size   = 256
  s3_bucket     = var.deployment_bucket
  s3_key        = "${local.lambda_s3_prefix}/optima_exporter.zip"

  environment {
    variables = merge(local.optima_common_env, {
      POWERTOOLS_SERVICE_NAME = "optima-interval-exporter"
      S3_UPLOAD_BUCKET        = "sbm-file-ingester"
      S3_UPLOAD_PREFIX        = "newTBP/"
      OPTIMA_DAYS_BACK        = "1"
      OPTIMA_MAX_WORKERS      = "20"
    })
  }

  tracing_config {
    mode = "PassThrough"
  }

  depends_on = [aws_cloudwatch_log_group.optima_interval_exporter]

  tags = local.common_tags
}

# Bunnings Interval - Daily 2:00 PM Sydney (taking the slot vacated by NEM12)
resource "aws_scheduler_schedule" "optima_bunnings_interval" {
  name       = "optima-bunnings-interval-daily"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "cron(0 14 * * ? *)"
  schedule_expression_timezone = "Australia/Sydney"

  target {
    arn      = aws_lambda_function.optima_interval_exporter.arn
    role_arn = aws_iam_role.optima_scheduler_role.arn
    input    = jsonencode({ project = "bunnings" })
  }
}

# RACV Interval - Daily 2:00 PM Sydney
resource "aws_scheduler_schedule" "optima_racv_interval" {
  name       = "optima-racv-interval-daily"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "cron(0 14 * * ? *)"
  schedule_expression_timezone = "Australia/Sydney"

  target {
    arn      = aws_lambda_function.optima_interval_exporter.arn
    role_arn = aws_iam_role.optima_scheduler_role.arn
    input    = jsonencode({ project = "racv" })
  }
}

resource "aws_cloudwatch_metric_alarm" "optima_interval_errors" {
  alarm_name          = "optima-interval-exporter-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 3600 # 1 hour
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Optima interval exporter Lambda errors"

  dimensions = {
    FunctionName = aws_lambda_function.optima_interval_exporter.function_name
  }

  alarm_actions = [data.aws_sns_topic.sbm_alerts.arn]
  ok_actions    = [data.aws_sns_topic.sbm_alerts.arn]

  tags = local.common_tags
}
```

- [ ] **Step 5: Validate the Terraform changes compile**

```bash
cd terraform && terraform fmt -check optima_exporter.tf && terraform validate
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 6: Run `terraform plan` and inspect carefully**

```bash
cd terraform && terraform plan -out=/tmp/interval.tfplan 2>&1 | tee /tmp/interval-plan.txt
```

Expected summary:
```
Plan: 5 to add, 1 to change, 2 to destroy.
```

Specifically:
- `+ aws_cloudwatch_log_group.optima_interval_exporter`
- `+ aws_lambda_function.optima_interval_exporter`
- `+ aws_scheduler_schedule.optima_bunnings_interval`
- `+ aws_scheduler_schedule.optima_racv_interval`
- `+ aws_cloudwatch_metric_alarm.optima_interval_errors`
- `~ aws_iam_role_policy.optima_scheduler_invoke_lambda` (in-place: add 4th ARN)
- `- aws_scheduler_schedule.optima_bunnings_nem12` (commented out)
- `- aws_scheduler_schedule.optima_racv_nem12` (commented out)

If the plan output mentions "moved" blocks anywhere, STOP — Step 1 was incomplete. If you see destroys for the NEM12 Lambda function, log group, or alarm, STOP — Step 2 over-deleted. The Lambda function/log group/alarm for `optima_nem12_exporter` must be untouched.

- [ ] **Step 7: STOP — emit plan summary for orchestrator gate**

Subagents cannot interactively prompt the user. Instead, finish this task by:

1. Outputting the entire `cat /tmp/interval-plan.txt` content (or at minimum the resource summary and the `Plan: X to add, Y to change, Z to destroy` line).
2. Reporting status `BLOCKED — awaiting user approval before terraform apply (Task 10b).` Do NOT run `terraform apply`. Do NOT commit yet (the commit happens in Task 10b after apply succeeds, so the commit message can reference the verified outcome).
3. Leaving `/tmp/interval.tfplan` on disk so Task 10b can apply the same plan without recomputing.

The orchestrator (parent session or user) will review the plan and dispatch Task 10b explicitly.

---

### Task 10b: Apply Terraform plan + verify + commit

**Pre-requisites:** Task 10a complete; orchestrator/user has explicitly approved the plan; `/tmp/interval.tfplan` exists.

**Files:**
- Modify: `terraform/optima_exporter.tf` (already edited in Task 10a; this task only commits it after apply succeeds)

- [ ] **Step 1: Apply the saved Terraform plan**

```bash
cd terraform && terraform apply /tmp/interval.tfplan
```

Expected: `Apply complete! Resources: 5 added, 1 changed, 2 destroyed.`

If the apply fails, report status `BLOCKED — terraform apply failed: <error>` and STOP. Do not retry without orchestrator instruction.

- [ ] **Step 2: Verify the new Lambda function exists**

```bash
aws lambda get-function-configuration --function-name optima-interval-exporter --region ap-southeast-2 --query '{name:FunctionName,handler:Handler,runtime:Runtime,memory:MemorySize,timeout:Timeout}' --output table
```

Expected: `name=optima-interval-exporter`, `handler=interval_exporter.app.lambda_handler`, `runtime=python3.13`, `memory=256`, `timeout=900`.

The Lambda code at this point is whatever was in the existing `optima_exporter.zip` artefact in S3 (which does not yet contain `interval_exporter/`). Invoking it now would fail with `Unable to import module 'interval_exporter.app'`. That is fixed by Task 11 (CI/CD policy update) + Task 12 (push triggers GitHub Actions deploy).

- [ ] **Step 3: Commit the Terraform change**

```bash
git add terraform/optima_exporter.tf
git commit -m "infra: add optima-interval-exporter Lambda + schedules; disable NEM12 schedules

- Remove 5 stale 'moved' blocks (leftover from April rename).
- Add aws_cloudwatch_log_group, aws_lambda_function, 2 aws_scheduler_schedule,
  and aws_cloudwatch_metric_alarm for optima-interval-exporter.
- Add the new Lambda ARN to the optima_scheduler_invoke_lambda IAM policy.
- Comment out optima_bunnings_nem12 and optima_racv_nem12 schedules
  (Lambda function/log group/alarm preserved for backup/debug).

Applied: 5 added, 1 changed, 2 destroyed."
```

> ⛔ Same sequencing rule as Task 9: **DO NOT `git push` here.** Push happens in Task 12.

---

### Task 11: Update CI/CD IAM policy (manual, then docs)

**Files:**
- Modify: `sbm-ingester/CLAUDE.md` (update whitelist + version number)
- Manual AWS step: create v10 of `sbm-ingester-cicd-policy`

**Why:** GitHub Actions deploys via the `sbm-ingester-github-actions` IAM user, whose `sbm-ingester-cicd-policy` whitelists Lambda function ARNs for `lambda:UpdateFunctionCode`. Without this update, the deploy from Task 9 will fail with `AccessDeniedException`.

This is a manual step (the policy is intentionally not Terraform-managed — see `sbm-ingester/CLAUDE.md` "Manual Sync: CI/CD IAM Policy"). The plan documents it for the human; the agent should pause and request the user perform it.

- [ ] **Step 1: Fetch the current default policy**

```bash
DEFAULT=$(aws iam get-policy --policy-arn arn:aws:iam::318396632821:policy/sbm-ingester-cicd-policy --query 'Policy.DefaultVersionId' --output text)
echo "Current default version: $DEFAULT"
aws iam get-policy-version \
  --policy-arn arn:aws:iam::318396632821:policy/sbm-ingester-cicd-policy \
  --version-id "$DEFAULT" \
  --query 'PolicyVersion.Document' > /tmp/policy-current.json
cat /tmp/policy-current.json | python3 -m json.tool | head -40
```

Expected: Should be `v9` and contain 9 Lambda ARNs already (including `optima-demand-exporter`).

- [ ] **Step 2: Edit policy to add `optima-interval-exporter`**

```bash
python3 - <<'PY'
import json, pathlib
p = pathlib.Path("/tmp/policy-current.json")
doc = json.loads(p.read_text())
new_arn = "arn:aws:lambda:ap-southeast-2:318396632821:function:optima-interval-exporter"
for stmt in doc["Statement"]:
    if stmt.get("Sid") == "LambdaUpdateFunctions":
        if new_arn not in stmt["Resource"]:
            stmt["Resource"].append(new_arn)
        break
else:
    raise SystemExit("ERROR: did not find LambdaUpdateFunctions statement")
p.write_text(json.dumps(doc, indent=2))
print("Updated. Resource count now:",
      sum(len(s["Resource"]) for s in doc["Statement"] if s.get("Sid") == "LambdaUpdateFunctions"))
PY
```

Expected: `Resource count now: 10`.

- [ ] **Step 3: If at 5 versions, delete the oldest non-default**

```bash
aws iam list-policy-versions --policy-arn arn:aws:iam::318396632821:policy/sbm-ingester-cicd-policy \
  --query 'Versions[].{Id:VersionId,Default:IsDefaultVersion,Created:CreateDate}' --output table
```

If there are 5 versions, delete the oldest non-default:

```bash
OLDEST=$(aws iam list-policy-versions --policy-arn arn:aws:iam::318396632821:policy/sbm-ingester-cicd-policy \
  --query 'sort_by(Versions[?IsDefaultVersion==`false`], &CreateDate)[0].VersionId' --output text)
aws iam delete-policy-version --policy-arn arn:aws:iam::318396632821:policy/sbm-ingester-cicd-policy --version-id "$OLDEST"
```

- [ ] **Step 4: Create v10 and set as default**

```bash
aws iam create-policy-version \
  --policy-arn arn:aws:iam::318396632821:policy/sbm-ingester-cicd-policy \
  --policy-document file:///tmp/policy-current.json \
  --set-as-default \
  --query 'PolicyVersion.{Version:VersionId,Default:IsDefaultVersion}' --output table
```

Expected: `Version: v10`, `Default: True`.

- [ ] **Step 5: Update `sbm-ingester/CLAUDE.md` with the new whitelist + version**

In `sbm-ingester/CLAUDE.md`, find the `## ⚠️ Manual Sync: CI/CD IAM Policy` section. Update:
1. The "as of last verified sync" date to today's date.
2. Append `optima-interval-exporter` to the bullet list of whitelisted Lambdas.
3. Update the Lambda functions table at line ~95 (the one starting with `| `sbm-files-ingester` | Python 3.13 | 512 MB | 900s |`) by adding a new row right after the existing `optima-demand-exporter` row:
   ```markdown
   | `optima-interval-exporter` | Python 3.13 | 256 MB | 900s | Daily export (2:00 PM Sydney) - downloads BidEnergy interval CSVs (POST exportdailyusagecsv), uploads to S3 (X-Ray disabled) |
   ```
4. (Optional but recommended) In the "Optima Exporter Tests" section near the bottom, add a row for the new test directory `tests/unit/optima_exporter/interval_exporter/` with the test counts after Task 12.

- [ ] **Step 6: Commit docs update**

```bash
git add sbm-ingester/CLAUDE.md
git commit -m "docs: document optima-interval-exporter in Lambda table and CI/CD whitelist"
```

---

### Task 12: Push, verify deploy, smoke test, monitor first scheduled run

**Files:** None (operational steps)

**Why:** Cutover validation — confirm the new Lambda is wired correctly end-to-end before relying on the daily 14:00 schedule.

- [ ] **Step 1: Confirm all preceding tasks committed**

```bash
git log --oneline -8
git status
```

Expected: Recent commits cover Task 1-11; `git status` shows `nothing to commit, working tree clean`.

- [ ] **Step 2: Push to main (triggers GitHub Actions)**

```bash
git push origin main
```

- [ ] **Step 3: Watch the GitHub Actions run**

```bash
gh run watch
```

Expected: All steps green. The `Build Optima Exporter Lambda` step now copies `interval_exporter/` into the zip; the `Upload Optima Exporter & Refresh` step now updates 4 Lambda functions (nem12, billing, demand, interval).

- [ ] **Step 4: Manual smoke test — single-NMI invoke**

```bash
aws lambda invoke \
  --function-name optima-interval-exporter \
  --payload '{"project":"bunnings","nmi":"Optima_2002105104"}' \
  --cli-binary-format raw-in-base64-out \
  --region ap-southeast-2 \
  /tmp/interval-smoke.json && \
cat /tmp/interval-smoke.json | python3 -m json.tool
```

Expected: `statusCode: 200`, `body.success_count: 1`, `body.error_count: 0`.

- [ ] **Step 5: Verify the CSV landed in S3**

```bash
aws s3 ls s3://sbm-file-ingester/newTBP/ --region ap-southeast-2 | grep -i "interval_NMI#OPTIMA_2002105104" | tail -3
```

Expected: One new file with prefix `optima_bunnings_interval_NMI#OPTIMA_2002105104_*.csv`.

- [ ] **Step 6: Wait ~1 minute, then verify file_processor consumed it**

```bash
aws logs tail /aws/lambda/sbm-files-ingester --since 2m --region ap-southeast-2 | grep -i "interval_NMI#OPTIMA_2002105104" | head -10
```

Expected: Log lines showing the file was downloaded, parsed (pandas DataFrame path), and channel-mapped.

- [ ] **Step 7: Verify file routed to `newP/` (or `newIrrevFiles/` if site has no data)**

```bash
aws s3 ls s3://sbm-file-ingester/newP/ s3://sbm-file-ingester/newIrrevFiles/ --region ap-southeast-2 \
  | grep -i "interval_NMI#OPTIMA_2002105104" | tail -3
```

Expected: The file appears in `newP/` (Hudi rows written) — no leftover in `newTBP/`.

- [ ] **Step 8: After Glue job runs, verify Athena rows**

Resolve the Hudi sensor IDs dynamically from the live mappings (do NOT hard-code — IDs may rotate):

```bash
SENSOR_E1=$(aws s3 cp s3://sbm-file-ingester/nem12_mappings.json - --region ap-southeast-2 \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['Optima_2002105104-E1'])")
SENSOR_B1=$(aws s3 cp s3://sbm-file-ingester/nem12_mappings.json - --region ap-southeast-2 \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['Optima_2002105104-B1'])")
echo "E1 sensor: $SENSOR_E1"
echo "B1 sensor: $SENSOR_B1"

QUERY_ID=$(aws athena start-query-execution \
  --query-string "SELECT sensorid, COUNT(*) AS cnt, MIN(ts) AS min_ts, MAX(ts) AS max_ts FROM sensordata_default WHERE sensorid IN ('$SENSOR_E1','$SENSOR_B1') GROUP BY sensorid" \
  --query-execution-context '{"Database":"default"}' \
  --result-configuration '{"OutputLocation":"s3://sbm-file-ingester/athena-results/"}' \
  --region ap-southeast-2 --query 'QueryExecutionId' --output text)
echo "Query: $QUERY_ID — wait ~5s then run get-query-results"
sleep 6
aws athena get-query-results --query-execution-id "$QUERY_ID" --region ap-southeast-2 \
  --query 'ResultSet.Rows[*].Data[*].VarCharValue' --output text
```

Expected: Recent rows for the smoke-test date showing both `cnt > 0` and a `max_ts` close to today.

If the mapping lookup raises `KeyError`, the smoke-test NMI's mapping has changed — pick another NMI from `aws s3 cp s3://sbm-file-ingester/nem12_mappings.json - | python3 -c "import json,sys; print([k for k in json.load(sys.stdin) if k.startswith('Optima_2002') and k.endswith('-E1')][:5])"` and re-run the smoke test from Step 4 with the chosen NMI.

- [ ] **Step 9: Watch the first 14:00 scheduled run**

The next day at 14:00 Sydney, watch the alarm dashboard:

```bash
aws cloudwatch describe-alarms --alarm-names optima-interval-exporter-errors \
  --region ap-southeast-2 --query 'MetricAlarms[0].{State:StateValue,Reason:StateReason}' --output table
```

Expected: `State: OK`. If `State: ALARM`, check `aws logs tail /aws/lambda/optima-interval-exporter --since 1h --region ap-southeast-2`.

- [ ] **Step 10: Confirm the old NEM12 schedules no longer fire**

```bash
aws scheduler list-schedules --region ap-southeast-2 --name-prefix optima- \
  --query 'Schedules[*].{Name:Name,State:State}' --output table
```

Expected: `optima-bunnings-nem12-daily` and `optima-racv-nem12-daily` should NOT appear (they were destroyed in Task 10). `optima-bunnings-interval-daily` and `optima-racv-interval-daily` should be present and `ENABLED`.

---

## Notes for the Implementer

- **Tests must be pytest-style** (no unittest classes inheriting from `unittest.TestCase`); the codebase uses bare classes.
- **Coverage:** lefthook pre-push gate enforces ≥ 90% — `uv run pytest --cov=src` should always be clean before push.
- **Commits:** Conventional Commits format. NO scope in parentheses (`feat: foo`, not `feat(thing): foo`). NO `Co-Authored-By: Claude` trailer. (See `~/.claude/CLAUDE.md`.)
- **Imports inside test functions:** the existing test pattern delays imports of the module under test to inside each test method (so `responses.activate` and `patch` decorators apply correctly). Mirror this pattern.
- **`responses` library:** when stubbing POST endpoints, `responses.calls[0].request.body` is bytes, but `parse_qs` handles bytes natively — no decode needed.
- **Don't touch** `optima_shared/`, `non_nem_parsers.py`, `file_processor/app.py`, `nem_adapter.py`, the demand or nem12 exporter modules, or `tests/unit/conftest.py:create_optima_csv`. Verified pre-existing wiring is correct.
- **The 4 fixture CSVs** at `tests/unit/fixtures/optima_interval/` are already committed (commit `86ab1bf`). Do not re-download.
