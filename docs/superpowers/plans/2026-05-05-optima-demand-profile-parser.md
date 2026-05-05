# Optima Demand Profile Parser Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an `Optima/BidEnergy "Demand Profile" CSV` parser that persists `kW`, `kVa`, and `Power Factor` per NMI into the Hudi data lake; create the necessary 1431 Neptune monitor points (3 per Bunnings Optima NMI × 477 NMIs); refactor the shared `nem12_mappings.json` loader out of `bunnings_billing` into `parsers/_mappings.py` along the way.

**Architecture:** New parser `src/shared/parsers/optima/demand.py` mirrors `bunnings_billing.py`'s pattern — read CSV, look up sensor IDs from `nem12_mappings.json`, write Hudi rows directly to `s3://hudibucketsrc/sensorDataFiles/`, return `[]` to dispatcher. Bypass needed because demand metric column names (`kw`/`kva`/`pf`) aren't NEM12 channel codes that the standard `file_processor` flow accepts. Two new operator scripts (`generate_demand_points.py` + `import_demand_points.py`) create the Neptune points.

**Tech Stack:** Python 3.13, uv, pytest, ruff, aws-lambda-powertools, boto3, Gremlin (via gemsNeptuneExplorer Lambda).

**Spec:** [`docs/superpowers/specs/2026-05-05-optima-demand-profile-parser-design.md`](../specs/2026-05-05-optima-demand-profile-parser-design.md)

**Pre-flight check before starting:**
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
git status --short          # confirm baseline state
uv sync --all-extras
uv run pytest --tb=short -q # baseline: 491 tests pass
```

There is pre-existing uncommitted WIP in `src/functions/optima_exporter/interval_exporter/`, `terraform/optima_exporter.tf`, and `.gitignore`. **Stash it before starting** so commits don't sweep it:
```bash
git stash push -u -m "pre-demand-parser-refactor" -- src/functions/optima_exporter/interval_exporter/ terraform/optima_exporter.tf .gitignore
```
Pop it back after Task 9 with `git stash pop`.

---

## Critical Execution Rules

These rules govern every task in this plan.

### R1: One commit per task

After each task's tests pass, commit immediately. Don't batch commits across tasks. Each commit message starts with `feat:` (new functionality), `refactor:` (no behaviour change), `test:` (test-only), `chore:` (scripts, infra), or `docs:` (documentation).

### R2: Use `TYPE_CHECKING` for `ParserResult` import

Whenever a new parser file imports `ParserResult`, use the deferred-import pattern that the recently refactored parsers established:

```python
from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from shared.parsers import ParserResult
```

This satisfies ruff's TC001 rule when `from __future__ import annotations` makes all annotations lazy strings.

### R3: Explicit `git add <paths>` (not `git add -A`)

Pre-flight stash protects unrelated WIP, but explicit paths in commits make intent visible and prevent accidental sweeps if a future operator forgets to stash.

### R4: Coverage gate (≥90%) and pre-commit hook

Lefthook runs ruff on commit. If a hook fails, fix the underlying issue (don't `--no-verify`). Don't `git push` until Task 9 — pushing mid-refactor risks tripping the ≥90% coverage hook on transient states.

### R5: Stop and report on test failure

If any verification step fails, STOP and report — don't push forward through failures. Diagnose with `uv run pytest <path> -v` to see the failing assertion.

---

## Task 1: Extract `_mappings.py` shared helper + refactor bunnings_billing

**Why first:** all subsequent parser tasks depend on the shared `get_nem12_mappings()` API. Doing this as a behaviour-preserving refactor first means later tasks can build on it without entangling new functionality with refactor noise.

**Files:**
- Create: `src/shared/parsers/_mappings.py`
- Modify: `src/shared/parsers/optima/bunnings_billing.py` (~10 lines removed, 1 import line added)
- Modify: `tests/unit/parsers/optima/test_bunnings_billing.py` (~4 patch sites + 1 fixture)

- [ ] **Step 1: Create `src/shared/parsers/_mappings.py`**

```python
"""Shared loader for nem12_mappings.json (cached per Lambda container).

Used by any parser that needs to resolve nem12-style sensor keys to Neptune
IDs without going through file_processor's standard NMI-mapping flow.
"""

from __future__ import annotations

import json

import boto3
from aws_lambda_powertools import Logger

logger = Logger(service="nem12-mappings-loader", child=True)

MAPPINGS_BUCKET = "sbm-file-ingester"
MAPPINGS_KEY = "nem12_mappings.json"

_cache: dict | None = None


def get_nem12_mappings() -> dict:
    """Lazy-load nem12_mappings.json from S3 once per Lambda container.

    Cached at module level; lives for the container's warm lifetime.
    Cold starts pay one ~1 MB S3 GET. Mapping refresh happens hourly via
    the sbm-files-ingester-nem12-mappings-to-s3 Lambda — stale containers
    miss new NMIs until they recycle, which is acceptable.
    """
    global _cache
    if _cache is None:
        logger.info(
            "Loading nem12_mappings.json from S3",
            extra={"bucket": MAPPINGS_BUCKET, "key": MAPPINGS_KEY},
        )
        obj = boto3.client("s3").get_object(Bucket=MAPPINGS_BUCKET, Key=MAPPINGS_KEY)
        _cache = json.loads(obj["Body"].read())
    return _cache
```

- [ ] **Step 2: Refactor `src/shared/parsers/optima/bunnings_billing.py` to use the shared helper**

First, locate the existing helper to remove:
```bash
grep -nE "_get_nem12_mappings|_nem12_mappings_cache|MAPPINGS_BUCKET|MAPPINGS_KEY" src/shared/parsers/optima/bunnings_billing.py
```

Remove the following from `bunnings_billing.py` (lines around 27-31 + 113-126):

```python
# REMOVE these constants near top:
MAPPINGS_BUCKET = "sbm-file-ingester"
MAPPINGS_KEY = "nem12_mappings.json"

# REMOVE module-level cache:
_nem12_mappings_cache: dict | None = None

# REMOVE the entire _get_nem12_mappings function (lines ~113-126):
def _get_nem12_mappings() -> dict:
    """..."""
    global _nem12_mappings_cache
    if _nem12_mappings_cache is None:
        ...
    return _nem12_mappings_cache
```

Add this import in the import block (alphabetic position):
```python
from shared.parsers._mappings import get_nem12_mappings
```

Find the call site (around line 210):
```python
mappings = _get_nem12_mappings()
```

Replace with:
```python
mappings = get_nem12_mappings()
```

Also check for any other `_get_nem12_mappings()` call in the file (`grep -n _get_nem12_mappings src/shared/parsers/optima/bunnings_billing.py` should now return zero) and any `import json` / `import boto3` that may now be unused. Note: `boto3` is still used for the S3 PUT in `_process_rows_and_write` — leave that import.

- [ ] **Step 3: Update mock patch targets in `tests/unit/parsers/optima/test_bunnings_billing.py`**

Locate every patch/setattr site that targets the old names:
```bash
grep -nE '_get_nem12_mappings|_nem12_mappings_cache|_reset_mappings_cache' tests/unit/parsers/optima/test_bunnings_billing.py
```

Add a new import at the top of the test file (alongside the existing `from shared.parsers.optima import bunnings_billing as bp` style import — adjust to whatever the file currently uses):

```python
from shared.parsers import _mappings as mappings_mod
```

Then perform these replacements file-wide:

| Old | New |
|---|---|
| `monkeypatch.setattr(bp, "_get_nem12_mappings", ...)` | `monkeypatch.setattr(mappings_mod, "get_nem12_mappings", ...)` |
| `bp._nem12_mappings_cache = None` | `mappings_mod._cache = None` |
| `bp._get_nem12_mappings = ...` | `mappings_mod.get_nem12_mappings = ...` |

If there's a fixture like:
```python
@pytest.fixture
def _reset_mappings_cache():
    bp._nem12_mappings_cache = None
    yield
    bp._nem12_mappings_cache = None
```

Update its body to:
```python
@pytest.fixture
def _reset_mappings_cache():
    mappings_mod._cache = None
    yield
    mappings_mod._cache = None
```

(Keep the fixture name `_reset_mappings_cache` so the ~11 tests that use it continue to work without per-test changes.)

- [ ] **Step 4: Run bunnings_billing tests to confirm refactor passes**

Run: `uv run pytest tests/unit/parsers/optima/test_bunnings_billing.py -v 2>&1 | tail -25`
Expected: 17 passed (same count as baseline).

If any test fails with `AttributeError: ... has no attribute '_get_nem12_mappings'` or similar, you missed a patch site — re-run the grep from Step 3.

- [ ] **Step 5: Run full suite to confirm no other tests broke**

Run: `uv run pytest --tb=short -q`
Expected: 491 passed (same as baseline).

- [ ] **Step 6: Commit**

```bash
git add src/shared/parsers/_mappings.py \
        src/shared/parsers/optima/bunnings_billing.py \
        tests/unit/parsers/optima/test_bunnings_billing.py
git commit -m "refactor: extract nem12_mappings loader into shared parsers/_mappings.py

bunnings_billing.py now imports get_nem12_mappings() from the shared
helper instead of defining its own _get_nem12_mappings() and module-level
cache. The shared loader lives at parsers/_mappings.py (not under
parsers/optima/) because nem12_mappings.json is consumed by any parser
that wants direct-write access, not just Optima.

Test patch targets updated accordingly. Behaviour identical."
```

---

## Task 2: Demand parser — scaffold + filename/content gates

**Files:**
- Create: `src/shared/parsers/optima/demand.py`
- Create: `tests/unit/parsers/optima/conftest.py`
- Create: `tests/unit/parsers/optima/test_demand.py`

- [ ] **Step 1: Create the test conftest with the `write_demand_csv` factory fixture**

```python
# tests/unit/parsers/optima/conftest.py
"""Shared fixtures for parsers/optima tests."""

import pytest


@pytest.fixture
def write_demand_csv(tmp_path):
    """Factory fixture: write a synthetic Demand Profile CSV, return path."""

    def _write(filename="Bunnings_Demand_Profile.csv", rows=None, body_override=None):
        csv_path = tmp_path / filename
        if body_override is not None:
            csv_path.write_text(body_override)
            return csv_path

        rows = (
            rows
            if rows is not None
            else [
                ("4001260599", "01-Feb-2026 00:00:00", "5.24", "10.48", "10.48", "1.0000"),
                ("4001260599", "01-Feb-2026 00:30:00", "5.21", "10.42", "10.42", "1.0000"),
                ("4001260599", "01-Feb-2026 05:30:00", "29.56", "59.12", "67.18", "0.8800"),
            ]
        )
        body_lines = [
            'Commodities:,"Electricity"',
            'Sites (NMIs):,"4001260599"',
            'Status:,"Active"',
            'Country:, Australia',
            'Start:,01-Feb-2026',
            'End:,30-Apr-2026',
            "",
            "",
            "Business Unit,Identifier,Identifier Type,ReadingDateTime,E,kW,kVa,Power Factor,Site Name",
        ]
        for nmi, ts, e, kw, kva, pf in rows:
            body_lines.append(
                f"Bunnings Australia,{nmi},NMI,{ts},{e},{kw},{kva},{pf},BUN AUS Forbes"
            )
        csv_path.write_text("\n".join(body_lines))
        return csv_path

    return _write
```

- [ ] **Step 2: Create `tests/unit/parsers/optima/test_demand.py` with the first three (gate) tests**

```python
"""Tests for shared.parsers.optima.demand.demand_parser."""

import pytest

from shared.parsers.optima.demand import demand_parser


class TestFilenameGate:
    def test_rejects_non_demand_files(self, write_demand_csv):
        path = write_demand_csv(filename="Bunnings_Interval_Usage.csv")
        with pytest.raises(Exception, match="Not a Demand Profile"):
            demand_parser(str(path), "/tmp/err.log")

    def test_accepts_lowercase_user_download(self, write_demand_csv):
        # The user's manual download is named "Bunnings demand profile.csv"
        # (lowercase). Must accept this casing.
        path = write_demand_csv(filename="Bunnings demand profile.csv")
        # Should NOT raise on filename gate; will fall through to other logic
        # (and succeed or fail there based on content). For this gate test,
        # we just assert no filename-gate exception.
        try:
            demand_parser(str(path), "/tmp/err.log")
        except Exception as e:
            assert "filename mismatch" not in str(e), (
                f"Filename gate rejected lowercase: {e}"
            )


class TestContentGate:
    def test_rejects_files_without_commodities_header(self, write_demand_csv):
        # Filename matches but content doesn't start with "Commodities:"
        path = write_demand_csv(
            filename="Bunnings_Demand_Profile.csv",
            body_override="Wrong,Header\nfoo,bar\n",
        )
        with pytest.raises(Exception, match="missing metadata header"):
            demand_parser(str(path), "/tmp/err.log")
```

- [ ] **Step 3: Run the tests to verify they fail (parser doesn't exist yet)**

Run: `uv run pytest tests/unit/parsers/optima/test_demand.py -v 2>&1 | tail -15`
Expected: FAIL with `ModuleNotFoundError: No module named 'shared.parsers.optima.demand'`

- [ ] **Step 4: Create `src/shared/parsers/optima/demand.py` skeleton with gates**

```python
"""Optima/BidEnergy "Demand Profile" CSV parser.

Persists three columns per interval per NMI:
  - kW           → sensor Optima_<NMI>-demand-kw,  unit "kw"
  - kVa          → sensor Optima_<NMI>-demand-kva, unit "kva"
  - Power Factor → sensor Optima_<NMI>-demand-pf,  unit ""  (dimensionless)

Like bunnings_billing_parser, this writes Hudi rows directly to
s3://hudibucketsrc/sensorDataFiles/ and returns [] to the dispatcher;
file_processor's channel-suffix gate would otherwise drop non-NEM12
column names like "kw"/"kva"/"pf".
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from aws_lambda_powertools import Logger

if TYPE_CHECKING:
    from shared.parsers import ParserResult

logger = Logger(service="optima-demand-parser", child=True)


def demand_parser(file_name: str, error_file_path: str) -> ParserResult:
    # 1. Fast filename reject (no I/O) — case-insensitive
    if "demand profile" not in Path(file_name).name.lower():
        raise Exception("Not a Demand Profile file (filename mismatch)")

    # 2. Content sniff (read first line only)
    with open(file_name, encoding="utf-8") as f:
        first_line = f.readline()
    if not first_line.startswith("Commodities:"):
        raise Exception("Not a Demand Profile file (missing metadata header)")

    # TODO: Tasks 3-4 fill in the body
    return []
```

- [ ] **Step 5: Run the tests to verify they now pass**

Run: `uv run pytest tests/unit/parsers/optima/test_demand.py -v 2>&1 | tail -15`
Expected: 3 passed.

- [ ] **Step 6: Run full suite for no regressions**

Run: `uv run pytest --tb=short -q`
Expected: 491 + 3 = 494 passed.

- [ ] **Step 7: Commit**

```bash
git add src/shared/parsers/optima/demand.py \
        tests/unit/parsers/optima/conftest.py \
        tests/unit/parsers/optima/test_demand.py
git commit -m "feat: scaffold demand_parser with filename + content gates

Adds skeleton parser that rejects non-Demand-Profile files via
case-insensitive filename substring + first-line 'Commodities:' content
sniff. Body returns [] for now; later tasks fill in CSV parsing,
mappings lookup, and Hudi write."
```

---

## Task 3: Demand parser — CSV parsing with "No data found" handling

**Files:**
- Modify: `src/shared/parsers/optima/demand.py` (add `_parse_demand_rows`, `CSV_FIELD_MAPPING`)
- Modify: `tests/unit/parsers/optima/test_demand.py` (add 2 more tests)

- [ ] **Step 1: Add the failing tests**

Append to `tests/unit/parsers/optima/test_demand.py`:

```python
class TestNoDataFoundSentinel:
    def test_no_data_found_returns_empty_list_no_exception(self, write_demand_csv):
        # BidEnergy returns this sentinel for sites with no demand data
        # (verified against NZ Bunnings sites 2026-05-05).
        body = (
            'Commodities:,"Electricity"\r\n'
            'Sites (NMIs):,"0000005438UN02B"\r\n'
            'Status:,"Active"\r\n'
            'Country:, New Zealand\r\n'
            'Start:,01-May-2026\r\n'
            'End:,03-May-2026\r\n'
            "\r\n"
            "\r\n"
            "No data found"
        )
        path = write_demand_csv(filename="NZ demand profile.csv", body_override=body)
        result = demand_parser(str(path), "/tmp/err.log")
        assert result == []


class TestEmptyData:
    def test_header_only_returns_empty_list(self, write_demand_csv):
        # File has the column header but zero data rows
        path = write_demand_csv(filename="Bunnings_Demand_Profile.csv", rows=[])
        result = demand_parser(str(path), "/tmp/err.log")
        assert result == []
```

- [ ] **Step 2: Run tests to verify the no-data test fails**

Run: `uv run pytest tests/unit/parsers/optima/test_demand.py -v 2>&1 | tail -15`
Expected: 3 still pass; the 2 new tests behaviour:
- `test_no_data_found_returns_empty_list_no_exception` may FAIL because the parser body currently doesn't handle the "No data found" sentinel — the file lacks a column header so DictReader will raise (or return empty).
- `test_header_only_returns_empty_list` should already PASS because parser stub returns [].

The exact failure mode depends on the stub. The point: this step exposes the gap in CSV parsing. Both tests should pass after Step 3.

- [ ] **Step 3: Add CSV parsing helpers and field map to `src/shared/parsers/optima/demand.py`**

Add these at module level (above the `demand_parser` function), and update the function body:

```python
"""Optima/BidEnergy "Demand Profile" CSV parser.

Persists three columns per interval per NMI:
  - kW           → sensor Optima_<NMI>-demand-kw,  unit "kw"
  - kVa          → sensor Optima_<NMI>-demand-kva, unit "kva"
  - Power Factor → sensor Optima_<NMI>-demand-pf,  unit ""  (dimensionless)

Like bunnings_billing_parser, this writes Hudi rows directly to
s3://hudibucketsrc/sensorDataFiles/ and returns [] to the dispatcher;
file_processor's channel-suffix gate would otherwise drop non-NEM12
column names like "kw"/"kva"/"pf".
"""

from __future__ import annotations

import csv
import io
from pathlib import Path
from typing import TYPE_CHECKING

from aws_lambda_powertools import Logger

if TYPE_CHECKING:
    from shared.parsers import ParserResult

logger = Logger(service="optima-demand-parser", child=True)

# (CSV column name, demand suffix in nem12_id, Hudi unit string)
CSV_FIELD_MAPPING: list[tuple[str, str, str]] = [
    ("kW", "kw", "kw"),
    ("kVa", "kva", "kva"),  # BidEnergy's actual capitalisation, not standard kVA
    ("Power Factor", "pf", ""),  # Dimensionless ratio
]


def _parse_demand_rows(file_path: str) -> list[dict[str, str]]:
    """Skip metadata rows, return data rows as DictReader dicts.

    Layout:
      Row 1-6: metadata key:value pairs (Commodities/Sites/Status/Country/Start/End)
      Row 7-8: blank
      Row 9: column header
      Row 10+: data    OR a single "No data found" sentinel line

    Returns [] if the file is the empty-data sentinel form.
    """
    with open(file_path, encoding="utf-8") as f:
        lines = f.read().splitlines()

    # Empty-data sentinel: BidEnergy returns "No data found" instead of
    # column header + data rows when a site has no demand profile data.
    if any("No data found" in line for line in lines):
        return []

    data_section = "\n".join(lines[8:])  # row 9 onward (0-indexed 8)
    reader = csv.DictReader(io.StringIO(data_section))
    return [row for row in reader if row.get("Identifier")]


def demand_parser(file_name: str, error_file_path: str) -> ParserResult:
    # 1. Fast filename reject (no I/O) — case-insensitive
    if "demand profile" not in Path(file_name).name.lower():
        raise Exception("Not a Demand Profile file (filename mismatch)")

    # 2. Content sniff (read first line only)
    with open(file_name, encoding="utf-8") as f:
        first_line = f.readline()
    if not first_line.startswith("Commodities:"):
        raise Exception("Not a Demand Profile file (missing metadata header)")

    # 3. Parse data rows; short-circuit on no-data sentinel or empty
    rows = _parse_demand_rows(file_name)
    if not rows:
        logger.info("demand_no_rows_to_process", extra={"file": file_name})
        return []

    # TODO: Tasks 4-5 implement mapping lookup + Hudi write
    return []
```

- [ ] **Step 4: Run tests to verify all 5 now pass**

Run: `uv run pytest tests/unit/parsers/optima/test_demand.py -v 2>&1 | tail -15`
Expected: 5 passed.

- [ ] **Step 5: Run full suite**

Run: `uv run pytest --tb=short -q`
Expected: 491 + 5 = 496 passed.

- [ ] **Step 6: Commit**

```bash
git add src/shared/parsers/optima/demand.py \
        tests/unit/parsers/optima/test_demand.py
git commit -m "feat: parse demand profile CSV body, handle no-data-found sentinel

Adds _parse_demand_rows() that skips the 8-row metadata+blank header
and returns DictReader dicts. Detects BidEnergy's 'No data found'
sentinel (returned for sites with no demand data, e.g., NZ Bunnings)
and short-circuits with [] — no exception, no S3 write.

CSV_FIELD_MAPPING declares the three persisted columns and their
unit strings (kW→kw, kVa→kva, Power Factor→empty)."
```

---

## Task 4: Demand parser — mapping lookup + Hudi write

**Files:**
- Modify: `src/shared/parsers/optima/demand.py`
- Modify: `tests/unit/parsers/optima/test_demand.py` (add 3 more tests)

- [ ] **Step 1: Add the three remaining behaviour tests**

Append to `tests/unit/parsers/optima/test_demand.py`:

```python
from datetime import datetime
from unittest.mock import patch

from shared.parsers import _mappings as mappings_mod


@pytest.fixture
def _reset_mappings_cache():
    """Clear the shared mappings cache before and after each test."""
    mappings_mod._cache = None
    yield
    mappings_mod._cache = None


class TestMappingLookupAndHudiWrite:
    def test_writes_kw_kva_pf_with_correct_sensor_ids(
        self, write_demand_csv, monkeypatch, _reset_mappings_cache
    ):
        # Arrange: synthetic mappings for the test NMI
        fake_mappings = {
            "Optima_4001260599-demand-kw": "p:bunnings:test-kw-id",
            "Optima_4001260599-demand-kva": "p:bunnings:test-kva-id",
            "Optima_4001260599-demand-pf": "p:bunnings:test-pf-id",
        }
        monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: fake_mappings)

        captured = {}

        def fake_put_object(**kwargs):
            captured["bucket"] = kwargs["Bucket"]
            captured["key"] = kwargs["Key"]
            captured["body"] = kwargs["Body"].decode()
            return {"ETag": "fake-etag"}

        with patch("shared.parsers.optima.demand.boto3.client") as mock_client:
            mock_client.return_value.put_object = fake_put_object
            path = write_demand_csv()
            result = demand_parser(str(path), "/tmp/err.log")

        # Assert: parser returned [] (signals dispatcher to not flow DataFrames)
        assert result == []

        # Assert: S3 PUT happened to the right place
        assert captured["bucket"] == "hudibucketsrc"
        assert captured["key"].startswith("sensorDataFiles/demand_export_")
        assert captured["key"].endswith(".csv")

        # Assert: 3 rows of input × 3 columns each = 9 Hudi rows
        body_lines = captured["body"].strip().split("\n")
        assert body_lines[0] == "sensorId,ts,val,unit,its,quality"
        data_lines = body_lines[1:]
        assert len(data_lines) == 9

        # Assert: each sensor ID appears 3 times (one per input row)
        assert sum(1 for L in data_lines if L.startswith("p:bunnings:test-kw-id,")) == 3
        assert sum(1 for L in data_lines if L.startswith("p:bunnings:test-kva-id,")) == 3
        assert sum(1 for L in data_lines if L.startswith("p:bunnings:test-pf-id,")) == 3

    def test_pf_unit_is_empty_string(
        self, write_demand_csv, monkeypatch, _reset_mappings_cache
    ):
        fake_mappings = {
            "Optima_4001260599-demand-kw": "p:bunnings:kw",
            "Optima_4001260599-demand-kva": "p:bunnings:kva",
            "Optima_4001260599-demand-pf": "p:bunnings:pf",
        }
        monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: fake_mappings)

        captured_body = []

        def fake_put_object(**kwargs):
            captured_body.append(kwargs["Body"].decode())
            return {"ETag": "fake-etag"}

        with patch("shared.parsers.optima.demand.boto3.client") as mock_client:
            mock_client.return_value.put_object = fake_put_object
            path = write_demand_csv()
            demand_parser(str(path), "/tmp/err.log")

        body = captured_body[0]
        # Find a PF row: its unit field (4th CSV column) must be empty
        pf_lines = [L for L in body.split("\n") if L.startswith("p:bunnings:pf,")]
        assert len(pf_lines) == 3
        for line in pf_lines:
            fields = line.split(",")
            # CSV: sensorId, ts, val, unit, its, quality
            assert fields[3] == "", f"PF unit should be empty string, got {fields[3]!r}"

    def test_unmapped_nmis_skipped_silently(
        self, write_demand_csv, monkeypatch, _reset_mappings_cache
    ):
        # Mappings only contain kw — kva and pf will be unmapped
        fake_mappings = {
            "Optima_4001260599-demand-kw": "p:bunnings:only-kw",
        }
        monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: fake_mappings)

        captured_body = []

        def fake_put_object(**kwargs):
            captured_body.append(kwargs["Body"].decode())
            return {"ETag": "fake-etag"}

        with patch("shared.parsers.optima.demand.boto3.client") as mock_client:
            mock_client.return_value.put_object = fake_put_object
            path = write_demand_csv()
            demand_parser(str(path), "/tmp/err.log")

        body = captured_body[0]
        data_lines = [L for L in body.strip().split("\n")[1:] if L]
        # 3 input rows × 1 mapped column = 3 Hudi rows
        assert len(data_lines) == 3
        assert all(L.startswith("p:bunnings:only-kw,") for L in data_lines)
```

- [ ] **Step 2: Run tests to verify they fail (Hudi write not yet implemented)**

Run: `uv run pytest tests/unit/parsers/optima/test_demand.py -v 2>&1 | tail -25`
Expected: 5 still pass; 3 new tests FAIL because parser still returns `[]` without writing.

- [ ] **Step 3: Implement the body in `src/shared/parsers/optima/demand.py`**

Replace the entire file contents with the final form:

```python
"""Optima/BidEnergy "Demand Profile" CSV parser.

Persists three columns per interval per NMI:
  - kW           → sensor Optima_<NMI>-demand-kw,  unit "kw"
  - kVa          → sensor Optima_<NMI>-demand-kva, unit "kva"
  - Power Factor → sensor Optima_<NMI>-demand-pf,  unit ""  (dimensionless)

Like bunnings_billing_parser, this writes Hudi rows directly to
s3://hudibucketsrc/sensorDataFiles/ and returns [] to the dispatcher;
file_processor's channel-suffix gate would otherwise drop non-NEM12
column names like "kw"/"kva"/"pf".
"""

from __future__ import annotations

import csv
import io
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import boto3
from aws_lambda_powertools import Logger

from shared.parsers._mappings import get_nem12_mappings

if TYPE_CHECKING:
    from shared.parsers import ParserResult

logger = Logger(service="optima-demand-parser", child=True)

HUDI_BUCKET = "hudibucketsrc"
HUDI_PREFIX = "sensorDataFiles"

# (CSV column name, demand suffix in nem12_id, Hudi unit string)
CSV_FIELD_MAPPING: list[tuple[str, str, str]] = [
    ("kW", "kw", "kw"),
    ("kVa", "kva", "kva"),  # BidEnergy's actual capitalisation, not standard kVA
    ("Power Factor", "pf", ""),  # Dimensionless ratio
]


def _parse_demand_rows(file_path: str) -> list[dict[str, str]]:
    """Skip metadata rows, return data rows as DictReader dicts.

    Layout:
      Row 1-6: metadata key:value pairs (Commodities/Sites/Status/Country/Start/End)
      Row 7-8: blank
      Row 9: column header
      Row 10+: data    OR a single "No data found" sentinel line

    Returns [] if the file is the empty-data sentinel form.
    """
    with open(file_path, encoding="utf-8") as f:
        lines = f.read().splitlines()

    if any("No data found" in line for line in lines):
        return []

    data_section = "\n".join(lines[8:])  # row 9 onward (0-indexed 8)
    reader = csv.DictReader(io.StringIO(data_section))
    return [row for row in reader if row.get("Identifier")]


def _build_hudi_csv(rows: list[dict[str, str]], mappings: dict) -> tuple[str, int, int]:
    """Build the Hudi CSV body and return (body, rows_written, unmapped_count).

    locale note: %b (abbreviated month name) is locale-dependent. AWS Lambda
    Python runtime defaults to en_US.UTF-8 / C.UTF-8, where %b matches "Feb",
    "Mar", etc. Local dev environments using non-English locales would fail
    parsing — if this becomes a problem, switch to an explicit dict mapping.
    """
    buf = io.StringIO()
    buf.write("sensorId,ts,val,unit,its,quality\n")
    rows_written = 0
    unmapped_count = 0

    for row in rows:
        nmi = (row.get("Identifier") or "").strip()
        raw_ts = (row.get("ReadingDateTime") or "").strip()
        if not nmi or not raw_ts:
            continue
        try:
            ts = datetime.strptime(raw_ts, "%d-%b-%Y %H:%M:%S")
        except ValueError:
            logger.warning("demand_bad_timestamp", extra={"nmi": nmi, "raw_ts": raw_ts})
            continue
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")

        for csv_col, suffix, unit in CSV_FIELD_MAPPING:
            raw_val = (row.get(csv_col) or "").strip()
            if not raw_val:
                continue
            try:
                val = float(raw_val)
            except ValueError:
                continue

            sensor_id = mappings.get(f"Optima_{nmi}-demand-{suffix}")
            if not sensor_id:
                unmapped_count += 1
                continue

            # Hudi format: sensorId,ts,val,unit,its,quality
            buf.write(f"{sensor_id},{ts_str},{val},{unit},{ts_str},\n")
            rows_written += 1

    return buf.getvalue(), rows_written, unmapped_count


def demand_parser(file_name: str, error_file_path: str) -> ParserResult:
    # 1. Fast filename reject (no I/O) — case-insensitive
    if "demand profile" not in Path(file_name).name.lower():
        raise Exception("Not a Demand Profile file (filename mismatch)")

    # 2. Content sniff (read first line only)
    with open(file_name, encoding="utf-8") as f:
        first_line = f.readline()
    if not first_line.startswith("Commodities:"):
        raise Exception("Not a Demand Profile file (missing metadata header)")

    # 3. Parse data rows; short-circuit on no-data sentinel or empty
    rows = _parse_demand_rows(file_name)
    if not rows:
        logger.info("demand_no_rows_to_process", extra={"file": file_name})
        return []

    # 4. Build Hudi CSV using cached nem12 mappings
    mappings = get_nem12_mappings()
    body, rows_written, unmapped_count = _build_hudi_csv(rows, mappings)

    if rows_written == 0:
        logger.info(
            "demand_no_rows_written",
            extra={"file": file_name, "unmapped": unmapped_count},
        )
        return []

    # 5. Upload Hudi CSV directly to S3 (bypasses file_processor channel gate)
    ts_key = datetime.now(UTC).strftime("%Y%m%d%H%M%S%f")
    key = f"{HUDI_PREFIX}/demand_export_{ts_key}.csv"
    boto3.client("s3").put_object(
        Bucket=HUDI_BUCKET,
        Key=key,
        Body=body.encode(),
    )
    logger.info(
        "demand_written",
        extra={"key": key, "rows": rows_written, "unmapped": unmapped_count},
    )
    return []
```

- [ ] **Step 4: Run tests to verify all 8 now pass**

Run: `uv run pytest tests/unit/parsers/optima/test_demand.py -v 2>&1 | tail -25`
Expected: 8 passed.

If `test_pf_unit_is_empty_string` fails — verify the float-to-string conversion preserves trailing zeros / format. Should output `1.0` not `1.0000` (Python's `float()` then `str()` round-trip drops trailing zeros). The CSV writer uses `f"{val},"` so trailing zeros are NOT preserved — that's intentional, Hudi stores them as numerics.

- [ ] **Step 5: Run full suite**

Run: `uv run pytest --tb=short -q`
Expected: 491 + 8 = 499 passed.

- [ ] **Step 6: Commit**

```bash
git add src/shared/parsers/optima/demand.py \
        tests/unit/parsers/optima/test_demand.py
git commit -m "feat: implement demand_parser mapping lookup and Hudi S3 write

Resolves Optima_<NMI>-demand-{kw,kva,pf} sensor IDs from the shared
nem12_mappings cache, builds a Hudi-format CSV (sensorId,ts,val,unit,
its,quality), and uploads to s3://hudibucketsrc/sensorDataFiles/.
Returns [] so the dispatcher routes the source file to newIrrevFiles/
(matches bunnings_billing_parser's flow).

Unmapped NMIs are skipped silently with the per-file unmapped_count
visible in the demand_written log line. Power Factor rows have an
empty unit string (dimensionless)."
```

---

## Task 5: Wire `demand_parser` into the dispatcher

**Files:**
- Modify: `src/shared/non_nem_parsers.py`
- Modify: `tests/unit/parsers/optima/test_demand.py` (add 1 dispatcher integration test)

- [ ] **Step 1: Add the failing dispatcher test**

Append to `tests/unit/parsers/optima/test_demand.py`:

```python
class TestDispatcherIntegration:
    def test_dispatcher_routes_demand_file(
        self, write_demand_csv, monkeypatch, _reset_mappings_cache
    ):
        from shared.non_nem_parsers import get_non_nem_df

        fake_mappings = {
            "Optima_4001260599-demand-kw": "p:bunnings:kw",
            "Optima_4001260599-demand-kva": "p:bunnings:kva",
            "Optima_4001260599-demand-pf": "p:bunnings:pf",
        }
        monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: fake_mappings)

        with patch("shared.parsers.optima.demand.boto3.client") as mock_client:
            mock_client.return_value.put_object.return_value = {"ETag": "fake"}
            path = write_demand_csv()
            result = get_non_nem_df(str(path), "/tmp/err.log")

        # Demand parser returns [], so the dispatcher returns [] too
        assert result == []
        # And the parser actually fired (not just dispatcher's no-parser-found path):
        # the boto3 mock was called means demand_parser ran.
        assert mock_client.called
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/unit/parsers/optima/test_demand.py::TestDispatcherIntegration -v 2>&1 | tail -10`
Expected: FAIL — dispatcher tries other parsers first, none match, eventually raises "No Valid Parser Found".

- [ ] **Step 3: Wire `demand_parser` into `src/shared/non_nem_parsers.py`**

Find the existing imports block in `src/shared/non_nem_parsers.py` and add (alphabetic position):

```python
from shared.parsers.optima.demand import demand_parser
```

Find the `parsers = [...]` list and add `demand_parser` between `bunnings_billing_parser` and `interval_parser`:

```python
def get_non_nem_df(file_name: str, error_file_path: str) -> ParserResult:
    parsers = [
        noosa_solar_parser,
        envizi_vertical_parser_water,
        envizi_vertical_parser_electricity,
        racv_elec_parser,
        racv_billing_parser,
        bunnings_billing_parser,
        demand_parser,                    # NEW — placed near other Optima report parsers
        interval_parser,
        envizi_vertical_parser_water_bulk,
        green_square_private_wire_schneider_comx_parser,
    ]
    ...
```

- [ ] **Step 4: Run the dispatcher test to verify it passes**

Run: `uv run pytest tests/unit/parsers/optima/test_demand.py::TestDispatcherIntegration -v 2>&1 | tail -10`
Expected: PASS.

- [ ] **Step 5: Run full suite**

Run: `uv run pytest --tb=short -q`
Expected: 491 + 9 = 500 passed.

- [ ] **Step 6: Commit**

```bash
git add src/shared/non_nem_parsers.py \
        tests/unit/parsers/optima/test_demand.py
git commit -m "feat: wire demand_parser into the non_nem_parsers dispatcher

Placed between bunnings_billing_parser and interval_parser to keep
BidEnergy report parsers grouped. Order doesn't affect behaviour —
each parser fast-fails on filename/content mismatch."
```

---

## Task 6: `scripts/generate_demand_points.py` — DynamoDB scan + Neptune lookup

**Files:**
- Create: `scripts/generate_demand_points.py`

This script is operator tooling, not Lambda code. It runs locally (or in CI) with `geg` AWS profile credentials.

- [ ] **Step 1: Create the script**

```python
#!/usr/bin/env python3
"""Generate data/demand_points.csv for Bunnings Optima sites.

Scans DynamoDB sbm-optima-config for Bunnings sites with NMIs starting with
'Optima_', then queries Neptune to find each site's meter_vertex_id by
walking the equipRef edge from any existing E1 (or B1 fallback) point.

Output: data/demand_points.csv with 3 rows per NMI (kw/kva/pf), suitable
for input to scripts/import_demand_points.py.

Usage:
    PYTHONPATH=src uv run scripts/generate_demand_points.py \\
        --output data/demand_points.csv \\
        [--project bunnings] [--dry-run]
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter
from pathlib import Path

import boto3

from scripts.billing_neptune_helper import gremlin_query

AWS_PROFILE = "geg"
AWS_REGION = "ap-southeast-2"
DYNAMODB_TABLE = "sbm-optima-config"

DEMAND_FIELDS = [
    # (field_short, suffix_in_nem12_id, label_descriptor)
    ("kw", "kw", "Demand kW"),
    ("kva", "kva", "Demand kVA"),
    ("pf", "pf", "Demand Power Factor"),
]


def scan_bunnings_optima_sites() -> list[dict]:
    """Scan DynamoDB for Bunnings Optima sites."""
    session = boto3.Session(profile_name=AWS_PROFILE)
    ddb = session.client("dynamodb", region_name=AWS_REGION)

    items: list[dict] = []
    kwargs = {
        "TableName": DYNAMODB_TABLE,
        "ProjectionExpression": "nmi,country,#p",
        "ExpressionAttributeNames": {"#p": "project"},
    }
    while True:
        resp = ddb.scan(**kwargs)
        items.extend(resp["Items"])
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    bunnings_optima = [
        {
            "nmi_full": i["nmi"]["S"],
            "nmi_bare": i["nmi"]["S"].replace("Optima_", ""),
            "country": i["country"]["S"],
        }
        for i in items
        if i["project"]["S"] == "bunnings" and i["nmi"]["S"].startswith("Optima_")
    ]
    return bunnings_optima


def find_meter_vertex_id(nmi_full: str) -> tuple[str | None, str]:
    """Find the meter_vertex_id for an NMI.

    Returns (vertex_id, strategy) where strategy is one of 'E1', 'B1', or
    'missing'. Strategy A: walk equipRef from <nmi_full>-E1 point. Strategy B
    (fallback): try -B1 instead. None if neither works.
    """
    for channel in ("E1", "B1"):
        nem12_id = f"{nmi_full}-{channel}"
        # Escape single quotes — NMIs are alphanumeric so this is just defensive
        escaped = nem12_id.replace("'", "\\'")
        query = f"g.V().has('nem12Id', '{escaped}').in('equipRef').id().limit(1).toList()"
        result = gremlin_query(query)
        if result:
            return result[0], channel
    return None, "missing"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        default="data/demand_points.csv",
        help="Output CSV path (default: data/demand_points.csv)",
    )
    parser.add_argument(
        "--project",
        default="bunnings",
        help="DynamoDB project filter (default: bunnings)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip writing the output file; print the summary only",
    )
    args = parser.parse_args(argv)

    print("=" * 60)
    print("Generate Demand Points CSV")
    print("=" * 60)
    print(f"  Project: {args.project}")
    print(f"  Output:  {args.output}")
    print(f"  Mode:    {'DRY RUN' if args.dry_run else 'LIVE'}")
    print("=" * 60)

    print("\nScanning DynamoDB...")
    sites = scan_bunnings_optima_sites()
    print(f"Found {len(sites)} Bunnings Optima sites.")

    print("\nResolving meter_vertex_id from Neptune (this may take a few minutes)...")
    rows: list[dict] = []
    strategy_counter: Counter = Counter()
    missing_nmis: list[str] = []

    for i, site in enumerate(sites, 1):
        if i % 50 == 0:
            print(f"  Processed {i}/{len(sites)}...")
        meter_id, strategy = find_meter_vertex_id(site["nmi_full"])
        strategy_counter[strategy] += 1
        if meter_id is None:
            missing_nmis.append(site["nmi_bare"])

        for field_short, suffix, label_desc in DEMAND_FIELDS:
            rows.append(
                {
                    "identifier": site["nmi_bare"],
                    "field": field_short,
                    "nem12_id": f"{site['nmi_full']}-demand-{suffix}",
                    "label": f"{site['nmi_bare']} {label_desc}",
                    "point_category": "demand",
                    "meter_vertex_id": meter_id or "",
                }
            )

    print(f"\nNeptune lookup summary:")
    print(f"  Found via E1: {strategy_counter['E1']}")
    print(f"  Found via B1: {strategy_counter['B1']}")
    print(f"  Missing:      {strategy_counter['missing']}")
    if missing_nmis:
        print(f"  Missing NMIs: {', '.join(sorted(missing_nmis)[:10])}{'...' if len(missing_nmis) > 10 else ''}")

    print(f"\nTotal rows: {len(rows)} (= {len(sites)} NMIs × 3 fields)")
    print(f"Rows with empty meter_vertex_id: {strategy_counter['missing'] * 3}")

    if args.dry_run:
        print("\n[DRY RUN] Skipping CSV write.")
        return 0

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["identifier", "field", "nem12_id", "label", "point_category", "meter_vertex_id"]
    with output_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n✓ Wrote {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Verify the script's `--help` works (no execution against AWS yet)**

Run: `PYTHONPATH=src uv run scripts/generate_demand_points.py --help 2>&1 | head -25`
Expected: argparse help text, no traceback.

- [ ] **Step 3: Verify ruff passes**

Run: `uv run ruff check scripts/generate_demand_points.py`
Expected: `All checks passed!`

- [ ] **Step 4: Commit**

```bash
git add scripts/generate_demand_points.py
git commit -m "chore: add scripts/generate_demand_points.py to produce data/demand_points.csv

Scans DynamoDB sbm-optima-config for Bunnings Optima sites (~477) and
walks the equipRef edge from each NMI's E1 (or B1 fallback) point to
find the meter_vertex_id. Outputs 1431 rows (3 fields × 477 NMIs).

Sites without E1/B1 mappings get empty meter_vertex_id; the import
script skips them and logs as orphans for follow-up."
```

---

## Task 7: `scripts/import_demand_points.py` — Neptune insert with idempotency

**Files:**
- Create: `scripts/import_demand_points.py`

- [ ] **Step 1: Create the script**

This mirrors `scripts/import_billing_points.py` — copy its structure verbatim, then change three things:
1. `pointCategory` from `'billing'` to `'demand'`.
2. Skip rows with empty `meter_vertex_id` (and log them).
3. Default output path to `data/demand_point_ids.csv`.

```python
#!/usr/bin/env python3
"""
Import demand points into Neptune from a CSV file.

Reads data/demand_points.csv and creates Neptune point vertices with equipRef
edges to their parent meter vertices. Each point is created idempotently
using nem12Id as the deduplication key.

Rows with empty meter_vertex_id are skipped and logged as orphans (the
NMI's meter vertex doesn't exist in Neptune yet — manual investigation
needed).

Usage:
    PYTHONPATH=src uv run scripts/import_demand_points.py --csv data/demand_points.csv --dry-run
    PYTHONPATH=src uv run scripts/import_demand_points.py --csv data/demand_points.csv
"""

from __future__ import annotations

import argparse
import csv
import secrets
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from scripts.billing_neptune_helper import gremlin_query

EXISTENCE_CHECK_BATCH_SIZE = 200


def generate_point_id() -> str:
    """Generate a Neptune point ID matching existing convention.

    Format: p:bunnings:{hex_timestamp}-{hex_random}
    """
    hex_ts = format(int(time.time() * 1000), "x")
    hex_rand = secrets.token_hex(3)
    return f"p:bunnings:{hex_ts}-{hex_rand}"


def batch_check_existing_nem12_ids(nem12_ids: list[str]) -> set[str]:
    """Batch check which nem12Ids already exist in Neptune."""
    existing: set[str] = set()
    for i in range(0, len(nem12_ids), EXISTENCE_CHECK_BATCH_SIZE):
        chunk = nem12_ids[i : i + EXISTENCE_CHECK_BATCH_SIZE]
        id_list = "[" + ",".join(f"'{nid}'" for nid in chunk) + "]"
        try:
            query = f"g.V().has('nem12Id', within({id_list})).values('nem12Id').toList()"
            result = gremlin_query(query)
            existing.update(result)
        except Exception as e:
            print(f"  WARN: batch existence check failed ({e}); falling back to per-id checks")
            for nid in chunk:
                try:
                    single_query = f"g.V().has('nem12Id', '{nid}').hasNext()"
                    if gremlin_query(single_query):
                        existing.add(nid)
                except Exception:
                    pass
    return existing


_print_lock = threading.Lock()


def create_demand_point(
    point_id: str, label: str, nem12_id: str, meter_vertex_id: str
) -> bool:
    """Create a single demand point vertex with equipRef edge to its meter."""
    label_escaped = label.replace("'", "\\'")
    nem12_id_escaped = nem12_id.replace("'", "\\'")

    query = (
        f"g.addV('point')"
        f".property(id, '{point_id}')"
        f".property('label', '{label_escaped}')"
        f".property('nem12Id', '{nem12_id_escaped}')"
        f".property('pointCategory', 'demand')"
        f".as('pt')"
        f".V('{meter_vertex_id}')"
        f".addE('equipRef').from('pt')"
    )
    gremlin_query(query)
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", required=True, dest="csv_file", help="Path to demand_points.csv")
    parser.add_argument("--dry-run", action="store_true", help="Preview mode, no Neptune writes")
    parser.add_argument(
        "--output",
        default="data/demand_point_ids.csv",
        help="Output CSV mapping point_vertex_id to nem12_id (default: data/demand_point_ids.csv)",
    )
    parser.add_argument(
        "--workers", type=int, default=10, help="Number of parallel workers (default: 10)"
    )
    args = parser.parse_args(argv)

    print("=" * 60)
    print("Demand Points Neptune Import")
    print("=" * 60)
    print(f"  CSV File:  {args.csv_file}")
    print(f"  Mode:      {'DRY RUN' if args.dry_run else 'LIVE'}")
    print("=" * 60)
    print()

    print("Reading CSV...")
    with Path(args.csv_file).open() as f:
        reader = csv.DictReader(f)
        all_rows = list(reader)
    print(f"  Read {len(all_rows)} rows.")

    # Filter out orphans (empty meter_vertex_id)
    orphan_rows = [r for r in all_rows if not r.get("meter_vertex_id", "").strip()]
    valid_rows = [r for r in all_rows if r.get("meter_vertex_id", "").strip()]
    print(f"  Valid rows: {len(valid_rows)}")
    print(f"  Orphan rows (empty meter_vertex_id, skipped): {len(orphan_rows)}")
    if orphan_rows:
        orphan_nmis = sorted({r["identifier"] for r in orphan_rows})
        print(
            f"  Orphan NMIs ({len(orphan_nmis)}): "
            f"{', '.join(orphan_nmis[:10])}{'...' if len(orphan_nmis) > 10 else ''}"
        )

    print("\nBatch-checking existing nem12Ids in Neptune...")
    nem12_ids = [r["nem12_id"] for r in valid_rows]
    existing = batch_check_existing_nem12_ids(nem12_ids)
    print(f"  Already exist: {len(existing)}")
    new_rows = [r for r in valid_rows if r["nem12_id"] not in existing]
    print(f"  To create:     {len(new_rows)}")

    if args.dry_run:
        print("\n[DRY RUN] Would create the above points. Exiting.")
        return 0

    if not new_rows:
        print("\n✓ Nothing to do — all nem12Ids already exist.")
        return 0

    print(f"\nCreating {len(new_rows)} points with {args.workers} workers...")
    created: list[tuple[str, str]] = []
    failed = 0

    def _create(row: dict) -> tuple[str, str] | None:
        try:
            point_id = generate_point_id()
            create_demand_point(
                point_id=point_id,
                label=row["label"],
                nem12_id=row["nem12_id"],
                meter_vertex_id=row["meter_vertex_id"],
            )
            return point_id, row["nem12_id"]
        except Exception as e:
            with _print_lock:
                print(f"  FAIL nem12Id={row['nem12_id']}: {e}")
            return None

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(_create, row) for row in new_rows]
        for i, fut in enumerate(as_completed(futures), 1):
            result = fut.result()
            if result:
                created.append(result)
            else:
                failed += 1
            if i % 100 == 0:
                with _print_lock:
                    print(f"  Progress: {i}/{len(new_rows)} (created={len(created)}, failed={failed})")

    print(f"\n✓ Created {len(created)} points; {failed} failed.")

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["point_vertex_id", "nem12_id"])
        writer.writerows(created)
    print(f"✓ Wrote mapping to {output_path}")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Verify `--help` works**

Run: `PYTHONPATH=src uv run scripts/import_demand_points.py --help 2>&1 | head -25`
Expected: argparse help text.

- [ ] **Step 3: Verify ruff passes**

Run: `uv run ruff check scripts/import_demand_points.py`
Expected: `All checks passed!`

- [ ] **Step 4: Commit**

```bash
git add scripts/import_demand_points.py
git commit -m "chore: add scripts/import_demand_points.py for Neptune point creation

Mirrors scripts/import_billing_points.py with three differences:
  - pointCategory='demand' (was 'billing')
  - skips rows with empty meter_vertex_id, logging them as orphans
  - default output path data/demand_point_ids.csv

Idempotent via existence check by nem12Id; safe to re-run after
partial completion. Caveat: stale meter_vertex_id (deleted-meter case)
is not detected — see spec."
```

---

## Task 8: Operational — generate CSV, dry run, live import

**This task creates Neptune state and is irreversible without manual cleanup. Run carefully.**

**Files (artifacts produced):**
- `data/demand_points.csv` (1431 rows, generated)
- `data/demand_point_ids.csv` (mapping, generated by import)
- Live: ~1431 new vertex records in Neptune

- [ ] **Step 1: Run the generator script**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
PYTHONPATH=src uv run scripts/generate_demand_points.py \
    --output data/demand_points.csv
```

Expected output:
```
Found 477 Bunnings Optima sites.
Resolving meter_vertex_id from Neptune (this may take a few minutes)...
  Processed 50/477...
  ...
Neptune lookup summary:
  Found via E1: ~468
  Found via B1: 0-N
  Missing:      ~9-N
  Missing NMIs: 3120787756, 3120898626, ...
Total rows: 1431
Rows with empty meter_vertex_id: ~27 (= ~9 × 3)
✓ Wrote data/demand_points.csv
```

If the "Found via E1" count differs significantly from 468, investigate — DynamoDB or Neptune data may have shifted.

- [ ] **Step 2: Inspect the generated CSV**

```bash
head -5 data/demand_points.csv
wc -l data/demand_points.csv      # expect 1432 (1 header + 1431 data)
awk -F, 'NR>1 && $6=="" {print $1}' data/demand_points.csv | sort -u | wc -l
# expect ~9 (orphan NMIs)
```

- [ ] **Step 3: Dry-run the importer**

```bash
PYTHONPATH=src uv run scripts/import_demand_points.py \
    --csv data/demand_points.csv --dry-run
```

Expected output:
```
Read 1431 rows.
  Valid rows: ~1404  (= 1431 - orphan_count)
  Orphan rows (empty meter_vertex_id, skipped): ~27
  Orphan NMIs: 3120787756, ...
Batch-checking existing nem12Ids in Neptune...
  Already exist: 0  (first run; should be 0)
  To create:     ~1404
[DRY RUN] Would create the above points. Exiting.
```

If "Already exist" is non-zero on first run, an earlier partial run left state — that's fine, idempotency handles it.

- [ ] **Step 4: Live import**

⚠️ This step writes ~1404 vertices to production Neptune. Confirm CSV looks right before proceeding.

```bash
PYTHONPATH=src uv run scripts/import_demand_points.py \
    --csv data/demand_points.csv
```

Expected: progress log every 100 points, final summary `✓ Created ~1404 points; 0 failed.` and `✓ Wrote mapping to data/demand_point_ids.csv`.

If `failed > 0`, inspect the failure log lines, fix the underlying issue (likely a stale `meter_vertex_id`), and re-run — idempotency means already-created points are skipped.

- [ ] **Step 5: Verify in Neptune**

```bash
# Quick sanity check via the Neptune helper (count of demand points):
PYTHONPATH=src uv run python -c "
from scripts.billing_neptune_helper import gremlin_query
result = gremlin_query(\"g.V().has('pointCategory', 'demand').count().toList()\")
print(f'Demand points in Neptune: {result}')
"
```

Expected: a count close to 1404 (created points). If this is significantly off, something went wrong — look at the import log.

- [ ] **Step 6: Commit the generated artifacts**

```bash
git add data/demand_points.csv data/demand_point_ids.csv
git commit -m "chore: generated data/demand_points.csv and demand_point_ids.csv

Output of:
  PYTHONPATH=src uv run scripts/generate_demand_points.py
  PYTHONPATH=src uv run scripts/import_demand_points.py --csv data/demand_points.csv

Live Neptune state now contains ~1404 new point vertices with
pointCategory='demand'. The next hourly run of the
sbm-files-ingester-nem12-mappings-to-s3 Lambda will export them to
nem12_mappings.json, after which demand_parser can resolve them."
```

---

## Task 9: End-to-end manual verification

**Files:** none modified (verification only).

- [ ] **Step 1: Wait for the next hourly mapping export**

```bash
# Check whether nem12_mappings.json contains demand keys yet:
aws s3 cp s3://sbm-file-ingester/nem12_mappings.json /tmp/mappings.json --profile geg --region ap-southeast-2 2>&1 | tail -2
python3 -c "
import json
with open('/tmp/mappings.json') as f:
    m = json.load(f)
demand = [k for k in m if '-demand-' in k]
print(f'Demand keys in mappings: {len(demand)}')
print('Examples:', demand[:5])
"
```

Expected: `Demand keys in mappings: ~1404` (after the hourly Lambda runs). If 0, wait until the next hour and re-check.

- [ ] **Step 2: Upload a real demand profile CSV to trigger the parser**

Use the user's sample file:

```bash
aws s3 cp "/Users/zeyu/Downloads/Bunnings demand profile.csv" \
    "s3://sbm-file-ingester/newTBP/Bunnings demand profile.csv" \
    --profile geg --region ap-southeast-2
```

This triggers the SQS-driven file_processor Lambda, which calls `get_non_nem_df`, which routes to `demand_parser`.

- [ ] **Step 3: Check parser logs**

```bash
# Tail recent CloudWatch logs for the file processor:
aws logs tail /aws/lambda/sbm-files-ingester --since 5m --profile geg --region ap-southeast-2 | grep -E "demand_(written|no_rows|bad_timestamp)"
```

Expected: a `"demand_written"` line with `rows: ~12000+` (4233 input rows × 3 fields, minus any unmapped).

- [ ] **Step 4: Verify Hudi data lake received the rows**

Trigger the Glue job that imports `sensorDataFiles/` into Hudi (or wait for its hourly trigger), then query:

```bash
# Quick query via Athena (use existing query patterns):
aws athena start-query-execution \
    --query-string "SELECT sensorid, COUNT(*) AS cnt, MIN(ts) AS min_ts, MAX(ts) AS max_ts FROM sensordata_default WHERE sensorid LIKE 'p:bunnings:%' AND unit IN ('kw', 'kva', '') AND ts > now() - INTERVAL '1' DAY GROUP BY sensorid LIMIT 10" \
    --query-execution-context '{"Database":"default"}' \
    --result-configuration '{"OutputLocation":"s3://sbm-file-ingester/athena-results/"}' \
    --profile geg --region ap-southeast-2
```

Get the query execution ID from output, then:

```bash
aws athena get-query-execution --query-execution-id <ID> --profile geg --region ap-southeast-2
# wait for SUCCEEDED, then:
aws athena get-query-results --query-execution-id <ID> --profile geg --region ap-southeast-2
```

Expected: rows with `unit IN ('kw', 'kva', '')` matching the demand sensor IDs. If empty, check:
- Did the Glue job run after the parser write? (Check `sensorDataFiles/demand_export_*.csv` in `s3://hudibucketsrc/`.)
- Did the parser actually write? (Look for `demand_written` log line in Step 3.)

- [ ] **Step 5: Verify the source file moved to `newIrrevFiles/`**

```bash
aws s3 ls "s3://sbm-file-ingester/newIrrevFiles/" --profile geg --region ap-southeast-2 | grep -i "demand profile"
```

Expected: the file `Bunnings demand profile.csv` appears here (or in `newIrrevFiles/archived/<week>/` if the weekly archiver has run since). This is the **expected** destination per the spec — demand_parser returns `[]`, so file_processor treats the file as having no Neptune-mapped rows and routes to `newIrrevFiles/`.

If the file is in `newP/` instead, something is wrong — check parser logs for an exception.

- [ ] **Step 6: Restore stashed WIP**

```bash
git stash pop
```

Verify the optima_exporter/interval_exporter and terraform changes are back:
```bash
git status --short
```

---

## Done

Total: 9 tasks, ~7 commits.

Verification:
- ✅ 491 + 9 new tests = **500 unit tests pass**, coverage stays ≥90%
- ✅ ruff clean
- ✅ ~1404 new Neptune `point` vertices with `pointCategory='demand'`
- ✅ End-to-end: real CSV → CloudWatch `demand_written` log → Hudi sensor rows queryable in Athena
- ✅ Bunnings billing parser still works (refactored to use shared `_mappings.py`, all 17 of its tests still pass)

The remaining work for end-to-end automation is **Step 4** (`demand_exporter` Lambda — separate PR, will mirror the in-flight `interval_exporter` pattern). Until that lands, demand CSVs must be downloaded manually from BidEnergy and uploaded to `s3://sbm-file-ingester/newTBP/` to trigger this parser.
