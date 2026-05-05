# Non-NEM Parsers Modularization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor `src/shared/non_nem_parsers.py` (211 lines, 7 bundled parsers + dispatcher) into a domain-organised `src/shared/parsers/` subpackage; rename 3 misleadingly-named functions; reorganise tests under `tests/unit/parsers/` mirroring the new source structure. Pure structural refactor — zero behaviour change.

**Architecture:** Per-platform subdirectory layout (`parsers/optima/`, `parsers/racv/`, `parsers/envizi/`, `parsers/green_square/`); each parser gets its own file with its own module-level `Logger`; the dispatcher (`get_non_nem_df`) stays at `shared.non_nem_parsers` for import-path stability; tests are split out of two bundled files into per-parser files matching the source structure.

**Tech Stack:** Python 3.13, uv, pytest, ruff, aws-lambda-powertools (Logger).

**Spec:** [`docs/superpowers/specs/2026-05-05-non-nem-parsers-modularization-design.md`](../specs/2026-05-05-non-nem-parsers-modularization-design.md)

**Working principle:** Move one parser per task. After each task, the dispatcher's `import` block reflects the new location of that one parser; the function disappears from `non_nem_parsers.py` (or `billing_parser.py` / `noosa_solar_parser.py`). Full test suite passes after every task. The 9 parser-move tasks (Tasks 2–10) are independent in principle but **must run sequentially** because each one mutates `non_nem_parsers.py`'s dispatcher imports.

**Pre-flight check before starting:**
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
git status --short          # confirm baseline state
uv sync --all-extras
uv run pytest --cov=src     # establish baseline: 525+ tests pass, coverage ≥90%
```

There is pre-existing uncommitted work in `src/functions/optima_exporter/interval_exporter/` and `terraform/optima_exporter.tf` that this plan must NOT touch. **Stash it before starting** so the working tree is clean:
```bash
git stash push -u -m "pre-parsers-refactor" -- src/functions/optima_exporter/interval_exporter/ terraform/optima_exporter.tf .gitignore
```
Pop it back at the end with `git stash pop`. This protects the unrelated WIP from being swept into the refactor's commits.

---

## Critical Execution Rules (apply to every task)

These rules override any per-task instruction that contradicts them. Read once before starting; refer back when each task feels ambiguous.

### R1: Extraction step order — never break the dispatcher mid-task

For Tasks 4–10 (extracting parsers from the bundled `non_nem_parsers.py`), execute in this order to keep the dispatcher importable AND callable at every checkpoint:

1. **Create** the new `src/shared/parsers/<X>/<Y>.py` with the function copied verbatim + `Logger` declaration + `from shared.parsers import ParserResult`.
2. **Add** the new `from shared.parsers.<X>.<Y> import <new_name>` import to `src/shared/non_nem_parsers.py`. **Swap** the bare name in the `parsers = [...]` list (so the list now references the new name, which resolves to the import you just added).
3. **Then delete** the old function body from `non_nem_parsers.py`.

Doing 3 before 2 leaves the dispatcher's `parsers = [...]` list referencing a name that doesn't resolve — `get_non_nem_df()` raises `NameError` at call time even though the module imports fine.

### R2: Mock-path migration — only the parser that LOGS gets its patches rewritten

Of the 7 parsers being extracted, **only `envizi_vertical_parser_water` calls `logger.<level>` from inside its body** (line 34 of the original `non_nem_parsers.py`). The other 6 parsers don't log — they just `raise`.

Tests for parsers that don't log themselves often `patch("shared.non_nem_parsers.logger")` to silence the **dispatcher's** swallow-all-failures debug log. Those patches target the dispatcher's logger, NOT the parser's. After moving the parser, those patches must STAY pointing at `shared.non_nem_parsers.logger` (the dispatcher hasn't moved).

Per-task rule:
- **Task 7 (envizi_vertical_parser_water):** Rewrite `shared.non_nem_parsers.logger` patches to `shared.parsers.envizi.vertical_water.logger` ONLY for tests that assert on log calls (`mock_logger.error.assert_called_once`, etc.). Leave dispatcher-silencing patches alone.
- **All other extraction tasks (4, 5, 6, 8, 9, 10):** Do NOT rewrite `shared.non_nem_parsers.logger` patches. Leave them pointing at the dispatcher's logger.

When in doubt, check what each `with patch(...)` block does:
- Asserts on the mock (`mock_log.error.assert_called`, etc.) → patches the parser's own logger → migrate to new path
- Just suppresses (no assertion on the mock object) → patches the dispatcher's logger → leave alone

### R3: Locate test extractions by function name, not by line number

Hard-coded line numbers go stale as earlier extractions delete content above them. Instead of "cut lines X–Y", use:

```bash
# Find the line ranges of test functions to extract (re-run before EACH task)
grep -nE "^def test_<func_name>|^class Test<Func>" tests/unit/test_non_nem_parsers.py
grep -nE "^def test_<func_name>|^class Test<Func>" tests/unit/test_non_nem_parsers_edge_cases.py
```

For each parser-extraction task, the test function names to find are predictable — they typically share the parser name as a substring. E.g., for `optima_parser`, grep for `test_optima_parser` (and any test class named `TestOptimaParser`). After locating, copy the relevant blocks (including any decorators and helper fixtures they reference) into the new test file.

### R4: Use explicit `git add <paths>`, never `git add -A`

The pre-flight check stashes unrelated WIP, so a stray `git add -A` would no longer sweep it. But to be safe — and because explicit adds also make the commit's intent visible — use explicit paths in every commit:

```bash
git add src/shared/parsers/<X>/<Y>.py \
        src/shared/non_nem_parsers.py \
        tests/unit/parsers/<X>/test_<Y>.py \
        tests/unit/test_non_nem_parsers.py \
        tests/unit/test_non_nem_parsers_edge_cases.py
git commit -m "..."
```

For `git mv` operations (Tasks 2, 3), `git add -A` is fine because the mv already staged the rename — no other changes can sneak in.

### R5: Handle multi-line imports

`tests/unit/test_non_nem_parsers.py:385` has a parenthesised multi-line import:
```python
from shared.non_nem_parsers import (
    envizi_vertical_parser_electricity,
    envizi_vertical_parser_water,
    optima_parser,
    ...
)
```

Before each extraction task that involves a name in this multi-line block, manually update the block — sed will not handle it cleanly. Replace each removed name with its new import path (potentially splitting the multi-line into multiple single-line imports if the names now come from different modules).

### R6: Do not `git push` until Task 13 completes

The lefthook pre-push hook enforces ≥90% coverage. Mid-refactor states may transiently dip if a parser's tests are temporarily orphaned during a step transition. Push only after Task 13 verifies the final state is clean.

### R7: When migrating standalone files, replace local `ParserResult` definition with subpackage import

Both `billing_parser.py` AND `noosa_solar_parser.py` define their own `ParserResult` at module scope (a duplicate of what `parsers/__init__.py` exports). When git-mv'ing each file, replace the local `ParserResult = list[tuple[str, pd.DataFrame]]` line with `from shared.parsers import ParserResult`. This avoids two parallel type aliases that could drift apart.

---

## Task 1: Create empty subpackage skeleton

**Files:**
- Create: `src/shared/parsers/__init__.py`
- Create: `src/shared/parsers/optima/__init__.py`
- Create: `src/shared/parsers/racv/__init__.py`
- Create: `src/shared/parsers/envizi/__init__.py`
- Create: `src/shared/parsers/green_square/__init__.py`
- Create: `tests/unit/parsers/__init__.py`
- Create: `tests/unit/parsers/optima/__init__.py`
- Create: `tests/unit/parsers/racv/__init__.py`
- Create: `tests/unit/parsers/envizi/__init__.py`
- Create: `tests/unit/parsers/green_square/__init__.py`

- [ ] **Step 1: Create `src/shared/parsers/__init__.py`** with the `ParserResult` type alias

```python
"""Non-NEM file parsers, organised by source platform."""
from __future__ import annotations

import pandas as pd

ParserResult = list[tuple[str, pd.DataFrame]]
```

- [ ] **Step 2: Create empty platform `__init__.py` files**

Each of the four files below has identical content (a single docstring):

`src/shared/parsers/optima/__init__.py`:
```python
"""Optima/BidEnergy platform parsers."""
```

`src/shared/parsers/racv/__init__.py`:
```python
"""RACV-internal parsers (not via BidEnergy)."""
```

`src/shared/parsers/envizi/__init__.py`:
```python
"""Envizi platform parsers."""
```

`src/shared/parsers/green_square/__init__.py`:
```python
"""Green Square ComX 510 parser."""
```

- [ ] **Step 3: Create empty test `__init__.py` files**

Five empty files (just `""` content is fine, but use `"""Tests for shared.parsers..."""` for clarity):

- `tests/unit/parsers/__init__.py`
- `tests/unit/parsers/optima/__init__.py`
- `tests/unit/parsers/racv/__init__.py`
- `tests/unit/parsers/envizi/__init__.py`
- `tests/unit/parsers/green_square/__init__.py`

Each contains:
```python
"""Tests for shared.parsers subpackage."""
```

- [ ] **Step 4: Verify baseline tests still pass (no behaviour change yet)**

Run: `uv run pytest --tb=short -q`
Expected: PASS, same test count as baseline (525+).

- [ ] **Step 5: Commit**

```bash
git add src/shared/parsers/ tests/unit/parsers/
git commit -m "chore: scaffold parsers/ subpackage and mirrored test directories"
```

---

## Task 2: Move `noosa_solar_parser` to `parsers/racv/noosa_solar.py`

**Why first:** It's already a standalone module (`src/shared/noosa_solar_parser.py`) — `git mv` preserves history, no function rename, no extraction from a bundled file. Lowest-risk task to validate the migration pattern before tackling harder cases.

**Files:**
- `git mv` source: `src/shared/noosa_solar_parser.py` → `src/shared/parsers/racv/noosa_solar.py`
- `git mv` test: `tests/unit/test_noosa_solar_parser.py` → `tests/unit/parsers/racv/test_noosa_solar.py`
- Modify: `src/shared/non_nem_parsers.py` (update dispatcher import path)
- Modify: moved test file (update mock patch paths)

- [ ] **Step 1: Move source and test files with git mv**

```bash
git mv src/shared/noosa_solar_parser.py src/shared/parsers/racv/noosa_solar.py
git mv tests/unit/test_noosa_solar_parser.py tests/unit/parsers/racv/test_noosa_solar.py
```

- [ ] **Step 2: Replace the local `ParserResult` definition in the moved source file** (per R7)

In `src/shared/parsers/racv/noosa_solar.py`, find:
```python
# Type alias — defined locally to avoid circular import with non_nem_parsers.py
ParserResult = list[tuple[str, pd.DataFrame]]
```

Replace with:
```python
from shared.parsers import ParserResult
```

(The accompanying comment about circular imports no longer applies — `parsers/__init__.py` is the canonical source now.)

- [ ] **Step 3: Update the dispatcher's import in `src/shared/non_nem_parsers.py`**

Find the existing line:
```python
from shared.noosa_solar_parser import noosa_solar_parser
```

Replace with:
```python
from shared.parsers.racv.noosa_solar import noosa_solar_parser
```

- [ ] **Step 4: Update mock patch paths in `tests/unit/parsers/racv/test_noosa_solar.py`**

In the moved test file, run a search-and-replace:
- `shared.noosa_solar_parser.logger` → `shared.parsers.racv.noosa_solar.logger`
- `from shared.noosa_solar_parser import noosa_solar_parser` → `from shared.parsers.racv.noosa_solar import noosa_solar_parser`

Use `sed` (or your editor):
```bash
sed -i '' 's|shared\.noosa_solar_parser|shared.parsers.racv.noosa_solar|g' tests/unit/parsers/racv/test_noosa_solar.py
```

Note: noosa parser DOES log itself (it has its own `logger` and uses it), so all 17 `patch("shared.noosa_solar_parser.logger")` patches DO need to be rewritten — handled by the sed above.

The cross-module patch on line ~523 (`patch("shared.non_nem_parsers.logger")`) STAYS UNCHANGED — that one targets the dispatcher's logger, which has not moved.

- [ ] **Step 5: Verify the moved tests pass**

Run: `uv run pytest tests/unit/parsers/racv/test_noosa_solar.py -v`
Expected: PASS — all noosa solar tests (count should match the original test_noosa_solar_parser.py count, ~20 tests).

- [ ] **Step 6: Verify the full suite still passes**

Run: `uv run pytest --tb=short -q`
Expected: PASS, same total count as baseline.

- [ ] **Step 7: Commit**

`git add -A` is acceptable here because all tracked changes belong to this task (the pre-flight stash protected unrelated WIP):

```bash
git add -A
git commit -m "refactor: move noosa_solar_parser into parsers/racv/ subpackage"
```

---

## Task 3: Move `bunnings_usage_and_spend_parser` → `parsers/optima/bunnings_billing.py` (with rename to `bunnings_billing_parser`)

**Files:**
- `git mv` source: `src/shared/billing_parser.py` → `src/shared/parsers/optima/bunnings_billing.py`
- `git mv` test: `tests/unit/test_billing_parser.py` → `tests/unit/parsers/optima/test_bunnings_billing.py`
- Modify: `src/shared/parsers/optima/bunnings_billing.py` (rename function, update Logger service name)
- Modify: `src/shared/non_nem_parsers.py` (update dispatcher import + name)
- Modify: moved test file (rename function references, update mock paths)

- [ ] **Step 1: Move files with git mv**

```bash
git mv src/shared/billing_parser.py src/shared/parsers/optima/bunnings_billing.py
git mv tests/unit/test_billing_parser.py tests/unit/parsers/optima/test_bunnings_billing.py
```

- [ ] **Step 2: Rename function and Logger service name in `src/shared/parsers/optima/bunnings_billing.py`**

Find:
```python
logger = Logger(service="bunnings-billing-parser", child=True)
```

Logger service name is already `"bunnings-billing-parser"` — no change needed.

Find the function definition:
```python
def bunnings_usage_and_spend_parser(file_name: str, error_file_path: str) -> ParserResult:
```

Replace with:
```python
def bunnings_billing_parser(file_name: str, error_file_path: str) -> ParserResult:
```

Also update the docstring's first line if it references the old name.

Also update the `ParserResult` import. The current file has its own `ParserResult` definition at the top:
```python
ParserResult = list[tuple[str, pd.DataFrame]]
```

Replace that line with an import from the subpackage:
```python
from shared.parsers import ParserResult
```

- [ ] **Step 3: Update the dispatcher in `src/shared/non_nem_parsers.py`**

Find:
```python
from shared.billing_parser import bunnings_usage_and_spend_parser
```

Replace with:
```python
from shared.parsers.optima.bunnings_billing import bunnings_billing_parser
```

In the `parsers = [...]` list, replace `bunnings_usage_and_spend_parser` with `bunnings_billing_parser`.

- [ ] **Step 4: Update test file `tests/unit/parsers/optima/test_bunnings_billing.py`**

Run sed for mechanical replacements:
```bash
sed -i '' \
  -e 's|shared\.billing_parser|shared.parsers.optima.bunnings_billing|g' \
  -e 's|bunnings_usage_and_spend_parser|bunnings_billing_parser|g' \
  tests/unit/parsers/optima/test_bunnings_billing.py
```

The two cross-module imports on lines ~517 and ~531 (`from shared.non_nem_parsers import get_non_nem_df`) STAY UNCHANGED — those target the dispatcher's path.

- [ ] **Step 5: Verify the moved tests pass**

Run: `uv run pytest tests/unit/parsers/optima/test_bunnings_billing.py -v`
Expected: PASS — all bunnings billing tests (~34 tests).

- [ ] **Step 6: Verify the full suite still passes**

Run: `uv run pytest --tb=short -q`
Expected: PASS, same total count as baseline.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "refactor: move + rename bunnings billing parser to parsers/optima/

- src/shared/billing_parser.py -> src/shared/parsers/optima/bunnings_billing.py
- bunnings_usage_and_spend_parser -> bunnings_billing_parser (file format
  contains both usage and spend; _billing_ captures purpose without false
  narrowing)"
```

---

## Task 4: Extract `optima_parser` → `parsers/optima/interval.py` (rename to `interval_parser`)

**Files:**
- Create: `src/shared/parsers/optima/interval.py`
- Modify: `src/shared/non_nem_parsers.py` (remove function, update dispatcher import)
- Create: `tests/unit/parsers/optima/test_interval.py` (extracted from `tests/unit/test_non_nem_parsers.py`)
- Modify: `tests/unit/test_non_nem_parsers.py` (remove the extracted tests)

- [ ] **Step 1: Create `src/shared/parsers/optima/interval.py`**

```python
"""Optima/BidEnergy "Export Interval Usage Csv" parser.

Handles the 12-column long-format CSV produced by the BidEnergy
"Export Interval Usage Csv" download (POST /BuyerReport/exportdailyusagecsv).
File contains both Usage and Generation columns per interval; both are
persisted as separate channels (E1_kWh and B1_kWh respectively) keyed by NMI.
"""
from __future__ import annotations

import pandas as pd
from aws_lambda_powertools import Logger

from shared.parsers import ParserResult

logger = Logger(service="optima-interval-parser", child=True)


def interval_parser(file_name: str, error_file_path: str) -> ParserResult:
    raw_df = pd.read_csv(file_name)
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

- [ ] **Step 2: Wire the dispatcher to the new module FIRST** (per R1)

In `src/shared/non_nem_parsers.py`, add this import alongside the other parser imports (alphabetic position):
```python
from shared.parsers.optima.interval import interval_parser
```

In the `parsers = [...]` list inside `get_non_nem_df`, find:
```python
optima_parser,
```
Replace with:
```python
interval_parser,
```

⚠️ Also handle the multi-line import at `tests/unit/test_non_nem_parsers.py:385` per R5 — it lists `optima_parser` among other parser names. Update that one block manually now (replace `optima_parser` with the new import path; sed won't handle the multi-line form):

```python
# Before:
from shared.non_nem_parsers import (
    envizi_vertical_parser_electricity,
    envizi_vertical_parser_water,
    optima_parser,
    ...
)
# After (split into two import statements since optima_parser now comes from a different module):
from shared.non_nem_parsers import (
    envizi_vertical_parser_electricity,
    envizi_vertical_parser_water,
    ...
)
from shared.parsers.optima.interval import interval_parser
```

After this step, the dispatcher is callable end-to-end via the new module. The old `optima_parser` definition still exists in `non_nem_parsers.py` but is no longer imported by anything.

- [ ] **Step 3: NOW delete the old function body from `src/shared/non_nem_parsers.py`** (per R1)

Locate the function by content (not by line number — the file has been edited above):
```bash
grep -n "^def optima_parser" src/shared/non_nem_parsers.py
```

Delete the function definition (the block starting with `def optima_parser(...)` through its closing `return dfs`).

- [ ] **Step 4: Create `tests/unit/parsers/optima/test_interval.py`** (extract from bundled file, per R3)

Locate the test functions by name, not by line number:
```bash
grep -nE "^def test_optima_parser|^class TestOptimaParser" tests/unit/test_non_nem_parsers.py
```

Cut the located test blocks from `tests/unit/test_non_nem_parsers.py` (typically 5 test functions — names like `test_optima_parser_*`). Copy them into the new file:

```python
"""Tests for shared.parsers.optima.interval.interval_parser."""
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# ... (paste the extracted test functions here verbatim)
```

After pasting, also preserve any module-level imports the extracted tests depend on (e.g., a fixture path constant). Inspect the original file's top-of-file imports and copy whatever the moved tests reference.

⚠️ **Apply mock-path migration per R2:** `optima_parser` does NOT log itself (only the dispatcher does). Any `patch("shared.non_nem_parsers.logger")` in the extracted tests targets the dispatcher's logger — leave them unchanged. The sed below does NOT touch logger paths:

```bash
sed -i '' \
  -e 's|optima_parser|interval_parser|g' \
  -e 's|from shared\.non_nem_parsers import interval_parser|from shared.parsers.optima.interval import interval_parser|g' \
  tests/unit/parsers/optima/test_interval.py
```

⚠️ **Watch for `pytest.fixture` decorators or shared helpers** in the extracted block — if a fixture (e.g., `optima_csv_path`) is defined in a `conftest.py` or referenced by other still-bundled tests, the fixture must either move with the test (paste into the new file or new conftest) or be left in place if shared.

- [ ] **Step 5: Remove the extracted tests from `tests/unit/test_non_nem_parsers.py`**

Delete the same test functions/classes you just copied. Use the same grep from Step 4 to locate them by name (line numbers will have shifted from any earlier task's deletions). Verify the file's class structure remains valid (no orphaned `class TestX:` headers or trailing fixtures with no consumers).

- [ ] **Step 6: Verify the new test file passes in isolation**

Run: `uv run pytest tests/unit/parsers/optima/test_interval.py -v`
Expected: PASS — same number of test cases as were in the original (typically ~5 tests for `optima_parser`).

- [ ] **Step 7: Verify the full suite still passes**

Run: `uv run pytest --tb=short -q`
Expected: PASS, same total count as baseline.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "refactor: extract optima_parser as parsers/optima/interval.py

- Renamed optima_parser -> interval_parser. The file it parses contains both
  Usage and Generation columns; the BidEnergy UI calls it 'Export Interval
  Usage Csv'. Plain 'interval' is honest; 'interval_usage' would imply only
  usage."
```

---

## Task 5: Extract `optima_usage_and_spend_to_s3` → `parsers/optima/racv_billing.py` (rename to `racv_billing_parser`)

**Files:**
- Create: `src/shared/parsers/optima/racv_billing.py`
- Modify: `src/shared/non_nem_parsers.py` (remove function, update dispatcher)
- Create: `tests/unit/parsers/optima/test_racv_billing.py` (extracted from `tests/unit/test_non_nem_parsers_edge_cases.py`)
- Modify: `tests/unit/test_non_nem_parsers_edge_cases.py` (remove extracted tests)

- [ ] **Step 1: Create `src/shared/parsers/optima/racv_billing.py`**

```python
"""RACV "Usage and Spend Report" archiver.

Accepts the monthly RACV billing CSV emitted by BidEnergy and uploads it
unchanged to the gegoptimareports S3 bucket. Returns an empty ParserResult
because no rows are written into the Hudi data lake — RACV billing is
consumed by a downstream system that reads the archived CSV directly.
"""
from __future__ import annotations

from pathlib import Path

import boto3
from aws_lambda_powertools import Logger

from shared.parsers import ParserResult

logger = Logger(service="racv-billing-parser", child=True)


def racv_billing_parser(file_name: str, error_file_path: str) -> ParserResult:
    if "OptimaGenerationData" in file_name:
        raise Exception("Not Relevant Parser For File")

    if "RACV-Usage and Spend Report" not in file_name:
        raise Exception("Not Valid Optima Usage And Spend File")

    # boto3 will use IAM role or env vars — no hardcoding creds
    s3 = boto3.client("s3")
    S3_BUCKET = "gegoptimareports"
    S3_KEY = "usageAndSpendReports/racvUsageAndSpend.csv"

    with Path(file_name).open("rb") as file:
        file_data = file.read()

    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY, Body=file_data)
    return []
```

- [ ] **Step 2: Wire the dispatcher to the new module FIRST** (per R1)

In `src/shared/non_nem_parsers.py`, add import:
```python
from shared.parsers.optima.racv_billing import racv_billing_parser
```

In the `parsers = [...]` list, find:
```python
optima_usage_and_spend_to_s3,  # RACV — unchanged
```
Replace with:
```python
racv_billing_parser,
```

(`optima_usage_and_spend_to_s3` is not in the multi-line import at line 385 of the test file, so no R5 handling needed for this task.)

- [ ] **Step 3: NOW delete the old function from `src/shared/non_nem_parsers.py`** (per R1)

Locate by content:
```bash
grep -n "^def optima_usage_and_spend_to_s3" src/shared/non_nem_parsers.py
```

Delete the function definition.

- [ ] **Step 4: Create `tests/unit/parsers/optima/test_racv_billing.py`** (extract from edge_cases bundled file)

Cut the test cases for `optima_usage_and_spend_to_s3` from `tests/unit/test_non_nem_parsers_edge_cases.py` (currently around lines 100-141 — approximately 3 test functions). Copy into the new file with this header:

```python
"""Tests for shared.parsers.optima.racv_billing.racv_billing_parser."""
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ... (the extracted test functions, with these mechanical replacements applied)
# - All occurrences of `optima_usage_and_spend_to_s3` → `racv_billing_parser`
# - All occurrences of `shared.non_nem_parsers.logger` → `shared.parsers.optima.racv_billing.logger`
# - All occurrences of `from shared.non_nem_parsers import optima_usage_and_spend_to_s3` → `from shared.parsers.optima.racv_billing import racv_billing_parser`
```

⚠️ **Apply mock-path migration per R2:** `optima_usage_and_spend_to_s3` does NOT log itself — `patch("shared.non_nem_parsers.logger")` patches in extracted tests target the dispatcher's logger; leave them unchanged. The sed below only changes function name + import path:

```bash
sed -i '' \
  -e 's|optima_usage_and_spend_to_s3|racv_billing_parser|g' \
  -e 's|from shared\.non_nem_parsers import racv_billing_parser|from shared.parsers.optima.racv_billing import racv_billing_parser|g' \
  tests/unit/parsers/optima/test_racv_billing.py
```

If any test patches `shared.non_nem_parsers.boto3.client` (the function does call `boto3.client("s3")` — check this), update to `shared.parsers.optima.racv_billing.boto3.client`. boto3 IS used by the parser body, so a `boto3.client` patch must follow the parser to the new module path.

- [ ] **Step 5: Remove the extracted tests from `tests/unit/test_non_nem_parsers_edge_cases.py`**

Delete the same lines you just copied.

- [ ] **Step 6: Verify the new test file passes**

Run: `uv run pytest tests/unit/parsers/optima/test_racv_billing.py -v`
Expected: PASS — ~3 tests pass.

- [ ] **Step 7: Verify the full suite still passes**

Run: `uv run pytest --tb=short -q`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "refactor: extract optima_usage_and_spend_to_s3 as parsers/optima/racv_billing.py

- Renamed optima_usage_and_spend_to_s3 -> racv_billing_parser. Function only
  ever accepted 'RACV-Usage and Spend Report' files; the optima_ prefix was
  misleading. Behaviour unchanged: archives the file to gegoptimareports
  bucket, returns []."
```

---

## Task 6: Extract `racv_elec_parser` → `parsers/racv/elec.py`

**Files:**
- Create: `src/shared/parsers/racv/elec.py`
- Modify: `src/shared/non_nem_parsers.py` (remove function, update dispatcher)
- Create: `tests/unit/parsers/racv/test_elec.py` (extract tests from BOTH bundled test files)
- Modify: `tests/unit/test_non_nem_parsers.py` and `tests/unit/test_non_nem_parsers_edge_cases.py` (remove extracted tests)

- [ ] **Step 1: Create `src/shared/parsers/racv/elec.py`**

```python
"""RACV electricity multi-meter wide-format CSV parser.

Reads the RACV-internal electricity export (skiprows=2 to drop two header
rows; column names contain 'kWh'). One row per interval; columns named
"<meter-name> kWh" are each emitted as a separate (NMI, DataFrame) pair
keyed by Optima_<meter-name-prefix>. Days where the meter sums to zero
across all intervals are filtered out as invalid.
"""
from __future__ import annotations

import pandas as pd
from aws_lambda_powertools import Logger

from shared.parsers import ParserResult

logger = Logger(service="racv-elec-parser", child=True)


def racv_elec_parser(file_name: str, error_file_path: str) -> ParserResult:
    if "OptimaGenerationData" in file_name:
        raise Exception("Not Relevant Parser For File")

    raw_df = pd.read_csv(file_name, skiprows=[0, 1])
    cols = [x for x in raw_df.columns if "kWh" in x or x in ["Date", "Start Time"]]
    meter_cols = [x for x in cols if "kWh" in x]

    raw_df["Interval_Start"] = pd.to_datetime(raw_df["Date"] + " " + raw_df["Start Time"])

    dfs: ParserResult = []
    for mn in meter_cols:
        buf_df = raw_df[["Interval_Start", mn]].rename(columns={"Interval_Start": "t_start", mn: "E1_kWh"})
        buf_df = buf_df.set_index("t_start")

        # Daily aggregation to filter out invalid days
        daily_sum = buf_df.resample("D").sum(numeric_only=True)
        non_zero_dates = daily_sum[daily_sum["E1_kWh"] != 0].index
        buf_df = buf_df[buf_df.index.normalize().isin(non_zero_dates)]

        if not non_zero_dates.empty:
            dfs.append((f"Optima_{mn.split(' ')[0]}", buf_df))

    if dfs:
        return dfs
    raise Exception(f"No Valid Data in file: {file_name}")
```

- [ ] **Step 2: Wire the dispatcher to the new module FIRST** (per R1)

In `src/shared/non_nem_parsers.py`, add:
```python
from shared.parsers.racv.elec import racv_elec_parser
```

The name in the `parsers = [...]` list stays `racv_elec_parser` — no rename for this one. The new import shadows the local definition (still present in the file) — Python's name resolution uses the import.

(`racv_elec_parser` is not in the multi-line import at line 385 of the test file, so no R5 handling needed.)

- [ ] **Step 3: NOW delete the old function from `src/shared/non_nem_parsers.py`** (per R1)

```bash
grep -n "^def racv_elec_parser" src/shared/non_nem_parsers.py
```

Delete the function definition.

- [ ] **Step 4: Create `tests/unit/parsers/racv/test_elec.py`** (extract from BOTH bundled files)

Two extraction sources:
- `tests/unit/test_non_nem_parsers.py` lines ~264-314 (3 racv_elec_parser test cases)
- `tests/unit/test_non_nem_parsers_edge_cases.py` lines ~164-185 (2 edge case tests)

Combine both blocks into the new file with this header:

```python
"""Tests for shared.parsers.racv.elec.racv_elec_parser."""
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# ... (paste both blocks of test functions here)
```

⚠️ **Apply mock-path migration per R2:** `racv_elec_parser` does NOT log itself. Leave `patch("shared.non_nem_parsers.logger")` patches alone. Only update import path:
```bash
sed -i '' \
  -e 's|from shared\.non_nem_parsers import racv_elec_parser|from shared.parsers.racv.elec import racv_elec_parser|g' \
  tests/unit/parsers/racv/test_elec.py
```

- [ ] **Step 5: Remove extracted tests from both bundled files**

Delete lines 264-314 from `tests/unit/test_non_nem_parsers.py`.
Delete lines 164-185 from `tests/unit/test_non_nem_parsers_edge_cases.py`.

- [ ] **Step 6: Verify the new test file passes**

Run: `uv run pytest tests/unit/parsers/racv/test_elec.py -v`
Expected: PASS — ~5 tests.

- [ ] **Step 7: Verify the full suite still passes**

Run: `uv run pytest --tb=short -q`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "refactor: extract racv_elec_parser as parsers/racv/elec.py"
```

---

## Task 7: Extract `envizi_vertical_parser_water` → `parsers/envizi/vertical_water.py`

**Files:**
- Create: `src/shared/parsers/envizi/vertical_water.py`
- Modify: `src/shared/non_nem_parsers.py`
- Create: `tests/unit/parsers/envizi/test_vertical_water.py` (extract from `test_non_nem_parsers.py`)
- Modify: `tests/unit/test_non_nem_parsers.py`

- [ ] **Step 1: Create `src/shared/parsers/envizi/vertical_water.py`**

```python
"""Envizi vertical-format water CSV parser."""
from __future__ import annotations

import pandas as pd
from aws_lambda_powertools import Logger

from shared.parsers import ParserResult

logger = Logger(service="envizi-vertical-water-parser", child=True)


def envizi_vertical_parser_water(file_name: str, error_file_path: str) -> ParserResult:
    if "OptimaGenerationData" in file_name:
        raise Exception("Not Relevant Parser For File")

    raw_df = pd.read_csv(file_name)
    raw_df["Interval_Start"] = pd.to_datetime(raw_df["Interval_Start"])
    raw_df["Serial_No"] = raw_df["Serial_No"].astype(str)

    dfs: ParserResult = []
    for name in sorted(raw_df["Serial_No"].unique()):
        buf_df = raw_df.loc[
            raw_df["Serial_No"] == name, ["Interval_Start", "Interval_End", "Consumption", "Consumption Unit"]
        ]

        unit_count = buf_df["Consumption Unit"].nunique()
        if unit_count != 1:
            logger.error(
                "envizi_vertical_parser_water: Multiple units", extra={"file": file_name, "unit_count": unit_count}
            )

        unit = buf_df["Consumption Unit"].iloc[0]
        buf_df = buf_df[["Interval_Start", "Consumption"]].rename(
            columns={"Interval_Start": "t_start", "Consumption": f"E1_{unit}"}
        )
        buf_df = buf_df.set_index("t_start")
        dfs.append((f"Envizi_{name}", buf_df))

    return dfs
```

- [ ] **Step 2: Wire the dispatcher to the new module FIRST** (per R1)

In `src/shared/non_nem_parsers.py`, add:
```python
from shared.parsers.envizi.vertical_water import envizi_vertical_parser_water
```

⚠️ Also handle the multi-line import at `tests/unit/test_non_nem_parsers.py:385` per R5 — `envizi_vertical_parser_water` is in that block. Update manually:

```python
# Add separately:
from shared.parsers.envizi.vertical_water import envizi_vertical_parser_water
# Remove envizi_vertical_parser_water from the multi-line block.
```

- [ ] **Step 3: NOW delete the old function from `src/shared/non_nem_parsers.py`** (per R1)

```bash
grep -n "^def envizi_vertical_parser_water" src/shared/non_nem_parsers.py
```

Delete the function definition.

- [ ] **Step 4: Create `tests/unit/parsers/envizi/test_vertical_water.py`** (extract from bundled file)

Cut lines 25-98 from `tests/unit/test_non_nem_parsers.py` (the `envizi_vertical_parser_water` test cases — typically 4 tests). Paste into the new file with appropriate header:

```python
"""Tests for shared.parsers.envizi.vertical_water.envizi_vertical_parser_water."""
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# ... (paste extracted test functions)
```

⚠️ **Apply mock-path migration per R2:** `envizi_vertical_parser_water` IS the one parser that calls `logger.error` itself (line 34 of original). Tests that ASSERT on the logger (e.g., `mock_log.error.assert_called_once`) target the parser's own logger and MUST be migrated. Tests that just suppress logger output (no assertion on the mock) target the dispatcher's logger and must be LEFT ALONE.

Audit each `with patch("shared.non_nem_parsers.logger"...) as mock_log:` (or similar) block in the extracted tests:
- Does the test body reference `mock_log` (e.g., `mock_log.error.assert_called`)? → Rewrite to `patch("shared.parsers.envizi.vertical_water.logger")`
- Just `with patch("shared.non_nem_parsers.logger"):` with no body reference? → Leave it (dispatcher logger silence)

Apply import-path replacement (safe — no logger paths touched):
```bash
sed -i '' \
  -e 's|from shared\.non_nem_parsers import envizi_vertical_parser_water|from shared.parsers.envizi.vertical_water import envizi_vertical_parser_water|g' \
  tests/unit/parsers/envizi/test_vertical_water.py
```

Then manually rewrite the asserting-on-logger patches per the audit above.

- [ ] **Step 5: Remove the extracted tests from `tests/unit/test_non_nem_parsers.py`**

Delete the same lines (25-98).

- [ ] **Step 6: Run new test file**

Run: `uv run pytest tests/unit/parsers/envizi/test_vertical_water.py -v`
Expected: PASS — 4 tests.

- [ ] **Step 7: Run full suite**

Run: `uv run pytest --tb=short -q`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "refactor: extract envizi_vertical_parser_water as parsers/envizi/vertical_water.py"
```

---

## Task 8: Extract `envizi_vertical_parser_electricity` → `parsers/envizi/vertical_electricity.py`

**Files:**
- Create: `src/shared/parsers/envizi/vertical_electricity.py`
- Modify: `src/shared/non_nem_parsers.py`
- Create: `tests/unit/parsers/envizi/test_vertical_electricity.py`
- Modify: `tests/unit/test_non_nem_parsers.py`

- [ ] **Step 1: Create `src/shared/parsers/envizi/vertical_electricity.py`**

```python
"""Envizi vertical-format electricity CSV parser."""
from __future__ import annotations

import pandas as pd
from aws_lambda_powertools import Logger

from shared.parsers import ParserResult

logger = Logger(service="envizi-vertical-electricity-parser", child=True)


def envizi_vertical_parser_electricity(file_name: str, error_file_path: str) -> ParserResult:
    if "OptimaGenerationData" in file_name:
        raise Exception("Not Relevant Parser For File")

    raw_df = pd.read_csv(file_name)
    raw_df["Interval_Start"] = pd.to_datetime(raw_df["Interval_Start"])
    raw_df["Serial_No"] = raw_df["Serial_No"].astype(str)

    dfs: ParserResult = []
    for name in sorted(raw_df["Serial_No"].unique()):
        buf_df = raw_df.loc[raw_df["Serial_No"] == name, ["Interval_Start", "Interval_End", "kWh"]]
        buf_df = buf_df.rename(columns={"Interval_Start": "t_start", "kWh": "E1_kWh"})
        buf_df = buf_df.set_index("t_start")
        dfs.append((f"Envizi_{name}", buf_df))

    return dfs
```

- [ ] **Step 2: Wire the dispatcher to the new module FIRST** (per R1)

In `src/shared/non_nem_parsers.py`, add:
```python
from shared.parsers.envizi.vertical_electricity import envizi_vertical_parser_electricity
```

⚠️ Also handle the multi-line import at `tests/unit/test_non_nem_parsers.py:385` per R5 — `envizi_vertical_parser_electricity` is in that block. Update manually as in Task 7.

- [ ] **Step 3: NOW delete the old function from `src/shared/non_nem_parsers.py`** (per R1)

```bash
grep -n "^def envizi_vertical_parser_electricity" src/shared/non_nem_parsers.py
```

Delete the function definition.

- [ ] **Step 4: Create `tests/unit/parsers/envizi/test_vertical_electricity.py`**

Extract tests for `envizi_vertical_parser_electricity` from `tests/unit/test_non_nem_parsers.py` (lines ~98-130 — 2 test functions). Use the same pattern as Task 7 Step 4:

```python
"""Tests for shared.parsers.envizi.vertical_electricity.envizi_vertical_parser_electricity."""
# ... (paste extracted test functions)
```

⚠️ **Apply mock-path migration per R2:** `envizi_vertical_parser_electricity` does NOT log itself. Leave `patch("shared.non_nem_parsers.logger")` patches alone. Only update import path:
```bash
sed -i '' \
  -e 's|from shared\.non_nem_parsers import envizi_vertical_parser_electricity|from shared.parsers.envizi.vertical_electricity import envizi_vertical_parser_electricity|g' \
  tests/unit/parsers/envizi/test_vertical_electricity.py
```

- [ ] **Step 5: Remove extracted tests from `tests/unit/test_non_nem_parsers.py`**

- [ ] **Step 6: Run new test file**

Run: `uv run pytest tests/unit/parsers/envizi/test_vertical_electricity.py -v`
Expected: PASS — 2 tests.

- [ ] **Step 7: Run full suite**

Run: `uv run pytest --tb=short -q`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "refactor: extract envizi_vertical_parser_electricity as parsers/envizi/vertical_electricity.py"
```

---

## Task 9: Extract `envizi_vertical_parser_water_bulk` → `parsers/envizi/vertical_water_bulk.py`

**Files:**
- Create: `src/shared/parsers/envizi/vertical_water_bulk.py`
- Modify: `src/shared/non_nem_parsers.py`
- Create: `tests/unit/parsers/envizi/test_vertical_water_bulk.py`
- Modify: `tests/unit/test_non_nem_parsers_edge_cases.py`

- [ ] **Step 1: Create `src/shared/parsers/envizi/vertical_water_bulk.py`**

```python
"""Envizi vertical-format bulk water CSV parser."""
from __future__ import annotations

import pandas as pd
from aws_lambda_powertools import Logger

from shared.parsers import ParserResult

logger = Logger(service="envizi-vertical-water-bulk-parser", child=True)


def envizi_vertical_parser_water_bulk(file_name: str, error_file_path: str) -> ParserResult:
    if "OptimaGenerationData" in file_name:
        raise Exception("Not Relevant Parser For File")

    raw_df = pd.read_csv(file_name)
    raw_df["Date_Time"] = pd.to_datetime(raw_df["Date_Time"])
    raw_df["Serial_No"] = raw_df["Serial_No"].astype(str)

    dfs: ParserResult = []
    for name in sorted(raw_df["Serial_No"].unique()):
        buf_df = raw_df.loc[raw_df["Serial_No"] == name, ["Date_Time", "kL"]]
        buf_df = buf_df.rename(columns={"Date_Time": "t_start", "kL": "E1_kL"})
        buf_df = buf_df.set_index("t_start")
        dfs.append((f"Envizi_{name}", buf_df))

    return dfs
```

- [ ] **Step 2: Wire the dispatcher to the new module FIRST** (per R1)

In `src/shared/non_nem_parsers.py`, add:
```python
from shared.parsers.envizi.vertical_water_bulk import envizi_vertical_parser_water_bulk
```

(`envizi_vertical_parser_water_bulk` is NOT in the multi-line import at line 385 of the test file — verify with `grep -n envizi_vertical_parser_water_bulk tests/unit/test_non_nem_parsers.py`.)

- [ ] **Step 3: NOW delete the old function from `src/shared/non_nem_parsers.py`** (per R1)

```bash
grep -n "^def envizi_vertical_parser_water_bulk" src/shared/non_nem_parsers.py
```

Delete the function definition.

- [ ] **Step 4: Create `tests/unit/parsers/envizi/test_vertical_water_bulk.py`**

Extract tests from `tests/unit/test_non_nem_parsers_edge_cases.py` (lines ~21-69 + ~343 — multiple test cases). Combine into the new file:

```python
"""Tests for shared.parsers.envizi.vertical_water_bulk.envizi_vertical_parser_water_bulk."""
# ... (paste extracted test functions)
```

⚠️ **Apply mock-path migration per R2:** `envizi_vertical_parser_water_bulk` does NOT log itself. Leave `patch("shared.non_nem_parsers.logger")` patches alone. Only update import path:
```bash
sed -i '' \
  -e 's|from shared\.non_nem_parsers import envizi_vertical_parser_water_bulk|from shared.parsers.envizi.vertical_water_bulk import envizi_vertical_parser_water_bulk|g' \
  tests/unit/parsers/envizi/test_vertical_water_bulk.py
```

- [ ] **Step 5: Remove extracted tests from `tests/unit/test_non_nem_parsers_edge_cases.py`**

- [ ] **Step 6: Run new test file**

Run: `uv run pytest tests/unit/parsers/envizi/test_vertical_water_bulk.py -v`
Expected: PASS — ~4 tests.

- [ ] **Step 7: Run full suite**

Run: `uv run pytest --tb=short -q`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "refactor: extract envizi_vertical_parser_water_bulk as parsers/envizi/vertical_water_bulk.py"
```

---

## Task 10: Extract `green_square_private_wire_schneider_comx_parser` → `parsers/green_square/comx.py`

**Files:**
- Create: `src/shared/parsers/green_square/comx.py`
- Modify: `src/shared/non_nem_parsers.py`
- Create: `tests/unit/parsers/green_square/test_comx.py`
- Modify: `tests/unit/test_non_nem_parsers.py` and `tests/unit/test_non_nem_parsers_edge_cases.py`

- [ ] **Step 1: Create `src/shared/parsers/green_square/comx.py`**

```python
"""Green Square Schneider ComX 510 private wire CSV parser."""
from __future__ import annotations

import pandas as pd
from aws_lambda_powertools import Logger

from shared.parsers import ParserResult

logger = Logger(service="green-square-comx-parser", child=True)


def green_square_private_wire_schneider_comx_parser(file_name: str, error_file_path: str) -> ParserResult:
    first_rows = pd.read_csv(file_name, header=None, nrows=2)
    if first_rows.iloc[1, 0] != "ComX510_Green_Square":
        raise Exception("Not Relevant Parser For File")

    site_name = first_rows.iloc[1, 4].replace(" ", "")
    raw_df = pd.read_csv(file_name, header=6, skip_blank_lines=False)

    if "Active energy (Wh)" in raw_df.columns:
        raw_df = raw_df[pd.to_numeric(raw_df["Active energy (Wh)"], errors="coerce").notnull()]
        raw_df["Active energy (Wh)"] = raw_df["Active energy (Wh)"].astype(float) / 1000
        energy_col = "Active energy (Wh)"
    elif "Active energy (kWh)" in raw_df.columns:
        raw_df = raw_df[pd.to_numeric(raw_df["Active energy (kWh)"], errors="coerce").notnull()]
        raw_df["Active energy (kWh)"] = raw_df["Active energy (kWh)"].astype(float)
        energy_col = "Active energy (kWh)"
    else:
        raise Exception("Missing Active energy column in file.")

    raw_df["Local Time Stamp"] = pd.to_datetime(raw_df["Local Time Stamp"], dayfirst=True)

    buf_df = raw_df[["Local Time Stamp", energy_col]].rename(
        columns={"Local Time Stamp": "t_start", energy_col: "E1_kWh"}
    )
    buf_df = buf_df.set_index("t_start")

    return [(f"GPWComX_{site_name}", buf_df)]
```

- [ ] **Step 2: Wire the dispatcher to the new module FIRST** (per R1)

In `src/shared/non_nem_parsers.py`, add:
```python
from shared.parsers.green_square.comx import green_square_private_wire_schneider_comx_parser
```

(Not in the multi-line import block — verify with `grep -n green_square_private tests/unit/test_non_nem_parsers.py`.)

- [ ] **Step 3: NOW delete the old function from `src/shared/non_nem_parsers.py`** (per R1)

```bash
grep -n "^def green_square_private_wire_schneider_comx_parser" src/shared/non_nem_parsers.py
```

Delete the function definition.

- [ ] **Step 4: Create `tests/unit/parsers/green_square/test_comx.py`** (extract from BOTH bundled files)

Combine:
- `tests/unit/test_non_nem_parsers.py` lines ~335-388 (3 test functions)
- `tests/unit/test_non_nem_parsers_edge_cases.py` lines ~213-263 + ~363 (4 test functions)

⚠️ **Apply mock-path migration per R2:** `green_square_private_wire_schneider_comx_parser` does NOT log itself. Leave `patch("shared.non_nem_parsers.logger")` patches alone. Only update import path:
```bash
sed -i '' \
  -e 's|from shared\.non_nem_parsers import green_square_private_wire_schneider_comx_parser|from shared.parsers.green_square.comx import green_square_private_wire_schneider_comx_parser|g' \
  tests/unit/parsers/green_square/test_comx.py
```

- [ ] **Step 5: Remove extracted tests from both bundled files**

- [ ] **Step 6: Run new test file**

Run: `uv run pytest tests/unit/parsers/green_square/test_comx.py -v`
Expected: PASS — ~7 tests.

- [ ] **Step 7: Run full suite**

Run: `uv run pytest --tb=short -q`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "refactor: extract green_square_private_wire_schneider_comx_parser as parsers/green_square/comx.py"
```

---

## Task 11: Move dispatcher tests to `tests/unit/test_dispatcher.py` and finalise `non_nem_parsers.py`

**At this point** all 9 parsers live under `src/shared/parsers/`, and `src/shared/non_nem_parsers.py` should contain only the dispatcher imports + `get_non_nem_df`. Both `test_non_nem_parsers.py` and `test_non_nem_parsers_edge_cases.py` should now contain only `get_non_nem_df` test cases (everything else extracted).

**Files:**
- Modify: `src/shared/non_nem_parsers.py` (verify final dispatcher form)
- Create: `tests/unit/test_dispatcher.py`
- Delete: `tests/unit/test_non_nem_parsers.py`
- Delete: `tests/unit/test_non_nem_parsers_edge_cases.py`

- [ ] **Step 1: Verify `src/shared/non_nem_parsers.py` final form**

Open the file and confirm it matches this form (the only file content):

```python
"""Dispatcher for non-NEM file parsers."""
from aws_lambda_powertools import Logger

from shared.parsers import ParserResult
from shared.parsers.envizi.vertical_electricity import envizi_vertical_parser_electricity
from shared.parsers.envizi.vertical_water import envizi_vertical_parser_water
from shared.parsers.envizi.vertical_water_bulk import envizi_vertical_parser_water_bulk
from shared.parsers.green_square.comx import green_square_private_wire_schneider_comx_parser
from shared.parsers.optima.bunnings_billing import bunnings_billing_parser
from shared.parsers.optima.interval import interval_parser
from shared.parsers.optima.racv_billing import racv_billing_parser
from shared.parsers.racv.elec import racv_elec_parser
from shared.parsers.racv.noosa_solar import noosa_solar_parser

logger = Logger(service="non-nem-parsers", child=True)


def get_non_nem_df(file_name: str, error_file_path: str) -> ParserResult:
    parsers = [
        noosa_solar_parser,
        envizi_vertical_parser_water,
        envizi_vertical_parser_electricity,
        racv_elec_parser,
        racv_billing_parser,
        bunnings_billing_parser,
        interval_parser,
        envizi_vertical_parser_water_bulk,
        green_square_private_wire_schneider_comx_parser,
    ]

    for parser in parsers:
        try:
            return parser(file_name, error_file_path)
        except Exception as e:
            logger.debug("Parser failed", extra={"parser": parser.__name__, "file": file_name, "error": str(e)})

    logger.error("No valid parser found", extra={"file": file_name})
    raise Exception(f"get_non_nem_df: {file_name}: No Valid Parser Found")
```

If anything beyond this remains (orphaned imports like `boto3`, `Path`, `pandas`, or leftover function bodies), delete it.

- [ ] **Step 2: Create `tests/unit/test_dispatcher.py`** by combining the dispatcher tests from both bundled files

Cut the `get_non_nem_df` test classes/functions from:
- `tests/unit/test_non_nem_parsers.py` (lines ~213-264 — `TestGetNonNemDf` class with 3 tests)
- `tests/unit/test_non_nem_parsers_edge_cases.py` (lines ~286-340 — class `TestGetNonNemDfEdgeCases` or similar)

Combine into the new file:

```python
"""Tests for the get_non_nem_df dispatcher in shared.non_nem_parsers."""
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ... (paste both blocks of dispatcher test functions)
```

The patch path `shared.non_nem_parsers.logger` STAYS UNCHANGED in this file — it targets the dispatcher's own logger, which has not moved.

- [ ] **Step 3: Verify the bundled test files contain only dispatcher tests, then delete**

Before `git rm`, audit each file for orphaned tests:

```bash
grep -nE "^def test_|^class Test" tests/unit/test_non_nem_parsers.py
grep -nE "^def test_|^class Test" tests/unit/test_non_nem_parsers_edge_cases.py
```

Expected output: **only** functions/classes whose names contain `get_non_nem_df` or `Dispatcher` — i.e., the dispatcher tests you cut into `test_dispatcher.py` in Step 2. If you see any other test name (e.g., `test_optima_parser_*`, `test_envizi_*`), STOP — that test was missed in an earlier task. Route it to the correct new test file before continuing.

Once verified empty of non-dispatcher tests:

```bash
git rm tests/unit/test_non_nem_parsers.py
git rm tests/unit/test_non_nem_parsers_edge_cases.py
```

- [ ] **Step 4: Run dispatcher tests**

Run: `uv run pytest tests/unit/test_dispatcher.py -v`
Expected: PASS — ~5-7 dispatcher tests.

- [ ] **Step 5: Run full suite**

Run: `uv run pytest --tb=short -q`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor: shrink non_nem_parsers.py to dispatcher-only and move its tests

- non_nem_parsers.py now contains only get_non_nem_df + 9 imports + the
  module-level Logger. Behaviour identical to the original (dispatch order
  preserved verbatim).
- Dispatcher tests moved to tests/unit/test_dispatcher.py (sibling of
  tests/unit/parsers/ since the dispatcher source lives at
  shared.non_nem_parsers, not under shared.parsers)."
```

---

## Task 12: Add Usage + Generation regression-lock test

**Note:** This is a **regression lock**, not a TDD red-green cycle. The behaviour being asserted (both `Usage→E1_kWh` and `Generation→B1_kWh` are produced) already works — the test exists to prevent future regressions. It will pass on the first run; that's expected and correct for this kind of test.

**Files:**
- Modify: `tests/unit/parsers/optima/test_interval.py` (add new test)

- [ ] **Step 1: Add the regression test**

Append to `tests/unit/parsers/optima/test_interval.py`:

```python
def test_interval_parser_persists_both_usage_and_generation(tmp_path):
    """Both Usage→E1_kWh and Generation→B1_kWh must be produced when present.

    Regression guard: if a future change to interval_parser drops one of the
    Usage or Generation channels, this test breaks.
    """
    csv_path = tmp_path / "Bunnings-AU-Electricity-TEST-NMI-ENERGYAP.csv"
    csv_path.write_text(
        "BuyerShortName,Country,Commodity,Identifier,IdentifierType,DistributorId,"
        "Date,Start Time,Usage,Generation,DemandKva,Reactive\n"
        '"Bunnings","AU","Electricity","TEST","NMI","ENERGYAP",01 May 2026,00:00,1.5,0.8,3.0,0.0\n'
        '"Bunnings","AU","Electricity","TEST","NMI","ENERGYAP",01 May 2026,00:30,1.7,0.9,3.4,0.0\n'
    )
    result = interval_parser(str(csv_path), str(tmp_path / "err.log"))
    assert len(result) == 1
    nmi_key, df = result[0]
    assert nmi_key == "Optima_TEST"
    assert "E1_kWh" in df.columns
    assert "B1_kWh" in df.columns
    assert df["E1_kWh"].sum() == pytest.approx(3.2)  # 1.5 + 1.7
    assert df["B1_kWh"].sum() == pytest.approx(1.7)  # 0.8 + 0.9 — Generation persists
```

(Make sure `pytest` is imported at the top of the file; it should already be from extracted tests.)

- [ ] **Step 2: Run the new test — expected PASS on first run**

Run: `uv run pytest tests/unit/parsers/optima/test_interval.py::test_interval_parser_persists_both_usage_and_generation -v`
Expected: PASS — `interval_parser` already implements both channels (the implementation has not changed in this refactor).

If the test FAILS, that means the function body extracted in Task 4 is not byte-identical to the original. Stop and diff Task 4's `src/shared/parsers/optima/interval.py` against the original `optima_parser` in `non_nem_parsers.py` (use `git show <pre-Task-4-commit>:src/shared/non_nem_parsers.py` to retrieve the original).

- [ ] **Step 3: Run full suite**

Run: `uv run pytest --tb=short -q`
Expected: PASS, total count = baseline + 1 (the new regression test).

- [ ] **Step 4: Commit**

```bash
git add tests/unit/parsers/optima/test_interval.py
git commit -m "test: lock interval_parser dual-channel contract (Usage + Generation)"
```

---

## Task 13: Final sweeps and verification

**Files:** None modified — verification only.

- [ ] **Step 1: Verify no stale imports remain**

Run:
```bash
grep -rn 'from shared\.\(billing_parser\|noosa_solar_parser\)\b' --include='*.py' src/ tests/
grep -rn 'shared\.non_nem_parsers\.\(logger\|optima_parser\|optima_usage_and_spend_to_s3\|racv_elec_parser\|envizi_vertical_parser_water\|envizi_vertical_parser_water_bulk\|envizi_vertical_parser_electricity\|green_square_private_wire_schneider_comx_parser\)' --include='*.py' src/ tests/
```

Expected output:
- First grep: zero results.
- Second grep: only ~5 sites legitimately patching the dispatcher's logger (`shared.non_nem_parsers.logger`) inside `tests/unit/test_dispatcher.py`. No references to the renamed/moved function names.

If either grep returns unexpected results, fix the offending file and re-run.

- [ ] **Step 2: Verify `from shared.non_nem_parsers import get_non_nem_df` still works**

Run:
```bash
grep -rn 'from shared.non_nem_parsers import get_non_nem_df' --include='*.py' src/ tests/
```

Expected: ~6 sites (test files that exercise the dispatcher, plus `src/shared/__init__.py:20`). All should still resolve correctly.

- [ ] **Step 3: Verify deleted files are actually deleted**

Run:
```bash
ls src/shared/billing_parser.py src/shared/noosa_solar_parser.py 2>&1
ls tests/unit/test_non_nem_parsers.py tests/unit/test_non_nem_parsers_edge_cases.py tests/unit/test_billing_parser.py tests/unit/test_noosa_solar_parser.py 2>&1
```

Expected: every line says `No such file or directory`. If any remain, investigate (they should have been moved or deleted in earlier tasks).

- [ ] **Step 4: Run linter and formatter**

Run:
```bash
uv run ruff check . --fix
uv run ruff format .
```

Expected: zero errors, zero unfixable warnings. If ruff finds issues (unused imports, missing types, etc.), fix them.

- [ ] **Step 5: Run full test suite with coverage**

Run: `uv run pytest --cov=src --cov-report=term-missing -q`
Expected:
- All tests pass.
- Total test count = baseline + 1 (the new regression test from Task 12).
- Coverage stays ≥ 90% (project gate from `lefthook.yml`).

If any coverage is below 90% or any test fails, stop and investigate.

- [ ] **Step 6: Verify the import-count migration**

Run:
```bash
grep -rn 'from shared\.parsers\.' --include='*.py' src/ tests/ | wc -l
```

Expected: roughly the same order of magnitude as the original 60 sites (give or take a few from extractions/consolidations).

- [ ] **Step 7: Inspect the final file tree**

Run:
```bash
find src/shared/parsers tests/unit/parsers -type f | sort
```

Expected list (28 files including all `__init__.py` files):
```
src/shared/parsers/__init__.py
src/shared/parsers/envizi/__init__.py
src/shared/parsers/envizi/vertical_electricity.py
src/shared/parsers/envizi/vertical_water.py
src/shared/parsers/envizi/vertical_water_bulk.py
src/shared/parsers/green_square/__init__.py
src/shared/parsers/green_square/comx.py
src/shared/parsers/optima/__init__.py
src/shared/parsers/optima/bunnings_billing.py
src/shared/parsers/optima/interval.py
src/shared/parsers/optima/racv_billing.py
src/shared/parsers/racv/__init__.py
src/shared/parsers/racv/elec.py
src/shared/parsers/racv/noosa_solar.py
tests/unit/parsers/__init__.py
tests/unit/parsers/envizi/__init__.py
tests/unit/parsers/envizi/test_vertical_electricity.py
tests/unit/parsers/envizi/test_vertical_water.py
tests/unit/parsers/envizi/test_vertical_water_bulk.py
tests/unit/parsers/green_square/__init__.py
tests/unit/parsers/green_square/test_comx.py
tests/unit/parsers/optima/__init__.py
tests/unit/parsers/optima/test_bunnings_billing.py
tests/unit/parsers/optima/test_interval.py
tests/unit/parsers/optima/test_racv_billing.py
tests/unit/parsers/racv/__init__.py
tests/unit/parsers/racv/test_elec.py
tests/unit/parsers/racv/test_noosa_solar.py
```

(Adjust if the count differs by 1-2 due to an optional `conftest.py` you may have added during extraction.)

- [ ] **Step 8: Commit any post-cleanup changes**

If ruff applied any auto-fixes:

```bash
git add -A
git commit -m "chore: ruff auto-fix after parsers modularization"
```

If nothing changed, skip this step.

- [ ] **Step 9: Final summary git log**

Run: `git log --oneline -15`
Expected: 11-13 commits since the spec commit (`db0f107`), one per task.

---

## Done

Total: 13 tasks, ~12 commits. All 525+ existing tests pass; 1 new regression test added; behaviour identical to baseline; codebase organised by source platform; 3 misleadingly-named functions renamed.
