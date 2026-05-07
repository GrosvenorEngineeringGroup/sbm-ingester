# Per-File Ingest Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor `src/functions/file_processor/app.py` from a 1165-line god-module into three focused modules so that the per-file unit of work (`ingest_file(SourceFile) -> ParserOutcome`) is a single explicitly-named function decorated with `@idempotent_function`, with all side effects living inside that boundary; fix two production-correctness bugs (hardcoded SQS URL fallback and visibility-timeout race) along the way.

**Architecture:** Three-module split inside `src/functions/file_processor/`: `app.py` (SQS adapter only, ~80 lines), `pipeline.py` (`ingest_file` orchestrator inside `@idempotent_function`), `csv_writer.py` (`HudiSourceCsvWriter`). Cross-cutting helpers move to `src/shared/`. Powertools idempotency caches `ParserOutcome` returns; only transient infrastructure failures raise.

**Tech Stack:** Python 3.13, AWS Lambda Powertools (Logger, Tracer, Metrics, idempotent_function with DynamoDBPersistenceLayer), boto3, pandas, pytest with moto, terraform, SQS, S3, X-Ray.

**Spec:** [`docs/superpowers/specs/2026-05-07-per-file-ingest-refactor-design.md`](../specs/2026-05-07-per-file-ingest-refactor-design.md)

---

## File Map

### Files created

| Path | Responsibility |
|---|---|
| `src/shared/source_file.py` | `SourceFile` frozen dataclass — S3 object reference. |
| `src/functions/file_processor/csv_writer.py` | `HudiSourceCsvWriter` (renamed from `DirectCSVWriter`); `StagedCsvUpload` (renamed from `CSVUploadJob`). |
| `src/functions/file_processor/persistence.py` | `InstrumentedDynamoDBPersistenceLayer` — emits `idempotent_cache_hit` log on conflicts. |
| `src/functions/file_processor/pipeline.py` | `ingest_file(source_file)` orchestrator, all side effects inside `@idempotent_function`. |
| `tests/helpers/__init__.py` | Empty. Marks `tests/helpers/` as a package. |
| `tests/helpers/outcome_invariants.py` | Moved from `tests/_outcome_invariants.py`; gains `parse_failed` branch. |
| `tests/unit/test_source_file.py` | Tests for `SourceFile` dataclass + Powertools `_prepare_data` compatibility. |
| `tests/unit/test_outcome_derive_final.py` | Tests for `ParserOutcome.derive_final`. |
| `tests/unit/test_nem_envelope_only.py` | Tests for `_is_nem_envelope_only` in `nem_adapter.py`. |
| `tests/unit/test_csv_writer.py` | Tests for `HudiSourceCsvWriter` staging/commit/abort. |
| `tests/unit/test_persistence_cache_hit_log.py` | Tests for `InstrumentedDynamoDBPersistenceLayer` log emission. |
| `tests/unit/test_pipeline.py` | End-to-end `ingest_file` tests with moto. |
| `tests/unit/test_idempotency_boundary.py` | Cache-hit / cache-miss / raise-vs-return contract tests. |
| `tests/unit/test_lambda_handler.py` | SQS adapter tests; mocks `ingest_file`. |
| `tests/unit/test_dataframe_partial_skip.py` | Row-skip + skip_reasons aggregation behavior. |
| `tests/unit/test_unmapped_disposition.py` | Routing to `newIrrevFiles/` (UNMAPPED_DIR). |
| `tests/unit/test_audit_sidecar_contract.py` | Audit JSON schema + sample cap. |

### Files modified

| Path | Change |
|---|---|
| `src/shared/parsers/outcome.py` | Add `parse_failed` ParserStatus; `parser_error`/`processing_error` ParserReasons; `derive_final` method; rename field `dfs` → `dataframes`. |
| `src/shared/nem_adapter.py` | Add `_is_nem_envelope_only(file_path: str) -> bool` (moved from `app.py`). |
| `src/shared/common.py` | Rename `BUCKET_NAME` → `INPUT_BUCKET`; rename `IRREVFILES_DIR` → `UNMAPPED_DIR`; add `HUDI_BUCKET`, `HUDI_FINAL_PREFIX`, `HUDI_STAGING_PREFIX`. |
| `src/shared/__init__.py` | Update exports for renamed symbols + add new constants. |
| `src/shared/non_nem_parsers.py` | **Renamed** to `src/shared/parsers/dispatcher.py`; `get_non_nem_outcome` renamed to `dispatch_non_nem`; drop `error_file_path` parameter. |
| `src/shared/parsers/envizi/vertical_water.py` | Drop `error_file_path` parameter. |
| `src/shared/parsers/envizi/vertical_water_bulk.py` | Drop `error_file_path` parameter. |
| `src/shared/parsers/envizi/vertical_electricity.py` | Drop `error_file_path` parameter. |
| `src/shared/parsers/optima/racv_billing.py` | Drop `error_file_path` parameter. |
| `src/shared/parsers/optima/interval.py` | Drop `error_file_path` parameter. |
| `src/shared/parsers/optima/bunnings_billing.py` | Drop `error_file_path` parameter. |
| `src/shared/parsers/optima/demand.py` | Drop `error_file_path` parameter. |
| `src/shared/parsers/racv/elec.py` | Drop `error_file_path` parameter. |
| `src/shared/parsers/racv/noosa_solar.py` | Drop `error_file_path` parameter. |
| `src/shared/parsers/green_square/comx.py` | Drop `error_file_path` parameter. |
| `src/functions/file_processor/app.py` | Slim down to ~80 lines — SQS adapter only. |
| `tests/conftest.py` | Set `SQS_QUEUE_URL` env var at module load; add shared moto fixtures (`mock_s3_buckets`, `mock_dynamodb_idempotency`, `file_in_newtbp`). |
| `tests/unit/conftest.py` | Re-export shared fixtures if needed. |
| `terraform/ingester.tf` | Add `environment {}` block for `SQS_QUEUE_URL`; bump main queue `visibility_timeout_seconds` 900 → 1080; align `maxReceiveCount` to match `MAX_REQUEUE_RETRIES`; add `deletion_protection_enabled = true` to `aws_dynamodb_table.sbm_ingester_idempotency`. |
| `terraform/monitoring.tf` | Add four new alarms (`MaxRetriesExceeded`, `ParseErrorSpike`, `ErrorRate`, `IdempotentSkipSpike`). |

### Files deleted

| Path | Reason |
|---|---|
| `tests/_outcome_invariants.py` | Moved to `tests/helpers/outcome_invariants.py` (Task 1). |
| `tests/unit/test_batch_s3_writes.py` | All 422 lines test `_flush_buffer_to_s3` dead code. |
| `tests/unit/test_edge_cases.py` | 1946-line god file split into focused files. |

### Functions/classes deleted (within `app.py`)

`_flush_buffer_to_s3`, `DirectCSVWriter`, `CSVUploadJob`, `read_nem12_mappings`, `_processed_destination_for_status`, `_compute_dataframe_final_status`, `_looks_like_nem_envelope`, `_candidate_values`, `_is_blank_value`, `_move_final_source_or_parse_error`, `_upload_csv_to_s3`, `download_files_to_tmp`, `move_s3_file`, `parse_and_write_data`. (Behavior either moves to `pipeline.py`/`csv_writer.py` or is genuinely dead.)

---

## Conventions

- **Python**: Python 3.13. Follow `ruff` rules already configured.
- **Lint/format/test commands** (from `CLAUDE.md`):
  - Lint: `uv run ruff check .`
  - Format: `uv run ruff format .`
  - Tests: `uv run pytest`
- **Commit message style**: `type: subject` (no scope in parentheses, no `Co-Authored-By` line). E.g. `feat: add SourceFile dataclass`.
- **Pre-commit hook (lefthook)** runs `ruff check`, `ruff format`, trailing-whitespace check on staged files; pre-push runs `pytest` + coverage check (≥ 90 %). Do not bypass with `--no-verify`.
- **Test invariants**: Every test that asserts a returned `ParserOutcome` must also call `assert_parser_outcome_invariants(outcome)` from `tests/helpers/outcome_invariants` (cross-field invariants).
- **All file paths in this plan are absolute from the repo root.**

---

## Task 1: Move outcome invariants helper into a `tests/helpers/` package

**Files:**
- Create: `tests/helpers/__init__.py`
- Create: `tests/helpers/outcome_invariants.py` (content moved from `tests/_outcome_invariants.py`)
- Delete: `tests/_outcome_invariants.py`
- Modify: every test file that imports from the old location (search-and-replace)

**Why this first:** Later tasks add new branches to the invariants (for `parse_failed`); having a stable home reduces churn.

- [ ] **Step 1: Verify the old file's location and consumers**

```bash
git ls-files tests/_outcome_invariants.py
grep -rn "_outcome_invariants\|tests\._outcome_invariants\|from tests._outcome_invariants\|from _outcome_invariants" tests/ src/
```

Expected: `tests/_outcome_invariants.py` exists; consumers are in `tests/unit/` (one or more files import `assert_parser_outcome_invariants`).

- [ ] **Step 2: Create empty package marker**

```bash
mkdir -p tests/helpers
:> tests/helpers/__init__.py
```

- [ ] **Step 3: Move file content (preserve verbatim)**

```bash
git mv tests/_outcome_invariants.py tests/helpers/outcome_invariants.py
```

- [ ] **Step 4: Update all consumer imports**

Find every consumer:

```bash
grep -rln "from tests._outcome_invariants\|from _outcome_invariants\|tests\._outcome_invariants\b\|_outcome_invariants\b" tests/ src/
```

For each match, replace `_outcome_invariants` (or `tests._outcome_invariants`) with `tests.helpers.outcome_invariants`. Function name `assert_parser_outcome_invariants` stays the same.

- [ ] **Step 5: Run the entire test suite**

```bash
uv run pytest -q
```

Expected: all currently-green tests stay green. If any test fails with `ModuleNotFoundError: No module named 'tests._outcome_invariants'` or similar, the import update was missed — search again and fix.

- [ ] **Step 6: Commit**

```bash
git add tests/helpers/__init__.py tests/helpers/outcome_invariants.py
git add -u tests/_outcome_invariants.py
git add -u $(git diff --name-only)
git commit -m "refactor: move outcome invariants helper into tests.helpers package"
```

---

## Task 2: Add `SourceFile` frozen dataclass

**Files:**
- Create: `src/shared/source_file.py`
- Create: `tests/unit/test_source_file.py`
- Modify: `src/shared/__init__.py` (export `SourceFile`)

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_source_file.py`:

```python
"""Tests for SourceFile dataclass — used as Powertools idempotency key payload."""

from __future__ import annotations

import json
from dataclasses import asdict, FrozenInstanceError

import pytest

from shared.source_file import SourceFile


class TestSourceFile:
    def test_constructs_with_bucket_and_key(self) -> None:
        src = SourceFile(bucket="sbm-file-ingester", key="newTBP/foo.csv")
        assert src.bucket == "sbm-file-ingester"
        assert src.key == "newTBP/foo.csv"

    def test_is_frozen(self) -> None:
        src = SourceFile(bucket="b", key="k")
        with pytest.raises(FrozenInstanceError):
            src.bucket = "other"  # type: ignore[misc]

    def test_is_hashable(self) -> None:
        src = SourceFile(bucket="b", key="k")
        # frozen + slots dataclass is hashable by default
        assert hash(src) == hash(SourceFile(bucket="b", key="k"))

    def test_uses_slots_no_dict(self) -> None:
        src = SourceFile(bucket="b", key="k")
        # slots=True removes __dict__
        assert not hasattr(src, "__dict__")

    def test_powertools_prepare_data_compatibility(self) -> None:
        """SourceFile must be JSON-serialisable via dataclasses.asdict.

        This is the path Powertools' _prepare_data takes when an instance has
        __dataclass_fields__ — see aws_lambda_powertools/utilities/idempotency/base.py.
        """
        src = SourceFile(bucket="a", key="b")
        as_dict = asdict(src)
        assert as_dict == {"bucket": "a", "key": "b"}
        # Must round-trip through json.dumps — that is what Powertools hashes.
        assert json.dumps(as_dict) == '{"bucket": "a", "key": "b"}'
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/unit/test_source_file.py -v
```

Expected: `ModuleNotFoundError: No module named 'shared.source_file'`.

- [ ] **Step 3: Create the module**

`src/shared/source_file.py`:

```python
"""SourceFile: an S3 object reference identifying one input file.

Used as the ``data_keyword_argument`` for Powertools idempotency in
``functions.file_processor.pipeline.ingest_file``. Powertools natively
supports plain dataclasses as idempotency-key payloads — its
``_prepare_data`` (in ``aws_lambda_powertools/utilities/idempotency/base.py``)
detects ``__dataclass_fields__`` and calls ``dataclasses.asdict(data)``, which
works on ``frozen=True, slots=True`` instances because ``asdict`` iterates
``__dataclass_fields__`` rather than ``__dict__``. No custom serializer is
needed.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SourceFile:
    """An S3 object reference identifying one input file."""

    bucket: str
    key: str
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/unit/test_source_file.py -v
```

Expected: 5 PASSED.

- [ ] **Step 5: Re-export from `shared`**

In `src/shared/__init__.py`, add `SourceFile` to imports and `__all__`:

```python
from shared.source_file import SourceFile
```

```python
__all__ = [
    # ... existing entries ...
    "SourceFile",
]
```

Keep alphabetical ordering of `__all__` (the ruff `I` rule will complain otherwise — `ruff format` does not sort `__all__` automatically; sort manually).

- [ ] **Step 6: Verify nothing else broke**

```bash
uv run ruff check src/shared/ tests/unit/test_source_file.py
uv run pytest -q
```

Expected: lint clean; all tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/shared/source_file.py src/shared/__init__.py tests/unit/test_source_file.py
git commit -m "feat: add SourceFile frozen dataclass for idempotency key payload"
```

---

## Task 3: Add Hudi prefix constants and rename two existing constants in `shared/common.py`

**Files:**
- Modify: `src/shared/common.py`
- Modify: `src/shared/__init__.py`
- Modify: every file that imports `BUCKET_NAME` or `IRREVFILES_DIR` (search across entire repo)

**Goal:** Centralise the three hard-coded Hudi-related strings. Rename `BUCKET_NAME` (input bucket) to `INPUT_BUCKET` (system has 3 buckets — disambiguate). Rename `IRREVFILES_DIR` (reads as "irrevocable") to `UNMAPPED_DIR` (matches the `unmapped` ParserStatus).

- [ ] **Step 1: Survey current consumers**

```bash
grep -rln "\bBUCKET_NAME\b\|\bIRREVFILES_DIR\b\|\"hudibucketsrc\"\|'hudibucketsrc'\|\"sensorDataFiles\"\|'sensorDataFiles'\|\"sensorDataFilesStaging\"\|'sensorDataFilesStaging'" src/ tests/
```

Expected: a dozen-or-so files. Note them.

- [ ] **Step 2: Update `src/shared/common.py`**

Replace the file contents with:

```python
# S3 and CloudWatch constants for SBM Ingester

PARSE_ERROR_LOG_GROUP = "sbm-ingester-parse-error-log"
RUNTIME_ERROR_LOG_GROUP = "sbm-ingester-runtime-error-log"
ERROR_LOG_GROUP = "sbm-ingester-error-log"
EXECUTION_LOG_GROUP = "sbm-ingester-execution-log"
METRICS_LOG_GROUP = "sbm-ingester-metrics-log"

# The S3 bucket where source files arrive under newTBP/ and are routed by
# disposition to newP/ / newIrrevFiles/ / newParseErr/. The system also
# touches two other buckets (hudibucketsrc, gegoptimareports); use the
# specifically-named constant below for them.
INPUT_BUCKET = "sbm-file-ingester"

# Disposition prefixes inside INPUT_BUCKET.
PARSE_ERR_DIR = "newParseErr/"
UNMAPPED_DIR = "newIrrevFiles/"   # historical name kept on S3; logical name is "unmapped"
PROCESSED_DIR = "newP/"

# Hudi data lake source bucket — CSV objects under HUDI_FINAL_PREFIX are
# consumed by the DataImportIntoLake Glue job into the Hudi table.
HUDI_BUCKET = "hudibucketsrc"
HUDI_FINAL_PREFIX = "sensorDataFiles"
HUDI_STAGING_PREFIX = "sensorDataFilesStaging"
```

- [ ] **Step 3: Update `src/shared/__init__.py`**

Replace imports/exports:

```python
from shared.common import (
    ERROR_LOG_GROUP,
    EXECUTION_LOG_GROUP,
    HUDI_BUCKET,
    HUDI_FINAL_PREFIX,
    HUDI_STAGING_PREFIX,
    INPUT_BUCKET,
    METRICS_LOG_GROUP,
    PARSE_ERR_DIR,
    PARSE_ERROR_LOG_GROUP,
    PROCESSED_DIR,
    RUNTIME_ERROR_LOG_GROUP,
    UNMAPPED_DIR,
)
from shared.nem_adapter import output_as_data_frames, stream_as_data_frames
from shared.non_nem_parsers import get_non_nem_df
from shared.source_file import SourceFile

__all__ = [
    "ERROR_LOG_GROUP",
    "EXECUTION_LOG_GROUP",
    "HUDI_BUCKET",
    "HUDI_FINAL_PREFIX",
    "HUDI_STAGING_PREFIX",
    "INPUT_BUCKET",
    "METRICS_LOG_GROUP",
    "PARSE_ERROR_LOG_GROUP",
    "PARSE_ERR_DIR",
    "PROCESSED_DIR",
    "RUNTIME_ERROR_LOG_GROUP",
    "SourceFile",
    "UNMAPPED_DIR",
    "get_non_nem_df",
    "output_as_data_frames",
    "stream_as_data_frames",
]
```

- [ ] **Step 4: Replace consumer imports across the repo**

For every file matched in Step 1:

- Rename identifier: `BUCKET_NAME` → `INPUT_BUCKET`, `IRREVFILES_DIR` → `UNMAPPED_DIR`.
- Replace string literals `"hudibucketsrc"` → `HUDI_BUCKET`, `"sensorDataFiles"` → `HUDI_FINAL_PREFIX`, `"sensorDataFilesStaging"` → `HUDI_STAGING_PREFIX`. Add the necessary import where it is used.

Mechanical sed sweep (one file at a time, then re-read each to confirm intent):

```bash
# CAREFUL: review diff before commit; sed cannot tell strings from comments.
git grep -l "\bBUCKET_NAME\b" -- 'src/' 'tests/' | xargs sed -i '' 's/\bBUCKET_NAME\b/INPUT_BUCKET/g'
git grep -l "\bIRREVFILES_DIR\b" -- 'src/' 'tests/' | xargs sed -i '' 's/\bIRREVFILES_DIR\b/UNMAPPED_DIR/g'
```

(Hudi prefix renames need to be done by hand because they are bare strings that may also appear as test-fixture data.)

- [ ] **Step 5: Run lint and tests**

```bash
uv run ruff check src/ tests/
uv run pytest -q
```

Expected: lint clean; all tests pass. If any test fails because a fixture string was accidentally rewritten, revert that fixture.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor: rename BUCKET_NAME/IRREVFILES_DIR and add Hudi prefix constants"
```

---

## Task 4: Add `_is_nem_envelope_only` helper to `shared/nem_adapter.py`

**Files:**
- Modify: `src/shared/nem_adapter.py`
- Create: `tests/unit/test_nem_envelope_only.py`

**Note:** The old `_looks_like_nem_envelope` stays in `app.py` for now; it will be removed when `app.py` is slimmed in Task 11. The new helper lives in `shared/nem_adapter.py` so `pipeline.py` (Task 10) can call it.

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_nem_envelope_only.py`:

```python
"""Tests for shared.nem_adapter._is_nem_envelope_only."""

from __future__ import annotations

from pathlib import Path

import pytest

from shared.nem_adapter import _is_nem_envelope_only


def _write(tmp_path: Path, name: str, content: bytes) -> str:
    path = tmp_path / name
    path.write_bytes(content)
    return str(path)


class TestIsNemEnvelopeOnly:
    def test_nem12_header(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "a.csv", b"100,NEM12,202605060200,MDP1,Origin\n900\n")
        assert _is_nem_envelope_only(path) is True

    def test_nem13_header(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "a.csv", b"100,NEM13,202605060200,MDP1,Origin\n900\n")
        assert _is_nem_envelope_only(path) is True

    def test_nem12_with_utf8_bom(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "a.csv", b"\xef\xbb\xbf100,NEM12,202605060200,MDP1,Origin\n900\n")
        assert _is_nem_envelope_only(path) is True

    def test_non_nem_csv(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "a.csv", b"Date,Value,Quality\n2026-05-06,1.0,A\n")
        assert _is_nem_envelope_only(path) is False

    def test_missing_file_returns_false(self, tmp_path: Path) -> None:
        assert _is_nem_envelope_only(str(tmp_path / "does-not-exist.csv")) is False

    def test_binary_garbage_returns_false(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "a.bin", b"\x00\x01\x02\x03\x04\xff\xfe\xfd")
        # Either decodes (rare) or raises UnicodeDecodeError; helper must
        # return False without propagating.
        assert _is_nem_envelope_only(path) is False
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/unit/test_nem_envelope_only.py -v
```

Expected: `ImportError` or `AttributeError: module 'shared.nem_adapter' has no attribute '_is_nem_envelope_only'`.

- [ ] **Step 3: Add the helper to `src/shared/nem_adapter.py`**

Append to the end of `src/shared/nem_adapter.py`:

```python
def _is_nem_envelope_only(file_path: str) -> bool:
    """Return True if the file's first line is a NEM12 or NEM13 envelope header.

    Reads up to ~64 bytes (BOM-stripped via ``utf-8-sig``) and matches the
    prefix ``100,NEM12,`` OR ``100,NEM13,``. Used to short-circuit empty
    NEM-format files (only 100/900 records, no 200/300 payload) to
    ``processed_empty(reason="no_data_sentinel")`` instead of falling through
    to the non-NEM dispatcher (which never matches NEM-format files and would
    incorrectly route them to ``newParseErr/``).

    Defensive: returns ``False`` on any I/O or decoding error so a failing
    helper never crashes the lambda.
    """
    from pathlib import Path  # local import to keep top of module clean

    try:
        with Path(file_path).open(encoding="utf-8-sig") as f:
            first_line = f.readline(64)
    except (OSError, UnicodeDecodeError):
        return False
    return first_line.startswith("100,NEM12,") or first_line.startswith("100,NEM13,")
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/unit/test_nem_envelope_only.py -v
```

Expected: 6 PASSED.

- [ ] **Step 5: Commit**

```bash
git add src/shared/nem_adapter.py tests/unit/test_nem_envelope_only.py
git commit -m "feat: add _is_nem_envelope_only helper in shared.nem_adapter"
```

---

## Task 5: Extend `ParserOutcome` — `parse_failed` status, `derive_final` method, rename `dfs` → `dataframes`

**Files:**
- Modify: `src/shared/parsers/outcome.py`
- Modify: `tests/helpers/outcome_invariants.py`
- Create: `tests/unit/test_outcome_derive_final.py`
- Modify: every file that reads `outcome.dfs` (search-and-replace to `outcome.dataframes`)

**Why combined:** these three changes touch the same dataclass and want consistent test updates. `parse_failed` is needed for caching content failures; `derive_final` co-locates the disposition ladder; `dfs → dataframes` is a low-cost clarity rename.

- [ ] **Step 1: Survey `outcome.dfs` consumers**

```bash
grep -rln "outcome\.dfs\b\|\.dfs\b" src/ tests/ | head -30
```

Note all matches.

- [ ] **Step 2: Write failing tests**

Create `tests/unit/test_outcome_derive_final.py`:

```python
"""Tests for ParserOutcome.derive_final ladder and new parse_failed status."""

from __future__ import annotations

from collections import Counter

import pytest

from shared.parsers.outcome import ParserOutcome


class TestDeriveFinal:
    def test_rule1_rows_written_yields_processed(self) -> None:
        seed = ParserOutcome(status="processed", rows_written=0)
        final = seed.derive_final(
            rows_written=5,
            candidate_row_count=5,
            unmapped_count=0,
            unsupported_suffixes=frozenset(),
            rows_skipped=0,
        )
        assert final.status == "processed"
        assert final.reason is None
        assert final.rows_written == 5

    def test_rule2_all_unmapped_yields_unmapped(self) -> None:
        seed = ParserOutcome(status="processed")
        final = seed.derive_final(
            rows_written=0,
            candidate_row_count=10,
            unmapped_count=10,
            unsupported_suffixes=frozenset(),
            rows_skipped=0,
        )
        assert final.status == "unmapped"
        assert final.reason is None
        assert final.candidate_row_count == 10
        assert final.unmapped_count == 10

    def test_rule3_unknown_suffix_yields_processed_empty_all_unknown_suffix(self) -> None:
        seed = ParserOutcome(status="processed")
        final = seed.derive_final(
            rows_written=0,
            candidate_row_count=0,
            unmapped_count=0,
            unsupported_suffixes=frozenset({"X9"}),
            rows_skipped=0,
        )
        assert final.status == "processed_empty"
        assert final.reason == "all_unknown_suffix"

    def test_rule4_all_skipped_yields_processed_empty_all_skipped(self) -> None:
        seed = ParserOutcome(status="processed")
        final = seed.derive_final(
            rows_written=0,
            candidate_row_count=0,
            unmapped_count=0,
            unsupported_suffixes=frozenset(),
            rows_skipped=3,
        )
        assert final.status == "processed_empty"
        assert final.reason == "all_skipped"

    def test_rule5_inherits_seed_reason(self) -> None:
        seed = ParserOutcome(status="processed", reason="zero_rows")
        final = seed.derive_final(
            rows_written=0,
            candidate_row_count=0,
            unmapped_count=0,
            unsupported_suffixes=frozenset(),
            rows_skipped=0,
        )
        assert final.status == "processed_empty"
        assert final.reason == "zero_rows"

    def test_rule5_no_seed_reason_yields_none(self) -> None:
        seed = ParserOutcome(status="processed")
        final = seed.derive_final(
            rows_written=0,
            candidate_row_count=0,
            unmapped_count=0,
            unsupported_suffixes=frozenset(),
            rows_skipped=0,
        )
        assert final.status == "processed_empty"
        assert final.reason is None


class TestParseFailedStatus:
    def test_parse_failed_is_valid_status(self) -> None:
        outcome = ParserOutcome(
            status="parse_failed",
            reason="parser_error",
            source_row_count=0,
        )
        assert outcome.status == "parse_failed"
        assert outcome.reason == "parser_error"

    def test_parse_failed_with_processing_error_reason(self) -> None:
        outcome = ParserOutcome(
            status="parse_failed",
            reason="processing_error",
            source_row_count=0,
        )
        assert outcome.status == "parse_failed"
        assert outcome.reason == "processing_error"


class TestDataframesField:
    def test_field_name_is_dataframes(self) -> None:
        outcome = ParserOutcome(status="processed", dataframes=[("NMI1", None)])  # type: ignore[arg-type]
        assert outcome.dataframes == [("NMI1", None)]
        assert not hasattr(outcome, "dfs"), "Old field name 'dfs' must be removed"
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
uv run pytest tests/unit/test_outcome_derive_final.py -v
```

Expected: import errors for `derive_final` not existing AND for `parse_failed` / `processing_error` not in the Literal.

- [ ] **Step 4: Update `src/shared/parsers/outcome.py`**

Replace the whole file with:

```python
"""Parser outcome contract used by file disposition logic."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field, replace
from typing import Literal

import pandas as pd

ParserStatus = Literal[
    "processed",
    "processed_empty",
    "unmapped",
    "processed_external",
    "parse_failed",
]

ParserReason = Literal[
    "no_data_sentinel",
    "zero_rows",
    "all_blank",
    "all_zero_valid",
    "all_unknown_suffix",
    "all_skipped",
    "external_gegoptimareports",
    "parser_error",
    "processing_error",
]

SkipReason = Literal[
    "unparseable_value",
    "blank_value",
    "unparseable_timestamp",
    "row_anchor_failure",
    "row_shape_mismatch",
]

ParserResult = list[tuple[str, pd.DataFrame]]


@dataclass(frozen=True)
class ParserOutcome:
    status: ParserStatus
    dataframes: ParserResult = field(default_factory=list)
    source_row_count: int = 0
    candidate_row_count: int = 0
    rows_written: int = 0
    unmapped_count: int = 0
    reason: ParserReason | None = None
    unmapped_identifiers: tuple[tuple[str, str], ...] = ()
    unsupported_suffixes: frozenset[str] = field(default_factory=frozenset)
    rows_skipped: int = 0
    # Counter[SkipReason] is a static-typing constraint only; Counter does not
    # validate keys at runtime. Tests must assert key membership against the
    # SkipReason Literal values.
    skip_reasons: Counter[SkipReason] = field(default_factory=Counter)

    def derive_final(
        self,
        *,
        rows_written: int,
        candidate_row_count: int,
        unmapped_count: int,
        unsupported_suffixes: frozenset[str],
        rows_skipped: int,
    ) -> ParserOutcome:
        """Return a new outcome with final (status, reason) per spec ladder.

        Ladder (in order):
          1. rows_written > 0                                   -> processed
          2. candidate_row_count > 0 and unmapped_count == candidate_row_count
                                                               -> unmapped
          3. candidate_row_count == 0 and unsupported_suffixes -> processed_empty(all_unknown_suffix)
          4. rows_skipped > 0 and rows_written == 0 and candidate_row_count == 0
                                                               -> processed_empty(all_skipped)
          5. else                                              -> processed_empty(self.reason)

        ``derive_final`` never produces ``parse_failed``; that status only
        arises from caught ``ParserError`` in ``ingest_file``'s exception
        handler.
        """
        new_status: ParserStatus
        new_reason: ParserReason | None

        if rows_written > 0:
            new_status, new_reason = ("processed", None)
        elif candidate_row_count > 0 and unmapped_count == candidate_row_count:
            new_status, new_reason = ("unmapped", None)
        elif candidate_row_count == 0 and unsupported_suffixes:
            new_status, new_reason = ("processed_empty", "all_unknown_suffix")
        elif rows_skipped > 0 and rows_written == 0 and candidate_row_count == 0:
            new_status, new_reason = ("processed_empty", "all_skipped")
        else:
            new_status, new_reason = ("processed_empty", self.reason)

        return replace(
            self,
            status=new_status,
            reason=new_reason,
            rows_written=rows_written,
            candidate_row_count=candidate_row_count,
            unmapped_count=unmapped_count,
            unsupported_suffixes=unsupported_suffixes,
            rows_skipped=rows_skipped,
        )


class NotRelevantParser(Exception):
    """Raised when a parser does not apply to the file."""


class ParserError(Exception):
    """Raised when a matching file cannot be parsed."""


class ProcessingError(Exception):
    """Raised when parsed data cannot be written or otherwise handled."""
```

- [ ] **Step 5: Replace `outcome.dfs` → `outcome.dataframes` and `dfs=` keyword everywhere**

```bash
git grep -l "\.dfs\b" -- src/ tests/ | xargs sed -i '' 's/\.dfs\b/.dataframes/g'
git grep -l "\bdfs=" -- src/ tests/ | xargs sed -i '' 's/\bdfs=/dataframes=/g'
```

Re-read every modified file to confirm the rename did not affect unrelated identifiers (e.g., `pdfs`, `dfs_method`).

- [ ] **Step 6: Update `tests/helpers/outcome_invariants.py`**

Replace the body of `assert_parser_outcome_invariants` so it matches the new contract:

```python
def assert_parser_outcome_invariants(outcome: ParserOutcome) -> None:
    """Assert spec cross-field invariants hold on ``outcome``.

    Raises ``AssertionError`` if any invariant is violated. Test-only —
    do not call from production code.
    """
    if outcome.status == "processed":
        assert outcome.rows_written >= 1, f"status='processed' requires rows_written >= 1, got {outcome.rows_written}"
    elif outcome.status == "processed_empty":
        assert outcome.rows_written == 0, (
            f"status='processed_empty' requires rows_written == 0, got {outcome.rows_written}"
        )
        assert outcome.unmapped_count == 0, (
            f"status='processed_empty' requires unmapped_count == 0, got {outcome.unmapped_count}"
        )
    elif outcome.status == "unmapped":
        assert outcome.rows_written == 0, f"status='unmapped' requires rows_written == 0, got {outcome.rows_written}"
        assert outcome.candidate_row_count > 0, (
            f"status='unmapped' requires candidate_row_count > 0, got {outcome.candidate_row_count}"
        )
        assert outcome.unmapped_count == outcome.candidate_row_count, (
            f"status='unmapped' requires unmapped_count == candidate_row_count "
            f"({outcome.candidate_row_count}), got {outcome.unmapped_count}"
        )
    elif outcome.status == "processed_external":
        assert outcome.rows_written == 0, (
            f"status='processed_external' requires rows_written == 0, got {outcome.rows_written}"
        )
        assert list(outcome.dataframes) == [], (
            f"status='processed_external' requires dataframes == [], got {len(list(outcome.dataframes))} dataframes"
        )
    elif outcome.status == "parse_failed":
        assert outcome.rows_written == 0, (
            f"status='parse_failed' requires rows_written == 0, got {outcome.rows_written}"
        )
        assert outcome.reason in {"parser_error", "processing_error"}, (
            f"status='parse_failed' requires reason in {{parser_error, processing_error}}, got {outcome.reason}"
        )

    if outcome.skip_reasons:
        skip_total = sum(outcome.skip_reasons.values())
        assert outcome.rows_skipped <= skip_total, (
            f"rows_skipped ({outcome.rows_skipped}) must be <= sum(skip_reasons) ({skip_total})"
        )
```

- [ ] **Step 7: Run all tests**

```bash
uv run ruff check src/ tests/
uv run pytest -q
```

Expected: lint clean; all tests pass. Test failures here usually mean a `outcome.dfs` reference was missed (Step 5 sed) or a parser still constructs `ParserOutcome(dfs=...)`.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "feat: extend ParserOutcome with parse_failed status, derive_final, dataframes rename"
```

---

## Task 6: Drop `error_file_path` parameter from all parsers and the dispatcher

**Files (modify, drop parameter):**
- `src/shared/non_nem_parsers.py`
- `src/shared/parsers/envizi/vertical_water.py`
- `src/shared/parsers/envizi/vertical_water_bulk.py`
- `src/shared/parsers/envizi/vertical_electricity.py`
- `src/shared/parsers/optima/racv_billing.py`
- `src/shared/parsers/optima/interval.py`
- `src/shared/parsers/optima/bunnings_billing.py`
- `src/shared/parsers/optima/demand.py`
- `src/shared/parsers/racv/elec.py`
- `src/shared/parsers/racv/noosa_solar.py`
- `src/shared/parsers/green_square/comx.py`
- `src/functions/file_processor/app.py` (caller of `get_non_nem_outcome`)
- `tests/unit/**` (callers in tests)

**Goal:** the parameter is vestigial — no parser body reads it. Removing it cleans up the signature and prepares for the dispatcher rename in Task 7.

- [ ] **Step 1: Survey caller sites**

```bash
grep -rn "error_file_path" src/ tests/
```

Note every caller site. Each parser is called either directly in tests or through `get_non_nem_outcome` / `get_non_nem_df` in `non_nem_parsers.py`.

- [ ] **Step 2: Update parser signatures**

For each of the 10 parser files, change the function signature from:

```python
def parser_name(file_name: str, error_file_path: str) -> ParserOutcome:
```

to:

```python
def parser_name(file_name: str) -> ParserOutcome:
```

Remove any `_ = error_file_path` lines and any docstring lines that mention the parameter.

- [ ] **Step 3: Update `src/shared/non_nem_parsers.py`**

Change the body so it no longer takes or threads `error_file_path`. The new signatures:

```python
def get_non_nem_outcome(file_name: str) -> ParserOutcome:
    for parser in PARSERS:
        try:
            return _as_outcome(parser(file_name))
        except NotRelevantParser as e:
            logger.debug(
                "Parser not relevant",
                extra={"parser": parser.__name__, "file": file_name, "error": str(e)},
            )
        except (ParserError, ProcessingError):
            raise
        except Exception as e:
            logger.exception(
                "Unexpected parser failure",
                extra={"parser": parser.__name__, "file": file_name, "error": str(e)},
            )
            raise ParserError(f"Unexpected parser failure in {parser.__name__}: {e}") from e

    logger.error("No valid parser found", extra={"file": file_name})
    raise ParserError(f"get_non_nem_outcome: {file_name}: No Valid Parser Found")


def get_non_nem_df(file_name: str) -> ParserResult:
    return get_non_nem_outcome(file_name).dataframes
```

- [ ] **Step 4: Update `src/functions/file_processor/app.py` (caller site)**

Locate the call `get_non_nem_outcome(local_file_path, PARSE_ERROR_LOG_GROUP)` (around line 759 in the original file) and change to `get_non_nem_outcome(local_file_path)`.

- [ ] **Step 5: Update test callers**

```bash
grep -rln "error_file_path" tests/
```

For each test file: drop the second positional / keyword argument from any direct parser invocation.

- [ ] **Step 6: Run all tests + lint**

```bash
uv run ruff check src/ tests/
uv run pytest -q
```

Expected: lint clean; all tests pass.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "refactor: drop unused error_file_path parameter from all parsers"
```

---

## Task 7: Rename `non_nem_parsers.py` → `parsers/dispatcher.py`; rename `get_non_nem_outcome` → `dispatch_non_nem`

**Files:**
- Move (`git mv`): `src/shared/non_nem_parsers.py` → `src/shared/parsers/dispatcher.py`
- Update: `src/shared/__init__.py` (re-export path)
- Update: `src/functions/file_processor/app.py` (caller)
- Update: every test that imports `from shared.non_nem_parsers` or `from shared import get_non_nem_df`

- [ ] **Step 1: Move the file**

```bash
git mv src/shared/non_nem_parsers.py src/shared/parsers/dispatcher.py
```

- [ ] **Step 2: Rename `get_non_nem_outcome` → `dispatch_non_nem` inside the moved file**

Edit `src/shared/parsers/dispatcher.py`:

- Function name: `get_non_nem_outcome` → `dispatch_non_nem`.
- Update internal log message: `f"get_non_nem_outcome: {file_name}: ..."` → `f"dispatch_non_nem: {file_name}: ..."`.
- Function `get_non_nem_df` calls the renamed function — rename its body call site.
- Top-level imports: `from shared.parsers ...` already correct; verify nothing else points at the old module name.

- [ ] **Step 3: Update `src/shared/__init__.py`**

Replace `from shared.non_nem_parsers import get_non_nem_df` with:

```python
from shared.parsers.dispatcher import dispatch_non_nem, get_non_nem_df
```

Add `dispatch_non_nem` to `__all__` (sorted).

- [ ] **Step 4: Update consumers**

```bash
git grep -l "from shared.non_nem_parsers\|from shared\.non_nem_parsers\|shared\.non_nem_parsers\b\|\bget_non_nem_outcome\b" -- 'src/' 'tests/'
```

For every match:

- Replace `from shared.non_nem_parsers import` with `from shared.parsers.dispatcher import`.
- Replace identifier `get_non_nem_outcome` → `dispatch_non_nem`.

For the `app.py` caller, the call becomes:

```python
outcome = dispatch_non_nem(local_file_path)
```

(Drop the `PARSE_ERROR_LOG_GROUP` argument that was already removed in Task 6.)

- [ ] **Step 5: Lint and test**

```bash
uv run ruff check src/ tests/
uv run pytest -q
```

Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor: move non_nem_parsers to parsers.dispatcher and rename to dispatch_non_nem"
```

---

## Task 8: Extract `HudiSourceCsvWriter` (and `StagedCsvUpload`) into `csv_writer.py`; rename `BATCH_SIZE` → `CSV_FLUSH_ROW_THRESHOLD`

**Files:**
- Create: `src/functions/file_processor/csv_writer.py`
- Create: `tests/unit/test_csv_writer.py`
- Modify: `src/functions/file_processor/app.py` (delete the moved classes, update imports + `BATCH_SIZE` references)

- [ ] **Step 1: Write failing tests**

Create `tests/unit/test_csv_writer.py`:

```python
"""Tests for HudiSourceCsvWriter staging/commit/abort lifecycle."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import boto3
import pandas as pd
import pytest
from moto import mock_aws

from functions.file_processor.csv_writer import HudiSourceCsvWriter, StagedCsvUpload
from shared.common import HUDI_BUCKET, HUDI_FINAL_PREFIX, HUDI_STAGING_PREFIX


@pytest.fixture
def hudi_bucket():
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket=HUDI_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )
        yield s3


@pytest.fixture
def executor():
    pool = ThreadPoolExecutor(max_workers=2)
    yield pool
    pool.shutdown(wait=True)


class TestHudiSourceCsvWriter:
    def test_write_row_appends_to_buffer(self, hudi_bucket, executor) -> None:
        writer = HudiSourceCsvWriter(batch_timestamp="2026_May_07T00_00_00_000000", executor=executor)
        ts = pd.Timestamp("2026-05-07 00:00:00")
        writer.write_row("p:bunnings:abc", ts, 1.5, "kwh", "A")
        assert writer.row_count == 1

    def test_flush_uploads_to_staging_prefix(self, hudi_bucket, executor) -> None:
        writer = HudiSourceCsvWriter(batch_timestamp="2026_May_07T00_00_00_000000", executor=executor)
        writer.write_row("p:bunnings:abc", pd.Timestamp("2026-05-07 00:00:00"), 1.5, "kwh", "A")
        writer.flush()

        # Wait for upload to complete by polling .upload_jobs futures
        for job in writer.upload_jobs:
            job.future.result()

        listed = hudi_bucket.list_objects_v2(Bucket=HUDI_BUCKET, Prefix=HUDI_STAGING_PREFIX).get("Contents", [])
        assert len(listed) == 1
        assert listed[0]["Key"].startswith(f"{HUDI_STAGING_PREFIX}/")

    def test_commit_promotes_staging_to_final(self, hudi_bucket, executor) -> None:
        writer = HudiSourceCsvWriter(batch_timestamp="2026_May_07T00_00_00_000000", executor=executor)
        writer.write_row("p:bunnings:abc", pd.Timestamp("2026-05-07 00:00:00"), 1.5, "kwh", "A")
        writer.flush()
        writer.commit()

        final_objs = hudi_bucket.list_objects_v2(Bucket=HUDI_BUCKET, Prefix=HUDI_FINAL_PREFIX).get("Contents", [])
        staging_objs = hudi_bucket.list_objects_v2(Bucket=HUDI_BUCKET, Prefix=HUDI_STAGING_PREFIX).get("Contents", [])
        assert len(final_objs) == 1
        assert len(staging_objs) == 0  # staging cleaned up after copy
        assert final_objs[0]["Key"].startswith(f"{HUDI_FINAL_PREFIX}/")

    def test_abort_deletes_staging_and_final(self, hudi_bucket, executor) -> None:
        writer = HudiSourceCsvWriter(batch_timestamp="2026_May_07T00_00_00_000000", executor=executor)
        writer.write_row("p:bunnings:abc", pd.Timestamp("2026-05-07 00:00:00"), 1.5, "kwh", "A")
        writer.flush()
        writer.commit()
        writer.abort()  # rolls back the committed final keys

        final_objs = hudi_bucket.list_objects_v2(Bucket=HUDI_BUCKET, Prefix=HUDI_FINAL_PREFIX).get("Contents", [])
        assert len(final_objs) == 0

    def test_quality_none_renders_empty_cell(self, hudi_bucket, executor) -> None:
        writer = HudiSourceCsvWriter(batch_timestamp="2026_May_07T00_00_00_000000", executor=executor)
        writer.write_row("p:bunnings:abc", pd.Timestamp("2026-05-07 00:00:00"), 1.5, "kwh", None)
        writer.flush()
        writer.commit()

        final_objs = hudi_bucket.list_objects_v2(Bucket=HUDI_BUCKET, Prefix=HUDI_FINAL_PREFIX).get("Contents", [])
        body = hudi_bucket.get_object(Bucket=HUDI_BUCKET, Key=final_objs[0]["Key"])["Body"].read().decode()
        # Header line + data line with empty trailing quality cell
        assert body.endswith(",\n")  # ends with empty quality cell


class TestStagedCsvUpload:
    def test_dataclass_fields(self) -> None:
        # Quick smoke test: dataclass exists and has the expected fields.
        import dataclasses

        fields = {f.name for f in dataclasses.fields(StagedCsvUpload)}
        assert fields == {"future", "staging_key", "final_key"}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/unit/test_csv_writer.py -v
```

Expected: `ModuleNotFoundError: No module named 'functions.file_processor.csv_writer'`.

- [ ] **Step 3: Create `src/functions/file_processor/csv_writer.py`**

```python
"""HudiSourceCsvWriter — writes Hudi-shaped CSV objects to S3.

Despite the name "Hudi", this class does NOT write Apache Hudi tables. It
writes plain CSV objects to ``s3://<HUDI_BUCKET>/<HUDI_FINAL_PREFIX>/`` that
the downstream ``DataImportIntoLake`` Glue job consumes into the actual
Hudi table.

Lifecycle:
  - ``write_row`` appends to an in-memory buffer.
  - ``flush`` uploads the buffer to a staging key under HUDI_STAGING_PREFIX
    via a ThreadPoolExecutor (parallelism for large files).
  - ``commit`` copies all staged keys to their final HUDI_FINAL_PREFIX
    locations and deletes the staging copies.
  - ``abort`` deletes everything this writer staged or committed (rollback
    on parse error or downstream-move failure).
"""

from __future__ import annotations

import io
import random
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

import boto3
from aws_lambda_powertools import Logger, Tracer

from shared.common import HUDI_BUCKET, HUDI_FINAL_PREFIX, HUDI_STAGING_PREFIX

logger = Logger(service="hudi-source-csv-writer", child=True)
tracer = Tracer(service="hudi-source-csv-writer")

s3_resource = boto3.resource("s3")


@dataclass(frozen=True)
class StagedCsvUpload:
    future: Future[None]
    staging_key: str
    final_key: str


def _upload_csv_to_s3(csv_content: str, output_key: str, parent_xray_trace_entity: Any = None) -> None:
    """Upload CSV content to S3. Used by ThreadPoolExecutor.

    Powertools' Tracer auto-instrumentation does not cross thread boundaries.
    To make the parallel S3 PUT subsegments children of the parent ingest_file
    segment (instead of orphan segments), the caller passes the parent X-Ray
    entity captured on the main thread, and we re-attach it inside the worker
    via aws_xray_sdk.core.xray_recorder.set_trace_entity.
    """
    if parent_xray_trace_entity is not None:
        try:
            from aws_xray_sdk.core import xray_recorder

            xray_recorder.set_trace_entity(parent_xray_trace_entity)
        except Exception:  # X-Ray not available in test env — best-effort.
            pass

    s3_resource.Object(HUDI_BUCKET, output_key).put(Body=csv_content)
    logger.debug("Uploaded CSV to S3", extra={"output_key": output_key})


class HudiSourceCsvWriter:
    """Writes Hudi-shaped CSV objects to S3 with a staging/commit/abort lifecycle."""

    CSV_HEADER = "sensorId,ts,val,unit,its,quality\n"
    TS_FORMAT = "%Y-%m-%d %H:%M:%S"

    def __init__(self, batch_timestamp: str, executor: ThreadPoolExecutor) -> None:
        self.batch_timestamp = batch_timestamp
        self.executor = executor
        self.writer_token = uuid.uuid4().hex
        self.buffer = io.StringIO()
        self.buffer.write(self.CSV_HEADER)
        self.row_count = 0
        self.upload_jobs: list[StagedCsvUpload] = []
        self.committed_final_keys: list[str] = []

    def write_row(self, sensor_id: str, ts: Any, val: float, unit: str, quality: str | None = None) -> None:
        """Write a single row to the buffer.

        ``quality=None`` (vendor did not supply a quality value) is serialised
        as an empty cell so Athena/Presto reads the column as NULL. Vendor-
        supplied codes (``A``/``E``/``S14``/etc.) pass through verbatim.
        """
        ts_str = ts.strftime(self.TS_FORMAT) if hasattr(ts, "strftime") else str(ts)
        quality_field = "" if quality is None else quality
        self.buffer.write(f"{sensor_id},{ts_str},{val},{unit},{ts_str},{quality_field}\n")
        self.row_count += 1

    def flush(self) -> None:
        """Upload current buffer to staging asynchronously."""
        if self.row_count == 0:
            return

        csv_content = self.buffer.getvalue()
        batch_file_name = f"batch_{self.batch_timestamp}_{self.writer_token}_{random.randint(1, 1000000)}.csv"
        staging_key = f"{HUDI_STAGING_PREFIX}/{self.writer_token}/{batch_file_name}"
        final_key = f"{HUDI_FINAL_PREFIX}/{batch_file_name}"

        # Capture parent X-Ray entity on the calling thread so the worker
        # can re-attach it (Powertools Tracer does not cross thread boundaries).
        parent_entity = None
        try:
            from aws_xray_sdk.core import xray_recorder

            parent_entity = xray_recorder.get_trace_entity()
        except Exception:
            pass

        future = self.executor.submit(_upload_csv_to_s3, csv_content, staging_key, parent_entity)
        self.upload_jobs.append(StagedCsvUpload(future=future, staging_key=staging_key, final_key=final_key))

        logger.debug("Submitted CSV upload", extra={"output_key": staging_key, "rows": self.row_count})

        self.buffer = io.StringIO()
        self.buffer.write(self.CSV_HEADER)
        self.row_count = 0

    @tracer.capture_method
    def commit(self) -> None:
        """Publish staged uploads to final Hudi keys."""
        jobs = list(self.upload_jobs)
        for job in jobs:
            job.future.result()

        for job in jobs:
            s3_resource.Object(HUDI_BUCKET, job.final_key).copy({"Bucket": HUDI_BUCKET, "Key": job.staging_key})
            self.committed_final_keys.append(job.final_key)
            s3_resource.Object(HUDI_BUCKET, job.staging_key).delete()
            logger.debug(
                "Committed staged CSV upload",
                extra={"staging_key": job.staging_key, "final_key": job.final_key},
            )

        self.upload_jobs.clear()

    @tracer.capture_method
    def abort(self) -> None:
        """Observe pending uploads and delete writer-owned staged/final objects."""
        jobs = list(self.upload_jobs)
        for job in jobs:
            try:
                job.future.result()
            except Exception as e:
                logger.warning(
                    "Staged CSV upload failed during abort",
                    extra={"staging_key": job.staging_key, "final_key": job.final_key, "error": str(e)},
                )

        keys_to_delete = [job.staging_key for job in jobs] + self.committed_final_keys
        seen_keys: set[str] = set()
        for key in keys_to_delete:
            if key in seen_keys:
                continue
            seen_keys.add(key)
            try:
                s3_resource.Object(HUDI_BUCKET, key).delete()
            except Exception as e:
                logger.warning("Failed to delete staged CSV object", extra={"key": key, "error": str(e)})

        self.upload_jobs.clear()
        self.committed_final_keys.clear()
```

- [ ] **Step 4: Update `src/functions/file_processor/app.py`**

Delete `_upload_csv_to_s3`, `CSVUploadJob`, `DirectCSVWriter` from `app.py`. Replace local references in `parse_and_write_data` with imports:

```python
from functions.file_processor.csv_writer import HudiSourceCsvWriter
```

And rename references inside `parse_and_write_data`:

- `csv_writer = DirectCSVWriter(...)` → `csv_writer = HudiSourceCsvWriter(...)`.
- `BATCH_SIZE` → `CSV_FLUSH_ROW_THRESHOLD`. Update the constant definition at the top of `app.py`:

```python
# Maximum rows held in HudiSourceCsvWriter buffer before flushing to staging.
CSV_FLUSH_ROW_THRESHOLD = 50000
```

(Tests reading the old `BATCH_SIZE` symbol need to be updated; check `tests/unit/test_batch_s3_writes.py` which is going to be deleted in Task 15 anyway, plus `test_edge_cases.py` references — search and rename.)

```bash
git grep -l "BATCH_SIZE" -- 'src/' 'tests/' | xargs sed -i '' 's/\bBATCH_SIZE\b/CSV_FLUSH_ROW_THRESHOLD/g'
```

Skim each match to confirm no false positive (e.g., a comment that meant SQS `batch_size`).

- [ ] **Step 5: Lint and test**

```bash
uv run ruff check src/ tests/
uv run pytest -q
```

Expected: clean. (Tests in `test_batch_s3_writes.py` may need short-term patching to keep importing the new class names; we delete that file in Task 15.)

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor: extract HudiSourceCsvWriter into csv_writer.py and rename BATCH_SIZE"
```

---

## Task 9: Create `persistence.py` with `InstrumentedDynamoDBPersistenceLayer`

**Files:**
- Create: `src/functions/file_processor/persistence.py`
- Create: `tests/unit/test_persistence_cache_hit_log.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_persistence_cache_hit_log.py`:

```python
"""Tests for InstrumentedDynamoDBPersistenceLayer cache-hit log emission."""

from __future__ import annotations

import json
import logging
from unittest.mock import patch

import boto3
import pytest
from aws_lambda_powertools.utilities.idempotency.exceptions import (
    IdempotencyItemAlreadyExistsError,
)
from moto import mock_aws

from functions.file_processor.persistence import InstrumentedDynamoDBPersistenceLayer


@pytest.fixture
def idempotency_table():
    with mock_aws():
        ddb = boto3.client("dynamodb")
        ddb.create_table(
            TableName="sbm-ingester-idempotency",
            KeySchema=[{"AttributeName": "file_key", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "file_key", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        yield ddb


class TestInstrumentedPersistenceLayer:
    def test_logs_idempotent_cache_hit_on_existing_record(self, idempotency_table, caplog) -> None:
        layer = InstrumentedDynamoDBPersistenceLayer(
            table_name="sbm-ingester-idempotency",
            key_attr="file_key",
        )

        # Simulate a parent-class save_inprogress that detects an existing record.
        with patch.object(
            type(layer).__mro__[1],  # parent class (DynamoDBPersistenceLayer)
            "save_inprogress",
            side_effect=IdempotencyItemAlreadyExistsError(),
        ):
            with caplog.at_level(logging.INFO):
                with pytest.raises(IdempotencyItemAlreadyExistsError):
                    layer.save_inprogress(
                        data={"bucket": "sbm-file-ingester", "key": "newTBP/foo.csv"}
                    )

        # Find the structured log record by message.
        cache_hit_records = [r for r in caplog.records if r.message == "idempotent_cache_hit"]
        assert len(cache_hit_records) == 1
        # Logger may emit as JSON or as extra kwargs; the test asserts that
        # source_bucket and source_key fields are reachable on the record.
        record = cache_hit_records[0]
        # Powertools structures via 'extra'; the attributes land on the LogRecord.
        assert getattr(record, "source_bucket", None) == "sbm-file-ingester"
        assert getattr(record, "source_key", None) == "newTBP/foo.csv"

    def test_no_log_on_first_call(self, idempotency_table, caplog) -> None:
        layer = InstrumentedDynamoDBPersistenceLayer(
            table_name="sbm-ingester-idempotency",
            key_attr="file_key",
        )

        with patch.object(
            type(layer).__mro__[1],
            "save_inprogress",
            return_value=None,
        ):
            with caplog.at_level(logging.INFO):
                layer.save_inprogress(data={"bucket": "b", "key": "k"})

        cache_hit_records = [r for r in caplog.records if r.message == "idempotent_cache_hit"]
        assert len(cache_hit_records) == 0
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/unit/test_persistence_cache_hit_log.py -v
```

Expected: `ModuleNotFoundError: No module named 'functions.file_processor.persistence'`.

- [ ] **Step 3: Create `src/functions/file_processor/persistence.py`**

```python
"""Persistence layer subclass that logs idempotency cache hits.

Powertools does not expose a native cache-hit hook. The cache-hit code path
is the IdempotencyItemAlreadyExistsError raised by save_inprogress when a
record already exists. We intercept that exception here, emit a structured
log line, and re-raise so Powertools handles the cached response normally.

The CloudWatch alarm for cache-hit rate uses DynamoDB's native
ConditionalCheckFailedRequests metric on the idempotency table — no custom
Lambda metric is emitted.
"""

from __future__ import annotations

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.idempotency import DynamoDBPersistenceLayer
from aws_lambda_powertools.utilities.idempotency.exceptions import (
    IdempotencyItemAlreadyExistsError,
)

logger = Logger(service="instrumented-persistence", child=True)


class InstrumentedDynamoDBPersistenceLayer(DynamoDBPersistenceLayer):
    """DynamoDB persistence layer that logs cache hits."""

    def save_inprogress(self, data, remaining_time_in_millis=None):
        try:
            return super().save_inprogress(data, remaining_time_in_millis)
        except IdempotencyItemAlreadyExistsError:
            payload = data if isinstance(data, dict) else {}
            logger.info(
                "idempotent_cache_hit",
                extra={
                    "source_bucket": payload.get("bucket"),
                    "source_key": payload.get("key"),
                },
            )
            raise
```

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/unit/test_persistence_cache_hit_log.py -v
```

Expected: 2 PASSED.

- [ ] **Step 5: Commit**

```bash
git add src/functions/file_processor/persistence.py tests/unit/test_persistence_cache_hit_log.py
git commit -m "feat: add InstrumentedDynamoDBPersistenceLayer for cache-hit logging"
```

---

## Task 10: Create `pipeline.py` with `ingest_file`

**Files:**
- Create: `src/functions/file_processor/pipeline.py`
- Create: `tests/unit/test_pipeline.py`
- Create: `tests/unit/test_idempotency_boundary.py`

**Goal:** This is the structural heart of the refactor. `pipeline.py` owns all per-file orchestration: download, parse (NEM12 stream → non-NEM dispatcher; **no batch fallback**), extract candidates, look up Neptune IDs from `nem12_mappings`, write CSV via `HudiSourceCsvWriter`, move source by outcome status, emit metrics, write audit sidecar, emit `parser_outcome` log. Catch `ParserError` → return `ParserOutcome(status="parse_failed", reason="parser_error")` so the cached outcome is honored on retry. Decorator order: `@tracer.capture_method` outer, `@idempotent_function` inner.

**This is the single biggest task in the plan. The code is long but is mostly relocation from `app.py`'s `parse_and_write_data` body. Keep behaviour-preserving except for the explicitly listed deltas.**

- [ ] **Step 1: Write end-to-end failing tests**

Create `tests/unit/test_pipeline.py`:

```python
"""End-to-end tests for ingest_file using moto-mocked S3 + DynamoDB."""

from __future__ import annotations

import json
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

from functions.file_processor.pipeline import ingest_file
from shared.common import HUDI_BUCKET, INPUT_BUCKET, PROCESSED_DIR, UNMAPPED_DIR
from shared.source_file import SourceFile
from tests.helpers.outcome_invariants import assert_parser_outcome_invariants


NEM12_BODY = b"""\
100,NEM12,202605060200,MDP1,Origin
200,NMI001,E1,1,E1,N1,METER1,kWh,30,
300,20260506,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,A,,,
900
"""


@pytest.fixture
def aws_environment(monkeypatch):
    monkeypatch.setenv("SQS_QUEUE_URL", "https://sqs.test.local/queue")
    with mock_aws():
        s3 = boto3.client("s3")
        ddb = boto3.client("dynamodb")
        for bucket in [INPUT_BUCKET, HUDI_BUCKET]:
            s3.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
            )
        ddb.create_table(
            TableName="sbm-ingester-idempotency",
            KeySchema=[{"AttributeName": "file_key", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "file_key", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        # Provide nem12 mappings JSON the loader fetches.
        s3.put_object(
            Bucket=INPUT_BUCKET,
            Key="nem12_mappings.json",
            Body=json.dumps({"NMI001-E1": "p:bunnings:abc"}),
        )
        yield s3, ddb


def _put_source(s3, body: bytes, key: str = "newTBP/sample.csv") -> None:
    s3.put_object(Bucket=INPUT_BUCKET, Key=key, Body=body)


class TestIngestFileNem12HappyPath:
    def test_processed_outcome_routes_to_newp(self, aws_environment) -> None:
        s3, _ = aws_environment
        _put_source(s3, NEM12_BODY)

        outcome = ingest_file(source_file=SourceFile(bucket=INPUT_BUCKET, key="newTBP/sample.csv"))

        assert_parser_outcome_invariants(outcome)
        assert outcome.status == "processed"
        assert outcome.rows_written > 0

        listed = s3.list_objects_v2(Bucket=INPUT_BUCKET, Prefix=PROCESSED_DIR).get("Contents", [])
        assert any(o["Key"].endswith("sample.csv") for o in listed)

    def test_hudi_csv_written_to_final_prefix(self, aws_environment) -> None:
        s3, _ = aws_environment
        _put_source(s3, NEM12_BODY)

        ingest_file(source_file=SourceFile(bucket=INPUT_BUCKET, key="newTBP/sample.csv"))

        listed = s3.list_objects_v2(Bucket=HUDI_BUCKET, Prefix="sensorDataFiles/").get("Contents", [])
        assert len(listed) == 1


class TestIngestFileNem12Empty:
    def test_envelope_only_yields_processed_empty(self, aws_environment) -> None:
        s3, _ = aws_environment
        _put_source(s3, b"100,NEM12,202605060200,MDP1,Origin\n900\n")

        outcome = ingest_file(source_file=SourceFile(bucket=INPUT_BUCKET, key="newTBP/sample.csv"))

        assert_parser_outcome_invariants(outcome)
        assert outcome.status == "processed_empty"
        assert outcome.reason == "no_data_sentinel"


class TestIngestFileParseFailedCachable:
    def test_parser_error_returns_parse_failed_outcome(self, aws_environment) -> None:
        """Structurally broken NEM12 + non-NEM dispatcher cannot parse it -> parse_failed.

        Asserts: file moved to newParseErr/, outcome returned (not raised),
        outcome cached so a duplicate call returns the same outcome without
        re-attempting parsing.
        """
        s3, _ = aws_environment
        s3.put_object(
            Bucket=INPUT_BUCKET,
            Key="newTBP/broken.csv",
            Body=b"100,NEM12,bad\n200,malformed\n900\n",
        )

        outcome1 = ingest_file(source_file=SourceFile(bucket=INPUT_BUCKET, key="newTBP/broken.csv"))
        assert outcome1.status == "parse_failed"
        assert outcome1.reason == "parser_error"

        # File moved to newParseErr/
        listed = s3.list_objects_v2(Bucket=INPUT_BUCKET, Prefix="newParseErr/").get("Contents", [])
        assert any(o["Key"].endswith("broken.csv") for o in listed)


class TestIngestFileTransientFailureRaises:
    def test_dynamodb_throttle_propagates_as_processing_error(self, aws_environment) -> None:
        s3, _ = aws_environment
        _put_source(s3, NEM12_BODY)

        # Simulate a 5xx-equivalent: HudiSourceCsvWriter.commit raises during
        # the S3 copy step. Pipeline must call abort and re-raise.
        from functions.file_processor.csv_writer import HudiSourceCsvWriter

        with patch.object(HudiSourceCsvWriter, "commit", side_effect=RuntimeError("simulated S3 5xx")):
            with pytest.raises(RuntimeError):
                ingest_file(source_file=SourceFile(bucket=INPUT_BUCKET, key="newTBP/sample.csv"))
```

Create `tests/unit/test_idempotency_boundary.py`:

```python
"""Tests for the cache-hit / cache-miss / raise-vs-return contract."""

from __future__ import annotations

import json

import boto3
import pytest
from moto import mock_aws

from functions.file_processor.pipeline import ingest_file
from shared.common import HUDI_BUCKET, INPUT_BUCKET
from shared.source_file import SourceFile


NEM12_BODY = b"""\
100,NEM12,202605060200,MDP1,Origin
200,NMI001,E1,1,E1,N1,METER1,kWh,30,
300,20260506,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,A,,,
900
"""


@pytest.fixture
def aws_environment(monkeypatch):
    monkeypatch.setenv("SQS_QUEUE_URL", "https://sqs.test.local/queue")
    with mock_aws():
        s3 = boto3.client("s3")
        ddb = boto3.client("dynamodb")
        for bucket in [INPUT_BUCKET, HUDI_BUCKET]:
            s3.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
            )
        ddb.create_table(
            TableName="sbm-ingester-idempotency",
            KeySchema=[{"AttributeName": "file_key", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "file_key", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        s3.put_object(
            Bucket=INPUT_BUCKET,
            Key="nem12_mappings.json",
            Body=json.dumps({"NMI001-E1": "p:bunnings:abc"}),
        )
        yield s3, ddb


class TestIdempotencyBoundary:
    def test_duplicate_call_returns_cached_outcome_without_reprocessing(self, aws_environment) -> None:
        s3, _ = aws_environment
        s3.put_object(Bucket=INPUT_BUCKET, Key="newTBP/sample.csv", Body=NEM12_BODY)

        src = SourceFile(bucket=INPUT_BUCKET, key="newTBP/sample.csv")
        outcome1 = ingest_file(source_file=src)

        # Manually re-place the source file at newTBP/ so the second call
        # would re-process if the cache were missed.
        s3.put_object(Bucket=INPUT_BUCKET, Key="newTBP/sample.csv", Body=NEM12_BODY)

        outcome2 = ingest_file(source_file=src)

        assert outcome2.status == outcome1.status
        assert outcome2.rows_written == outcome1.rows_written

        # Crucially: the second call did NOT move the re-placed file to newP/
        # again — it returned the cached outcome.
        newtbp_listing = s3.list_objects_v2(Bucket=INPUT_BUCKET, Prefix="newTBP/").get("Contents", [])
        # The re-placed file is still in newTBP/ because cache-hit short-circuits.
        assert any(o["Key"] == "newTBP/sample.csv" for o in newtbp_listing)

    def test_runtime_error_in_nem_path_propagates_does_not_reach_dispatcher(
        self, aws_environment, monkeypatch
    ) -> None:
        from functions.file_processor import pipeline

        s3, _ = aws_environment
        s3.put_object(Bucket=INPUT_BUCKET, Key="newTBP/sample.csv", Body=NEM12_BODY)

        # Streaming parser raises a RuntimeError (not in _NEM_FALLTHROUGH_ERRORS).
        # Per spec: must propagate and must NOT be silently routed to non-NEM dispatcher.
        def boom(*_a, **_kw):
            raise RuntimeError("simulated nemreader internal bug")

        monkeypatch.setattr(pipeline, "stream_as_data_frames", boom)

        # dispatch_non_nem must NOT be reached.
        called = {"non_nem": False}

        def must_not_be_called(*_a, **_kw):
            called["non_nem"] = True
            raise AssertionError("dispatcher must not be reached on RuntimeError")

        monkeypatch.setattr(pipeline, "dispatch_non_nem", must_not_be_called)

        with pytest.raises(RuntimeError):
            ingest_file(source_file=SourceFile(bucket=INPUT_BUCKET, key="newTBP/sample.csv"))

        assert called["non_nem"] is False
```

- [ ] **Step 2: Run tests to verify they fail (and to catch import-level surprises)**

```bash
uv run pytest tests/unit/test_pipeline.py tests/unit/test_idempotency_boundary.py -v
```

Expected: `ModuleNotFoundError: No module named 'functions.file_processor.pipeline'`.

- [ ] **Step 3: Create `src/functions/file_processor/pipeline.py`**

```python
"""ingest_file — process one source file end-to-end inside the idempotent boundary.

All side effects live INSIDE the @idempotent_function boundary so that
duplicate SQS deliveries hit the Powertools cache and do not replay any
state-changing operation.

Contract evolution: deterministic content failures (ParserError) are caught
and RETURNED as ParserOutcome(status="parse_failed", reason="parser_error").
Returned outcomes are cached for 12 h. Transient infrastructure failures
(S3 5xx, DynamoDB throttle, etc.) are RAISED so Powertools deletes the
in-progress record and SQS retry can re-execute.

Decorator order is load-bearing: @tracer.capture_method must be the OUTER
decorator and @idempotent_function the INNER (closest to def). Powertools
docs require this order so cache hits are still captured in X-Ray.
"""

from __future__ import annotations

import shutil
import tempfile
import traceback
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from itertools import chain
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import boto3
import pandas as pd
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities.idempotency import (
    IdempotencyConfig,
    idempotent_function,
)

from functions.file_processor.csv_writer import HudiSourceCsvWriter
from functions.file_processor.persistence import InstrumentedDynamoDBPersistenceLayer
from shared.audit import SAMPLE_CAP as AUDIT_SAMPLE_CAP
from shared.audit import write_audit_sidecar
from shared.common import (
    INPUT_BUCKET,
    PARSE_ERR_DIR,
    PROCESSED_DIR,
    UNMAPPED_DIR,
)
from shared.nem_adapter import _is_nem_envelope_only, stream_as_data_frames
from shared.parsers import ParserError, ParserOutcome, ParserReason, ProcessingError
from shared.parsers._mappings import get_nem12_mappings
from shared.parsers.dispatcher import dispatch_non_nem
from shared.parsers.outcome import SkipReason
from shared.source_file import SourceFile

logger = Logger(service="file-processor")
tracer = Tracer(service="file-processor")
metrics = Metrics(namespace="SBM/Ingester")

persistence_layer = InstrumentedDynamoDBPersistenceLayer(
    table_name="sbm-ingester-idempotency",
    key_attr="file_key",
)
idempotency_config = IdempotencyConfig(
    expires_after_seconds=43200,  # 12 hours TTL
)

S3_WRITE_WORKERS = 4
CSV_FLUSH_ROW_THRESHOLD = 50000

s3_resource = boto3.resource("s3")
s3_client = boto3.client("s3")

NMI_DATA_STREAM_SUFFIX = list("ABCDEFJKLPQRSTUGHYMWVZ")
NMI_DATA_STREAM_CHANNEL = list("123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
NMI_DATA_STREAM_COMBINED = frozenset(i + j for i in NMI_DATA_STREAM_SUFFIX for j in NMI_DATA_STREAM_CHANNEL)

# Narrowed exception tuple for "this is not a NEM12 file or has no payload".
# Anything outside this set propagates so genuine parser bugs surface.
_NEM_FALLTHROUGH_ERRORS: tuple[type[BaseException], ...] = (
    ValueError,
    KeyError,
    IndexError,
    AssertionError,
    UnicodeDecodeError,
    StopIteration,
)


@dataclass(frozen=True)
class DataFrameCandidate:
    ts: Any
    val: float
    quality: str | None


def _is_blank_value(value: Any) -> bool:
    return isinstance(value, str) and value.strip() == ""


def extract_valid_readings(
    df: pd.DataFrame,
    col: str,
    t_start_col: pd.Series,
    quality_col: pd.Series | None = None,
    skip_counter: Counter[SkipReason] | None = None,
    samples_sink: list[dict[str, Any]] | None = None,
) -> list[DataFrameCandidate]:
    """Return valid candidate rows; record row-level skips in ``skip_counter``.

    Per the parser-outcome contract, row-level data quality issues never raise.
    Bad rows are skipped silently with the disqualifying reason recorded in
    ``skip_counter`` (mutated in place when supplied).
    """
    candidates: list[DataFrameCandidate] = []
    value_col = df[col]
    quality_values = quality_col if quality_col is not None else [None] * len(value_col)

    def _record_sample(row_idx: int, raw: Any, reason: str) -> None:
        if samples_sink is None:
            return
        if len(samples_sink) >= AUDIT_SAMPLE_CAP:
            return
        samples_sink.append({"row": row_idx, "column": col, "value": str(raw), "reason": reason})

    for row_idx, (ts_raw, val_raw, quality_raw) in enumerate(
        zip(t_start_col, value_col, quality_values, strict=False)
    ):
        if pd.isna(val_raw) or _is_blank_value(val_raw):
            if skip_counter is not None:
                skip_counter["blank_value"] += 1
            _record_sample(row_idx, val_raw, "blank_value")
            continue

        ts = pd.to_datetime(ts_raw, errors="coerce")
        if pd.isna(ts):
            if skip_counter is not None:
                skip_counter["unparseable_timestamp"] += 1
            _record_sample(row_idx, ts_raw, "unparseable_timestamp")
            continue

        try:
            val = float(val_raw)
        except (TypeError, ValueError):
            if skip_counter is not None:
                skip_counter["unparseable_value"] += 1
            _record_sample(row_idx, val_raw, "unparseable_value")
            continue

        if pd.isna(val):
            if skip_counter is not None:
                skip_counter["unparseable_value"] += 1
            _record_sample(row_idx, val_raw, "unparseable_value")
            continue

        quality = None if pd.isna(quality_raw) else str(quality_raw)
        candidates.append(DataFrameCandidate(ts=ts, val=val, quality=quality))

    return candidates


def _processed_destination_for_status(status: str) -> str:
    if status in {"processed", "processed_empty", "processed_external"}:
        return PROCESSED_DIR
    if status == "unmapped":
        return UNMAPPED_DIR
    if status == "parse_failed":
        return PARSE_ERR_DIR
    raise ValueError(f"Unsupported parser outcome status: {status}")


def _move_source_file(source_key: str, dest_prefix: str) -> str | None:
    """Copy source under newTBP/<file> to <dest_prefix>/<file>, then delete original."""
    file_name = source_key.split("/")[-1]
    full_source_key = source_key if source_key.startswith("newTBP/") else f"newTBP/{file_name}"
    dest_key = f"{dest_prefix.rstrip('/')}/{file_name}"
    copied_dest = False

    try:
        bucket = s3_resource.Bucket(INPUT_BUCKET)
        bucket.Object(dest_key).copy({"Bucket": INPUT_BUCKET, "Key": full_source_key})
        copied_dest = True
        bucket.Object(full_source_key).delete()
        return dest_key
    except Exception as e:
        if copied_dest:
            try:
                s3_resource.Object(INPUT_BUCKET, dest_key).delete()
            except Exception as cleanup_error:
                logger.warning(
                    "Failed to clean up destination after source move failure",
                    extra={"source": full_source_key, "dest": dest_key, "error": str(cleanup_error)},
                )
        logger.error(
            "File move failed",
            exc_info=True,
            extra={"source": full_source_key, "dest": dest_key, "error": str(e)},
        )
        return None


def _download_to_tmp(source_file: SourceFile, tmp_dir: Path) -> Path:
    decoded_key = unquote(source_file.key.replace("+", "%20"))
    file_name = Path(decoded_key).name
    local_path = tmp_dir / file_name
    s3_resource.Bucket(source_file.bucket).download_file(decoded_key, str(local_path))
    return local_path


def _parse_one_file(local_path: Path) -> ParserOutcome:
    """Return a ParserOutcome from streaming parser → non-NEM dispatcher.

    Raises ParserError for both "parser found nothing" and any narrow
    fallthrough error that the non-NEM dispatcher cannot handle either.
    """
    file_path = str(local_path)
    try:
        stream = stream_as_data_frames(file_path, split_days=True)
        first_item = next(stream, None)
        if first_item is None:
            if _is_nem_envelope_only(file_path):
                return ParserOutcome(
                    status="processed_empty",
                    reason="no_data_sentinel",
                    source_row_count=0,
                )
            raise ValueError("No data parsed from file")
        return ParserOutcome(status="processed", dataframes=chain([first_item], stream))  # type: ignore[arg-type]
    except _NEM_FALLTHROUGH_ERRORS:
        return dispatch_non_nem(file_path)


def _process_dataframes(
    outcome: ParserOutcome,
    csv_writer: HudiSourceCsvWriter,
    nem12_mappings: dict,
) -> tuple[ParserOutcome, dict[str, Any]]:
    """Walk parser DataFrames; write rows; build per-file metric / audit accumulators.

    Returns the final outcome (via derive_final) and an accumulators dict
    used downstream for metrics + audit.
    """
    candidate_row_count = 0
    unmapped_count = 0
    rows_written = 0
    mapped_monitor_points_count = 0
    skip_counter: Counter[SkipReason] = Counter(outcome.skip_reasons)
    unsupported_suffixes: set[str] = set(outcome.unsupported_suffixes)
    unmapped_identifiers: set[tuple[str, str]] = set(outcome.unmapped_identifiers)
    skipped_samples: list[dict[str, Any]] = []

    for nmi, df in outcome.dataframes:
        if "t_start" not in df.columns and df.index.name == "t_start":
            df = df.reset_index()
        if "t_start" not in df.columns:
            raise ParserError(f"Missing t_start column for {nmi}")

        t_start_col = df["t_start"]

        for col in df.columns:
            suffix = col.split("_")[0]
            if suffix not in NMI_DATA_STREAM_COMBINED:
                if col not in {"t_start", "t_end", "event_code", "event_desc"} and not col.startswith("quality_"):
                    unsupported_suffixes.add(suffix)
                continue

            quality_col_name = f"quality_{suffix}"
            quality_col = df[quality_col_name] if quality_col_name in df.columns else None
            candidates = extract_valid_readings(df, col, t_start_col, quality_col, skip_counter, skipped_samples)
            if not candidates:
                continue

            candidate_row_count += len(candidates)

            if nmi.startswith("p:"):
                neptune_id = nmi
            else:
                lookup_key = f"{nmi}-{suffix}"
                neptune_id = nem12_mappings.get(lookup_key)

            if neptune_id is None:
                unmapped_count += len(candidates)
                if nmi.startswith("p:"):
                    if len(unmapped_identifiers) < 100:
                        unmapped_identifiers.add(("p_id", nmi))
                else:
                    if len(unmapped_identifiers) < 100:
                        unmapped_identifiers.add(("nem12_nmi", f"{nmi}-{suffix}"))
                continue

            mapped_monitor_points_count += 1
            unit_name = col.split("_")[1].lower() if "_" in col else "kwh"

            for candidate in candidates:
                csv_writer.write_row(neptune_id, candidate.ts, candidate.val, unit_name, candidate.quality)
                rows_written += 1
                if csv_writer.row_count >= CSV_FLUSH_ROW_THRESHOLD:
                    csv_writer.flush()

    csv_writer.flush()

    rows_skipped_total = sum(skip_counter.values())
    final_outcome = outcome.derive_final(
        rows_written=rows_written,
        candidate_row_count=candidate_row_count,
        unmapped_count=unmapped_count,
        unsupported_suffixes=frozenset(unsupported_suffixes),
        rows_skipped=rows_skipped_total,
    )
    accumulators = {
        "candidate_row_count": candidate_row_count,
        "unmapped_count": unmapped_count,
        "rows_written": rows_written,
        "rows_skipped": rows_skipped_total,
        "skip_counter": skip_counter,
        "unsupported_suffixes": unsupported_suffixes,
        "unmapped_identifiers": unmapped_identifiers,
        "skipped_samples": skipped_samples,
        "mapped_monitor_points_count": mapped_monitor_points_count,
    }
    return final_outcome, accumulators


def _emit_per_file_metrics(outcome: ParserOutcome, accumulators: dict[str, Any]) -> None:
    if outcome.status == "processed":
        metrics.add_metric(name="ValidProcessedFiles", unit=MetricUnit.Count, value=1)
    elif outcome.status == "unmapped":
        metrics.add_metric(name="IrrelevantFiles", unit=MetricUnit.Count, value=1)
    elif outcome.status == "parse_failed":
        metrics.add_metric(name="ParseErrorFiles", unit=MetricUnit.Count, value=1)

    metrics.add_metric(
        name="ProcessedMonitorPoints",
        unit=MetricUnit.Count,
        value=accumulators.get("mapped_monitor_points_count", 0),
    )

    candidate = accumulators.get("candidate_row_count", 0)
    unmapped = accumulators.get("unmapped_count", 0)
    if candidate > 0:
        metrics.add_metric(name="PartialMappedRatio", unit=MetricUnit.Percent, value=(unmapped / candidate) * 100.0)

    rows_skipped = accumulators.get("rows_skipped", 0)
    source_rows = max(outcome.source_row_count, candidate + rows_skipped)
    if source_rows > 0:
        metrics.add_metric(name="RowsSkippedRatio", unit=MetricUnit.Percent, value=(rows_skipped / source_rows) * 100.0)

    skip_counter: Counter[SkipReason] = accumulators.get("skip_counter", Counter())
    metrics.add_metric(
        name="MalformedValueCount",
        unit=MetricUnit.Count,
        value=int(skip_counter.get("unparseable_value", 0)),
    )

    if accumulators.get("unsupported_suffixes"):
        metrics.add_metric(name="UnsupportedSuffixesFound", unit=MetricUnit.Count, value=1)

    unmapped_identifiers = accumulators.get("unmapped_identifiers", set())
    if unmapped_identifiers:
        kinds: Counter[str] = Counter(kind for kind, _ in unmapped_identifiers)
        for kind, count in kinds.items():
            metrics.add_metric(name=f"UnmappedIdentifierKind_{kind}", unit=MetricUnit.Count, value=count)


def _emit_parser_outcome_log(
    source_file: SourceFile,
    outcome: ParserOutcome,
    accumulators: dict[str, Any],
    duration_ms: float,
    dest_prefix: str,
) -> None:
    logger.info(
        "parser_outcome",
        extra={
            "bucket": source_file.bucket,
            "key": source_file.key,
            "final_status": outcome.status,
            "final_reason": outcome.reason,
            "source_row_count": outcome.source_row_count,
            "candidate_row_count": accumulators.get("candidate_row_count", 0),
            "rows_written": accumulators.get("rows_written", 0),
            "rows_skipped": accumulators.get("rows_skipped", 0),
            "unmapped_count": accumulators.get("unmapped_count", 0),
            "skip_reasons": dict(accumulators.get("skip_counter", Counter())),
            "unsupported_suffixes": sorted(accumulators.get("unsupported_suffixes", set())),
            "unmapped_identifiers_truncated": list(sorted(accumulators.get("unmapped_identifiers", set())))[:50],
            "destination_prefix": dest_prefix,
            "duration_ms": duration_ms,
        },
    )


@tracer.capture_method
@idempotent_function(
    data_keyword_argument="source_file",
    persistence_store=persistence_layer,
    config=idempotency_config,
)
def ingest_file(source_file: SourceFile) -> ParserOutcome:
    """Process one source file end-to-end inside the idempotent boundary."""
    start_ts = pd.Timestamp.now()

    nem12_mappings = get_nem12_mappings()

    accumulators: dict[str, Any] = {}
    final_outcome: ParserOutcome | None = None

    with tempfile.TemporaryDirectory() as tmp_dir:
        executor = ThreadPoolExecutor(max_workers=S3_WRITE_WORKERS)
        csv_writer = HudiSourceCsvWriter(batch_timestamp=str(uuid.uuid4()), executor=executor)
        try:
            local_path = _download_to_tmp(source_file, Path(tmp_dir))
            try:
                parsed = _parse_one_file(local_path)
            except (ParserError, ProcessingError):
                # Deterministic content failure → cache this outcome so retry
                # does not keep re-attempting the broken file.
                _move_source_file(source_file.key, PARSE_ERR_DIR)
                final_outcome = ParserOutcome(
                    status="parse_failed",
                    reason="parser_error",
                    source_row_count=0,
                )
                _emit_per_file_metrics(final_outcome, {})
                duration_ms = (pd.Timestamp.now() - start_ts).total_seconds() * 1000.0
                metrics.add_metric(name="FileProcessingDurationMs", unit=MetricUnit.Milliseconds, value=duration_ms)
                _emit_parser_outcome_log(source_file, final_outcome, {}, duration_ms, PARSE_ERR_DIR)
                return final_outcome

            try:
                if parsed.dataframes:
                    final_outcome, accumulators = _process_dataframes(parsed, csv_writer, nem12_mappings)
                else:
                    final_outcome = parsed

                csv_writer.commit()
            except (ParserError, ProcessingError):
                csv_writer.abort()
                _move_source_file(source_file.key, PARSE_ERR_DIR)
                final_outcome = ParserOutcome(
                    status="parse_failed",
                    reason="parser_error",
                    source_row_count=0,
                )
                _emit_per_file_metrics(final_outcome, {})
                duration_ms = (pd.Timestamp.now() - start_ts).total_seconds() * 1000.0
                metrics.add_metric(name="FileProcessingDurationMs", unit=MetricUnit.Milliseconds, value=duration_ms)
                _emit_parser_outcome_log(source_file, final_outcome, {}, duration_ms, PARSE_ERR_DIR)
                return final_outcome
            except Exception:
                # Transient infrastructure failure — abort writer, do NOT
                # move source, raise so Powertools deletes in-progress and
                # SQS retry can re-execute.
                csv_writer.abort()
                logger.error(
                    "Transient failure during ingest; raising for retry",
                    exc_info=True,
                    extra={"bucket": source_file.bucket, "key": source_file.key},
                )
                raise

            try:
                dest_prefix = _processed_destination_for_status(final_outcome.status)
            except ValueError:
                # Unknown status — treat as parse_failed.
                csv_writer.abort()
                _move_source_file(source_file.key, PARSE_ERR_DIR)
                final_outcome = ParserOutcome(
                    status="parse_failed",
                    reason="processing_error",
                    source_row_count=0,
                )
                _emit_per_file_metrics(final_outcome, {})
                duration_ms = (pd.Timestamp.now() - start_ts).total_seconds() * 1000.0
                metrics.add_metric(name="FileProcessingDurationMs", unit=MetricUnit.Milliseconds, value=duration_ms)
                _emit_parser_outcome_log(source_file, final_outcome, {}, duration_ms, PARSE_ERR_DIR)
                return final_outcome

            move_dest = _move_source_file(source_file.key, dest_prefix)
            if move_dest is None:
                # Source-move failed AFTER Hudi commit — roll back Hudi and
                # raise so retry re-executes (transient failure assumption).
                csv_writer.abort()
                raise ProcessingError(
                    f"Source-move to {dest_prefix} failed after Hudi commit for {source_file.key}"
                )

            # Audit sidecar — best-effort; never fails the file's disposition.
            if (
                accumulators.get("rows_skipped", 0) > 0
                or accumulators.get("unmapped_count", 0) > 0
                or accumulators.get("unsupported_suffixes")
            ):
                try:
                    write_audit_sidecar(
                        batch_ts=csv_writer.batch_timestamp,
                        source_filename=Path(source_file.key).name,
                        outcome_summary={
                            "status": final_outcome.status,
                            "reason": final_outcome.reason,
                            "source_row_count": final_outcome.source_row_count,
                            "candidate_row_count": accumulators.get("candidate_row_count", 0),
                            "rows_written": accumulators.get("rows_written", 0),
                            "rows_skipped": accumulators.get("rows_skipped", 0),
                            "unmapped_count": accumulators.get("unmapped_count", 0),
                        },
                        skip_reasons=dict(accumulators.get("skip_counter", Counter())),
                        unmapped_identifiers=sorted(accumulators.get("unmapped_identifiers", set())),
                        unsupported_suffixes=sorted(accumulators.get("unsupported_suffixes", set())),
                        skipped_samples=accumulators.get("skipped_samples", []),
                        s3_client=s3_client,
                        total_skipped=accumulators.get("rows_skipped", 0),
                    )
                except Exception as audit_err:
                    logger.warning(
                        "audit_sidecar_write_failed",
                        extra={"key": source_file.key, "error": str(audit_err)},
                    )

            _emit_per_file_metrics(final_outcome, accumulators)
            duration_ms = (pd.Timestamp.now() - start_ts).total_seconds() * 1000.0
            metrics.add_metric(name="FileProcessingDurationMs", unit=MetricUnit.Milliseconds, value=duration_ms)
            _emit_parser_outcome_log(source_file, final_outcome, accumulators, duration_ms, dest_prefix)
            return final_outcome
        finally:
            executor.shutdown(wait=True)
```

- [ ] **Step 4: Run new tests**

```bash
uv run pytest tests/unit/test_pipeline.py tests/unit/test_idempotency_boundary.py -v
```

Expected: all PASS. If any fail with `ParserError` not caught, walk the test through the dispatcher to confirm it raises (not returns) on the broken file — adjust the body's `except` clauses to match.

- [ ] **Step 5: Lint and run full test suite**

```bash
uv run ruff check src/ tests/
uv run pytest -q
```

Expected: lint clean. Pre-existing tests in `test_edge_cases.py` may still pass because `app.py`'s `parse_and_write_data` still exists at this point (it's removed in Task 11).

- [ ] **Step 6: Commit**

```bash
git add src/functions/file_processor/pipeline.py tests/unit/test_pipeline.py tests/unit/test_idempotency_boundary.py
git commit -m "feat: add ingest_file pipeline orchestrator with parse_failed caching"
```

---

## Task 11: Slim `app.py` to handler-only; require `SQS_QUEUE_URL`; delete dead helpers

**Files:**
- Modify (rewrite): `src/functions/file_processor/app.py`
- Modify: `tests/conftest.py` (set `SQS_QUEUE_URL` env var at module load)
- Create: `tests/unit/test_lambda_handler.py`

- [ ] **Step 1: Update `tests/conftest.py`**

Replace the contents of `tests/conftest.py` with:

```python
"""Pytest configuration for sbm-ingester tests."""

import os
import sys
from pathlib import Path

# Add function directories to sys.path for Lambda-style imports.
optima_exporter_path = Path(__file__).parent.parent / "src" / "functions" / "optima_exporter"
if str(optima_exporter_path) not in sys.path:
    sys.path.insert(0, str(optima_exporter_path))

# Required env vars for module-import-time reads. Without this, importing
# functions.file_processor.app raises KeyError because production code reads
# os.environ["SQS_QUEUE_URL"] at import time (no fallback).
os.environ.setdefault("SQS_QUEUE_URL", "https://sqs.test.local/queue")
```

- [ ] **Step 2: Write the lambda-handler test**

Create `tests/unit/test_lambda_handler.py`:

```python
"""Tests for the SQS adapter (lambda_handler)."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from functions.file_processor.app import lambda_handler
from shared.parsers.outcome import ParserOutcome


def _sqs_event(bucket: str, key: str, retry_count: int | None = None) -> dict:
    body = {
        "Records": [
            {"s3": {"bucket": {"name": bucket}, "object": {"key": key}}},
        ],
    }
    if retry_count is not None:
        body["_retry_count"] = retry_count
    return {"Records": [{"messageId": "abc-123", "body": json.dumps(body)}]}


class TestLambdaHandler:
    def test_calls_ingest_file_with_source_file(self) -> None:
        event = _sqs_event("sbm-file-ingester", "newTBP/foo.csv")

        with (
            patch("functions.file_processor.app.check_file_stability", return_value=(True, 100)),
            patch("functions.file_processor.app.ingest_file") as mock_ingest,
        ):
            mock_ingest.return_value = ParserOutcome(status="processed", rows_written=1)
            result = lambda_handler(event, context=None)

        mock_ingest.assert_called_once()
        kwargs = mock_ingest.call_args.kwargs
        assert kwargs["source_file"].bucket == "sbm-file-ingester"
        assert kwargs["source_file"].key == "newTBP/foo.csv"
        assert result["statusCode"] == 200

    def test_unstable_file_requeues(self) -> None:
        event = _sqs_event("sbm-file-ingester", "newTBP/in_flight.csv", retry_count=0)

        with (
            patch("functions.file_processor.app.check_file_stability", return_value=(False, 0)),
            patch("functions.file_processor.app.requeue_message", return_value=True) as mock_requeue,
            patch("functions.file_processor.app.ingest_file") as mock_ingest,
        ):
            result = lambda_handler(event, context=None)

        mock_requeue.assert_called_once()
        mock_ingest.assert_not_called()
        assert result["statusCode"] == 200

    def test_unstable_after_max_retries_skips(self) -> None:
        event = _sqs_event("sbm-file-ingester", "newTBP/never_stabilises.csv", retry_count=3)

        with (
            patch("functions.file_processor.app.check_file_stability", return_value=(False, 0)),
            patch("functions.file_processor.app.requeue_message") as mock_requeue,
            patch("functions.file_processor.app.ingest_file") as mock_ingest,
            patch("functions.file_processor.app.MAX_REQUEUE_RETRIES", 3),
        ):
            result = lambda_handler(event, context=None)

        mock_requeue.assert_not_called()
        mock_ingest.assert_not_called()
        assert result["statusCode"] == 200


class TestSqsQueueUrlRequired:
    def test_module_reload_without_env_var_raises(self, monkeypatch) -> None:
        """Without SQS_QUEUE_URL in env, re-importing app.py must raise KeyError."""
        monkeypatch.delenv("SQS_QUEUE_URL", raising=False)
        import importlib

        import functions.file_processor.app as app_module

        with pytest.raises(KeyError):
            importlib.reload(app_module)
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
uv run pytest tests/unit/test_lambda_handler.py -v
```

Expected: most tests fail because `app.py` still has the old `parse_and_write_data` shape.

- [ ] **Step 4: Replace `src/functions/file_processor/app.py`**

Rewrite the entire file:

```python
"""SQS-triggered Lambda handler for the SBM file ingester.

This module is a thin SQS adapter. All business logic lives in
functions.file_processor.pipeline.ingest_file.

Pre-conditions (enforced at import time):
  - SQS_QUEUE_URL env var is set; KeyError on import otherwise so deploy
    fails fast rather than silently targeting the production queue.

Per-record flow:
  1. Decode the SQS record → bucket, key.
  2. Check file stability (S3 size stable for 2 consecutive checks).
  3. If unstable: requeue with backoff (up to MAX_REQUEUE_RETRIES); skip otherwise.
  4. If stable: build SourceFile, call ingest_file (which is idempotent +
     traced + emits per-file structured log + metrics).
  5. Return statusCode 200.
"""

from __future__ import annotations

import json
import os
import time
from typing import Any
from urllib.parse import unquote

import boto3
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.metrics import MetricUnit

from functions.file_processor.pipeline import ingest_file
from shared.source_file import SourceFile

# Required env var — KeyError on import if missing.
SQS_QUEUE_URL = os.environ["SQS_QUEUE_URL"]

# File-stability tuning (preserved from previous shape).
FILE_STABILITY_CHECK_INTERVAL = 5  # seconds between checks
FILE_STABILITY_MAX_WAIT = 30  # max seconds to wait for stabilisation
FILE_STABILITY_REQUIRED_CHECKS = 2  # consecutive stable checks required
MAX_REQUEUE_RETRIES = 3  # aligned with SQS maxReceiveCount = 3 (per spec)
REQUEUE_DELAY_SECONDS = 60

logger = Logger(service="file-processor")
tracer = Tracer(service="file-processor")
metrics = Metrics(namespace="SBM/Ingester")

s3_client = boto3.client("s3")
sqs_client = boto3.client("sqs")


@tracer.capture_method
def check_file_stability(bucket: str, key: str) -> tuple[bool, int]:
    """Wait for an S3 object's size to stabilise across 2 consecutive HEADs."""
    last_size = -1
    stable_count = 0
    total_wait = 0

    while total_wait < FILE_STABILITY_MAX_WAIT:
        try:
            response = s3_client.head_object(Bucket=bucket, Key=key)
            current_size = response["ContentLength"]

            if current_size == 0:
                logger.debug(
                    "File is empty, waiting",
                    extra={"bucket": bucket, "key": key, "waited": total_wait},
                )
                time.sleep(FILE_STABILITY_CHECK_INTERVAL)
                total_wait += FILE_STABILITY_CHECK_INTERVAL
                continue

            if current_size == last_size:
                stable_count += 1
                if stable_count >= FILE_STABILITY_REQUIRED_CHECKS:
                    logger.info(
                        "File is stable",
                        extra={"bucket": bucket, "key": key, "size": current_size},
                    )
                    return True, current_size
            else:
                stable_count = 0

            last_size = current_size
            time.sleep(FILE_STABILITY_CHECK_INTERVAL)
            total_wait += FILE_STABILITY_CHECK_INTERVAL
        except Exception as e:
            if hasattr(e, "response") and e.response.get("Error", {}).get("Code") == "NoSuchKey":
                logger.warning("File not found", extra={"bucket": bucket, "key": key})
                return False, 0
            logger.error(
                "Error checking file stability",
                exc_info=True,
                extra={"bucket": bucket, "key": key, "error": str(e)},
            )
            return False, 0

    logger.warning(
        "File stability check timed out",
        extra={"bucket": bucket, "key": key, "last_size": last_size, "waited": total_wait},
    )
    return False, 0


def requeue_message(original_body: dict, retry_count: int) -> bool:
    try:
        new_body = original_body.copy()
        new_body["_retry_count"] = retry_count + 1
        sqs_client.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps(new_body),
            DelaySeconds=REQUEUE_DELAY_SECONDS,
        )
        logger.info(
            "Message requeued for later processing",
            extra={"retry_count": retry_count + 1, "delay_seconds": REQUEUE_DELAY_SECONDS},
        )
        return True
    except Exception as e:
        logger.error("Failed to requeue message", exc_info=True, extra={"error": str(e)})
        return False


@tracer.capture_lambda_handler
@metrics.log_metrics(capture_cold_start_metric=True)
@logger.inject_lambda_context(correlation_id_path="Records[0].messageId")
def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    requeued_count = 0
    skipped_count = 0
    processed_count = 0

    for record in event["Records"]:
        try:
            message_body = json.loads(record["body"])
            retry_count = message_body.get("_retry_count", 0)

            s3_event = message_body["Records"][0]
            bucket_name = s3_event["s3"]["bucket"]["name"]
            file_key = s3_event["s3"]["object"]["key"]
            decoded_key = unquote(file_key.replace("+", "%20"))

            logger.info(
                "Processing file",
                extra={"bucket": bucket_name, "key": decoded_key, "retry_count": retry_count},
            )

            is_stable, _ = check_file_stability(bucket_name, decoded_key)
            if not is_stable:
                if retry_count >= MAX_REQUEUE_RETRIES:
                    logger.error(
                        "Max retries exceeded for unstable file",
                        extra={"bucket": bucket_name, "key": decoded_key, "retry_count": retry_count},
                    )
                    metrics.add_metric(name="MaxRetriesExceeded", unit=MetricUnit.Count, value=1)
                    skipped_count += 1
                    continue
                if requeue_message(message_body, retry_count):
                    requeued_count += 1
                    metrics.add_metric(name="MessagesRequeued", unit=MetricUnit.Count, value=1)
                continue

            ingest_file(source_file=SourceFile(bucket=bucket_name, key=file_key))
            processed_count += 1
        except Exception:
            logger.error("Error processing SQS record", exc_info=True)
            continue

    return {
        "statusCode": 200,
        "body": "Successfully processed files.",
        "processed": processed_count,
        "requeued": requeued_count,
        "skipped": skipped_count,
    }
```

- [ ] **Step 5: Run lambda-handler tests**

```bash
uv run pytest tests/unit/test_lambda_handler.py -v
```

Expected: all 4 PASS.

- [ ] **Step 6: Run full test suite**

```bash
uv run ruff check src/ tests/
uv run pytest -q
```

Expected (very likely): a number of tests in `test_edge_cases.py` fail because they reference symbols deleted from `app.py` (`parse_and_write_data`, `_flush_buffer_to_s3`, `DirectCSVWriter`, `_compute_dataframe_final_status`, etc.). **Do not fix those failures here** — Task 16 splits/rewrites that file. Only fix tests that are NOT in `test_edge_cases.py` or `test_batch_s3_writes.py`.

For `test_edge_cases.py` and `test_batch_s3_writes.py`, mark the file with `pytest.skip` at the top temporarily so CI stays green:

Add to the very top of `tests/unit/test_edge_cases.py` (after imports):

```python
import pytest
pytest.skip(
    "Will be split into focused files in Task 16; symbols referenced here "
    "(parse_and_write_data, DirectCSVWriter, etc.) were removed from app.py.",
    allow_module_level=True,
)
```

Same skip-marker at the top of `tests/unit/test_batch_s3_writes.py`:

```python
import pytest
pytest.skip(
    "Tests _flush_buffer_to_s3 dead code, deleted in Task 15.",
    allow_module_level=True,
)
```

Re-run:

```bash
uv run pytest -q
```

Expected: all non-skipped tests pass.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "refactor: slim app.py to SQS adapter; require SQS_QUEUE_URL env var"
```

---

## Task 12: Terraform — add `environment {}` block; bump SQS visibility timeout; align retry budget; enable DynamoDB deletion protection

**Files:**
- Modify: `terraform/ingester.tf`

- [ ] **Step 1: Edit `terraform/ingester.tf`**

Apply these three changes in one pass:

(A) Add `environment {}` to `aws_lambda_function.sbm_files_ingester` (currently lines 16-30):

```hcl
resource "aws_lambda_function" "sbm_files_ingester" {
  function_name                  = "sbm-files-ingester"
  role                           = data.aws_iam_role.ingester_role.arn
  handler                        = "functions.file_processor.app.lambda_handler"
  runtime                        = "python3.13"
  memory_size                    = 512
  timeout                        = 900
  reserved_concurrent_executions = 10
  s3_bucket                      = var.deployment_bucket
  s3_key                         = "${local.lambda_s3_prefix}/ingester.zip"

  tracing_config {
    mode = "Active"
  }

  environment {
    variables = {
      SQS_QUEUE_URL = aws_sqs_queue.sbm_files_ingester_queue.url
    }
  }
}
```

(B) Bump main queue visibility timeout from 900 to 1080:

```hcl
resource "aws_sqs_queue" "sbm_files_ingester_queue" {
  name                       = "sbm-files-ingester-queue"
  visibility_timeout_seconds = 1080 # Lambda timeout (900) + 180s buffer

  tags = {
    Name = "sbm-files-ingester-queue"
  }
}
```

DLQ visibility stays 900 (it does not trigger Lambda; comment that explicitly):

```hcl
resource "aws_sqs_queue" "sbm_files_ingester_dlq" {
  name                       = "sbm-files-ingester-dlq"
  message_retention_seconds  = 1209600 # 14 days
  visibility_timeout_seconds = 900     # DLQ is not Lambda-triggered; main queue uses 1080.

  tags = {
    Name = "sbm-files-ingester-dlq"
  }
}
```

(C) Add `deletion_protection_enabled = true` to the idempotency table:

```hcl
resource "aws_dynamodb_table" "sbm_ingester_idempotency" {
  name                        = "sbm-ingester-idempotency"
  billing_mode                = "PAY_PER_REQUEST"
  hash_key                    = "file_key"
  deletion_protection_enabled = true

  attribute {
    name = "file_key"
    type = "S"
  }

  ttl {
    attribute_name = "expiration"
    enabled        = true
  }

  tags = {
    Name = "sbm-ingester-idempotency"
  }
}
```

`maxReceiveCount = 3` already matches the new `MAX_REQUEUE_RETRIES = 3` set in Task 11; no change needed in the redrive policy.

- [ ] **Step 2: Validate the terraform**

```bash
cd terraform && terraform validate
```

Expected: `Success! The configuration is valid.`

If `terraform validate` requires init first, run `terraform init` once.

- [ ] **Step 3: Plan the change (read-only sanity check)**

```bash
cd terraform && terraform plan -out=/tmp/plan-task12.binary
```

Expected: 3 changes — modifications on `aws_lambda_function.sbm_files_ingester`, `aws_sqs_queue.sbm_files_ingester_queue`, and `aws_dynamodb_table.sbm_ingester_idempotency`. No destructive plan steps.

- [ ] **Step 4: Commit**

```bash
git add terraform/ingester.tf
git commit -m "feat: add SQS_QUEUE_URL env, bump visibility 900->1080, dynamodb deletion_protection"
```

---

## Task 13: Verify (and remediate if needed) S3 server-side encryption on the three buckets

**Files:**
- (Possibly) modify: `terraform/ingester.tf` and/or relevant terraform files in this repo
- (Possibly) raise change in another repo if the bucket lives there

- [ ] **Step 1: Run the three checks**

```bash
aws s3api get-bucket-encryption --bucket sbm-file-ingester
aws s3api get-bucket-encryption --bucket hudibucketsrc
aws s3api get-bucket-encryption --bucket gegoptimareports
```

Expected: each returns a JSON document with `ServerSideEncryptionConfiguration.Rules[].ApplyServerSideEncryptionByDefault.SSEAlgorithm` = `AES256` or `aws:kms`.

If any bucket returns `ServerSideEncryptionConfigurationNotFoundError`, proceed with Step 2. Otherwise this task is complete (no code change needed).

- [ ] **Step 2: Locate the bucket's terraform**

```bash
git grep -n "sbm-file-ingester\|hudibucketsrc\|gegoptimareports" terraform/
```

If a `aws_s3_bucket` resource exists in this repo, add SSE configuration:

```hcl
resource "aws_s3_bucket_server_side_encryption_configuration" "<bucket_resource_name>_sse" {
  bucket = aws_s3_bucket.<bucket_resource_name>.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
```

If the bucket is not managed in this repo (no terraform match), document this in the PR description: "Bucket `<name>` is not managed in this repo — SSE remediation deferred to <other-repo> PR."

- [ ] **Step 3: Validate (if terraform changed)**

```bash
cd terraform && terraform validate
```

- [ ] **Step 4: Commit (if changes)**

```bash
git add terraform/
git commit -m "fix: enable SSE-S3 on sbm-ingester buckets"
```

If no remediation was needed (Step 1 confirms encryption already), commit an empty marker (or skip the commit entirely):

```bash
echo "Confirmed S3 SSE on all three buckets — see PR description for output of get-bucket-encryption." > /tmp/sse-verification-note.txt
# No commit needed; document in the PR body.
```

---

## Task 14: Add new CloudWatch alarms

**Files:**
- Modify: `terraform/monitoring.tf`

- [ ] **Step 1: Append the new alarms to `terraform/monitoring.tf`**

```hcl
# -----------------------------
# File Processor: extended alarms
# -----------------------------
resource "aws_cloudwatch_metric_alarm" "max_retries_exceeded" {
  alarm_name          = "FileProcessor-MaxRetriesExceeded"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "MaxRetriesExceeded"
  namespace           = "SBM/Ingester"
  period              = 86400 # 1 day
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "File-stability retry budget exhausted on at least one file in 24h."
  alarm_actions       = [aws_sns_topic.sbm_alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "parse_error_spike" {
  alarm_name          = "FileProcessor-ParseErrorSpike"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ParseErrorFiles"
  namespace           = "SBM/Ingester"
  period              = 3600
  statistic           = "Sum"
  threshold           = 5 # baseline placeholder; tune after 1-2 weeks
  alarm_description   = "Parse-error file count exceeded threshold over 1 hour."
  alarm_actions       = [aws_sns_topic.sbm_alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "file_processor_error_rate" {
  alarm_name          = "FileProcessor-ErrorRate"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  threshold           = 1 # 1% — Lambda Errors / Invocations
  alarm_description   = "Lambda error rate > 1% over 5 min."
  alarm_actions       = [aws_sns_topic.sbm_alerts.arn]

  metric_query {
    id          = "errorRate"
    expression  = "100 * errors / invocations"
    label       = "Error rate (%)"
    return_data = "true"
  }

  metric_query {
    id = "errors"
    metric {
      metric_name = "Errors"
      namespace   = "AWS/Lambda"
      period      = 300
      stat        = "Sum"
      dimensions = {
        FunctionName = aws_lambda_function.sbm_files_ingester.function_name
      }
    }
  }

  metric_query {
    id = "invocations"
    metric {
      metric_name = "Invocations"
      namespace   = "AWS/Lambda"
      period      = 300
      stat        = "Sum"
      dimensions = {
        FunctionName = aws_lambda_function.sbm_files_ingester.function_name
      }
    }
  }
}

resource "aws_cloudwatch_metric_alarm" "idempotent_skip_spike" {
  alarm_name          = "FileProcessor-IdempotentSkipSpike"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ConditionalCheckFailedRequests"
  namespace           = "AWS/DynamoDB"
  period              = 3600
  statistic           = "Sum"
  threshold           = 50 # placeholder; tune after baseline measured
  alarm_description   = "Cache-hit rate on idempotency table is unusually high (> threshold over 1 hour)."
  alarm_actions       = [aws_sns_topic.sbm_alerts.arn]

  dimensions = {
    TableName = aws_dynamodb_table.sbm_ingester_idempotency.name
  }
}
```

- [ ] **Step 2: Validate**

```bash
cd terraform && terraform validate
```

Expected: success.

- [ ] **Step 3: Plan**

```bash
cd terraform && terraform plan -out=/tmp/plan-task14.binary
```

Expected: 4 new alarm resources to add.

- [ ] **Step 4: Commit**

```bash
git add terraform/monitoring.tf
git commit -m "feat: add file-processor cloudwatch alarms (retries, parse errors, error rate, idempotent skip)"
```

---

## Task 15: Delete `tests/unit/test_batch_s3_writes.py` and `_flush_buffer_to_s3` artifacts

**Files:**
- Delete: `tests/unit/test_batch_s3_writes.py`

(`_flush_buffer_to_s3` was already removed from `app.py` when it was rewritten in Task 11.)

- [ ] **Step 1: Confirm dead-code status**

```bash
grep -rn "_flush_buffer_to_s3" src/ tests/
```

Expected: no matches in `src/`. If matches still appear in `tests/unit/test_batch_s3_writes.py`, that's the file we're about to delete.

- [ ] **Step 2: Delete the test file**

```bash
git rm tests/unit/test_batch_s3_writes.py
```

- [ ] **Step 3: Run tests**

```bash
uv run pytest -q
```

Expected: all (non-skipped) tests pass; total count drops by ~16.

- [ ] **Step 4: Commit**

```bash
git commit -m "chore: delete tests for removed _flush_buffer_to_s3 dead code"
```

---

## Task 16: Split `tests/unit/test_edge_cases.py` into focused files

**Files:**
- Delete: `tests/unit/test_edge_cases.py`
- Create (or extend): `tests/unit/test_dataframe_partial_skip.py`
- Create (or extend): `tests/unit/test_unmapped_disposition.py`
- Create (or extend): `tests/unit/test_audit_sidecar_contract.py`
- Create (or extend): `tests/unit/test_nem_envelope_short_circuit.py`
- Create (or extend): `tests/unit/test_lambda_handler.py` (already exists from Task 11)
- Create (or extend): `tests/unit/test_pipeline.py` (already exists from Task 10)
- Modify: `tests/conftest.py` (add shared moto fixtures)

**Strategy:** every test in `test_edge_cases.py` is currently skipped via `pytest.skip(allow_module_level=True)` from Task 11. Move each test into the file matching its behaviour, rewriting `parse_and_write_data` calls to `ingest_file` with `SourceFile(...)` arguments and removing `output_as_data_frames` mocks (no longer in the call path).

- [ ] **Step 1: Add shared moto fixtures to `tests/conftest.py`**

Append to `tests/conftest.py`:

```python
import json

import boto3
import pytest
from moto import mock_aws

from shared.common import HUDI_BUCKET, INPUT_BUCKET


@pytest.fixture
def mock_s3_buckets():
    """Yield a moto-mocked S3 client with the three buckets created."""
    with mock_aws():
        s3 = boto3.client("s3")
        for bucket in [INPUT_BUCKET, HUDI_BUCKET]:
            s3.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
            )
        # Seed nem12 mappings JSON the loader fetches.
        s3.put_object(
            Bucket=INPUT_BUCKET,
            Key="nem12_mappings.json",
            Body=json.dumps({"NMI001-E1": "p:bunnings:abc"}),
        )
        yield s3


@pytest.fixture
def mock_dynamodb_idempotency():
    """Yield a moto-mocked DynamoDB client with the idempotency table created."""
    with mock_aws():
        ddb = boto3.client("dynamodb")
        ddb.create_table(
            TableName="sbm-ingester-idempotency",
            KeySchema=[{"AttributeName": "file_key", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "file_key", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        yield ddb


@pytest.fixture
def file_in_newtbp(mock_s3_buckets):
    """Factory: place a CSV body at newTBP/<key> and return the SourceFile."""
    from shared.source_file import SourceFile

    def _factory(body: bytes, key: str = "newTBP/sample.csv"):
        mock_s3_buckets.put_object(Bucket=INPUT_BUCKET, Key=key, Body=body)
        return SourceFile(bucket=INPUT_BUCKET, key=key)

    return _factory
```

- [ ] **Step 2: Categorise each test in `test_edge_cases.py`**

Open `tests/unit/test_edge_cases.py` and walk through each test class / function. Categorise into one of:

| Category | Target file |
|---|---|
| SQS adapter (lambda_handler) | `tests/unit/test_lambda_handler.py` |
| End-to-end ingest_file with NEM12 / non-NEM | `tests/unit/test_pipeline.py` |
| HudiSourceCsvWriter buffer / staging / commit / abort | `tests/unit/test_csv_writer.py` |
| Idempotency cache hit / miss / raise-vs-return | `tests/unit/test_idempotency_boundary.py` |
| NEM12 envelope short-circuit | `tests/unit/test_nem_envelope_short_circuit.py` (create) |
| DataFrame partial skip + skip_reasons | `tests/unit/test_dataframe_partial_skip.py` (create) |
| Disposition: unmapped → newIrrevFiles/ | `tests/unit/test_unmapped_disposition.py` (create) |
| Audit sidecar JSON shape + 100-sample cap | `tests/unit/test_audit_sidecar_contract.py` (create) |

Make the categorisation in a brief markdown table inside `test_edge_cases.py` at the top (commented out) so review is easy.

- [ ] **Step 3: Move each test, rewriting the call path**

For each test in `test_edge_cases.py`:

1. Identify what behavior it asserts.
2. Rewrite the test in the target file using:
   - Setup: `aws_environment` or `mock_s3_buckets` + `mock_dynamodb_idempotency` + `file_in_newtbp`.
   - Call: `ingest_file(source_file=...)` (not `parse_and_write_data(tbp_files=[...])`).
   - Assertion: existing assertions about S3 disposition, ParserOutcome, structured logs, metrics.
   - Drop: any `patch("functions.file_processor.app.output_as_data_frames", ...)` line — that import and call path are gone.
3. Add `from tests.helpers.outcome_invariants import assert_parser_outcome_invariants` and call it on every returned `ParserOutcome` for cross-field invariant guarantees.

- [ ] **Step 4: Delete the original `test_edge_cases.py`**

```bash
git rm tests/unit/test_edge_cases.py
```

- [ ] **Step 5: Run all tests**

```bash
uv run ruff check tests/
uv run pytest -q
```

Expected: count is in the 770-790 range (per spec). All pass.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "test: split test_edge_cases.py into focused files; add shared moto fixtures"
```

---

## Task 17: Add the new spec-required tests not yet covered

**Files:**
- Modify: `tests/unit/test_idempotency_boundary.py`
- Modify: `tests/unit/test_pipeline.py`
- Modify: `tests/unit/test_persistence_cache_hit_log.py`
- Modify: `tests/unit/test_lambda_handler.py`

**Coverage check** — the spec lists five new tests; cross-reference them against what already exists from Tasks 9-11:

| Test | Already in | Status |
|---|---|---|
| Idempotency-collision integration | `test_idempotency_boundary.py::test_duplicate_call_returns_cached_outcome_without_reprocessing` | DONE |
| NEM12 RuntimeError propagation | `test_idempotency_boundary.py::test_runtime_error_in_nem_path_propagates_does_not_reach_dispatcher` | DONE |
| Cache-hit log | `test_persistence_cache_hit_log.py::test_logs_idempotent_cache_hit_on_existing_record` (low-level) AND need an end-to-end test in `test_pipeline.py` to assert the log line is emitted on a real cache-hit path | PARTIAL |
| `SQS_QUEUE_URL` missing | `test_lambda_handler.py::TestSqsQueueUrlRequired::test_module_reload_without_env_var_raises` | DONE |
| Visibility-timeout regression (terraform plan) | (no runtime test; covered by terraform plan output in Task 12) | DONE |

- [ ] **Step 1: Add end-to-end cache-hit log test to `tests/unit/test_pipeline.py`**

Append to `tests/unit/test_pipeline.py`:

```python
class TestCacheHitEndToEnd:
    def test_duplicate_ingest_emits_idempotent_cache_hit_log(self, aws_environment, caplog) -> None:
        import logging

        s3, _ = aws_environment
        s3.put_object(Bucket=INPUT_BUCKET, Key="newTBP/sample.csv", Body=NEM12_BODY)

        src = SourceFile(bucket=INPUT_BUCKET, key="newTBP/sample.csv")
        ingest_file(source_file=src)

        with caplog.at_level(logging.INFO):
            ingest_file(source_file=src)

        cache_hit_records = [r for r in caplog.records if r.message == "idempotent_cache_hit"]
        assert len(cache_hit_records) == 1
        assert getattr(cache_hit_records[0], "source_bucket", None) == INPUT_BUCKET
        assert getattr(cache_hit_records[0], "source_key", None) == "newTBP/sample.csv"
```

- [ ] **Step 2: Run the new test**

```bash
uv run pytest tests/unit/test_pipeline.py::TestCacheHitEndToEnd -v
```

Expected: PASS.

- [ ] **Step 3: Run the entire suite**

```bash
uv run ruff check src/ tests/
uv run pytest -q
```

Expected: all green.

- [ ] **Step 4: Commit**

```bash
git add tests/unit/test_pipeline.py
git commit -m "test: add end-to-end cache-hit log assertion in test_pipeline"
```

---

## Final verification

- [ ] **Step 1: Lint everything**

```bash
uv run ruff check .
uv run ruff format --check .
```

- [ ] **Step 2: Run the full test suite**

```bash
uv run pytest --cov=src
```

Expected: ≥ 90 % line coverage (the lefthook pre-push gate), all tests passing, count in the 770-790 range.

- [ ] **Step 3: Validate terraform**

```bash
cd terraform && terraform validate && terraform plan
```

Expected: success; the plan diff matches the union of Tasks 12 + 14 (Lambda environment block, SQS visibility 1080, DynamoDB deletion_protection, four new alarms).

- [ ] **Step 4: Inspect file count for the renamed handler**

```bash
wc -l src/functions/file_processor/app.py
```

Expected: ≤ 200 lines (target is ~80 per spec; file-stability and requeue helpers are kept in this module so 100-200 is realistic).

- [ ] **Step 5: Confirm the deleted artifacts are gone**

```bash
grep -rn "_flush_buffer_to_s3\|class DirectCSVWriter\|class CSVUploadJob\|read_nem12_mappings\|parse_and_write_data\|tbp_files" src/
```

Expected: no matches in `src/`. (May still appear in `docs/superpowers/specs/` and `docs/superpowers/plans/` — those are historical references and OK.)

- [ ] **Step 6: Confirm new symbols exist**

```bash
grep -rn "class SourceFile\|class HudiSourceCsvWriter\|def ingest_file\|class InstrumentedDynamoDBPersistenceLayer\|def derive_final\|def dispatch_non_nem\|def _is_nem_envelope_only" src/
```

Expected: each pattern matches exactly once at the expected location.

---

## Post-deploy operational checklist

(Performed manually after merge + deploy; not part of the per-task TDD loop.)

- [ ] Confirm Lambda cold-start succeeds (no `KeyError: SQS_QUEUE_URL`).
- [ ] Watch `FileProcessor-DLQDepth` alarm for 1 week — should remain at 0 if happy path works.
- [ ] Compare `ConditionalCheckFailedRequests` on `sbm-ingester-idempotency` to baseline; should be < 1 % of Lambda invocations.
- [ ] Sample one `idempotent_cache_hit` log line from CloudWatch Logs Insights to confirm `source_bucket` and `source_key` fields are present.
- [ ] Confirm `FileProcessingDurationMs` p50/p99 baseline for memory-tuning input later.
- [ ] Walk the `newParseErr/` prefix in S3 once a day for the first week — restore any misclassified file to `newTBP/`.
