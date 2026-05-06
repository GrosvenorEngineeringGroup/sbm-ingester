# Parser Outcome Semantics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace implicit parser disposition based on `[]` / mapped IDs with explicit parser outcomes so valid empty files and side-effect parser successes move to `newP/`, genuinely unmapped files move to `newIrrevFiles/`, and parser/processing failures move to `newParseErr/`.

**Architecture:** Add a small parser outcome contract, migrate parser functions to return `ParserOutcome` and raise typed exceptions, then update `file_processor` to centralize source-file movement by outcome status. Keep Hudi row formatting unchanged; only classification, parser return values, and error semantics change.

**Tech Stack:** Python 3.13, pandas, boto3/moto, pytest, aws-lambda-powertools, existing `uv run pytest` workflow.

---

## File Structure

- Create `src/shared/parsers/outcome.py`
  - Owns `ParserOutcome`, status literals, parser exception types, and small constructors/helpers.
- Modify `src/shared/parsers/__init__.py`
  - Re-export `ParserResult`, `ParserOutcome`, `ParserStatus`, `NotRelevantParser`, `ParserError`, `ProcessingError`.
- Modify `src/shared/non_nem_parsers.py`
  - Adds an outcome-returning dispatcher API while preserving the existing `get_non_nem_df()` list-returning API until `file_processor` is migrated.
- Modify side-effect parsers:
  - `src/shared/parsers/optima/demand.py`
  - `src/shared/parsers/optima/bunnings_billing.py`
  - `src/shared/parsers/optima/racv_billing.py`
- Modify DataFrame-returning non-NEM parsers:
  - `src/shared/parsers/optima/interval.py`
  - `src/shared/parsers/envizi/vertical_electricity.py`
  - `src/shared/parsers/envizi/vertical_water.py`
  - `src/shared/parsers/envizi/vertical_water_bulk.py`
  - `src/shared/parsers/racv/elec.py`
  - `src/shared/parsers/racv/noosa_solar.py`
  - `src/shared/parsers/green_square/comx.py`
- Modify `src/functions/file_processor/app.py`
  - Wrap NEM parser output in `ParserOutcome`.
  - Process DataFrame candidates with counts.
  - Move source files from outcome status.
- Modify tests:
  - `tests/unit/parsers/test_outcome.py`
  - `tests/unit/test_dispatcher.py`
  - `tests/unit/parsers/optima/test_demand.py`
  - `tests/unit/parsers/optima/test_bunnings_billing.py`
  - `tests/unit/parsers/optima/test_racv_billing.py`
  - `tests/unit/parsers/optima/test_interval.py`
  - `tests/unit/test_edge_cases.py`
  - `tests/unit/optima_exporter/test_e2e_full_chain.py`

---

### Task 1: Add Parser Outcome Contract

**Files:**
- Create: `src/shared/parsers/outcome.py`
- Modify: `src/shared/parsers/__init__.py`
- Create: `tests/unit/parsers/test_outcome.py`

- [ ] **Step 1: Write failing outcome model tests**

Create `tests/unit/parsers/test_outcome.py`:

```python
"""Tests for parser outcome contract."""

import pytest

from shared.parsers import (
    NotRelevantParser,
    ParserError,
    ParserOutcome,
    ProcessingError,
)


def test_processed_empty_outcome_defaults_to_no_rows() -> None:
    outcome = ParserOutcome(status="processed_empty", reason="no_data_sentinel")

    assert outcome.status == "processed_empty"
    assert outcome.dfs == []
    assert outcome.source_row_count == 0
    assert outcome.candidate_row_count == 0
    assert outcome.rows_written == 0
    assert outcome.unmapped_count == 0
    assert outcome.reason == "no_data_sentinel"


def test_unmapped_outcome_records_candidate_and_unmapped_counts() -> None:
    outcome = ParserOutcome(
        status="unmapped",
        source_row_count=3,
        candidate_row_count=9,
        unmapped_count=9,
    )

    assert outcome.status == "unmapped"
    assert outcome.source_row_count == 3
    assert outcome.candidate_row_count == 9
    assert outcome.unmapped_count == 9


@pytest.mark.parametrize("exc_type", [NotRelevantParser, ParserError, ProcessingError])
def test_parser_exceptions_preserve_message(exc_type: type[Exception]) -> None:
    with pytest.raises(exc_type, match="specific failure"):
        raise exc_type("specific failure")
```

- [ ] **Step 2: Run the outcome tests and verify they fail**

Run:

```bash
uv run pytest tests/unit/parsers/test_outcome.py -v
```

Expected: FAIL with an import error for `ParserOutcome` or `shared.parsers.outcome`.

- [ ] **Step 3: Implement `src/shared/parsers/outcome.py`**

Create `src/shared/parsers/outcome.py`:

```python
"""Parser outcome contract used by file disposition logic."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

import pandas as pd

ParserStatus = Literal[
    "processed",
    "processed_empty",
    "unmapped",
    "processed_external",
]

ParserResult = list[tuple[str, pd.DataFrame]]


@dataclass(frozen=True)
class ParserOutcome:
    status: ParserStatus
    dfs: ParserResult = field(default_factory=list)
    source_row_count: int = 0
    candidate_row_count: int = 0
    rows_written: int = 0
    unmapped_count: int = 0
    reason: str | None = None


class NotRelevantParser(Exception):
    """Raised when a parser does not apply to the file."""


class ParserError(Exception):
    """Raised when a matching file cannot be parsed."""


class ProcessingError(Exception):
    """Raised when parsed data cannot be written or otherwise handled."""
```

- [ ] **Step 4: Re-export outcome symbols**

Replace `src/shared/parsers/__init__.py` with:

```python
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
```

- [ ] **Step 5: Run tests and verify they pass**

Run:

```bash
uv run pytest tests/unit/parsers/test_outcome.py -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/shared/parsers/outcome.py src/shared/parsers/__init__.py tests/unit/parsers/test_outcome.py
git commit -m "feat: add parser outcome contract"
```

---

### Task 2: Add Outcome-Aware Dispatcher API With Compatibility

**Files:**
- Modify: `src/shared/non_nem_parsers.py`
- Modify: `tests/unit/test_dispatcher.py`

- [ ] **Step 1: Add failing dispatcher tests for typed outcomes and exceptions**

Append these tests to `tests/unit/test_dispatcher.py`:

```python
class TestOutcomeDispatcher:
    def test_wraps_legacy_parser_result_as_processed_outcome(self, tmp_path, monkeypatch) -> None:
        from shared.non_nem_parsers import get_non_nem_outcome

        def parser(file_name: str, error_file_path: str):
            df = pd.DataFrame({"t_start": ["2026-01-01 00:00:00"], "E1_kWh": [1.0]})
            return [("NMI1", df)]

        monkeypatch.setattr("shared.non_nem_parsers.PARSERS", [parser])

        result = get_non_nem_outcome(str(tmp_path / "file.csv"), "error_log")

        assert result.status == "processed"
        assert len(result.dfs) == 1

    def test_legacy_get_non_nem_df_still_returns_raw_dfs(self, tmp_path, monkeypatch) -> None:
        from shared.non_nem_parsers import get_non_nem_df

        def parser(file_name: str, error_file_path: str):
            df = pd.DataFrame({"t_start": ["2026-01-01 00:00:00"], "E1_kWh": [1.0]})
            return [("NMI1", df)]

        monkeypatch.setattr("shared.non_nem_parsers.PARSERS", [parser])

        result = get_non_nem_df(str(tmp_path / "file.csv"), "error_log")

        assert isinstance(result, list)
        assert result[0][0] == "NMI1"

    def test_not_relevant_parser_continues_to_next_parser(self, tmp_path, monkeypatch) -> None:
        from shared.parsers import NotRelevantParser, ParserOutcome
        from shared.non_nem_parsers import get_non_nem_outcome

        def first_parser(file_name: str, error_file_path: str):
            raise NotRelevantParser("not mine")

        def second_parser(file_name: str, error_file_path: str):
            return ParserOutcome(status="processed_empty", reason="matched")

        monkeypatch.setattr("shared.non_nem_parsers.PARSERS", [first_parser, second_parser])

        result = get_non_nem_outcome(str(tmp_path / "file.csv"), "error_log")

        assert result.status == "processed_empty"
        assert result.reason == "matched"

    def test_parser_error_stops_dispatch(self, tmp_path, monkeypatch) -> None:
        from shared.parsers import NotRelevantParser, ParserError
        from shared.non_nem_parsers import get_non_nem_outcome

        def first_parser(file_name: str, error_file_path: str):
            raise ParserError("matched but malformed")

        def second_parser(file_name: str, error_file_path: str):
            raise NotRelevantParser("should not run")

        monkeypatch.setattr("shared.non_nem_parsers.PARSERS", [first_parser, second_parser])

        with pytest.raises(ParserError, match="matched but malformed"):
            get_non_nem_outcome(str(tmp_path / "file.csv"), "error_log")

    def test_processing_error_stops_dispatch(self, tmp_path, monkeypatch) -> None:
        from shared.parsers import NotRelevantParser, ProcessingError
        from shared.non_nem_parsers import get_non_nem_outcome

        def first_parser(file_name: str, error_file_path: str):
            raise ProcessingError("s3 write failed")

        def second_parser(file_name: str, error_file_path: str):
            raise NotRelevantParser("should not run")

        monkeypatch.setattr("shared.non_nem_parsers.PARSERS", [first_parser, second_parser])

        with pytest.raises(ProcessingError, match="s3 write failed"):
            get_non_nem_outcome(str(tmp_path / "file.csv"), "error_log")
```

- [ ] **Step 2: Run dispatcher tests and verify they fail**

Run:

```bash
uv run pytest tests/unit/test_dispatcher.py::TestOutcomeDispatcher -v
```

Expected: FAIL because `PARSERS` and `get_non_nem_outcome()` do not exist.

- [ ] **Step 3: Implement outcome-aware dispatcher**

Update `src/shared/non_nem_parsers.py` to this structure:

```python
"""Dispatcher for non-NEM file parsers."""

from aws_lambda_powertools import Logger

from shared.parsers import (
    NotRelevantParser,
    ParserError,
    ParserOutcome,
    ParserResult,
    ProcessingError,
)
from shared.parsers.envizi.vertical_electricity import envizi_vertical_parser_electricity
from shared.parsers.envizi.vertical_water import envizi_vertical_parser_water
from shared.parsers.envizi.vertical_water_bulk import envizi_vertical_parser_water_bulk
from shared.parsers.green_square.comx import green_square_private_wire_schneider_comx_parser
from shared.parsers.optima.bunnings_billing import bunnings_billing_parser
from shared.parsers.optima.demand import demand_parser
from shared.parsers.optima.interval import interval_parser
from shared.parsers.optima.racv_billing import racv_billing_parser
from shared.parsers.racv.elec import racv_elec_parser
from shared.parsers.racv.noosa_solar import noosa_solar_parser

logger = Logger(service="non-nem-parsers", child=True)

PARSERS = [
    noosa_solar_parser,
    envizi_vertical_parser_water,
    envizi_vertical_parser_electricity,
    racv_elec_parser,
    racv_billing_parser,
    bunnings_billing_parser,
    demand_parser,
    interval_parser,
    envizi_vertical_parser_water_bulk,
    green_square_private_wire_schneider_comx_parser,
]


def _as_outcome(result: ParserOutcome | ParserResult) -> ParserOutcome:
    if isinstance(result, ParserOutcome):
        return result
    return ParserOutcome(status="processed", dfs=result)


def get_non_nem_outcome(file_name: str, error_file_path: str) -> ParserOutcome:
    for parser in PARSERS:
        try:
            return _as_outcome(parser(file_name, error_file_path))
        except NotRelevantParser as e:
            logger.debug("Parser not relevant", extra={"parser": parser.__name__, "file": file_name, "error": str(e)})
        except (ParserError, ProcessingError):
            raise
        except Exception as e:
            logger.debug("Legacy parser failed", extra={"parser": parser.__name__, "file": file_name, "error": str(e)})

    logger.error("No valid parser found", extra={"file": file_name})
    raise ParserError(f"get_non_nem_outcome: {file_name}: No Valid Parser Found")


def get_non_nem_df(file_name: str, error_file_path: str) -> ParserResult:
    return get_non_nem_outcome(file_name, error_file_path).dfs
```

The generic `Exception` compatibility branch stays only until all parsers are migrated in later tasks. Keeping `get_non_nem_df()` as a list-returning wrapper prevents Task 2 from breaking the current `file_processor`, which still consumes raw DataFrame tuples until Task 6.

- [ ] **Step 4: Run dispatcher tests**

Run:

```bash
uv run pytest tests/unit/test_dispatcher.py -v
```

Expected: PASS. Existing tests that call `get_non_nem_df()` should continue to assert raw list results; new outcome tests should call `get_non_nem_outcome()`.

- [ ] **Step 5: Commit**

```bash
git add src/shared/non_nem_parsers.py tests/unit/test_dispatcher.py
git commit -m "feat: return parser outcomes from dispatcher"
```

---

### Task 3: Migrate Optima Demand Parser Outcomes

**Files:**
- Modify: `src/shared/parsers/optima/demand.py`
- Modify: `tests/unit/parsers/optima/test_demand.py`

- [ ] **Step 1: Update demand parser tests for explicit outcomes**

Modify existing demand tests so direct parser calls assert `ParserOutcome`:

```python
def test_no_data_found_returns_processed_empty(write_demand_csv):
    body = (
        'Commodities:,"Electricity"\r\n'
        'Sites (NMIs):,"0000005438UN02B"\r\n'
        'Status:,"Active"\r\n'
        "Country:, New Zealand\r\n"
        "Start:,01-May-2026\r\n"
        "End:,03-May-2026\r\n"
        "\r\n"
        "\r\n"
        "No data found"
    )
    path = write_demand_csv(filename="NZ demand profile.csv", body_override=body)

    result = demand_parser(str(path), "/tmp/err.log")

    assert result.status == "processed_empty"
    assert result.reason == "no_data_sentinel"
    assert result.rows_written == 0
```

Add these new tests:

```python
def test_all_valid_candidates_unmapped_returns_unmapped(write_demand_csv, monkeypatch, _reset_mappings_cache):
    monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: {})

    with patch("shared.parsers.optima.demand.boto3.client") as mock_client:
        path = write_demand_csv()
        result = demand_parser(str(path), "/tmp/err.log")

    assert result.status == "unmapped"
    assert result.source_row_count == 3
    assert result.candidate_row_count == 9
    assert result.rows_written == 0
    assert result.unmapped_count == 9
    mock_client.return_value.put_object.assert_not_called()


def test_all_bad_timestamps_raise_parser_error(write_demand_csv, monkeypatch, _reset_mappings_cache):
    from shared.parsers import ParserError

    monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: {})
    rows = [("4001260599", "bad-date", "5.2400", "10.4800", "10.4800", "1.0000")]
    path = write_demand_csv(rows=rows)

    with pytest.raises(ParserError, match="No valid demand candidates"):
        demand_parser(str(path), "/tmp/err.log")


def test_put_object_failure_raises_processing_error(write_demand_csv, monkeypatch, _reset_mappings_cache):
    from shared.parsers import ProcessingError

    fake_mappings = {
        "Optima_4001260599-demand-kw": "p:bunnings:kw",
        "Optima_4001260599-demand-kva": "p:bunnings:kva",
        "Optima_4001260599-demand-pf": "p:bunnings:pf",
    }
    monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: fake_mappings)

    with patch("shared.parsers.optima.demand.boto3.client") as mock_client:
        mock_client.return_value.put_object.side_effect = RuntimeError("boom")
        path = write_demand_csv()

        with pytest.raises(ProcessingError, match="Failed to write demand Hudi CSV"):
            demand_parser(str(path), "/tmp/err.log")
```

- [ ] **Step 2: Run demand parser tests and verify they fail**

Run:

```bash
uv run pytest tests/unit/parsers/optima/test_demand.py -v
```

Expected: FAIL because `demand_parser` still returns `[]` and raises generic `Exception`.

- [ ] **Step 3: Implement demand outcome stats and errors**

In `src/shared/parsers/optima/demand.py`:

- Import:

```python
from dataclasses import dataclass

from shared.parsers import (
    NotRelevantParser,
    ParserError,
    ParserOutcome,
    ProcessingError,
)
```

- Add:

```python
@dataclass(frozen=True)
class DemandBuildResult:
    body: str
    source_row_count: int
    candidate_row_count: int
    rows_written: int
    unmapped_count: int
    invalid_count: int
```

- Change `_build_hudi_csv()` to return `DemandBuildResult`, incrementing:
  - `source_row_count = len(rows)`
  - `invalid_count` for missing identifier, missing/invalid timestamp, and non-numeric non-blank values
  - `candidate_row_count` only after identifier, timestamp, and numeric value are valid for one demand field
  - `unmapped_count` only for valid candidates whose mapping lookup misses
  - `rows_written` only for written Hudi rows

Use this core logic:

```python
for row in rows:
    nmi = (row.get("Identifier") or "").strip()
    raw_ts = (row.get("ReadingDateTime") or "").strip()
    if not nmi or not raw_ts:
        invalid_count += 1
        continue
    try:
        ts = datetime.strptime(raw_ts, "%d-%b-%Y %H:%M:%S")
    except ValueError:
        invalid_count += 1
        logger.warning("demand_bad_timestamp", extra={"nmi": nmi, "raw_ts": raw_ts})
        continue

    ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
    for csv_col, suffix, unit in CSV_FIELD_MAPPING:
        raw_val = (row.get(csv_col) or "").strip()
        if not raw_val:
            continue
        try:
            float(raw_val)
        except ValueError:
            invalid_count += 1
            continue

        candidate_row_count += 1
        sensor_id = mappings.get(f"Optima_{nmi}-demand-{suffix}")
        if not sensor_id:
            unmapped_count += 1
            continue

        buf.write(f"{sensor_id},{ts_str},{raw_val},{unit},{ts_str},\n")
        rows_written += 1
```

- Change `demand_parser()` disposition:

```python
if "demand profile" not in Path(file_name).name.lower().replace("_", " "):
    raise NotRelevantParser("Not a Demand Profile file (filename mismatch)")

with Path(file_name).open(encoding="utf-8") as f:
    first_line = f.readline()
if not first_line.startswith("Commodities:"):
    raise NotRelevantParser("Not a Demand Profile file (missing metadata header)")

rows = _parse_demand_rows(file_name)
if not rows:
    logger.info("demand_no_rows_to_process", extra={"file": file_name})
    return ParserOutcome(status="processed_empty", reason="no_data_sentinel")

mappings = _mappings_mod.get_nem12_mappings()
build = _build_hudi_csv(rows, mappings)

if build.rows_written == 0:
    if build.candidate_row_count > 0 and build.unmapped_count == build.candidate_row_count:
        return ParserOutcome(
            status="unmapped",
            source_row_count=build.source_row_count,
            candidate_row_count=build.candidate_row_count,
            rows_written=0,
            unmapped_count=build.unmapped_count,
            reason="all_candidates_unmapped",
        )
    if build.candidate_row_count == 0 and build.invalid_count == 0:
        return ParserOutcome(
            status="processed_empty",
            source_row_count=build.source_row_count,
            reason="blank_values",
        )
    raise ParserError(f"No valid demand candidates in {file_name}")

try:
    boto3.client("s3").put_object(Bucket=HUDI_BUCKET, Key=key, Body=build.body.encode())
except Exception as e:
    raise ProcessingError(f"Failed to write demand Hudi CSV: {e}") from e

return ParserOutcome(
    status="processed",
    source_row_count=build.source_row_count,
    candidate_row_count=build.candidate_row_count,
    rows_written=build.rows_written,
    unmapped_count=build.unmapped_count,
)
```

Keep the existing Hudi CSV header and row format unchanged.

- [ ] **Step 4: Run demand tests**

Run:

```bash
uv run pytest tests/unit/parsers/optima/test_demand.py -v
```

Expected: PASS after updating legacy `assert result == []` assertions to outcome assertions.

- [ ] **Step 5: Commit**

```bash
git add src/shared/parsers/optima/demand.py tests/unit/parsers/optima/test_demand.py
git commit -m "feat: return explicit demand parser outcomes"
```

---

### Task 4: Migrate Bunnings and RACV Billing Outcomes

**Files:**
- Modify: `src/shared/parsers/optima/bunnings_billing.py`
- Modify: `src/shared/parsers/optima/racv_billing.py`
- Modify: `tests/unit/parsers/optima/test_bunnings_billing.py`
- Modify: `tests/unit/parsers/optima/test_racv_billing.py`

- [ ] **Step 1: Add failing billing outcome tests**

Add Bunnings tests:

```python
def test_all_valid_billing_candidates_unmapped_returns_unmapped(_reset_mappings_cache, tmp_path, monkeypatch) -> None:
    src = FIXTURE_DIR / "bunnings_billing_sample.csv"
    dst = tmp_path / "20260414.155519-Bunnings-Usage and Spend Report.csv"
    dst.write_bytes(src.read_bytes())
    monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: {})

    result = bp_mod.bunnings_billing_parser(str(dst), "dummy")

    assert result.status == "unmapped"
    assert result.source_row_count == 3
    assert result.candidate_row_count > 0
    assert result.rows_written == 0
    assert result.unmapped_count == result.candidate_row_count


def test_bunnings_hudi_write_failure_raises_processing_error(_reset_mappings_cache, tmp_path, monkeypatch) -> None:
    from shared.parsers import ProcessingError

    src = FIXTURE_DIR / "bunnings_billing_sample.csv"
    dst = tmp_path / "20260414.155519-Bunnings-Usage and Spend Report.csv"
    dst.write_bytes(src.read_bytes())
    mappings = {}
    for nmi in ("VCCCLG0019", "VAAA000266"):
        for _, suffix, _unit_source in bp_mod.CSV_FIELD_MAPPING:
            mappings[f"{nmi}-{suffix}"] = f"p:test:{nmi}:{suffix}"
    monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: mappings)

    with patch("shared.parsers.optima.bunnings_billing.boto3.client") as mock_client:
        mock_client.return_value.put_object.side_effect = RuntimeError("boom")

        with pytest.raises(ProcessingError, match="Failed to write Bunnings billing Hudi CSV"):
            bp_mod.bunnings_billing_parser(str(dst), "dummy")
```

Add RACV billing test:

```python
def test_racv_billing_success_returns_processed_external(tmp_path) -> None:
    path = tmp_path / "20260414-RACV-Usage and Spend Report.csv"
    path.write_text("a,b\n1,2\n")

    with patch("shared.parsers.optima.racv_billing.boto3.client") as mock_client:
        mock_client.return_value.put_object.return_value = {"ETag": "etag"}
        result = racv_billing_parser(str(path), "error_log")

    assert result.status == "processed_external"
    assert result.reason == "gegoptimareports"


def test_racv_billing_upload_failure_raises_processing_error(tmp_path) -> None:
    from shared.parsers import ProcessingError

    path = tmp_path / "20260414-RACV-Usage and Spend Report.csv"
    path.write_text("a,b\n1,2\n")

    with patch("shared.parsers.optima.racv_billing.boto3.client") as mock_client:
        mock_client.return_value.put_object.side_effect = RuntimeError("boom")

        with pytest.raises(ProcessingError, match="Failed to upload RACV billing report"):
            racv_billing_parser(str(path), "error_log")
```

- [ ] **Step 2: Run billing tests and verify they fail**

Run:

```bash
uv run pytest tests/unit/parsers/optima/test_bunnings_billing.py tests/unit/parsers/optima/test_racv_billing.py -v
```

Expected: FAIL because both parsers still return `[]` and generic exceptions.

- [ ] **Step 3: Implement Bunnings billing outcome stats**

In `bunnings_billing.py`:

- Import `dataclass`, typed parser classes, and `ProcessingError`.
- Add `BillingBuildResult` with `body`, `source_row_count`, `candidate_row_count`, `rows_written`, `unmapped_count`, `invalid_count`.
- Count a candidate only after a valid NMI, valid billing date, and non-blank field value exist.
- If all valid candidates miss mappings, return `ParserOutcome(status="unmapped", source_row_count=build.source_row_count, candidate_row_count=build.candidate_row_count, rows_written=0, unmapped_count=build.unmapped_count, reason="all_candidates_unmapped")`.
- If source rows exist but all candidate formation fails because dates/values are invalid, raise `ParserError`.
- Wrap `put_object` failures:

```python
try:
    boto3.client("s3").put_object(Bucket=HUDI_BUCKET, Key=key, Body=buf.getvalue().encode())
except Exception as e:
    raise ProcessingError(f"Failed to write Bunnings billing Hudi CSV: {e}") from e
```

- Change filename mismatch to:

```python
raise NotRelevantParser("Not Bunnings Usage and Spend File")
```

- Return `ParserOutcome(status="processed", source_row_count=build.source_row_count, candidate_row_count=build.candidate_row_count, rows_written=build.rows_written, unmapped_count=build.unmapped_count)` after a successful write.

- [ ] **Step 4: Implement RACV billing outcome**

In `racv_billing.py`:

```python
from shared.parsers import NotRelevantParser, ParserOutcome, ProcessingError
```

Use:

```python
if "OptimaGenerationData" in file_name:
    raise NotRelevantParser("Not Relevant Parser For File")

if "RACV-Usage and Spend Report" not in file_name:
    raise NotRelevantParser("Not Valid Optima Usage And Spend File")

try:
    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY, Body=file_data)
except Exception as e:
    raise ProcessingError(f"Failed to upload RACV billing report: {e}") from e

return ParserOutcome(status="processed_external", reason="gegoptimareports")
```

- [ ] **Step 5: Run billing tests**

Run:

```bash
uv run pytest tests/unit/parsers/optima/test_bunnings_billing.py tests/unit/parsers/optima/test_racv_billing.py -v
```

Expected: PASS after updating legacy `assert result == []` assertions to `result.status`.

- [ ] **Step 6: Commit**

```bash
git add src/shared/parsers/optima/bunnings_billing.py src/shared/parsers/optima/racv_billing.py tests/unit/parsers/optima/test_bunnings_billing.py tests/unit/parsers/optima/test_racv_billing.py
git commit -m "feat: return explicit billing parser outcomes"
```

---

### Task 5: Migrate Interval Parser and Standard Non-NEM Parsers

**Files:**
- Modify: `src/shared/parsers/optima/interval.py`
- Modify: `src/shared/parsers/envizi/vertical_electricity.py`
- Modify: `src/shared/parsers/envizi/vertical_water.py`
- Modify: `src/shared/parsers/envizi/vertical_water_bulk.py`
- Modify: `src/shared/parsers/racv/elec.py`
- Modify: `src/shared/parsers/racv/noosa_solar.py`
- Modify: `src/shared/parsers/green_square/comx.py`
- Modify: `tests/unit/parsers/optima/test_interval.py`
- Modify: `tests/unit/test_dispatcher.py`
- Modify: `tests/unit/test_non_nem_parsers.py`
- Modify: `tests/unit/test_non_nem_parsers_edge_cases.py`
- Modify: `tests/unit/parsers/racv/test_noosa_solar.py`

- [ ] **Step 1: Update interval direct parser tests**

In `tests/unit/parsers/optima/test_interval.py`, change no-data and happy-path assertions:

```python
result = interval_parser(path, "error_log")
assert result.status == "processed_empty"
assert result.reason == "no_data_sentinel"
assert result.dfs == []
```

For normal interval CSV:

```python
result = interval_parser(path, "error_log")
assert result.status == "processed"
assert len(result.dfs) == 1
nmi, df = result.dfs[0]
assert nmi.startswith("Optima_")
```

- [ ] **Step 2: Run direct parser tests and verify failures**

Run:

```bash
uv run pytest tests/unit/parsers/optima/test_interval.py tests/unit/test_non_nem_parsers.py tests/unit/test_non_nem_parsers_edge_cases.py tests/unit/parsers/racv/test_noosa_solar.py -v
```

Expected: FAIL where tests still expect raw lists.

- [ ] **Step 3: Implement interval outcome**

In `interval.py`:

```python
from shared.parsers import NotRelevantParser, ParserError, ParserOutcome
```

Use:

```python
try:
    raw_df = pd.read_csv(file_name)
except Exception as e:
    raise NotRelevantParser(f"Not readable as an Optima interval CSV: {e}") from e

required_columns = {"Date", "Start Time", "Identifier"}
if not required_columns.issubset(raw_df.columns):
    raise NotRelevantParser("Not an Optima interval CSV")

if len(raw_df) == 1 and raw_df["Date"].isna().all():
    logger.info("interval_no_data_sentinel", extra={"file": file_name})
    return ParserOutcome(status="processed_empty", reason="no_data_sentinel")

try:
    raw_df["Interval_Start"] = pd.to_datetime(raw_df["Date"] + " " + raw_df["Start Time"])
except Exception as e:
    raise ParserError(f"Failed to parse interval timestamps: {e}") from e

raw_df["Identifier"] = raw_df["Identifier"].astype(str)

dfs: list[tuple[str, pd.DataFrame]] = []
for name in sorted(raw_df["Identifier"].unique()):
    base_df = raw_df.loc[raw_df["Identifier"] == name].copy()
    output_df = base_df[["Interval_Start"]].copy()
    output_df = output_df.rename(columns={"Interval_Start": "t_start"})
    if "Usage" in raw_df.columns:
        output_df["E1_kWh"] = base_df["Usage"].values
    if "Generation" in raw_df.columns:
        output_df["B1_kWh"] = base_df["Generation"].values
    output_df = output_df.set_index("t_start")
    dfs.append((f"Optima_{name}", output_df))

return ParserOutcome(
    status="processed",
    dfs=dfs,
    source_row_count=len(raw_df),
)
```

- [ ] **Step 4: Migrate other DataFrame parsers**

For each standard parser:

- Raise `NotRelevantParser` for filename/content/schema gates that prove the parser does not own the file.
- For broad CSV parsers that appear early in `PARSERS`, missing parser-specific required columns must be `NotRelevantParser`, not `ParserError`, so later parsers can still run.
- For broad CSV parsers that fail before the parser-specific schema gate, including decode/read failures, raise `NotRelevantParser`; without a passed gate the parser has not proved ownership.
- Raise `ParserError` only after the parser-specific relevance gate passes, for malformed timestamps, invalid required values, or no valid parsed data in a matched file.
- Return `ParserOutcome(status="processed", dfs=dfs, source_row_count=len(df))` instead of `dfs`.
- Do not set `candidate_row_count`, `rows_written`, or `unmapped_count` in DataFrame-returning parsers. Those counts become final only after `file_processor` applies suffix filtering, null-value filtering, and mapping lookup.

Use this shape for Envizi parsers:

```python
from shared.parsers import NotRelevantParser, ParserError, ParserOutcome

if "OptimaGenerationData" in file_name:
    raise NotRelevantParser("Not Relevant Parser For File")

try:
    raw_df = pd.read_csv(file_name)
except Exception as e:
    raise NotRelevantParser(f"Not readable as an Envizi CSV: {e}") from e

required_columns = {"Serial_No", "Interval_Start", "Interval_End", "kWh"}
if not required_columns.issubset(raw_df.columns):
    raise NotRelevantParser("Not an Envizi electricity CSV")

try:
    raw_df["Interval_Start"] = pd.to_datetime(raw_df["Interval_Start"])
    raw_df["Serial_No"] = raw_df["Serial_No"].astype(str)
except Exception as e:
    raise ParserError(f"Failed to parse Envizi electricity CSV: {e}") from e

dfs: list[tuple[str, pd.DataFrame]] = []
for name in sorted(raw_df["Serial_No"].unique()):
    buf_df = raw_df.loc[raw_df["Serial_No"] == name, ["Interval_Start", "Interval_End", "kWh"]]
    buf_df = buf_df.rename(columns={"Interval_Start": "t_start", "kWh": "E1_kWh"})
    buf_df = buf_df.set_index("t_start")
    dfs.append((f"Envizi_{name}", buf_df))

return ParserOutcome(
    status="processed",
    dfs=dfs,
    source_row_count=len(raw_df),
)
```

Use parser-specific required columns:

```python
ENVIZI_ELECTRICITY_REQUIRED = {"Serial_No", "Interval_Start", "Interval_End", "kWh"}
ENVIZI_WATER_REQUIRED = {"Serial_No", "Interval_Start", "Interval_End", "Consumption", "Consumption Unit"}
ENVIZI_BULK_WATER_REQUIRED = {"Serial_No", "Date_Time", "kL"}
COMX_REQUIRED = {"Local Time Stamp"}  # plus one active-energy column checked after ComX header marker matches
```

Add direct relevance-gate tests so broad parsers do not stop later parser attempts:

```python
def test_envizi_electricity_missing_required_columns_is_not_relevant(tmp_path) -> None:
    from shared.parsers import NotRelevantParser
    from shared.parsers.envizi.vertical_electricity import envizi_vertical_parser_electricity

    path = tmp_path / "bunnings_demand_profile.csv"
    path.write_text('Commodities:,"Electricity"\nNo data found\n')

    with pytest.raises(NotRelevantParser, match="Not an Envizi electricity CSV"):
        envizi_vertical_parser_electricity(str(path), "error_log")


def test_envizi_electricity_decode_error_is_not_relevant(tmp_path) -> None:
    from shared.parsers import NotRelevantParser
    from shared.parsers.envizi.vertical_electricity import envizi_vertical_parser_electricity

    path = tmp_path / "20260414-RACV-Usage and Spend Report.csv"
    path.write_bytes("Commodities:\n".encode("utf-16-le"))

    with pytest.raises(NotRelevantParser, match="Not readable as an Envizi CSV"):
        envizi_vertical_parser_electricity(str(path), "error_log")
```

Use this shape for Noosa and ComX:

```python
if not relevant:
    raise NotRelevantParser("Not a Noosa Solar file")

if required_column_missing:
    raise ParserError("Missing timestamp column in Noosa Solar file")

if not results:
    return ParserOutcome(status="processed_empty", source_row_count=len(df), reason="no_valid_point_rows")

return ParserOutcome(
    status="processed",
    dfs=results,
    source_row_count=len(df),
)
```

- [ ] **Step 5: Update tests that inspect raw parser results**

For every direct parser call changed in tests:

```python
result = parser(path, "error")
assert result.status == "processed"
assert len(result.dfs) == expected_count
nmi, df = result.dfs[0]
```

For dispatcher calls:

```python
result = get_non_nem_outcome(path, "error")
assert result.status == "processed"
assert len(result.dfs) == expected_count
```

- [ ] **Step 6: Run parser test subset**

Run:

```bash
uv run pytest tests/unit/test_dispatcher.py tests/unit/test_non_nem_parsers.py tests/unit/test_non_nem_parsers_edge_cases.py tests/unit/parsers/optima/test_interval.py tests/unit/parsers/racv/test_noosa_solar.py -v
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/shared/parsers tests/unit/test_dispatcher.py tests/unit/test_non_nem_parsers.py tests/unit/test_non_nem_parsers_edge_cases.py tests/unit/parsers/optima/test_interval.py tests/unit/parsers/racv/test_noosa_solar.py
git commit -m "feat: return outcomes from dataframe parsers"
```

---

### Task 6: Update File Processor Disposition Logic

**Files:**
- Modify: `src/functions/file_processor/app.py`
- Modify: `tests/unit/test_edge_cases.py`
- Modify: `tests/unit/optima_exporter/test_e2e_full_chain.py`

- [ ] **Step 1: Add failing file movement tests for outcome statuses**

In `tests/unit/test_edge_cases.py`, first add `import pytest` to the existing top import block, then add tests that patch NEM parsing to fail and non-NEM parsing to return explicit outcomes:

```python
@mock_aws
def test_processed_empty_outcome_moves_to_newp(temp_directory: str) -> None:
    from shared.parsers import ParserOutcome
    from functions.file_processor.app import parse_and_write_data

    s3_resource = boto3.resource("s3", region_name="ap-southeast-2")
    s3_resource.create_bucket(Bucket="sbm-file-ingester", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"})
    s3_resource.create_bucket(Bucket="hudibucketsrc", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"})
    s3_resource.Object("sbm-file-ingester", "newTBP/no_data.csv").put(Body=b"no data")
    s3_resource.Object("sbm-file-ingester", "nem12_mappings.json").put(Body=json.dumps({}))

    with (
        patch("functions.file_processor.app.s3_resource", s3_resource),
        patch("functions.file_processor.app.stream_as_data_frames", side_effect=ValueError("not nem")),
        patch("functions.file_processor.app.output_as_data_frames", side_effect=ValueError("not nem")),
        patch(
            "functions.file_processor.app.get_non_nem_outcome",
            return_value=ParserOutcome(status="processed_empty", reason="no_data"),
            create=True,
        ),
    ):
        result = parse_and_write_data(tbp_files=[{"bucket": "sbm-file-ingester", "file_name": "newTBP/no_data.csv"}])

    assert result == 1
    keys = [obj.key for obj in s3_resource.Bucket("sbm-file-ingester").objects.filter(Prefix="newP/")]
    assert keys == ["newP/no_data.csv"]


@mock_aws
def test_processed_external_outcome_moves_to_newp(temp_directory: str) -> None:
    from shared.parsers import ParserOutcome
    from functions.file_processor.app import parse_and_write_data

    s3_resource = boto3.resource("s3", region_name="ap-southeast-2")
    s3_resource.create_bucket(Bucket="sbm-file-ingester", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"})
    s3_resource.create_bucket(Bucket="hudibucketsrc", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"})
    s3_resource.Object("sbm-file-ingester", "newTBP/racv_billing.csv").put(Body=b"ok")
    s3_resource.Object("sbm-file-ingester", "nem12_mappings.json").put(Body=json.dumps({}))

    with (
        patch("functions.file_processor.app.s3_resource", s3_resource),
        patch("functions.file_processor.app.stream_as_data_frames", side_effect=ValueError("not nem")),
        patch("functions.file_processor.app.output_as_data_frames", side_effect=ValueError("not nem")),
        patch(
            "functions.file_processor.app.get_non_nem_outcome",
            return_value=ParserOutcome(status="processed_external", reason="external"),
            create=True,
        ),
    ):
        result = parse_and_write_data(tbp_files=[{"bucket": "sbm-file-ingester", "file_name": "newTBP/racv_billing.csv"}])

    assert result == 1
    keys = [obj.key for obj in s3_resource.Bucket("sbm-file-ingester").objects.filter(Prefix="newP/")]
    assert keys == ["newP/racv_billing.csv"]


@mock_aws
def test_unmapped_outcome_moves_to_new_irrev_files(temp_directory: str) -> None:
    from shared.parsers import ParserOutcome
    from functions.file_processor.app import parse_and_write_data

    s3_resource = boto3.resource("s3", region_name="ap-southeast-2")
    s3_resource.create_bucket(Bucket="sbm-file-ingester", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"})
    s3_resource.create_bucket(Bucket="hudibucketsrc", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"})
    s3_resource.Object("sbm-file-ingester", "newTBP/unmapped.csv").put(Body=b"ok")
    s3_resource.Object("sbm-file-ingester", "nem12_mappings.json").put(Body=json.dumps({}))

    with (
        patch("functions.file_processor.app.s3_resource", s3_resource),
        patch("functions.file_processor.app.stream_as_data_frames", side_effect=ValueError("not nem")),
        patch("functions.file_processor.app.output_as_data_frames", side_effect=ValueError("not nem")),
        patch(
            "functions.file_processor.app.get_non_nem_outcome",
            return_value=ParserOutcome(status="unmapped", candidate_row_count=3, unmapped_count=3),
            create=True,
        ),
    ):
        result = parse_and_write_data(tbp_files=[{"bucket": "sbm-file-ingester", "file_name": "newTBP/unmapped.csv"}])

    assert result == 1
    keys = [obj.key for obj in s3_resource.Bucket("sbm-file-ingester").objects.filter(Prefix="newIrrevFiles/")]
    assert keys == ["newIrrevFiles/unmapped.csv"]


@mock_aws
def test_dataframe_all_unmapped_moves_to_new_irrev_files(temp_directory: str) -> None:
    from shared.parsers import ParserOutcome
    from functions.file_processor.app import parse_and_write_data

    s3_resource = boto3.resource("s3", region_name="ap-southeast-2")
    s3_resource.create_bucket(Bucket="sbm-file-ingester", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"})
    s3_resource.create_bucket(Bucket="hudibucketsrc", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"})
    s3_resource.Object("sbm-file-ingester", "newTBP/all_unmapped.csv").put(Body=b"ok")
    s3_resource.Object("sbm-file-ingester", "nem12_mappings.json").put(Body=json.dumps({}))

    df = pd.DataFrame(
        {
            "t_start": ["2026-01-01 00:00:00", "2026-01-01 00:30:00"],
            "E1_kWh": [1.0, 2.0],
        }
    )

    with (
        patch("functions.file_processor.app.s3_resource", s3_resource),
        patch("functions.file_processor.app.stream_as_data_frames", side_effect=ValueError("not nem")),
        patch("functions.file_processor.app.output_as_data_frames", side_effect=ValueError("not nem")),
        patch(
            "functions.file_processor.app.get_non_nem_outcome",
            return_value=ParserOutcome(status="processed", dfs=[("Optima_4001260599", df)], source_row_count=2),
            create=True,
        ),
    ):
        result = parse_and_write_data(tbp_files=[{"bucket": "sbm-file-ingester", "file_name": "newTBP/all_unmapped.csv"}])

    assert result == 1
    keys = [obj.key for obj in s3_resource.Bucket("sbm-file-ingester").objects.filter(Prefix="newIrrevFiles/")]
    assert keys == ["newIrrevFiles/all_unmapped.csv"]


@mock_aws
def test_dataframe_partial_mapping_moves_to_newp(temp_directory: str) -> None:
    from shared.parsers import ParserOutcome
    from functions.file_processor.app import parse_and_write_data

    s3_resource = boto3.resource("s3", region_name="ap-southeast-2")
    s3_resource.create_bucket(Bucket="sbm-file-ingester", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"})
    s3_resource.create_bucket(Bucket="hudibucketsrc", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"})
    s3_resource.Object("sbm-file-ingester", "newTBP/partial_mapped.csv").put(Body=b"ok")
    s3_resource.Object("sbm-file-ingester", "nem12_mappings.json").put(Body=json.dumps({"Optima_4001260599-E1": "p:test:e1"}))

    df = pd.DataFrame(
        {
            "t_start": ["2026-01-01 00:00:00", "2026-01-01 00:30:00"],
            "E1_kWh": [1.0, 2.0],
            "B1_kWh": [3.0, 4.0],
        }
    )

    with (
        patch("functions.file_processor.app.s3_resource", s3_resource),
        patch("functions.file_processor.app.stream_as_data_frames", side_effect=ValueError("not nem")),
        patch("functions.file_processor.app.output_as_data_frames", side_effect=ValueError("not nem")),
        patch(
            "functions.file_processor.app.get_non_nem_outcome",
            return_value=ParserOutcome(status="processed", dfs=[("Optima_4001260599", df)], source_row_count=2),
            create=True,
        ),
    ):
        result = parse_and_write_data(tbp_files=[{"bucket": "sbm-file-ingester", "file_name": "newTBP/partial_mapped.csv"}])

    assert result == 1
    keys = [obj.key for obj in s3_resource.Bucket("sbm-file-ingester").objects.filter(Prefix="newP/")]
    assert keys == ["newP/partial_mapped.csv"]


@mock_aws
def test_side_effect_processed_outcome_moves_to_newp(temp_directory: str) -> None:
    from shared.parsers import ParserOutcome
    from functions.file_processor.app import parse_and_write_data

    s3_resource = boto3.resource("s3", region_name="ap-southeast-2")
    s3_resource.create_bucket(Bucket="sbm-file-ingester", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"})
    s3_resource.create_bucket(Bucket="hudibucketsrc", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"})
    s3_resource.Object("sbm-file-ingester", "newTBP/Bunnings_Demand_Profile.csv").put(Body=b"ok")
    s3_resource.Object("sbm-file-ingester", "nem12_mappings.json").put(Body=json.dumps({}))

    with (
        patch("functions.file_processor.app.s3_resource", s3_resource),
        patch("functions.file_processor.app.stream_as_data_frames", side_effect=ValueError("not nem")),
        patch("functions.file_processor.app.output_as_data_frames", side_effect=ValueError("not nem")),
        patch(
            "functions.file_processor.app.get_non_nem_outcome",
            return_value=ParserOutcome(
                status="processed",
                source_row_count=3,
                candidate_row_count=9,
                rows_written=9,
            ),
            create=True,
        ),
    ):
        result = parse_and_write_data(tbp_files=[{"bucket": "sbm-file-ingester", "file_name": "newTBP/Bunnings_Demand_Profile.csv"}])

    assert result == 1
    keys = [obj.key for obj in s3_resource.Bucket("sbm-file-ingester").objects.filter(Prefix="newP/")]
    assert keys == ["newP/Bunnings_Demand_Profile.csv"]


@mock_aws
def test_dataframe_unsupported_suffix_moves_to_newp_without_hudi_write(temp_directory: str) -> None:
    from shared.parsers import ParserOutcome
    from functions.file_processor.app import parse_and_write_data

    s3_resource = boto3.resource("s3", region_name="ap-southeast-2")
    s3_resource.create_bucket(Bucket="sbm-file-ingester", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"})
    s3_resource.create_bucket(Bucket="hudibucketsrc", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"})
    s3_resource.Object("sbm-file-ingester", "newTBP/unsupported_suffix.csv").put(Body=b"ok")
    s3_resource.Object("sbm-file-ingester", "nem12_mappings.json").put(Body=json.dumps({"Optima_4001260599-ZZ": "p:test:zz"}))

    df = pd.DataFrame(
        {
            "t_start": ["2026-01-01 00:00:00"],
            "ZZ_kWh": [1.0],
        }
    )

    with (
        patch("functions.file_processor.app.s3_resource", s3_resource),
        patch("functions.file_processor.app.stream_as_data_frames", side_effect=ValueError("not nem")),
        patch("functions.file_processor.app.output_as_data_frames", side_effect=ValueError("not nem")),
        patch(
            "functions.file_processor.app.get_non_nem_outcome",
            return_value=ParserOutcome(status="processed", dfs=[("Optima_4001260599", df)], source_row_count=1),
            create=True,
        ),
    ):
        result = parse_and_write_data(tbp_files=[{"bucket": "sbm-file-ingester", "file_name": "newTBP/unsupported_suffix.csv"}])

    assert result == 1
    processed_keys = [obj.key for obj in s3_resource.Bucket("sbm-file-ingester").objects.filter(Prefix="newP/")]
    hudi_keys = [obj.key for obj in s3_resource.Bucket("hudibucketsrc").objects.filter(Prefix="sensorDataFiles/")]
    assert processed_keys == ["newP/unsupported_suffix.csv"]
    assert hudi_keys == []


@mock_aws
def test_dataframe_nan_values_move_to_newp_without_hudi_write(temp_directory: str) -> None:
    from shared.parsers import ParserOutcome
    from functions.file_processor.app import parse_and_write_data

    s3_resource = boto3.resource("s3", region_name="ap-southeast-2")
    s3_resource.create_bucket(Bucket="sbm-file-ingester", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"})
    s3_resource.create_bucket(Bucket="hudibucketsrc", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"})
    s3_resource.Object("sbm-file-ingester", "newTBP/nan_values.csv").put(Body=b"ok")
    s3_resource.Object("sbm-file-ingester", "nem12_mappings.json").put(Body=json.dumps({"Optima_4001260599-E1": "p:test:e1"}))

    df = pd.DataFrame(
        {
            "t_start": ["2026-01-01 00:00:00"],
            "E1_kWh": [pd.NA],
        }
    )

    with (
        patch("functions.file_processor.app.s3_resource", s3_resource),
        patch("functions.file_processor.app.stream_as_data_frames", side_effect=ValueError("not nem")),
        patch("functions.file_processor.app.output_as_data_frames", side_effect=ValueError("not nem")),
        patch(
            "functions.file_processor.app.get_non_nem_outcome",
            return_value=ParserOutcome(status="processed", dfs=[("Optima_4001260599", df)], source_row_count=1),
            create=True,
        ),
    ):
        result = parse_and_write_data(tbp_files=[{"bucket": "sbm-file-ingester", "file_name": "newTBP/nan_values.csv"}])

    assert result == 1
    processed_keys = [obj.key for obj in s3_resource.Bucket("sbm-file-ingester").objects.filter(Prefix="newP/")]
    hudi_keys = [obj.key for obj in s3_resource.Bucket("hudibucketsrc").objects.filter(Prefix="sensorDataFiles/")]
    assert processed_keys == ["newP/nan_values.csv"]
    assert hudi_keys == []


@mock_aws
def test_direct_point_id_bypasses_mapping_and_moves_to_newp(temp_directory: str) -> None:
    from shared.parsers import ParserOutcome
    from functions.file_processor.app import parse_and_write_data

    s3_resource = boto3.resource("s3", region_name="ap-southeast-2")
    s3_resource.create_bucket(Bucket="sbm-file-ingester", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"})
    s3_resource.create_bucket(Bucket="hudibucketsrc", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"})
    s3_resource.Object("sbm-file-ingester", "newTBP/noosa_solar.csv").put(Body=b"ok")
    s3_resource.Object("sbm-file-ingester", "nem12_mappings.json").put(Body=json.dumps({}))

    df = pd.DataFrame(
        {
            "t_start": ["2026-01-01 00:00:00"],
            "E1_kWh": [1.0],
        }
    )

    with (
        patch("functions.file_processor.app.s3_resource", s3_resource),
        patch("functions.file_processor.app.stream_as_data_frames", side_effect=ValueError("not nem")),
        patch("functions.file_processor.app.output_as_data_frames", side_effect=ValueError("not nem")),
        patch(
            "functions.file_processor.app.get_non_nem_outcome",
            return_value=ParserOutcome(status="processed", dfs=[("p:racv:r:test-direct", df)], source_row_count=1),
            create=True,
        ),
    ):
        result = parse_and_write_data(tbp_files=[{"bucket": "sbm-file-ingester", "file_name": "newTBP/noosa_solar.csv"}])

    assert result == 1
    processed_keys = [obj.key for obj in s3_resource.Bucket("sbm-file-ingester").objects.filter(Prefix="newP/")]
    hudi_keys = [obj.key for obj in s3_resource.Bucket("hudibucketsrc").objects.filter(Prefix="sensorDataFiles/")]
    assert processed_keys == ["newP/noosa_solar.csv"]
    assert len(hudi_keys) == 1
    body = s3_resource.Object("hudibucketsrc", hudi_keys[0]).get()["Body"].read().decode()
    assert "p:racv:r:test-direct,2026-01-01 00:00:00,1.0,kwh" in body


@mock_aws
def test_quality_column_is_written_with_mapped_rows(temp_directory: str) -> None:
    from shared.parsers import ParserOutcome
    from functions.file_processor.app import parse_and_write_data

    s3_resource = boto3.resource("s3", region_name="ap-southeast-2")
    s3_resource.create_bucket(Bucket="sbm-file-ingester", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"})
    s3_resource.create_bucket(Bucket="hudibucketsrc", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"})
    s3_resource.Object("sbm-file-ingester", "newTBP/quality.csv").put(Body=b"ok")
    s3_resource.Object("sbm-file-ingester", "nem12_mappings.json").put(Body=json.dumps({"Optima_4001260599-E1": "p:test:e1"}))

    df = pd.DataFrame(
        {
            "t_start": ["2026-01-01 00:00:00"],
            "E1_kWh": [1.0],
            "quality_E1": ["A"],
        }
    )

    with (
        patch("functions.file_processor.app.s3_resource", s3_resource),
        patch("functions.file_processor.app.stream_as_data_frames", side_effect=ValueError("not nem")),
        patch("functions.file_processor.app.output_as_data_frames", side_effect=ValueError("not nem")),
        patch(
            "functions.file_processor.app.get_non_nem_outcome",
            return_value=ParserOutcome(status="processed", dfs=[("Optima_4001260599", df)], source_row_count=1),
            create=True,
        ),
    ):
        result = parse_and_write_data(tbp_files=[{"bucket": "sbm-file-ingester", "file_name": "newTBP/quality.csv"}])

    assert result == 1
    hudi_keys = [obj.key for obj in s3_resource.Bucket("hudibucketsrc").objects.filter(Prefix="sensorDataFiles/")]
    assert len(hudi_keys) == 1
    body = s3_resource.Object("hudibucketsrc", hudi_keys[0]).get()["Body"].read().decode()
    assert "p:test:e1,2026-01-01 00:00:00,1.0,kwh,2026-01-01 00:00:00,A" in body


@mock_aws
@pytest.mark.parametrize("exception_name", ["ParserError", "ProcessingError"])
def test_parser_errors_move_to_new_parse_err(temp_directory: str, exception_name: str) -> None:
    from shared.parsers import ParserError, ProcessingError
    from functions.file_processor.app import parse_and_write_data

    exception_type = ParserError if exception_name == "ParserError" else ProcessingError
    s3_resource = boto3.resource("s3", region_name="ap-southeast-2")
    s3_resource.create_bucket(Bucket="sbm-file-ingester", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"})
    s3_resource.create_bucket(Bucket="hudibucketsrc", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"})
    s3_resource.Object("sbm-file-ingester", "newTBP/bad.csv").put(Body=b"bad")
    s3_resource.Object("sbm-file-ingester", "nem12_mappings.json").put(Body=json.dumps({}))

    with (
        patch("functions.file_processor.app.s3_resource", s3_resource),
        patch("functions.file_processor.app.stream_as_data_frames", side_effect=ValueError("not nem")),
        patch("functions.file_processor.app.output_as_data_frames", side_effect=ValueError("not nem")),
        patch(
            "functions.file_processor.app.get_non_nem_outcome",
            side_effect=exception_type("matched parser failure"),
            create=True,
        ),
    ):
        result = parse_and_write_data(tbp_files=[{"bucket": "sbm-file-ingester", "file_name": "newTBP/bad.csv"}])

    assert result == 1
    keys = [obj.key for obj in s3_resource.Bucket("sbm-file-ingester").objects.filter(Prefix="newParseErr/")]
    assert keys == ["newParseErr/bad.csv"]
```

- [ ] **Step 2: Run new file processor tests and verify they fail**

Run:

```bash
uv run pytest tests/unit/test_edge_cases.py -k "outcome_moves or dataframe_ or side_effect_processed or direct_point_id or quality_column or parser_errors_move" -v
```

Expected: FAIL because `file_processor` still expects raw `dfs`, moves empty outcomes to `newIrrevFiles/`, does not derive DataFrame outcomes, does not preserve direct `p:` and quality-column behavior under outcomes, and does not route typed parser exceptions explicitly.

- [ ] **Step 3: Import parser outcome types in file processor**

In `src/functions/file_processor/app.py`, replace the existing `get_non_nem_df` import with:

```python
from shared.non_nem_parsers import get_non_nem_outcome
from shared.parsers import ParserError, ParserOutcome, ProcessingError
```

- [ ] **Step 4: Add small outcome helpers in file processor**

Add near the existing helper functions:

```python
def _processed_destination_for_status(status: str) -> str:
    if status in {"processed", "processed_empty", "processed_external"}:
        return PROCESSED_DIR
    if status == "unmapped":
        return IRREVFILES_DIR
    raise ValueError(f"Unsupported parser outcome status: {status}")


def _candidate_values(df: pd.DataFrame, col: str) -> list:
    return [val for val in df[col] if not pd.isna(val)]
```

- [ ] **Step 5: Wrap NEM parser output in `ParserOutcome`**

Where NEM streaming or batch parsing succeeds, set:

```python
outcome = ParserOutcome(status="processed", dfs=dfs)
```

Where non-NEM parsing succeeds:

```python
outcome = get_non_nem_outcome(local_file_path, PARSE_ERROR_LOG_GROUP)
```

Initialize `outcome: ParserOutcome | None = None` instead of `dfs = None`.

- [ ] **Step 6: Process `outcome.dfs` and derive final disposition**

Replace the `file_neptune_ids` block with logic equivalent to:

```python
mapped_ids: list[str] = []
candidate_row_count = 0 if outcome.dfs else outcome.candidate_row_count
unmapped_count = 0 if outcome.dfs else outcome.unmapped_count
rows_written_count = 0 if outcome.dfs else outcome.rows_written

try:
    for nmi, df in outcome.dfs:
        if "t_start" not in df.columns and df.index.name == "t_start":
            df = df.reset_index()
        if "t_start" not in df.columns:
            raise ProcessingError(f"Missing t_start column for {nmi}")

        t_start_col = df["t_start"]
        for col in df.columns:
            suffix = col.split("_")[0]
            if suffix not in NMI_DATA_STREAM_COMBINED:
                continue

            values = _candidate_values(df, col)
            if not values:
                continue
            candidate_row_count += len(values)

            if nmi.startswith("p:"):
                neptune_id = nmi
            else:
                monitor_point_name = f"{nmi}-{suffix}"
                neptune_id = nem12_mappings.get(monitor_point_name)

            if neptune_id is None:
                unmapped_count += len(values)
                continue

            mapped_ids.append(neptune_id)
            unit_name = col.split("_")[1].lower() if "_" in col else "kwh"
            quality_col_name = f"quality_{suffix}"
            quality_col = df[quality_col_name] if quality_col_name in df.columns else None

            if quality_col is not None:
                for ts, val, q in zip(t_start_col, df[col], quality_col, strict=False):
                    if pd.isna(val):
                        continue
                    csv_writer.write_row(neptune_id, ts, val, unit_name, "" if pd.isna(q) else q)
                    rows_written_count += 1
            else:
                for ts, val in zip(t_start_col, df[col], strict=False):
                    if pd.isna(val):
                        continue
                    csv_writer.write_row(neptune_id, ts, val, unit_name)
                    rows_written_count += 1

            processed_monitor_points_count += 1
            if csv_writer.row_count >= BATCH_SIZE:
                csv_writer.flush()
except Exception as e:
    logger.error("Error processing NMI data", exc_info=True, extra={"file": local_file_path, "error": str(e)})
    logs_dict[f"Processing Error: {local_file_path}"] = f"[{timestamp_now}] {e}"
    move_s3_file(BUCKET_NAME, local_file_path, PARSE_ERR_DIR)
    parse_err_files_count += 1
    continue
```

After writing DataFrame rows:

```python
if outcome.dfs:
    if mapped_ids:
        outcome = ParserOutcome(
            status="processed",
            dfs=outcome.dfs,
            source_row_count=outcome.source_row_count,
            candidate_row_count=candidate_row_count,
            rows_written=rows_written_count,
            unmapped_count=unmapped_count,
            reason=outcome.reason,
        )
    elif candidate_row_count > 0 and unmapped_count == candidate_row_count:
        outcome = ParserOutcome(
            status="unmapped",
            dfs=outcome.dfs,
            source_row_count=outcome.source_row_count,
            candidate_row_count=candidate_row_count,
            unmapped_count=unmapped_count,
            reason="all_candidates_unmapped",
        )
    else:
        outcome = ParserOutcome(
            status="processed_empty",
            dfs=outcome.dfs,
            source_row_count=outcome.source_row_count,
            candidate_row_count=candidate_row_count,
            reason="no_valid_candidate_rows",
        )
```

Then move by status:

```python
destination = _processed_destination_for_status(outcome.status)
move_s3_file(BUCKET_NAME, local_file_path, destination)
if destination == PROCESSED_DIR:
    valid_processed_files_count += 1
    total_monitor_points_count += len(mapped_ids)
else:
    irrev_files_count += 1
```

- [ ] **Step 7: Convert parser exceptions to parse error movement**

When NEM and non-NEM parsing fail:

```python
except (ParserError, ProcessingError) as e:
    logs_dict[f"Bad File: {local_file_path}"] = f"[{timestamp_now}] {e}"
    move_s3_file(BUCKET_NAME, local_file_path, PARSE_ERR_DIR)
    parse_err_files_count += 1
    parse_failed = True
```

Keep the existing final "no parser found" path as parse error.

- [ ] **Step 8: Run file processor tests**

Run:

```bash
uv run pytest tests/unit/test_edge_cases.py tests/unit/optima_exporter/test_e2e_full_chain.py -v
```

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add src/functions/file_processor/app.py tests/unit/test_edge_cases.py tests/unit/optima_exporter/test_e2e_full_chain.py
git commit -m "feat: move source files by parser outcome"
```

---

### Task 7: Tighten Dispatcher After Parser Migration

**Files:**
- Modify: `src/shared/non_nem_parsers.py`
- Modify: `tests/unit/test_dispatcher.py`

- [ ] **Step 1: Add failing test for unexpected exception after relevance gate**

Add to `tests/unit/test_dispatcher.py`:

```python
def test_unexpected_parser_exception_becomes_parser_error(tmp_path, monkeypatch) -> None:
    from shared.parsers import ParserError
    from shared.non_nem_parsers import get_non_nem_outcome

    def parser(file_name: str, error_file_path: str):
        raise RuntimeError("unexpected")

    monkeypatch.setattr("shared.non_nem_parsers.PARSERS", [parser])

    with pytest.raises(ParserError, match="Unexpected parser failure"):
        get_non_nem_outcome(str(tmp_path / "file.csv"), "error_log")
```

Also add routing regression tests that prove broad parsers do not block later Optima parsers:

```python
def test_envizi_schema_miss_does_not_block_later_parser(tmp_path, monkeypatch) -> None:
    from shared.parsers import NotRelevantParser, ParserOutcome
    from shared.non_nem_parsers import get_non_nem_outcome

    def envizi_like_parser(file_name: str, error_file_path: str):
        raise NotRelevantParser("Not an Envizi electricity CSV")

    def later_parser(file_name: str, error_file_path: str):
        return ParserOutcome(status="processed_empty", reason="later_parser_matched")

    monkeypatch.setattr("shared.non_nem_parsers.PARSERS", [envizi_like_parser, later_parser])

    result = get_non_nem_outcome(str(tmp_path / "bunnings_demand_profile.csv"), "error_log")

    assert result.status == "processed_empty"
    assert result.reason == "later_parser_matched"


def test_real_dispatcher_routes_optima_interval_after_early_schema_misses(temp_directory: str) -> None:
    from pathlib import Path

    from conftest import create_optima_csv
    from shared.non_nem_parsers import get_non_nem_outcome

    path = str(Path(temp_directory) / "optima_interval.csv")
    create_optima_csv(path, identifiers=["4001260599"], rows_per_id=1)

    result = get_non_nem_outcome(path, "error_log")

    assert result.status == "processed"
    assert len(result.dfs) == 1
    assert result.dfs[0][0] == "Optima_4001260599"
```

- [ ] **Step 2: Run dispatcher test and verify it fails**

Run:

```bash
uv run pytest tests/unit/test_dispatcher.py::test_unexpected_parser_exception_becomes_parser_error -v
```

Expected: FAIL because generic exceptions are still swallowed.

- [ ] **Step 3: Remove generic exception compatibility**

In `get_non_nem_outcome()`, replace the generic `except Exception` branch with:

```python
except Exception as e:
    logger.exception(
        "Unexpected parser failure",
        extra={"parser": parser.__name__, "file": file_name, "error": str(e)},
    )
    raise ParserError(f"Unexpected parser failure in {parser.__name__}: {e}") from e
```

All parser relevance misses should now use `NotRelevantParser`.

- [ ] **Step 4: Run dispatcher and parser tests**

Run:

```bash
uv run pytest tests/unit/test_dispatcher.py tests/unit/test_non_nem_parsers.py tests/unit/test_non_nem_parsers_edge_cases.py tests/unit/parsers -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/shared/non_nem_parsers.py tests/unit/test_dispatcher.py
git commit -m "feat: stop dispatch on unexpected parser errors"
```

---

### Task 8: Update Integration Assertions, Dispatcher Coverage, and Documentation

**Files:**
- Modify: `tests/unit/parsers/optima/test_demand.py`
- Modify: `tests/unit/parsers/optima/test_bunnings_billing.py`
- Modify: `tests/unit/parsers/optima/test_racv_billing.py`
- Modify: `docs/superpowers/specs/2026-05-06-parser-outcome-semantics-design.md` if implementation changes a planned field name
- Modify: `AGENTS.md` only if the repository guidance's "File Movement After Processing" table needs wording updates

- [ ] **Step 1: Update side-effect parser dispatcher integration tests**

In `tests/unit/parsers/optima/test_demand.py`, replace the existing `TestDispatcherIntegration` test with:

```python
class TestDispatcherIntegration:
    def test_dispatcher_routes_demand_file(self, write_demand_csv, monkeypatch, _reset_mappings_cache):
        from shared.non_nem_parsers import get_non_nem_outcome

        fake_mappings = {
            "Optima_4001260599-demand-kw": "p:bunnings:kw",
            "Optima_4001260599-demand-kva": "p:bunnings:kva",
            "Optima_4001260599-demand-pf": "p:bunnings:pf",
        }
        monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: fake_mappings)

        with patch("shared.parsers.optima.demand.boto3.client") as mock_client:
            mock_client.return_value.put_object.return_value = {"ETag": "fake"}
            path = write_demand_csv()
            result = get_non_nem_outcome(str(path), "/tmp/err.log")

        assert result.status == "processed"
        assert result.source_row_count == 3
        assert result.candidate_row_count == 9
        assert result.rows_written == 9
        assert result.unmapped_count == 0
        assert mock_client.called
```

In `tests/unit/parsers/optima/test_bunnings_billing.py`, replace `test_dispatcher_routes_bunnings_file` with:

```python
@mock_aws
def test_dispatcher_routes_bunnings_file(_reset_mappings_cache, tmp_path) -> None:
    """End-to-end: get_non_nem_outcome should route a Bunnings billing file to bunnings_billing_parser."""
    from shared.non_nem_parsers import get_non_nem_outcome

    mappings = {"VCCCLG0019-billing-peak-usage": "p:bunnings:peak"}
    s3 = _setup_s3_with_mappings(mappings)
    src = _make_fixture(tmp_path, "VCCCLG0019", "Mar 2026", {"Peak": "100.00"})

    result = get_non_nem_outcome(str(src), "dummy")

    assert result.status == "processed"
    assert result.source_row_count == 1
    assert result.candidate_row_count == 1
    assert result.rows_written == 1
    assert result.unmapped_count == 0

    listed = s3.list_objects_v2(Bucket="hudibucketsrc", Prefix="sensorDataFiles/")
    assert listed.get("KeyCount", 0) == 1
```

In the same file, replace `test_dispatcher_still_routes_racv_file_to_racv_parser` with:

```python
@mock_aws
def test_dispatcher_still_routes_racv_file_to_racv_parser(_reset_mappings_cache, tmp_path) -> None:
    """Regression guard: RACV files must still hit optima_usage_and_spend_to_s3, not the Bunnings parser."""
    from shared.non_nem_parsers import get_non_nem_outcome

    s3 = boto3.client("s3", region_name="ap-southeast-2")
    s3.create_bucket(
        Bucket="gegoptimareports",
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
    )
    dst = tmp_path / "20260414.024550-RACV-Usage and Spend Report.csv"
    dst.write_bytes(b"dummy content")

    result = get_non_nem_outcome(str(dst), "dummy")

    assert result.status == "processed_external"
    assert result.reason == "gegoptimareports"
    obj = s3.get_object(Bucket="gegoptimareports", Key="usageAndSpendReports/racvUsageAndSpend.csv")
    assert obj["Body"].read() == b"dummy content"
```

- [ ] **Step 2: Run side-effect dispatcher tests**

Run:

```bash
uv run pytest tests/unit/parsers/optima/test_demand.py::TestDispatcherIntegration::test_dispatcher_routes_demand_file tests/unit/parsers/optima/test_bunnings_billing.py::test_dispatcher_routes_bunnings_file tests/unit/parsers/optima/test_bunnings_billing.py::test_dispatcher_still_routes_racv_file_to_racv_parser -v
```

Expected: PASS after Tasks 3, 4, and 7 are complete. A failure here means a side-effect parser still returns `[]`, an early parser blocks routing, or the parser outcome counts are inconsistent.

- [ ] **Step 3: Search for stale assertions and docs**

Run:

```bash
rg -n 'returns \[\]|return \[\]|assert result == \[\]|newIrrevFiles/ by the|no mapped points -> `newIrrevFiles`|Parse succeeded but no Neptune mapping' tests src docs AGENTS.md --glob '!docs/superpowers/plans/2026-05-06-parser-outcome-semantics.md'
```

Expected: output only for intentionally historical comments or no output.

- [ ] **Step 4: Replace stale wording**

Use these wording rules:

- Replace "returns []" with "returns `ParserOutcome`".
- Replace "no mapped points -> newIrrevFiles" with "valid candidate rows but no mappings -> newIrrevFiles".
- Replace side-effect parser comments that say the caller moves to `newIrrevFiles/` with "`file_processor` moves the source by `ParserOutcome.status`".

- [ ] **Step 5: Run focused stale-text search again**

Run:

```bash
rg -n 'returns \[\]|assert result == \[\]|newIrrevFiles/ by the' tests src docs AGENTS.md --glob '!docs/superpowers/plans/2026-05-06-parser-outcome-semantics.md'
```

Expected: no output.

- [ ] **Step 6: Commit**

```bash
git add tests src docs AGENTS.md
git commit -m "docs: update parser outcome terminology"
```

---

### Task 9: Full Verification

**Files:**
- No planned source edits unless verification exposes a failure.

- [ ] **Step 1: Run formatting and lint**

Run:

```bash
uv run ruff format .
uv run ruff check .
```

Expected: both commands exit 0.

- [ ] **Step 2: Run focused parser and file processor tests**

Run:

```bash
uv run pytest tests/unit/test_dispatcher.py tests/unit/parsers tests/unit/test_edge_cases.py tests/unit/optima_exporter/test_e2e_full_chain.py -v
```

Expected: PASS.

- [ ] **Step 3: Run full test suite**

Run:

```bash
uv run pytest
```

Expected: PASS.

- [ ] **Step 4: Inspect git diff**

Run:

```bash
git status --short
git diff --stat
```

Expected: clean worktree after all task commits, or only intentional uncommitted verification fixes before the final commit.

- [ ] **Step 5: Commit verification fixes if any were needed**

If verification required code/test edits:

```bash
git add src tests docs AGENTS.md
git commit -m "test: stabilize parser outcome coverage"
```

If no edits were needed, do not create an empty commit.

---

## Implementation Notes

- Do not change Hudi CSV column order: `sensorId,ts,val,unit,its,quality`.
- Do not emit header-only Hudi files for `processed_empty`.
- Do not classify malformed timestamp/date/value data as `unmapped`.
- Do not reintroduce `archived` or `not_relevant` as parser statuses.
- `processed_external` is a success state and moves to `newP/`.
- `NotRelevantParser` is an exception for dispatcher control flow, not a source-file disposition.
- Keep source comments in English and do not add co-author comments or trailers.

## Self-Review Checklist

- Spec coverage:
  - Outcome model: Task 1.
  - Dispatcher typed exceptions: Tasks 2 and 7.
  - Side-effect parser success/empty/unmapped/error semantics: Tasks 3 and 4.
  - DataFrame parser outcome migration: Task 5.
  - File movement by status, no-valid-candidate DataFrames, direct `p:` IDs, and quality-column preservation: Task 6.
  - Tests for processing failures and dispatcher boundaries: Tasks 3, 4, 6, 7, and 8.
  - Stale terminology cleanup: Task 8.
  - Verification: Task 9.
- Placeholder scan:
  - The plan avoids placeholder tokens and vague deferred work.
  - Every task includes exact files, commands, and expected results.
- Type consistency:
  - `ParserOutcome.status`, `source_row_count`, `candidate_row_count`, `rows_written`, `unmapped_count`, and `reason` match the design spec.
  - Exception names match the spec: `NotRelevantParser`, `ParserError`, `ProcessingError`.
