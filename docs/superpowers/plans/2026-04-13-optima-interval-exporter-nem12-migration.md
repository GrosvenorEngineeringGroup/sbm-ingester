# Optima Interval Exporter NEM12 Migration — Implementation Plan (Revised)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Switch `optima-interval-exporter` Lambda from BidEnergy's flat-CSV endpoint to its NEM12 endpoint, rewriting `200` records to apply the `Optima_` namespace prefix so existing Neptune mappings resolve. Every commit must leave the suite GREEN; tests must be realistic (real BidEnergy-shaped fixtures, BOM / CRLF / multi-channel variants, end-to-end through `nem_adapter`).

**Architecture:** Endpoint URL change in `downloader.py` + a new byte-level helper `_prefix_nmi_in_nem12()` invoked via a required `nmi_prefix` keyword-only argument; processor passes `OPTIMA_NMI_PREFIX="Optima_"`. Default date range narrows to yesterday only (DAYS_BACK=1, MAX_WORKERS=20). Concurrency raised 10 → 20. Partial-date bug fixed and `start>end` validated. **No changes to file_processor, nem_adapter, shared parsers, or any other downstream component.**

**Tech Stack:** Python 3.13, `requests`, `re`, `pytest`, `responses` (HTTP mocking), `moto` (AWS mocking), `freezegun` (time mocking), Terraform.

**Spec:** `docs/superpowers/specs/2026-04-13-optima-interval-exporter-nem12-migration-design.md`

---

## File Structure

| File | Disposition | Responsibility |
|---|---|---|
| `src/functions/optima_exporter/interval_exporter/downloader.py` | Modify | Endpoint URL, content-type, timeout, prefix-rewrite helper, `nmi_prefix` arg |
| `src/functions/optima_exporter/interval_exporter/processor.py` | Modify | `OPTIMA_NMI_PREFIX` constant, pass `nmi_prefix` to download, partial-date fix, `start>end` validation |
| `src/functions/optima_exporter/optima_shared/config.py` | Modify | Source defaults `DAYS_BACK=1`, `MAX_WORKERS=20` |
| `terraform/optima_exporter.tf` | Modify | Env-var values aligned with source defaults |
| `tests/unit/optima_exporter/conftest.py` | Modify | Autouse env: `DAYS_BACK="1"`, add `OPTIMA_MAX_WORKERS="20"` |
| `tests/unit/optima_exporter/interval_exporter/test_downloader.py` | Modify | URL mocks (10 places) + new tests: prefix helper (8), content-type (4), nmi_prefix API (4), e2e via nem_adapter (3) |
| `tests/unit/optima_exporter/interval_exporter/test_processor.py` | Modify | Re-baseline existing date expectations to DAYS_BACK=1; add ProductionDefaults / partial-date / range-validation / nmi_prefix-passthrough tests |
| `tests/unit/optima_exporter/interval_exporter/test_prefix_scoping.py` | Create | Guard: `_prefix_nmi_in_nem12` referenced only inside `optima_exporter/` |
| `tests/unit/fixtures/optima_bidenergy_nem12_sample.csv` | Create | Real-shaped BidEnergy NEM12 sample, single NMI, 4 channels, 3 days, quality `A` |
| `tests/unit/fixtures/optima_bidenergy_nem12_bom.csv` | Create | Same shape with UTF-8 BOM prefix |
| `tests/unit/fixtures/optima_bidenergy_nem12_crlf.csv` | Create | Same shape with `\r\n` line endings |

---

## Working agreements

- All commits go on the current branch.
- Run from repo root: `/Users/zeyu/Desktop/GEG/sbm/sbm-ingester`.
- Use `uv run` for every Python command.
- Pre-commit hook runs ruff + format + trailing-whitespace; pre-push runs pytest with ≥ 90 % coverage gate.
- **Every commit must leave the suite GREEN.** No "intermediate red" commits — if a change requires both a test update and a source update, they go in the same commit.
- Conventional Commit messages (`feat:` / `fix:` / `refactor:` / `test:` / `chore:`); no `Co-Authored-By`; no scope in parentheses.

---

## Task 1: Add realistic NEM12 fixtures (3 variants)

**Files:**
- Create: `tests/unit/fixtures/optima_bidenergy_nem12_sample.csv`
- Create: `tests/unit/fixtures/optima_bidenergy_nem12_bom.csv`
- Create: `tests/unit/fixtures/optima_bidenergy_nem12_crlf.csv`

These fixtures mirror real BidEnergy NEM12 responses and are reused throughout the test suite. The sample file contains 1 NMI × 4 channels (B1/E1/K1/Q1) × 1 day × 288 intervals (5-min) — a faithful miniature of what production responses look like.

- [ ] **Step 1: Generate the canonical sample fixture programmatically**

Run this script to write `tests/unit/fixtures/optima_bidenergy_nem12_sample.csv`:

```bash
uv run python -c "
import pathlib

p = pathlib.Path('tests/unit/fixtures/optima_bidenergy_nem12_sample.csv')
p.parent.mkdir(parents=True, exist_ok=True)

intervals_b1 = ','.join(f'{i*0.01:.4f}' for i in range(288))
intervals_e1 = ','.join(f'{i*0.005:.4f}' for i in range(288))
intervals_k1 = ','.join('0.0100' for _ in range(288))
intervals_q1 = ','.join('0.0200' for _ in range(288))

lines = [
    '100,NEM12,202604120100,MDP1,Origin',
    '200,4001348123,B1E1K1Q1,B1,B1,B1,250920091,Kwh,5',
    f'300,20260410,{intervals_b1},A,,,20260411011219,',
    '200,4001348123,B1E1K1Q1,E1,E1,E1,250920091,Kwh,5',
    f'300,20260410,{intervals_e1},A,,,20260411011219,',
    '200,4001348123,B1E1K1Q1,K1,K1,K1,250920091,Kvarh,5',
    f'300,20260410,{intervals_k1},A,,,20260411011219,',
    '200,4001348123,B1E1K1Q1,Q1,Q1,Q1,250920091,Kvarh,5',
    f'300,20260410,{intervals_q1},A,,,20260411011219,',
    '900',
]
p.write_bytes(('\n'.join(lines) + '\n').encode())
print(f'wrote {p}, {p.stat().st_size} bytes')
"
```

- [ ] **Step 2: Generate the BOM-prefixed variant**

```bash
uv run python -c "
import pathlib
src = pathlib.Path('tests/unit/fixtures/optima_bidenergy_nem12_sample.csv').read_bytes()
dst = pathlib.Path('tests/unit/fixtures/optima_bidenergy_nem12_bom.csv')
dst.write_bytes(b'\xef\xbb\xbf' + src)
print(f'wrote {dst}, {dst.stat().st_size} bytes')
"
```

- [ ] **Step 3: Generate the CRLF variant**

```bash
uv run python -c "
import pathlib
src = pathlib.Path('tests/unit/fixtures/optima_bidenergy_nem12_sample.csv').read_bytes()
dst = pathlib.Path('tests/unit/fixtures/optima_bidenergy_nem12_crlf.csv')
dst.write_bytes(src.replace(b'\n', b'\r\n'))
print(f'wrote {dst}, {dst.stat().st_size} bytes')
"
```

- [ ] **Step 4: Verify all three fixtures parse cleanly via `nem_adapter`**

```bash
uv run python -c "
import sys
sys.path.insert(0, 'src')
from shared.nem_adapter import output_as_data_frames

for path in [
    'tests/unit/fixtures/optima_bidenergy_nem12_sample.csv',
    'tests/unit/fixtures/optima_bidenergy_nem12_bom.csv',
    'tests/unit/fixtures/optima_bidenergy_nem12_crlf.csv',
]:
    frames = output_as_data_frames(path)
    assert len(frames) == 1, f'{path}: expected 1 NMI, got {len(frames)}'
    nmi, df = frames[0]
    assert nmi == '4001348123', f'{path}: expected bare NMI, got {nmi}'
    assert {'B1_Kwh', 'E1_Kwh', 'K1_Kvarh', 'Q1_Kvarh'}.issubset(df.columns), f'{path}: missing channels {df.columns}'
    assert len(df) == 288, f'{path}: expected 288 intervals, got {len(df)}'
    print(f'OK {path}: {len(df)} rows, channels {sorted(c for c in df.columns if c[0] in \"BEK Q\")}')
"
```

Expected: three `OK` lines confirming all fixtures parse to the same shape.

- [ ] **Step 5: Run the full test suite to confirm fixtures don't break anything**

```bash
uv run pytest -q 2>&1 | tail -5
```

Expected: same green count as before this task; no new failures.

- [ ] **Step 6: Commit**

```bash
git add tests/unit/fixtures/optima_bidenergy_nem12_sample.csv tests/unit/fixtures/optima_bidenergy_nem12_bom.csv tests/unit/fixtures/optima_bidenergy_nem12_crlf.csv
git commit -m "test: add realistic BidEnergy NEM12 fixtures (plain, BOM, CRLF)"
```

---

## Task 2: Add `_prefix_nmi_in_nem12` helper with comprehensive tests (TDD)

**Files:**
- Modify: `src/functions/optima_exporter/interval_exporter/downloader.py`
- Modify: `tests/unit/optima_exporter/interval_exporter/test_downloader.py`

The helper is added in isolation (not yet wired into `download_csv`). Tests cover real-shaped fixtures, edge cases (BOM, CRLF, multi-channel), idempotence, and rejection of non-NEM12 input. The injection-attack defensive test (`300` row whose interval data contains literal `200,` bytes) is included to lock down the regex anchor.

- [ ] **Step 1: Append the comprehensive test class to `test_downloader.py`**

Append to `tests/unit/optima_exporter/interval_exporter/test_downloader.py`:

```python
class TestPrefixNmiInNem12:
    """Comprehensive tests for the byte-level NMI prefix rewriter."""

    def test_prefixes_single_200_record(self) -> None:
        from interval_exporter.downloader import _prefix_nmi_in_nem12

        content = (
            b"100,NEM12,202604120100,MDP1,Origin\n"
            b"200,4001348123,B1E1K1Q1,E1,E1,E1,250920091,Kwh,5\n"
            b"300,20260410,1.0,A,,,20260411011219,\n"
            b"900\n"
        )
        out = _prefix_nmi_in_nem12(content, prefix="Optima_")
        assert b"200,Optima_4001348123,B1E1K1Q1,E1,E1,E1," in out
        assert b"200,4001348123," not in out

    def test_prefixes_all_four_channels_consistently(self) -> None:
        """Real BidEnergy responses have one 200 record per channel; all must be rewritten."""
        from pathlib import Path
        from interval_exporter.downloader import _prefix_nmi_in_nem12

        content = Path("tests/unit/fixtures/optima_bidenergy_nem12_sample.csv").read_bytes()
        out = _prefix_nmi_in_nem12(content, prefix="Optima_")

        # 4 channels × 1 NMI = 4 prefixed records
        assert out.count(b"200,Optima_4001348123,") == 4
        assert b"200,4001348123," not in out

        # Channel suffixes preserved
        for ch in (b"B1", b"E1", b"K1", b"Q1"):
            assert b"200,Optima_4001348123,B1E1K1Q1," + ch + b"," in out

    def test_handles_crlf_line_endings(self) -> None:
        """BidEnergy is ASP.NET; CRLF responses must rewrite identically."""
        from pathlib import Path
        from interval_exporter.downloader import _prefix_nmi_in_nem12

        content = Path("tests/unit/fixtures/optima_bidenergy_nem12_crlf.csv").read_bytes()
        out = _prefix_nmi_in_nem12(content, prefix="Optima_")

        assert out.count(b"200,Optima_4001348123,") == 4
        # CRLF preserved (we never touched line endings)
        assert b"\r\n" in out
        assert b"\r\n200,Optima_4001348123," in out

    def test_handles_bom_prefixed_response(self) -> None:
        """ASP.NET may emit UTF-8 BOM; helper must accept it."""
        from pathlib import Path
        from interval_exporter.downloader import _prefix_nmi_in_nem12

        content = Path("tests/unit/fixtures/optima_bidenergy_nem12_bom.csv").read_bytes()
        out = _prefix_nmi_in_nem12(content, prefix="Optima_")

        assert out.startswith(b"\xef\xbb\xbf100,")  # BOM preserved at file head
        assert out.count(b"200,Optima_4001348123,") == 4

    def test_does_not_touch_300_records_with_numeric_dates(self) -> None:
        from interval_exporter.downloader import _prefix_nmi_in_nem12

        content = (
            b"100,NEM12,202604120100,MDP1,Origin\n"
            b"200,4001348123,B1E1K1Q1,E1,E1,E1,250920091,Kwh,5\n"
            b"300,20260410,1.0,A,,,20260411011219,\n"
            b"300,20260411,2.0,A,,,20260411011219,\n"
            b"900\n"
        )
        out = _prefix_nmi_in_nem12(content, prefix="Optima_")
        # 300 rows untouched (dates not prefixed)
        assert b"300,20260410,1.0," in out
        assert b"300,20260411,2.0," in out
        assert b"Optima_20260410" not in out

    def test_anchor_resists_embedded_200_bytes_in_data(self) -> None:
        """Defensive: a 300 row whose interval payload happens to contain the bytes '200,' is not rewritten."""
        from interval_exporter.downloader import _prefix_nmi_in_nem12

        # Construct a 300 row whose interval data contains '200,' as a value substring (impossible in practice
        # since values are decimal numbers, but tests the line-anchor strictness)
        content = (
            b"100,NEM12,202604120100,MDP1,Origin\n"
            b"200,4001348123,B1E1K1Q1,E1,E1,E1,250920091,Kwh,5\n"
            b"300,20260410,200,300,A,,,20260411011219,\n"  # '200,' appears mid-line
            b"900\n"
        )
        out = _prefix_nmi_in_nem12(content, prefix="Optima_")
        # Only the real 200 record rewritten
        assert out.count(b"200,Optima_") == 1
        # The mid-line '200,' inside the 300 row is untouched
        assert b"300,20260410,200,300," in out

    def test_idempotent_on_already_prefixed(self) -> None:
        from interval_exporter.downloader import _prefix_nmi_in_nem12

        content = (
            b"100,NEM12,202604120100,MDP1,Origin\n"
            b"200,Optima_4001348123,B1E1K1Q1,E1,E1,E1,250920091,Kwh,5\n"
            b"300,20260410,1.0,A,,,20260411011219,\n"
            b"900\n"
        )
        out = _prefix_nmi_in_nem12(content, prefix="Optima_")
        assert out == content
        assert b"Optima_Optima_" not in out

    def test_raises_on_non_nem12_inputs(self) -> None:
        import pytest
        from interval_exporter.downloader import _prefix_nmi_in_nem12

        for invalid in [
            b"<!DOCTYPE html><html>session expired</html>",
            b"<html><body>error</body></html>",
            b"",
            b'{"error":"unauthorized"}',
            b"PK\x03\x04random_zip_bytes",
        ]:
            with pytest.raises(ValueError, match="missing 100 header"):
                _prefix_nmi_in_nem12(invalid, prefix="Optima_")

    def test_uses_supplied_prefix_value(self) -> None:
        """Prefix string is parameterised — confirm it's not hard-coded to 'Optima_'."""
        from interval_exporter.downloader import _prefix_nmi_in_nem12

        content = (
            b"100,NEM12,202604120100,MDP1,Origin\n"
            b"200,4001348123,B1E1K1Q1,E1,E1,E1,250920091,Kwh,5\n"
            b"900\n"
        )
        out = _prefix_nmi_in_nem12(content, prefix="TestNS_")
        assert b"200,TestNS_4001348123," in out
        assert b"200,Optima_" not in out
```

- [ ] **Step 2: Run the new tests, verify all 9 fail**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_downloader.py::TestPrefixNmiInNem12 -v
```

Expected: 9 ERRORS — `ImportError: cannot import name '_prefix_nmi_in_nem12'`.

- [ ] **Step 3: Implement the helper in `downloader.py`**

In `src/functions/optima_exporter/interval_exporter/downloader.py`, add at module level (after the existing `import` block, before the `format_date_for_url` function):

```python
import re
from typing import Final

# UTF-8 BOM + ASCII whitespace tolerated before the NEM12 100 header.
# ASP.NET stacks may emit BOM after a server-side encoding-config change.
_NEM12_HEADER_PREFIXES: Final[bytes] = b"\xef\xbb\xbf \t\r\n"

# Anchored at line start (re.MULTILINE) so it only matches a real 200 record,
# never numeric data that happens to start with bytes "200," inside a 300 row.
_NEM12_200_RE: Final[re.Pattern[bytes]] = re.compile(rb"^200,([^,]+),", re.MULTILINE)


def _prefix_nmi_in_nem12(content: bytes, *, prefix: str) -> bytes:
    """
    Rewrite the NMI field of every `200` record in a NEM12 file by prepending `prefix`.

    Optima data uses the `Optima_<bare-nmi>` namespace in Neptune mappings, but
    BidEnergy emits the bare NMI. Applying the prefix here keeps downstream parsers
    (nem_adapter, file_processor) oblivious to the convention — they just see a
    NEM12 file whose 200-record NMI already matches Neptune.

    Idempotent: re-running on already-prefixed content produces identical bytes.
    Tolerates a leading UTF-8 BOM and ASCII whitespace before the 100 header.
    Raises ValueError if input is not a NEM12 file.
    """
    if not content.lstrip(_NEM12_HEADER_PREFIXES).startswith(b"100,"):
        raise ValueError("Input is not a NEM12 file (missing 100 header)")

    prefix_bytes = prefix.encode("ascii")

    def _replace(match: re.Match[bytes]) -> bytes:
        nmi = match.group(1)
        if nmi.startswith(prefix_bytes):  # idempotent
            return match.group(0)
        return b"200," + prefix_bytes + nmi + b","

    return _NEM12_200_RE.sub(_replace, content)
```

- [ ] **Step 4: Run the new tests, verify all 9 pass**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_downloader.py::TestPrefixNmiInNem12 -v
```

Expected: 9 PASSED.

- [ ] **Step 5: Run the full test suite, confirm no regressions**

```bash
uv run pytest -q 2>&1 | tail -5
```

Expected: previous green count + 9 new passing tests.

- [ ] **Step 6: Commit**

```bash
git add src/functions/optima_exporter/interval_exporter/downloader.py tests/unit/optima_exporter/interval_exporter/test_downloader.py
git commit -m "feat: add _prefix_nmi_in_nem12 byte-level helper for Optima namespace"
```

---

## Task 3: Re-baseline conftest.py and existing date-aware tests to `DAYS_BACK=1`

**Files:**
- Modify: `tests/unit/optima_exporter/conftest.py`
- Modify: `tests/unit/optima_exporter/interval_exporter/test_processor.py`

The autouse fixture in `conftest.py` currently sets `OPTIMA_DAYS_BACK="7"`. Several tests in `test_processor.py` hard-code 7-day expectations against that. Before changing the source default in Task 4, we re-baseline both conftest and the existing assertions to the new 1-day reality. **Source code is unchanged in this task.** The autouse env override does the actual behaviour shift.

- [ ] **Step 1: Update `conftest.py` autouse env**

In `tests/unit/optima_exporter/conftest.py`, locate the `reset_env` fixture (around line 67-105). Inside the env-setup block change:

```python
# OLD:
os.environ["OPTIMA_DAYS_BACK"] = "7"
# NEW:
os.environ["OPTIMA_DAYS_BACK"] = "1"
```

Add a new line after `OPTIMA_DAYS_BACK`:

```python
os.environ["OPTIMA_MAX_WORKERS"] = "20"
```

- [ ] **Step 2: Update `test_processor.py::TestGetDateRange` expectations**

In `tests/unit/optima_exporter/interval_exporter/test_processor.py`, replace `test_returns_correct_date_range` (around line 21-31) with:

```python
    @freeze_time("2026-01-23 10:00:00")
    def test_returns_correct_date_range(self) -> None:
        """Default DAYS_BACK=1 returns yesterday only (single-day range)."""
        processor_module = reload_processor_module()

        start_date, end_date = processor_module.get_date_range()

        # Both dates equal yesterday (2026-01-22) — single-day window
        assert end_date == "2026-01-22"
        assert start_date == "2026-01-22"
```

(`test_respects_optima_days_back` explicitly sets `DAYS_BACK="14"` and stays unchanged. `test_end_date_is_yesterday` and `test_at_midnight` only assert `end_date`, also unchanged.)

- [ ] **Step 3: Update `TestProcessExport::test_process_with_project_only` expectations**

In `test_processor.py` around line 234-240, change:

```python
            # Verify default dates are used (2026-01-16 to 2026-01-22)
            assert result["body"]["date_range"]["start"] == "2026-01-16"
            assert result["body"]["date_range"]["end"] == "2026-01-22"
```

to:

```python
            # Default DAYS_BACK=1 → both dates equal yesterday (2026-01-22)
            assert result["body"]["date_range"]["start"] == "2026-01-22"
            assert result["body"]["date_range"]["end"] == "2026-01-22"
```

- [ ] **Step 4: Update `TestPartialDateParameters::test_process_export_with_only_end_date_uses_default_start`**

In `test_processor.py` around line 524, the existing test asserts the buggy behaviour (`start = today - DAYS_BACK`). With `DAYS_BACK=1` this is now `today - 1` = yesterday = `2026-02-03`, not `2026-01-28`. Replace the assertion (line 558):

```python
            # start_date should be OPTIMA_DAYS_BACK (7) days before today (2026-02-04)
            # today - 7 = 2026-01-28
            assert result["body"]["date_range"]["start"] == "2026-01-28"
```

with:

```python
            # PRE-FIX behaviour (Task 5 will fix this):
            # When only endDate is provided, start defaults to (today - DAYS_BACK).
            # With DAYS_BACK=1: today - 1 = 2026-02-03 (yesterday).
            # Task 5 changes this to be anchored on end_date instead of today.
            assert result["body"]["date_range"]["start"] == "2026-02-03"
```

- [ ] **Step 5: Run the full processor test suite**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_processor.py -v 2>&1 | tail -40
```

Expected: ALL pass.

- [ ] **Step 6: Run the entire test suite**

```bash
uv run pytest -q 2>&1 | tail -5
```

Expected: green.

- [ ] **Step 7: Commit**

```bash
git add tests/unit/optima_exporter/conftest.py tests/unit/optima_exporter/interval_exporter/test_processor.py
git commit -m "test: rebaseline test env to DAYS_BACK=1 and MAX_WORKERS=20"
```

---

## Task 4: Update source defaults `OPTIMA_DAYS_BACK=1`, `OPTIMA_MAX_WORKERS=20`

**Files:**
- Modify: `src/functions/optima_exporter/optima_shared/config.py`
- Modify: `tests/unit/optima_exporter/interval_exporter/test_processor.py`

After Task 3, the autouse env makes tests behave as if defaults are already `1`/`20`. Now we move the source defaults to match, plus add tests that the **source defaults themselves** (not just test-environment overrides) hold the new values.

- [ ] **Step 1: Add `TestProductionDefaults` test class**

Append to `tests/unit/optima_exporter/interval_exporter/test_processor.py`:

```python
class TestProductionDefaults:
    """Verify source-code defaults match the design (DAYS_BACK=1, MAX_WORKERS=20).

    Uses monkeypatch to remove the autouse env override and observe the raw
    `os.environ.get(...)` fallback in config.py.
    """

    def test_default_days_back_is_one(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import importlib
        monkeypatch.delenv("OPTIMA_DAYS_BACK", raising=False)
        from optima_shared import config
        importlib.reload(config)
        assert config.OPTIMA_DAYS_BACK == 1

    def test_default_max_workers_is_twenty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import importlib
        monkeypatch.delenv("OPTIMA_MAX_WORKERS", raising=False)
        from optima_shared import config
        importlib.reload(config)
        assert config.MAX_WORKERS == 20
```

Add `import pytest` at the top of `test_processor.py` if not already present.

- [ ] **Step 2: Run the new tests, verify they fail**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_processor.py::TestProductionDefaults -v
```

Expected: 2 FAIL — current source defaults are `7` and `10`.

- [ ] **Step 3: Update `config.py` defaults**

In `src/functions/optima_exporter/optima_shared/config.py`, change:

```python
# OLD:
OPTIMA_DAYS_BACK = int(os.environ.get("OPTIMA_DAYS_BACK", "7"))
MAX_WORKERS = int(os.environ.get("OPTIMA_MAX_WORKERS", "10"))
# NEW:
OPTIMA_DAYS_BACK = int(os.environ.get("OPTIMA_DAYS_BACK", "1"))
MAX_WORKERS = int(os.environ.get("OPTIMA_MAX_WORKERS", "20"))
```

- [ ] **Step 4: Run the new tests + full suite**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_processor.py::TestProductionDefaults -v
uv run pytest -q 2>&1 | tail -5
```

Expected: both new tests pass; full suite green.

- [ ] **Step 5: Commit**

```bash
git add src/functions/optima_exporter/optima_shared/config.py tests/unit/optima_exporter/interval_exporter/test_processor.py
git commit -m "feat: align source defaults to DAYS_BACK=1 and MAX_WORKERS=20"
```

---

## Task 5: Fix partial-date bug — anchor `start_date` to provided `end_date`

**Files:**
- Modify: `src/functions/optima_exporter/interval_exporter/processor.py`
- Modify: `tests/unit/optima_exporter/interval_exporter/test_processor.py`

Current behaviour (before this task): when only `endDate` is supplied, `start_date = today - DAYS_BACK` — i.e. the start floats with today regardless of the supplied end. For backfills (`endDate` far in the past), this produces `start > end` or a window that overshoots into the future.

Fix: `start_date = end_date - (DAYS_BACK - 1)`.

- [ ] **Step 1: Rewrite the regression test to capture corrected behaviour**

In `tests/unit/optima_exporter/interval_exporter/test_processor.py`, replace `test_process_export_with_only_end_date_uses_default_start` (the test we partially updated in Task 3) with a renamed, intent-clear version. Find the test (now expecting `"2026-02-03"`) and replace its body and signature with:

```python
    @mock_aws
    @freeze_time("2026-02-04 10:00:00")
    def test_process_export_with_only_end_date_anchors_start_to_end(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When only endDate is provided, start must be derived from end (not today)."""
        # Use DAYS_BACK=7 to make the anchoring observable (start = end - 6)
        monkeypatch.setenv("OPTIMA_DAYS_BACK", "7")

        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        table = dynamodb.create_table(
            TableName="sbm-optima-config",
            KeySchema=[
                {"AttributeName": "project", "KeyType": "HASH"},
                {"AttributeName": "nmi", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "project", "AttributeType": "S"},
                {"AttributeName": "nmi", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()
        table.put_item(Item={"project": "bunnings", "nmi": "NMI001", "siteIdStr": "site-guid-001"})

        processor_module = reload_processor_module()

        with (
            patch("interval_exporter.processor.login_bidenergy", return_value=".ASPXAUTH=token"),
            patch.object(processor_module, "process_site") as mock_process,
        ):
            mock_process.return_value = {"success": True, "nmi": "NMI001"}

            # Backfill scenario — endDate far in the past
            result = processor_module.process_export(project="bunnings", end_date="2024-06-15")

            assert result["statusCode"] == 200
            assert result["body"]["date_range"]["end"] == "2024-06-15"
            # start = end - (DAYS_BACK - 1) = 2024-06-15 - 6 = 2024-06-09
            assert result["body"]["date_range"]["start"] == "2024-06-09"
```

(Use `monkeypatch.setenv` so the test doesn't pollute other tests' env.)

- [ ] **Step 2: Run the test, verify it fails**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_processor.py::TestPartialDateParameters::test_process_export_with_only_end_date_anchors_start_to_end -v
```

Expected: FAIL — current code produces `start_date = today - 7 = 2026-01-28`, not `2024-06-09`.

- [ ] **Step 3: Implement the fix in `processor.py`**

In `src/functions/optima_exporter/interval_exporter/processor.py`, change the imports at the top:

```python
# OLD:
from datetime import UTC, datetime, timedelta
# NEW:
from datetime import UTC, date, datetime, timedelta
```

Then locate the date-resolution block (around line 142-152). Replace:

```python
# Determine date range
if not start_date and not end_date:
    # Neither provided, use default range
    start_date, end_date = get_date_range()
else:
    # At least one provided, fill in the missing one
    today = datetime.now(UTC).date()
    if not end_date:
        end_date = (today - timedelta(days=1)).isoformat()  # Yesterday
    if not start_date:
        start_date = (today - timedelta(days=OPTIMA_DAYS_BACK)).isoformat()
```

with:

```python
# Determine date range
if not start_date and not end_date:
    # Neither provided, use default range
    start_date, end_date = get_date_range()
else:
    # At least one provided, fill in the missing one
    today = datetime.now(UTC).date()
    if not end_date:
        end_date = (today - timedelta(days=1)).isoformat()  # Yesterday
    if not start_date:
        # Anchor start to the (now-resolved) end_date so backfills behave correctly
        end_d = date.fromisoformat(end_date)
        start_date = (end_d - timedelta(days=OPTIMA_DAYS_BACK - 1)).isoformat()
```

- [ ] **Step 4: Run the updated test + full suite**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_processor.py -v 2>&1 | tail -40
uv run pytest -q 2>&1 | tail -5
```

Expected: ALL pass.

- [ ] **Step 5: Commit**

```bash
git add src/functions/optima_exporter/interval_exporter/processor.py tests/unit/optima_exporter/interval_exporter/test_processor.py
git commit -m "fix: anchor start_date to provided end_date in partial-date input"
```

---

## Task 6: Reject invalid date ranges (`startDate > endDate`)

**Files:**
- Modify: `src/functions/optima_exporter/interval_exporter/processor.py`
- Modify: `tests/unit/optima_exporter/interval_exporter/test_processor.py`

- [ ] **Step 1: Write failing test**

Append to `tests/unit/optima_exporter/interval_exporter/test_processor.py`:

```python
class TestDateRangeValidation:
    """Validation that startDate <= endDate."""

    def test_rejects_start_after_end(self) -> None:
        from interval_exporter.processor import process_export

        result = process_export(
            project="bunnings",
            start_date="2026-04-15",
            end_date="2026-04-10",
        )

        assert result["statusCode"] == 400
        assert "startDate" in result["body"]
        assert "endDate" in result["body"]
        assert "2026-04-15" in result["body"]
        assert "2026-04-10" in result["body"]

    def test_accepts_equal_start_and_end(self) -> None:
        """Single-day range (start == end) must be accepted."""
        from unittest.mock import patch
        import boto3
        from interval_exporter import processor

        # Need DynamoDB to find sites
        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        # Use a separate test for actual DynamoDB; here we just verify validation does not reject
        with patch("interval_exporter.processor.get_sites_for_project", return_value=[
            {"nmi": "NMI001", "siteIdStr": "site-guid-001", "country": "AU"}
        ]), patch("interval_exporter.processor.login_bidenergy", return_value=".ASPXAUTH=token"), \
             patch.object(processor, "process_site", return_value={"success": True, "nmi": "NMI001"}):
            result = processor.process_export(
                project="bunnings",
                start_date="2026-04-10",
                end_date="2026-04-10",
            )
            assert result["statusCode"] == 200
```

- [ ] **Step 2: Run the new tests, verify the first fails**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_processor.py::TestDateRangeValidation -v
```

Expected: `test_rejects_start_after_end` FAILS (current code returns 200 with no validation); `test_accepts_equal_start_and_end` PASSES.

- [ ] **Step 3: Implement validation in `processor.py`**

In `src/functions/optima_exporter/interval_exporter/processor.py`, immediately AFTER the date-resolution block from Task 5 (after the `if not start_date: ...` line in `process_export`) and BEFORE the `# Login to BidEnergy` log line, add:

```python
# Reject inverted ranges
if date.fromisoformat(start_date) > date.fromisoformat(end_date):
    logger.warning(
        "Export rejected: startDate after endDate",
        extra={"project": project, "start_date": start_date, "end_date": end_date},
    )
    return {
        "statusCode": 400,
        "body": f"Invalid range: startDate ({start_date}) > endDate ({end_date})",
    }
```

- [ ] **Step 4: Run new tests + full suite**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_processor.py::TestDateRangeValidation -v
uv run pytest -q 2>&1 | tail -5
```

Expected: both pass; full suite green.

- [ ] **Step 5: Commit**

```bash
git add src/functions/optima_exporter/interval_exporter/processor.py tests/unit/optima_exporter/interval_exporter/test_processor.py
git commit -m "feat: reject invalid date ranges with statusCode 400"
```

---

## Task 7: Atomic migration — endpoint URL, timeout, content-type, `nmi_prefix` wiring, processor passthrough

**Files:**
- Modify: `src/functions/optima_exporter/interval_exporter/downloader.py`
- Modify: `src/functions/optima_exporter/interval_exporter/processor.py`
- Modify: `tests/unit/optima_exporter/interval_exporter/test_downloader.py`
- Modify: `tests/unit/optima_exporter/interval_exporter/test_processor.py`

This single task atomically:

1. Changes the endpoint URL in source.
2. Updates all 10 mocked URLs in existing tests.
3. Bumps timeout 120 → 300 in source and the corresponding logging.
4. Replaces content-type acceptance logic.
5. Adds `*` and required `nmi_prefix` keyword-only argument to `download_csv`.
6. Wires `_prefix_nmi_in_nem12` into `download_csv` success path.
7. Adds `OPTIMA_NMI_PREFIX` constant in `processor.py` and updates `process_site` to pass `country=country, nmi_prefix=OPTIMA_NMI_PREFIX`.
8. Adds new realistic tests: `application/vnd.csv` acceptance, `nmi_prefix` API contract (4 tests), end-to-end via `nem_adapter` using the real fixture, processor passthrough regression.

Atomic because the mocks/URL/signature/processor-call all reference each other; splitting would leave intermediate RED commits.

- [ ] **Step 1: Update all mocked URLs in existing test_downloader.py**

```bash
sed -i '' 's|BuyerReport/ExportActualIntervalUsageProfile|BuyerReport/ExportIntervalUsageProfileNem12|g' tests/unit/optima_exporter/interval_exporter/test_downloader.py
```

Verify:

```bash
grep -c 'ExportIntervalUsageProfileNem12' tests/unit/optima_exporter/interval_exporter/test_downloader.py
grep -c 'ExportActualIntervalUsageProfile' tests/unit/optima_exporter/interval_exporter/test_downloader.py
```

Expected: first command outputs `10`; second outputs `0`.

- [ ] **Step 2: Update `download_csv` source — URL, timeout, content-type, signature, prefix wiring**

In `src/functions/optima_exporter/interval_exporter/downloader.py`, change:

URL literal (around line 56):

```python
# OLD:
export_url = f"{BIDENERGY_BASE_URL}/BuyerReport/ExportActualIntervalUsageProfile"
# NEW:
export_url = f"{BIDENERGY_BASE_URL}/BuyerReport/ExportIntervalUsageProfileNem12"
```

Function signature (lines 27-35):

```python
# OLD:
def download_csv(
    cookies: str,
    site_id_str: str,
    start_date: str,
    end_date: str,
    project: str,
    nmi: str,
    country: str = "AU",
) -> tuple[bytes, str] | None:
# NEW:
def download_csv(
    cookies: str,
    site_id_str: str,
    start_date: str,
    end_date: str,
    project: str,
    nmi: str,
    *,
    country: str = "AU",
    nmi_prefix: str,
) -> tuple[bytes, str] | None:
```

`requests.get` call — bump timeout (around line 79-84):

```python
# OLD:
response = requests.get(
    export_url,
    params=params,
    headers={"Cookie": cookies},
    timeout=120,
)
# NEW:
response = requests.get(
    export_url,
    params=params,
    headers={"Cookie": cookies},
    timeout=300,
)
```

Content-type acceptance (around line 88-94). Replace:

```python
if response.status_code == 200:
    # Check if response is actually CSV (not an error page)
    content_type = response.headers.get("Content-Type", "")

    # Check for HTML content (may have BOM prefix)
    content_start = response.content[:100].lower()
    is_html = b"<!doctype" in content_start or b"<html" in content_start

    if "text/csv" in content_type or "application/csv" in content_type or not is_html:
        # Generate filename with NMI and timestamp for traceability and uniqueness
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        filename = f"optima_{project.lower()}_NMI#{nmi.upper()}_{start_date}_{end_date}_{timestamp}.csv"
        logger.info(
            "CSV download successful",
            extra={
                "project": project,
                "nmi": nmi,
                "csv_filename": filename,
                "size_bytes": len(response.content),
            },
        )
        return response.content, filename
    logger.error(
        "CSV download failed: received HTML error page instead of CSV",
        extra={
            "project": project,
            "nmi": nmi,
            "site_id": site_id_str,
            "content_type": content_type,
            "response_preview": response.text[:500] if response.text else "empty",
        },
    )
```

with:

```python
if response.status_code == 200:
    content_type = response.headers.get("Content-Type", "").lower()

    # HTML detection (responses may have BOM prefix)
    content_start = response.content[:100].lower()
    is_html = b"<!doctype" in content_start or b"<html" in content_start

    # Accept anything whose content-type contains "csv" (text/csv, application/csv,
    # application/vnd.csv...) or whose body sniff begins with NEM12 header bytes.
    body_starts_like_nem12 = response.content.lstrip(b"\xef\xbb\xbf \t\r\n").startswith(b"100,")
    if "csv" in content_type or (not is_html and body_starts_like_nem12):
        # Apply Optima namespace prefix to 200 records if requested
        if nmi_prefix:
            try:
                body = _prefix_nmi_in_nem12(response.content, prefix=nmi_prefix)
            except ValueError as exc:
                logger.error(
                    "NEM12 prefix rewrite failed",
                    extra={
                        "project": project,
                        "nmi": nmi,
                        "site_id": site_id_str,
                        "error": str(exc),
                        "response_preview": response.content[:500].decode("utf-8", errors="replace"),
                    },
                )
                return None
        else:
            body = response.content

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        filename = f"optima_{project.lower()}_NMI#{nmi.upper()}_{start_date}_{end_date}_{timestamp}.csv"
        logger.info(
            "CSV download successful",
            extra={
                "project": project,
                "nmi": nmi,
                "csv_filename": filename,
                "size_bytes": len(body),
                "rewrote_nmi_prefix": bool(nmi_prefix),
            },
        )
        return body, filename
    logger.error(
        "CSV download failed: received HTML error page instead of CSV",
        extra={
            "project": project,
            "nmi": nmi,
            "site_id": site_id_str,
            "content_type": content_type,
            "response_preview": response.text[:500] if response.text else "empty",
        },
    )
```

Update the `requests.Timeout` log block (search for `"timeout_seconds": 120`) — change to `"timeout_seconds": 300`.

- [ ] **Step 3: Add `OPTIMA_NMI_PREFIX` constant + update `process_site` call in `processor.py`**

In `src/functions/optima_exporter/interval_exporter/processor.py`, after the existing imports (around line 14), add:

```python
# Optima sites live under the "Optima_" namespace in Neptune mappings.
# Applied to NMI fields in BidEnergy NEM12 responses by download_csv.
OPTIMA_NMI_PREFIX = "Optima_"
```

Locate the `download_csv(...)` call inside `process_site` (around line 71). Replace:

```python
download_result = download_csv(cookies, site_id_str, start_date, end_date, project, nmi, country)
```

with:

```python
download_result = download_csv(
    cookies,
    site_id_str,
    start_date,
    end_date,
    project,
    nmi,
    country=country,
    nmi_prefix=OPTIMA_NMI_PREFIX,
)
```

- [ ] **Step 4: Add new tests in `test_downloader.py`**

Append to `tests/unit/optima_exporter/interval_exporter/test_downloader.py`:

```python
class TestDownloadCsvNmiPrefix:
    """Tests for the required nmi_prefix keyword-only argument and rewrite wiring."""

    def test_nmi_prefix_is_required(self) -> None:
        """Omitting nmi_prefix must raise TypeError."""
        import pytest
        from interval_exporter.downloader import download_csv

        with pytest.raises(TypeError, match="nmi_prefix"):
            download_csv(
                cookies=".ASPXAUTH=token",
                site_id_str="site-guid-001",
                start_date="2026-04-10",
                end_date="2026-04-10",
                project="bunnings",
                nmi="Optima_4001348123",
            )

    def test_country_is_keyword_only(self) -> None:
        """country must be passed by keyword (not positional)."""
        import pytest
        from interval_exporter.downloader import download_csv

        with pytest.raises(TypeError):
            # 7th positional arg should be rejected (country is now keyword-only)
            download_csv(
                ".ASPXAUTH=token",
                "site-guid-001",
                "2026-04-10",
                "2026-04-10",
                "bunnings",
                "Optima_4001348123",
                "AU",  # positional country — should fail
            )

    @responses.activate
    def test_empty_nmi_prefix_does_not_rewrite(self) -> None:
        from interval_exporter.downloader import download_csv

        body = (
            b"100,NEM12,202604120100,MDP1,Origin\n"
            b"200,4001348123,B1E1K1Q1,E1,E1,E1,250920091,Kwh,5\n"
            b"300,20260410,1.0,A,,,20260411011219,\n"
            b"900\n"
        )
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
            status=200,
            body=body,
            content_type="application/vnd.csv",
        )

        result = download_csv(
            cookies=".ASPXAUTH=token",
            site_id_str="site-guid-001",
            start_date="2026-04-10",
            end_date="2026-04-10",
            project="bunnings",
            nmi="Optima_4001348123",
            nmi_prefix="",
        )
        assert result is not None
        content, _ = result
        assert content == body  # untouched

    @responses.activate
    def test_optima_nmi_prefix_rewrites_all_200_records(self) -> None:
        from pathlib import Path
        from interval_exporter.downloader import download_csv

        sample = Path("tests/unit/fixtures/optima_bidenergy_nem12_sample.csv").read_bytes()
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
            status=200,
            body=sample,
            content_type="application/vnd.csv",
        )

        result = download_csv(
            cookies=".ASPXAUTH=token",
            site_id_str="site-guid-001",
            start_date="2026-04-10",
            end_date="2026-04-10",
            project="bunnings",
            nmi="Optima_4001348123",
            nmi_prefix="Optima_",
        )
        assert result is not None
        content, _ = result
        # 4 channels × 1 NMI = 4 prefixed records
        assert content.count(b"200,Optima_4001348123,") == 4
        assert b"200,4001348123," not in content


class TestDownloadCsvContentTypes:
    """Verify content-type variants BidEnergy may emit are all accepted."""

    @responses.activate
    def test_accepts_application_vnd_csv(self) -> None:
        from pathlib import Path
        from interval_exporter.downloader import download_csv

        sample = Path("tests/unit/fixtures/optima_bidenergy_nem12_sample.csv").read_bytes()
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
            status=200,
            body=sample,
            content_type="application/vnd.csv",
        )
        result = download_csv(
            cookies=".ASPXAUTH=token",
            site_id_str="site-guid-001",
            start_date="2026-04-10",
            end_date="2026-04-10",
            project="bunnings",
            nmi="Optima_4001348123",
            nmi_prefix="Optima_",
        )
        assert result is not None

    @responses.activate
    def test_accepts_nem12_with_text_html_content_type_via_body_sniff(self) -> None:
        """If BidEnergy mis-labels the response, the body sniff (starts with 100,) saves us."""
        from interval_exporter.downloader import download_csv

        body = b"100,NEM12,202604120100,MDP1,Origin\n200,4001348123,B1E1K1Q1,E1,E1,E1,250920091,Kwh,5\n900\n"
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
            status=200,
            body=body,
            content_type="text/html",  # mis-labelled
        )
        result = download_csv(
            cookies=".ASPXAUTH=token",
            site_id_str="site-guid-001",
            start_date="2026-04-10",
            end_date="2026-04-10",
            project="bunnings",
            nmi="Optima_4001348123",
            nmi_prefix="Optima_",
        )
        assert result is not None

    @responses.activate
    def test_rejects_real_html_error_page(self) -> None:
        from interval_exporter.downloader import download_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
            status=200,
            body=b"<!DOCTYPE html><html><body>Session expired</body></html>",
            content_type="text/html",
        )
        result = download_csv(
            cookies=".ASPXAUTH=token",
            site_id_str="site-guid-001",
            start_date="2026-04-10",
            end_date="2026-04-10",
            project="bunnings",
            nmi="Optima_4001348123",
            nmi_prefix="Optima_",
        )
        assert result is None

    @responses.activate
    def test_rejects_html_with_bom(self) -> None:
        """HTML error page that happens to have a UTF-8 BOM still rejected."""
        from interval_exporter.downloader import download_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
            status=200,
            body=b"\xef\xbb\xbf<!DOCTYPE html><html>error</html>",
            content_type="text/html",
        )
        result = download_csv(
            cookies=".ASPXAUTH=token",
            site_id_str="site-guid-001",
            start_date="2026-04-10",
            end_date="2026-04-10",
            project="bunnings",
            nmi="Optima_4001348123",
            nmi_prefix="Optima_",
        )
        assert result is None


class TestDownloadCsvEndToEnd:
    """End-to-end: downloaded NEM12 must parse cleanly via nem_adapter and yield Optima-prefixed NMI."""

    @responses.activate
    def test_real_fixture_yields_optima_prefixed_nmi(self) -> None:
        import sys
        import tempfile
        from pathlib import Path

        sys.path.insert(0, "src")
        from shared.nem_adapter import output_as_data_frames
        from interval_exporter.downloader import download_csv

        sample = Path("tests/unit/fixtures/optima_bidenergy_nem12_sample.csv").read_bytes()
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
            status=200,
            body=sample,
            content_type="application/vnd.csv",
        )

        result = download_csv(
            cookies=".ASPXAUTH=token",
            site_id_str="site-guid-001",
            start_date="2026-04-10",
            end_date="2026-04-10",
            project="bunnings",
            nmi="Optima_4001348123",
            nmi_prefix="Optima_",
        )
        assert result is not None
        content, _ = result

        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
                tmp.write(content)
                tmp_path = tmp.name
            frames = output_as_data_frames(tmp_path)
            assert len(frames) == 1
            nmi, df = frames[0]
            assert nmi == "Optima_4001348123"
            # 4 channels preserved
            assert {"B1_Kwh", "E1_Kwh", "K1_Kvarh", "Q1_Kvarh"}.issubset(df.columns)
            assert len(df) == 288
        finally:
            if tmp_path:
                import os
                os.unlink(tmp_path)

    @responses.activate
    def test_quality_flag_preserved_through_pipeline(self) -> None:
        import sys
        import tempfile
        from pathlib import Path

        sys.path.insert(0, "src")
        from shared.nem_adapter import output_as_data_frames
        from interval_exporter.downloader import download_csv

        sample = Path("tests/unit/fixtures/optima_bidenergy_nem12_sample.csv").read_bytes()
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
            status=200,
            body=sample,
            content_type="application/vnd.csv",
        )

        result = download_csv(
            cookies=".ASPXAUTH=token",
            site_id_str="site-guid-001",
            start_date="2026-04-10",
            end_date="2026-04-10",
            project="bunnings",
            nmi="Optima_4001348123",
            nmi_prefix="Optima_",
        )
        assert result is not None
        content, _ = result

        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
                tmp.write(content)
                tmp_path = tmp.name
            frames = output_as_data_frames(tmp_path)
            _, df = frames[0]
            # Quality column should exist for at least one channel and have the "A" value
            quality_cols = [c for c in df.columns if c.startswith("quality_")]
            assert quality_cols, f"no quality columns: {list(df.columns)}"
            for qc in quality_cols:
                non_null = df[qc].dropna().unique().tolist()
                assert "A" in non_null, f"{qc} did not contain 'A': {non_null}"
        finally:
            if tmp_path:
                import os
                os.unlink(tmp_path)

    @responses.activate
    def test_300_response_rejected(self) -> None:
        """Non-200 status codes must return None."""
        from interval_exporter.downloader import download_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
            status=500,
            body=b"Internal Server Error",
        )
        result = download_csv(
            cookies=".ASPXAUTH=token",
            site_id_str="site-guid-001",
            start_date="2026-04-10",
            end_date="2026-04-10",
            project="bunnings",
            nmi="Optima_4001348123",
            nmi_prefix="Optima_",
        )
        assert result is None
```

- [ ] **Step 5: Add processor passthrough regression test in `test_processor.py`**

Append to `tests/unit/optima_exporter/interval_exporter/test_processor.py`:

```python
class TestProcessSitePassesNmiPrefix:
    """Regression: process_site must pass nmi_prefix='Optima_' (and country) to download_csv."""

    @mock_aws
    def test_process_site_passes_optima_prefix_and_country(self) -> None:
        from unittest.mock import patch
        import boto3
        from interval_exporter import processor

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        with patch.object(processor, "download_csv") as mock_dl, \
             patch.object(processor, "upload_to_s3", return_value=True):
            mock_dl.return_value = (b"100,NEM12,...", "fakefile.csv")
            processor.process_site(
                cookies=".ASPXAUTH=token",
                nmi="Optima_4001348123",
                site_id_str="site-guid-001",
                start_date="2026-04-10",
                end_date="2026-04-12",
                project="bunnings",
                country="NZ",
            )

        assert mock_dl.call_count == 1
        kwargs = mock_dl.call_args.kwargs
        assert kwargs.get("nmi_prefix") == "Optima_"
        assert kwargs.get("country") == "NZ"

    def test_optima_nmi_prefix_constant_value(self) -> None:
        """Constant must be 'Optima_' to match Neptune mapping convention."""
        from interval_exporter.processor import OPTIMA_NMI_PREFIX
        assert OPTIMA_NMI_PREFIX == "Optima_"
```

- [ ] **Step 6: Run the full test suite**

```bash
uv run pytest -q 2>&1 | tail -10
```

Expected: ALL pass. (Existing test_downloader tests with the SED'd URL still pass — they don't pass `nmi_prefix` because they're calling the function without it, and would fail TypeError. Wait — verify:)

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_downloader.py -v 2>&1 | tail -40
```

If existing tests like `test_successful_download_returns_content` etc. fail with TypeError because they don't pass `nmi_prefix`, **add `nmi_prefix=""` to each existing call site** in `test_downloader.py` to keep them working without altering their original semantics. Search for each `download_csv(` call in the existing tests and append `, nmi_prefix=""` to each.

```bash
uv run pytest -q 2>&1 | tail -5
```

Expected after fix: ALL pass.

- [ ] **Step 7: Commit**

```bash
git add src/functions/optima_exporter/interval_exporter/downloader.py src/functions/optima_exporter/interval_exporter/processor.py tests/unit/optima_exporter/interval_exporter/test_downloader.py tests/unit/optima_exporter/interval_exporter/test_processor.py
git commit -m "feat: switch to NEM12 endpoint with Optima_ namespace prefix rewrite"
```

---

## Task 8: Update Terraform env vars

**Files:**
- Modify: `terraform/optima_exporter.tf`

- [ ] **Step 1: Update env-var values for the interval exporter Lambda**

In `terraform/optima_exporter.tf`, find the `aws_lambda_function "optima_interval_exporter"` block (around line 84). Inside its `environment.variables` map, change:

```hcl
# OLD:
OPTIMA_DAYS_BACK   = "7"
OPTIMA_MAX_WORKERS = "10"
# NEW:
OPTIMA_DAYS_BACK   = "1"
OPTIMA_MAX_WORKERS = "20"
```

- [ ] **Step 2: Format and validate**

```bash
cd terraform && terraform fmt && terraform validate && cd ..
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 3: (Optional, requires AWS creds) Show plan diff**

```bash
cd terraform && terraform plan -target=aws_lambda_function.optima_interval_exporter 2>&1 | tail -20 && cd ..
```

Expected: only the two env var values change. If you don't have AWS creds locally, skip — CI applies on merge.

- [ ] **Step 4: Commit**

```bash
git add terraform/optima_exporter.tf
git commit -m "chore: align Terraform env vars with new source defaults"
```

---

## Task 9: Scoping guard test — `_prefix_nmi_in_nem12` must not leak

**Files:**
- Create: `tests/unit/optima_exporter/interval_exporter/test_prefix_scoping.py`

This test guards the namespace-isolation invariant: the prefix helper should only ever be referenced from within `optima_exporter/`. Any future import from elsewhere fails the test, alerting the reviewer that the rewrite is leaking outside its intended scope.

- [ ] **Step 1: Create the guard test**

Create `tests/unit/optima_exporter/interval_exporter/test_prefix_scoping.py`:

```python
"""Scoping guard: _prefix_nmi_in_nem12 must remain internal to optima_exporter.

The Optima_-prefix rewrite is a namespace convention specific to the Optima
pipeline. If this symbol ever gets imported elsewhere in src/, downstream files
that should keep their bare NMIs (e.g. AEMO MDFF pushes, building sensors)
could end up with the prefix.
"""

from pathlib import Path


def _repo_root() -> Path:
    """Locate the repo root by walking up until pyproject.toml is found."""
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "pyproject.toml").exists():
            return parent
    raise RuntimeError("Could not locate repo root (no pyproject.toml found upwards)")


REPO_ROOT = _repo_root()
SRC_ROOT = REPO_ROOT / "src"
ALLOWED_DIR = SRC_ROOT / "functions" / "optima_exporter"
SYMBOL = "_prefix_nmi_in_nem12"


def test_prefix_helper_is_only_referenced_inside_optima_exporter() -> None:
    offenders: list[str] = []

    for path in SRC_ROOT.rglob("*.py"):
        try:
            relative = path.resolve().relative_to(ALLOWED_DIR)
        except ValueError:
            relative = None
        if relative is not None:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        if SYMBOL in text:
            offenders.append(str(path.relative_to(REPO_ROOT)))

    assert not offenders, (
        f"{SYMBOL} leaked outside src/functions/optima_exporter/. "
        f"Found in: {offenders}"
    )


def test_prefix_helper_exists_inside_optima_exporter() -> None:
    """Sanity check: if this fails, the previous test gives a false negative."""
    matches: list[str] = []
    for path in ALLOWED_DIR.rglob("*.py"):
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        if f"def {SYMBOL}" in text:
            matches.append(str(path.relative_to(REPO_ROOT)))
    assert matches, f"{SYMBOL} definition not found anywhere in {ALLOWED_DIR}"
```

(Two tests: the negative guard plus a positive sanity that a future rename of the helper would also flag.)

- [ ] **Step 2: Run the test, verify it passes**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_prefix_scoping.py -v
```

Expected: 2 PASSED.

- [ ] **Step 3: Commit**

```bash
git add tests/unit/optima_exporter/interval_exporter/test_prefix_scoping.py
git commit -m "test: add scoping guard for _prefix_nmi_in_nem12"
```

---

## Task 10: Final regression sweep + lint + smoke test

**Files:** none (verification only)

- [ ] **Step 1: Run the full test suite with coverage**

```bash
uv run pytest --cov=src --cov-report=term-missing 2>&1 | tail -40
```

Expected: every test passes; coverage on `src/functions/optima_exporter/interval_exporter/downloader.py` and `processor.py` ≥ 90 %; overall coverage ≥ 90 %.

- [ ] **Step 2: Lint and format**

```bash
uv run ruff check .
uv run ruff format --check .
```

Expected: no errors. If `ruff format --check` reports differences, run `uv run ruff format .` and `git commit -m "chore: format"`.

- [ ] **Step 3: Smoke test against the real fixture (no mocks)**

```bash
uv run python -c "
import sys, tempfile, os
sys.path.insert(0, 'src')
sys.path.insert(0, 'src/functions/optima_exporter')
from interval_exporter.downloader import _prefix_nmi_in_nem12
from shared.nem_adapter import output_as_data_frames

raw = open('tests/unit/fixtures/optima_bidenergy_nem12_sample.csv', 'rb').read()
prefixed = _prefix_nmi_in_nem12(raw, prefix='Optima_')
assert prefixed.count(b'200,Optima_4001348123,') == 4, 'expected 4 prefixed records'
assert b'200,4001348123,' not in prefixed, 'bare NMI leaked'

with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
    tmp.write(prefixed)
    tmp_path = tmp.name
try:
    frames = output_as_data_frames(tmp_path)
    assert len(frames) == 1
    nmi, df = frames[0]
    assert nmi == 'Optima_4001348123', f'got {nmi}'
    assert len(df) == 288, f'got {len(df)}'
    print(f'Smoke test OK: NMI={nmi}, rows={len(df)}, channels={[c for c in df.columns if c.startswith((\"B\",\"E\",\"K\",\"Q\")) and \"_\" in c]}')
finally:
    os.unlink(tmp_path)
"
```

Expected: `Smoke test OK: NMI=Optima_4001348123, rows=288, channels=[...]`.

- [ ] **Step 4: Verify no leftover references to old endpoint anywhere in repo**

```bash
grep -rn "ExportActualIntervalUsageProfile" src tests terraform 2>&1 | grep -v ".pyc" | head -10
```

Expected: no output (no occurrences).

- [ ] **Step 5: Verify `_prefix_nmi_in_nem12` only in expected files**

```bash
grep -rn "_prefix_nmi_in_nem12" src tests 2>&1 | grep -v ".pyc"
```

Expected: matches only in `src/functions/optima_exporter/interval_exporter/downloader.py` and the two test files (`test_downloader.py`, `test_prefix_scoping.py`).

- [ ] **Step 6: No commit needed** — verification only.

---

## Post-merge staging verification (per spec §7.3)

After CI deploys the merged PR, BEFORE the next 14:00 Sydney scheduled run (if running close to schedule, disable EventBridge rules first):

1. **NZ site (priority 1)** — invoke Lambda with one `Optima_<NZ-NMI>` for `2026-04-10` to `2026-04-12`. Check S3 file is NEM12 with `200,Optima_<bare>,` line present, channel suffixes look like AU equivalents (B1/E1/K1/Q1 or whatever the NZ site exposes), and downstream lands in `newP/` not `newIrrevFiles/`.

2. **AU site** — invoke with `Optima_4001348123` for the same window. Same downstream checks.

3. **Multi-NMI sanity re-check** — pick 5 sites with the largest expected meter counts; download via the new endpoint; for each file count distinct NMIs in `200` records; assert each is in DynamoDB under the project. (Pre-merge 105-site scan saw 100 % single-NMI; this confirms post-deploy.)

4. **Athena** — `SELECT COUNT(*) FROM default.sensordata_default WHERE sensorid = '<neptune-id>' AND ts BETWEEN '2026-04-10' AND '2026-04-12'`; expect ≈ `4 channels × 3 days × 288 intervals = 3 456` rows.

5. **CloudWatch** — `sbm-ingester-metrics-log` reports non-zero monitor-point count; `sbm-ingester-parse-error-log` contains no entries traceable to the new exports.

If any step fails: revert the PR, redeploy via the same workflow. Hudi upsert means re-pulling the same window after fix is safe.

---

## Self-review (against revised reviewer findings)

Mapped against the issues found in the previous plan review:

| Reviewer issue | Resolution in this revision |
|---|---|
| URL count claim "11" wrong (actual 10) | Task 7 Step 1 verification asserts `10` exactly |
| `conftest.py` autouse env stuck at `DAYS_BACK=7` | Task 3 explicitly updates conftest before any source default change |
| Task 6 expectation collides with Task 8 default change | Task 5 (now using `monkeypatch.setenv("OPTIMA_DAYS_BACK", "7")`) is independent of the global default; Task 3 has already re-baselined the global expectation tests separately |
| Task 3 leaves suite RED at commit boundary | Task 7 is atomic — endpoint + URL mocks + signature + processor wiring all land together |
| Task 4 e2e test had `delete=False` leak | Task 7 Step 4 e2e tests use try/finally + `os.unlink` |
| Task 10 sanity step `git checkout` is destructive | Removed; replaced by a positive sanity test (`test_prefix_helper_exists_inside_optima_exporter`) that catches false negatives without filesystem mutation |
| No CRLF rewrite test | Task 1 ships CRLF fixture; Task 2 `test_handles_crlf_line_endings` |
| `country` becoming keyword-only could break callers | Task 7 Step 2 explicitly documents `country=country` keyword passthrough; Task 7 Step 4 `test_country_is_keyword_only` locks the contract; Task 7 Step 6 instructions to repair existing test calls |
| Existing tests calling `download_csv` positionally without `nmi_prefix` would TypeError | Task 7 Step 6 explicitly addresses this with a search-and-add `nmi_prefix=""` step |
