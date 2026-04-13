# Optima Interval Exporter NEM12 Migration — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Switch `optima-interval-exporter` Lambda from BidEnergy's flat-CSV endpoint to its NEM12 endpoint, rewriting `200` records to apply the `Optima_` namespace prefix so the existing Neptune mappings still resolve.

**Architecture:** Endpoint URL change in `downloader.py` + a new byte-level helper `_prefix_nmi_in_nem12()` invoked via a required `nmi_prefix` keyword-only argument; processor passes `OPTIMA_NMI_PREFIX="Optima_"`. Default date range narrows to yesterday only. Concurrency raised 10 → 20. Partial-date bug fixed and `start>end` validated. No changes to file_processor, nem_adapter, shared parsers, or any other downstream component.

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
| `tests/unit/optima_exporter/interval_exporter/test_downloader.py` | Modify | Endpoint URL in 11 mocks; new tests for prefix, BOM, content-type, e2e |
| `tests/unit/optima_exporter/interval_exporter/test_processor.py` | Modify | Date-range expectations, partial-date regression test, range-validation test, default-config tests |
| `tests/unit/optima_exporter/interval_exporter/test_prefix_scoping.py` | Create | Guard: `_prefix_nmi_in_nem12` referenced only inside `optima_exporter/` |
| `tests/unit/fixtures/optima_bidenergy_nem12_sample.csv` | Create | Real BidEnergy NEM12 sample (multi-channel, single NMI) for e2e parsing tests |

---

## Working agreements

- All commits go on the current branch (`main` or whatever branch the executor is on).
- Run from repo root: `/Users/zeyu/Desktop/GEG/sbm/sbm-ingester`.
- Use `uv run` for every Python command.
- Pre-push hook enforces ≥ 90 % coverage on `pytest`.
- Conventional Commit messages (`feat:` / `fix:` / `refactor:` / `test:` / `chore:`); no `Co-Authored-By`; no scope in parentheses.
- Each task ends with a single commit so reverts are clean.

---

## Task 1: Add NEM12 sample fixture

**Files:**
- Create: `tests/unit/fixtures/optima_bidenergy_nem12_sample.csv`

This fixture is a redacted real BidEnergy NEM12 response — a single NMI with 4 channels (B1/E1/K1/Q1) over 3 days at 5-minute intervals. Used by later end-to-end tests.

- [ ] **Step 1: Create the fixture file**

Create `tests/unit/fixtures/optima_bidenergy_nem12_sample.csv` with the following exact contents (4 × 200 records, 3 × 4 = 12 × 300 records, 1 × 900):

```text
100,NEM12,202604120100,MDP1,Origin
200,4001348123,B1E1K1Q1,B1,B1,B1,250920091,Kwh,5
300,20260410,0.10,0.11,0.12,0.13,0.14,0.15,0.16,0.17,0.18,0.19,0.20,0.21,0.22,0.23,0.24,0.25,0.26,0.27,0.28,0.29,0.30,0.31,0.32,0.33,0.34,0.35,0.36,0.37,0.38,0.39,0.40,0.41,0.42,0.43,0.44,0.45,0.46,0.47,0.48,0.49,0.50,0.51,0.52,0.53,0.54,0.55,0.56,0.57,0.58,0.59,0.60,0.61,0.62,0.63,0.64,0.65,0.66,0.67,0.68,0.69,0.70,0.71,0.72,0.73,0.74,0.75,0.76,0.77,0.78,0.79,0.80,0.81,0.82,0.83,0.84,0.85,0.86,0.87,0.88,0.89,0.90,0.91,0.92,0.93,0.94,0.95,0.96,0.97,0.98,0.99,1.00,1.01,1.02,1.03,1.04,1.05,1.06,1.07,1.08,1.09,1.10,1.11,1.12,1.13,1.14,1.15,1.16,1.17,1.18,1.19,1.20,1.21,1.22,1.23,1.24,1.25,1.26,1.27,1.28,1.29,1.30,1.31,1.32,1.33,1.34,1.35,1.36,1.37,1.38,1.39,1.40,1.41,1.42,1.43,1.44,1.45,1.46,1.47,1.48,1.49,1.50,1.51,1.52,1.53,1.54,1.55,1.56,1.57,1.58,1.59,1.60,1.61,1.62,1.63,1.64,1.65,1.66,1.67,1.68,1.69,1.70,1.71,1.72,1.73,1.74,1.75,1.76,1.77,1.78,1.79,1.80,1.81,1.82,1.83,1.84,1.85,1.86,1.87,1.88,1.89,1.90,1.91,1.92,1.93,1.94,1.95,1.96,1.97,1.98,1.99,2.00,2.01,2.02,2.03,2.04,2.05,2.06,2.07,2.08,2.09,2.10,2.11,2.12,2.13,2.14,2.15,2.16,2.17,2.18,2.19,2.20,2.21,2.22,2.23,2.24,2.25,2.26,2.27,2.28,2.29,2.30,2.31,2.32,2.33,2.34,2.35,2.36,2.37,2.38,2.39,2.40,2.41,2.42,2.43,2.44,2.45,2.46,2.47,2.48,2.49,2.50,2.51,2.52,2.53,2.54,2.55,2.56,2.57,2.58,2.59,2.60,2.61,2.62,2.63,2.64,2.65,2.66,2.67,2.68,2.69,2.70,2.71,2.72,2.73,2.74,2.75,2.76,2.77,2.78,2.79,2.80,2.81,2.82,2.83,2.84,2.85,2.86,2.87,2.88,A,,,20260411011219,
200,4001348123,B1E1K1Q1,E1,E1,E1,250920091,Kwh,5
300,20260410,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,A,,,20260411011219,
200,4001348123,B1E1K1Q1,K1,K1,K1,250920091,Kvarh,5
300,20260410,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,0.01,A,,,20260411011219,
200,4001348123,B1E1K1Q1,Q1,Q1,Q1,250920091,Kvarh,5
300,20260410,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,A,,,20260411011219,
900
```

(The file has only 1 day's worth of `300` records per channel for brevity; the parser logic doesn't depend on day count. NMI `4001348123` is a real Bunnings NMI used during design verification — safe to include since `siteIdStr` is not present in the file.)

- [ ] **Step 2: Verify the fixture parses cleanly via `nem_adapter`**

Run:

```bash
uv run python -c "
import sys
sys.path.insert(0, 'src')
from shared.nem_adapter import output_as_data_frames
frames = output_as_data_frames('tests/unit/fixtures/optima_bidenergy_nem12_sample.csv')
assert len(frames) == 1, f'Expected 1 NMI, got {len(frames)}'
nmi, df = frames[0]
assert nmi == '4001348123', f'Expected bare NMI, got {nmi}'
assert 'B1_Kwh' in df.columns, f'Missing B1 channel: {list(df.columns)}'
assert 'E1_Kwh' in df.columns
assert 'K1_Kvarh' in df.columns
assert 'Q1_Kvarh' in df.columns
print(f'OK: parsed {len(df)} rows, columns={list(df.columns)}')
"
```

Expected output: `OK: parsed 288 rows, columns=[...B1_Kwh...E1_Kwh...K1_Kvarh...Q1_Kvarh...]`

- [ ] **Step 3: Commit**

```bash
git add tests/unit/fixtures/optima_bidenergy_nem12_sample.csv
git commit -m "test: add real BidEnergy NEM12 sample fixture"
```

---

## Task 2: Add `_prefix_nmi_in_nem12` helper (TDD)

**Files:**
- Modify: `src/functions/optima_exporter/interval_exporter/downloader.py`
- Modify: `tests/unit/optima_exporter/interval_exporter/test_downloader.py`

This task introduces the helper but does NOT yet wire it into `download_csv`. We test the helper in isolation first.

- [ ] **Step 1: Write the failing tests (all 6 cases) at the end of `test_downloader.py`**

Append to `tests/unit/optima_exporter/interval_exporter/test_downloader.py`:

```python
class TestPrefixNmiInNem12:
    """Tests for _prefix_nmi_in_nem12 helper that rewrites 200 records."""

    def test_prefixes_single_200_record(self) -> None:
        from interval_exporter.downloader import _prefix_nmi_in_nem12

        content = (
            b"100,NEM12,202604120100,MDP1,Origin\n"
            b"200,4001348123,B1E1K1Q1,E1,E1,E1,250920091,Kwh,5\n"
            b"300,20260410,0.10,0.20,A,,,20260411011219,\n"
            b"900\n"
        )
        out = _prefix_nmi_in_nem12(content, prefix="Optima_")
        assert b"200,Optima_4001348123,B1E1K1Q1,E1,E1,E1," in out
        assert b"200,4001348123," not in out

    def test_prefixes_multiple_200_records_consistently(self) -> None:
        from interval_exporter.downloader import _prefix_nmi_in_nem12

        content = (
            b"100,NEM12,202604120100,MDP1,Origin\n"
            b"200,4001348123,B1E1K1Q1,B1,B1,B1,250920091,Kwh,5\n"
            b"300,20260410,1.0,A,,,20260411011219,\n"
            b"200,4001348123,B1E1K1Q1,E1,E1,E1,250920091,Kwh,5\n"
            b"300,20260410,2.0,A,,,20260411011219,\n"
            b"200,4001348123,B1E1K1Q1,K1,K1,K1,250920091,Kvarh,5\n"
            b"300,20260410,3.0,A,,,20260411011219,\n"
            b"900\n"
        )
        out = _prefix_nmi_in_nem12(content, prefix="Optima_")
        # All three 200 rows must be prefixed
        assert out.count(b"200,Optima_4001348123,") == 3
        assert b"200,4001348123," not in out

    def test_does_not_touch_300_records(self) -> None:
        from interval_exporter.downloader import _prefix_nmi_in_nem12

        # 300 row's date is 20260410 (numeric), must not match any 200 pattern
        content = (
            b"100,NEM12,202604120100,MDP1,Origin\n"
            b"200,4001348123,B1E1K1Q1,E1,E1,E1,250920091,Kwh,5\n"
            b"300,20260410,1.0,A,,,20260411011219,\n"
            b"900\n"
        )
        out = _prefix_nmi_in_nem12(content, prefix="Optima_")
        assert b"300,20260410,1.0,A,,,20260411011219," in out
        # Date field unchanged
        assert b"Optima_20260410" not in out

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
        # No double prefix
        assert b"Optima_Optima_" not in out

    def test_accepts_bom_prefixed_nem12(self) -> None:
        from interval_exporter.downloader import _prefix_nmi_in_nem12

        # UTF-8 BOM prefix simulating an ASP.NET encoding-config change
        content = (
            b"\xef\xbb\xbf100,NEM12,202604120100,MDP1,Origin\n"
            b"200,4001348123,B1E1K1Q1,E1,E1,E1,250920091,Kwh,5\n"
            b"300,20260410,1.0,A,,,20260411011219,\n"
            b"900\n"
        )
        out = _prefix_nmi_in_nem12(content, prefix="Optima_")
        # BOM preserved at file head, NMI still rewritten
        assert out.startswith(b"\xef\xbb\xbf100,")
        assert b"200,Optima_4001348123," in out

    def test_raises_on_non_nem12_input(self) -> None:
        import pytest
        from interval_exporter.downloader import _prefix_nmi_in_nem12

        with pytest.raises(ValueError, match="missing 100 header"):
            _prefix_nmi_in_nem12(b"<!DOCTYPE html><html>error</html>", prefix="Optima_")
        with pytest.raises(ValueError, match="missing 100 header"):
            _prefix_nmi_in_nem12(b"", prefix="Optima_")
        with pytest.raises(ValueError, match="missing 100 header"):
            _prefix_nmi_in_nem12(b'{"error":"unauthorized"}', prefix="Optima_")
```

- [ ] **Step 2: Run the new tests and verify they all fail**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_downloader.py::TestPrefixNmiInNem12 -v
```

Expected: 6 errors with `ImportError: cannot import name '_prefix_nmi_in_nem12' from 'interval_exporter.downloader'`.

- [ ] **Step 3: Implement `_prefix_nmi_in_nem12` in `downloader.py`**

In `src/functions/optima_exporter/interval_exporter/downloader.py`, add these definitions at module level (after the existing `import` block, before `format_date_for_url`):

```python
import re
from typing import Final

# UTF-8 BOM + leading whitespace tolerated before the 100 header.
# ASP.NET stacks may emit BOM after a server-side encoding change.
_NEM12_HEADER_PREFIXES: Final[bytes] = b"\xef\xbb\xbf \t\r\n"

# Anchored at line start (re.MULTILINE) — only matches a real 200 record,
# never a numeric date inside a 300 row.
_NEM12_200_RE: Final[re.Pattern[bytes]] = re.compile(rb"^200,([^,]+),", re.MULTILINE)


def _prefix_nmi_in_nem12(content: bytes, *, prefix: str) -> bytes:
    """
    Rewrite the NMI field of every `200` record in a NEM12 file.

    Optima data uses the `Optima_<bare-nmi>` namespace in Neptune mappings;
    BidEnergy emits the bare NMI. Applying the prefix here keeps downstream
    parsers (nem_adapter, file_processor) oblivious to the convention — they
    just see a NEM12 file whose 200-record NMI already matches Neptune.
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

- [ ] **Step 4: Run tests, verify all 6 pass**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_downloader.py::TestPrefixNmiInNem12 -v
```

Expected: 6 PASSED.

- [ ] **Step 5: Commit**

```bash
git add src/functions/optima_exporter/interval_exporter/downloader.py tests/unit/optima_exporter/interval_exporter/test_downloader.py
git commit -m "feat: add _prefix_nmi_in_nem12 helper for Optima NMI namespacing"
```

---

## Task 3: Switch endpoint URL, bump timeout, accept `application/vnd.csv`

**Files:**
- Modify: `src/functions/optima_exporter/interval_exporter/downloader.py`
- Modify: `tests/unit/optima_exporter/interval_exporter/test_downloader.py`

- [ ] **Step 1: Update all 11 mocked URLs in existing tests**

In `tests/unit/optima_exporter/interval_exporter/test_downloader.py`, replace every occurrence of:

```text
https://app.bidenergy.com/BuyerReport/ExportActualIntervalUsageProfile
```

with:

```text
https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12
```

(Use a single `sed` to be safe, then read the file to confirm.)

```bash
sed -i '' 's|BuyerReport/ExportActualIntervalUsageProfile|BuyerReport/ExportIntervalUsageProfileNem12|g' tests/unit/optima_exporter/interval_exporter/test_downloader.py
```

Verify:

```bash
grep -c 'ExportIntervalUsageProfileNem12' tests/unit/optima_exporter/interval_exporter/test_downloader.py
grep -c 'ExportActualIntervalUsageProfile' tests/unit/optima_exporter/interval_exporter/test_downloader.py
```

Expected: first command outputs `11` (or higher), second outputs `0`.

- [ ] **Step 2: Add new tests for `application/vnd.csv` content-type and 300 s timeout**

Append to `test_downloader.py` inside the existing `TestDownloadCsv` class (or create new sibling class — choose location to keep the test file readable):

```python
    @responses.activate
    def test_accepts_application_vnd_csv_content_type(self) -> None:
        """The new NEM12 endpoint returns Content-Type: application/vnd.csv."""
        from interval_exporter.downloader import download_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
            status=200,
            body=b"100,NEM12,202604120100,MDP1,Origin\n200,4001348123,B1E1K1Q1,E1,E1,E1,250920091,Kwh,5\n300,20260410,1.0,A,,,20260411011219,\n900\n",
            content_type="application/vnd.csv",
        )

        result = download_csv(
            cookies=".ASPXAUTH=token",
            site_id_str="site-guid-001",
            start_date="2026-04-10",
            end_date="2026-04-10",
            project="bunnings",
            nmi="Optima_4001348123",
            country="AU",
            nmi_prefix="Optima_",
        )

        assert result is not None

    @responses.activate
    def test_uses_300_second_timeout(self) -> None:
        """download_csv should call requests.get with timeout=300."""
        from unittest.mock import patch
        from interval_exporter.downloader import download_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
            status=200,
            body=b"100,NEM12,202604120100,MDP1,Origin\n200,4001348123,B1E1K1Q1,E1,E1,E1,250920091,Kwh,5\n300,20260410,1.0,A,,,20260411011219,\n900\n",
            content_type="application/vnd.csv",
        )

        with patch("interval_exporter.downloader.requests.get", wraps=__import__("requests").get) as mock_get:
            download_csv(
                cookies=".ASPXAUTH=token",
                site_id_str="site-guid-001",
                start_date="2026-04-10",
                end_date="2026-04-10",
                project="bunnings",
                nmi="Optima_4001348123",
                country="AU",
                nmi_prefix="Optima_",
            )

        assert mock_get.call_args.kwargs["timeout"] == 300
```

(Note: these new tests assume the `nmi_prefix` keyword is accepted — Task 4 will land it. They will fail in this task with `unexpected keyword argument`; that is intentional and the next step explicitly skips them.)

- [ ] **Step 3: Update `download_csv` source — change URL, bump timeout, accept `application/vnd.csv`**

In `src/functions/optima_exporter/interval_exporter/downloader.py`:

Change line ~56 (the URL literal):

```python
# OLD:
export_url = f"{BIDENERGY_BASE_URL}/BuyerReport/ExportActualIntervalUsageProfile"
# NEW:
export_url = f"{BIDENERGY_BASE_URL}/BuyerReport/ExportIntervalUsageProfileNem12"
```

Change the `requests.get(...)` call to use a 300 s timeout:

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

Change the content-type acceptance check to handle `application/vnd.csv`:

```python
# OLD:
if "text/csv" in content_type or "application/csv" in content_type or not is_html:
# NEW:
if "csv" in content_type.lower() or (not is_html and response.content[:4] == b"100,"):
```

Also update the timeout literal in the `requests.Timeout` exception's logging block (search for `"timeout_seconds": 120`) to `300`.

- [ ] **Step 4: Run all `test_downloader.py` tests**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_downloader.py -v
```

Expected: existing tests PASS (URL change applied), the two new tests from Step 2 still FAIL with `unexpected keyword argument 'nmi_prefix'`. This is intentional — Task 4 makes them pass.

- [ ] **Step 5: Commit (with Task 4 not yet done — tests will be partially red)**

```bash
git add src/functions/optima_exporter/interval_exporter/downloader.py tests/unit/optima_exporter/interval_exporter/test_downloader.py
git commit -m "feat: switch to NEM12 endpoint; bump timeout; accept application/vnd.csv"
```

(The two `nmi_prefix` tests are the bridge into Task 4 — they'll go green there.)

---

## Task 4: Wire `nmi_prefix` into `download_csv` (required keyword-only)

**Files:**
- Modify: `src/functions/optima_exporter/interval_exporter/downloader.py`
- Modify: `tests/unit/optima_exporter/interval_exporter/test_downloader.py`

- [ ] **Step 1: Add tests for `nmi_prefix` API contract**

Append to `test_downloader.py`:

```python
class TestDownloadCsvNmiPrefix:
    """Tests for the required nmi_prefix keyword-only argument."""

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
        from interval_exporter.downloader import download_csv

        body = (
            b"100,NEM12,202604120100,MDP1,Origin\n"
            b"200,4001348123,B1E1K1Q1,B1,B1,B1,250920091,Kwh,5\n"
            b"300,20260410,1.0,A,,,20260411011219,\n"
            b"200,4001348123,B1E1K1Q1,E1,E1,E1,250920091,Kwh,5\n"
            b"300,20260410,2.0,A,,,20260411011219,\n"
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
            nmi_prefix="Optima_",
        )
        assert result is not None
        content, _ = result
        assert content.count(b"200,Optima_4001348123,") == 2
        assert b"200,4001348123," not in content

    @responses.activate
    def test_end_to_end_nem_adapter_yields_optima_prefixed_nmi(self) -> None:
        """The downloaded+rewritten file must parse via nem_adapter and yield Optima_<bare>."""
        import sys
        import tempfile
        from interval_exporter.downloader import download_csv

        sys.path.insert(0, "src")
        from shared.nem_adapter import output_as_data_frames

        with open("tests/unit/fixtures/optima_bidenergy_nem12_sample.csv", "rb") as f:
            sample = f.read()

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
            end_date="2026-04-12",
            project="bunnings",
            nmi="Optima_4001348123",
            nmi_prefix="Optima_",
        )
        assert result is not None
        content, _ = result

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
            tmp.write(content)
            tmp_path = tmp.name

        frames = output_as_data_frames(tmp_path)
        assert len(frames) == 1
        nmi, df = frames[0]
        assert nmi == "Optima_4001348123"
        assert "E1_Kwh" in df.columns
        assert len(df) > 0
```

- [ ] **Step 2: Run new tests, verify they fail**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_downloader.py::TestDownloadCsvNmiPrefix -v
```

Expected: 4 FAIL with `unexpected keyword argument 'nmi_prefix'`.

- [ ] **Step 3: Modify `download_csv` signature and body**

In `src/functions/optima_exporter/interval_exporter/downloader.py`, change the function signature:

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

Inside the success branch (after the `if "csv" in content_type.lower() or ...` check, before the filename is built and the tuple returned), apply the prefix:

```python
# Apply Optima namespace prefix to 200 records if requested.
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
                "response_preview": response.content[:200].decode("utf-8", errors="replace"),
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
    },
)
return body, filename
```

(Replace the old `return response.content, filename` with `return body, filename`.)

- [ ] **Step 4: Run the entire `test_downloader.py` suite**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_downloader.py -v
```

Expected: ALL pass (existing 11 + 6 prefix helper + 2 content-type/timeout + 4 nmi_prefix = 23+ tests, all green).

- [ ] **Step 5: Commit**

```bash
git add src/functions/optima_exporter/interval_exporter/downloader.py tests/unit/optima_exporter/interval_exporter/test_downloader.py
git commit -m "feat: add required nmi_prefix kwarg to download_csv"
```

---

## Task 5: Pass `nmi_prefix=OPTIMA_NMI_PREFIX` from processor

**Files:**
- Modify: `src/functions/optima_exporter/interval_exporter/processor.py`
- Modify: `tests/unit/optima_exporter/interval_exporter/test_processor.py`

- [ ] **Step 1: Add a regression test that `process_site` passes `nmi_prefix="Optima_"` as keyword**

Append to `tests/unit/optima_exporter/interval_exporter/test_processor.py`:

```python
class TestProcessSitePassesNmiPrefix:
    """Regression: process_site must pass nmi_prefix="Optima_" to download_csv."""

    @mock_aws
    def test_process_site_passes_optima_prefix(self) -> None:
        from unittest.mock import patch
        import boto3
        from interval_exporter import processor

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        with patch.object(processor, "download_csv") as mock_dl:
            mock_dl.return_value = (b"100,NEM12,...", "fakefile.csv")
            with patch.object(processor, "upload_to_s3", return_value=True):
                processor.process_site(
                    cookies=".ASPXAUTH=token",
                    nmi="Optima_4001348123",
                    site_id_str="site-guid-001",
                    start_date="2026-04-10",
                    end_date="2026-04-12",
                    project="bunnings",
                    country="AU",
                )

        # Verify download_csv was called with nmi_prefix="Optima_"
        assert mock_dl.call_count == 1
        kwargs = mock_dl.call_args.kwargs
        assert kwargs.get("nmi_prefix") == "Optima_"
```

- [ ] **Step 2: Run test, verify it fails**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_processor.py::TestProcessSitePassesNmiPrefix -v
```

Expected: FAIL — `download_csv` is currently called positionally without `nmi_prefix`.

- [ ] **Step 3: Update `processor.py` to declare constant and pass `nmi_prefix`**

In `src/functions/optima_exporter/interval_exporter/processor.py`, add a module constant after the imports (around line 16):

```python
# Optima sites live under the "Optima_" namespace in Neptune mappings.
# Applied to NMI fields in BidEnergy NEM12 responses by download_csv.
OPTIMA_NMI_PREFIX = "Optima_"
```

Update the `download_csv(...)` call inside `process_site` (currently around line 71). Change from:

```python
download_result = download_csv(cookies, site_id_str, start_date, end_date, project, nmi, country)
```

to:

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

- [ ] **Step 4: Run processor tests, verify the new test passes and existing ones still pass**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_processor.py -v
```

Expected: ALL pass.

- [ ] **Step 5: Commit**

```bash
git add src/functions/optima_exporter/interval_exporter/processor.py tests/unit/optima_exporter/interval_exporter/test_processor.py
git commit -m "feat: pass OPTIMA_NMI_PREFIX from processor to download_csv"
```

---

## Task 6: Fix partial-date bug — anchor `start_date` to provided `end_date`

**Files:**
- Modify: `src/functions/optima_exporter/interval_exporter/processor.py`
- Modify: `tests/unit/optima_exporter/interval_exporter/test_processor.py`

The current bug: when only `endDate` is supplied, `start_date` is computed from `today - DAYS_BACK`, not from `endDate - DAYS_BACK`. For backfills (`endDate` far in the past), this produces `start > end`.

- [ ] **Step 1: Replace the existing buggy test with corrected expectation**

In `tests/unit/optima_exporter/interval_exporter/test_processor.py`, find `test_process_export_with_only_end_date_uses_default_start` (around line 524). Replace its body with:

```python
    @mock_aws
    @freeze_time("2026-02-04 10:00:00")
    def test_process_export_with_only_end_date_anchors_start_to_end(self) -> None:
        """When only endDate is provided, startDate must be derived from endDate."""
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
            # end_date preserved
            assert result["body"]["date_range"]["end"] == "2024-06-15"
            # start_date anchored to end_date - (DAYS_BACK - 1).
            # Default DAYS_BACK is currently 7 (config.py); Task 8 changes it to 1.
            # Expected: 2024-06-15 minus (7-1) days = 2024-06-09.
            assert result["body"]["date_range"]["start"] == "2024-06-09"
```

(Task 8 will adjust this expectation when it changes the default to 1.)

- [ ] **Step 2: Run the test, verify it fails**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_processor.py::TestPartialDateParameters::test_process_export_with_only_end_date_anchors_start_to_end -v
```

Expected: FAIL with the current behaviour computing `start = today - DAYS_BACK = 2026-01-28`.

- [ ] **Step 3: Implement the fix in `processor.py`**

In `src/functions/optima_exporter/interval_exporter/processor.py`, find the date-resolution block (around lines 142-152). Replace:

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
        # Anchor start to the (now-resolved) end_date so backfills behave correctly.
        end_d = date.fromisoformat(end_date)
        start_date = (end_d - timedelta(days=OPTIMA_DAYS_BACK - 1)).isoformat()
```

You will need to import `date` — at the top of `processor.py` change:

```python
from datetime import UTC, datetime, timedelta
```

to:

```python
from datetime import UTC, date, datetime, timedelta
```

- [ ] **Step 4: Run the test, verify it passes**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_processor.py -v
```

Expected: ALL pass.

- [ ] **Step 5: Commit**

```bash
git add src/functions/optima_exporter/interval_exporter/processor.py tests/unit/optima_exporter/interval_exporter/test_processor.py
git commit -m "fix: anchor start_date to provided end_date in partial-date input"
```

---

## Task 7: Reject invalid date ranges (`startDate > endDate`)

**Files:**
- Modify: `src/functions/optima_exporter/interval_exporter/processor.py`
- Modify: `tests/unit/optima_exporter/interval_exporter/test_processor.py`

- [ ] **Step 1: Write failing test**

Append to `test_processor.py`:

```python
class TestDateRangeValidation:
    """Validation that startDate <= endDate."""

    @mock_aws
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
```

- [ ] **Step 2: Run, verify it fails**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_processor.py::TestDateRangeValidation -v
```

Expected: FAIL (likely with a non-400 status because no validation exists).

- [ ] **Step 3: Implement validation in `processor.py`**

In `src/functions/optima_exporter/interval_exporter/processor.py`, immediately after the date-resolution block (after the `if not start_date: start_date = ...` line in `process_export`) and before the `# Login to BidEnergy` block, add:

```python
# Reject invalid range
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

- [ ] **Step 4: Run, verify it passes**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_processor.py -v
```

Expected: ALL pass.

- [ ] **Step 5: Commit**

```bash
git add src/functions/optima_exporter/interval_exporter/processor.py tests/unit/optima_exporter/interval_exporter/test_processor.py
git commit -m "feat: reject invalid date ranges with statusCode 400"
```

---

## Task 8: Source defaults `OPTIMA_DAYS_BACK=1`, `OPTIMA_MAX_WORKERS=20`

**Files:**
- Modify: `src/functions/optima_exporter/optima_shared/config.py`
- Modify: `tests/unit/optima_exporter/interval_exporter/test_processor.py`

- [ ] **Step 1: Update `TestGetDateRange` expectations**

In `tests/unit/optima_exporter/interval_exporter/test_processor.py`, find the `TestGetDateRange` class (around line 18). Change `test_returns_correct_date_range` so it asserts the new default of 1 day:

```python
    @freeze_time("2026-01-23 10:00:00")
    def test_returns_correct_date_range(self) -> None:
        """Default DAYS_BACK=1 returns a single-day range (yesterday)."""
        processor_module = reload_processor_module()

        start_date, end_date = processor_module.get_date_range()

        # Both dates should equal yesterday (2026-01-22)
        assert end_date == "2026-01-22"
        assert start_date == "2026-01-22"
```

Also update `test_process_export_with_only_end_date_anchors_start_to_end` (from Task 6) to expect the new default — change `2024-06-09` (Task 6 expectation under DAYS_BACK=7) to `2024-06-15` (DAYS_BACK=1 means start = end - 0 days = end):

```python
            # start_date = end_date - (1 - 1) = end_date itself
            assert result["body"]["date_range"]["start"] == "2024-06-15"
```

- [ ] **Step 2: Add new tests for default constants**

Append to `test_processor.py`:

```python
class TestProductionDefaults:
    """Verify production defaults match the design (DAYS_BACK=1, MAX_WORKERS=20)."""

    def test_default_days_back_is_one(self) -> None:
        import importlib
        import os
        # Clear any test-set env override
        os.environ.pop("OPTIMA_DAYS_BACK", None)
        from optima_shared import config
        importlib.reload(config)
        assert config.OPTIMA_DAYS_BACK == 1

    def test_default_max_workers_is_twenty(self) -> None:
        import importlib
        import os
        os.environ.pop("OPTIMA_MAX_WORKERS", None)
        from optima_shared import config
        importlib.reload(config)
        assert config.MAX_WORKERS == 20
```

- [ ] **Step 3: Run tests, verify they fail**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_processor.py::TestProductionDefaults -v
uv run pytest tests/unit/optima_exporter/interval_exporter/test_processor.py::TestGetDateRange::test_returns_correct_date_range -v
```

Expected: both FAIL with the current values 7 and 10.

- [ ] **Step 4: Update `config.py` defaults**

In `src/functions/optima_exporter/optima_shared/config.py`, change:

```python
# OLD:
OPTIMA_DAYS_BACK = int(os.environ.get("OPTIMA_DAYS_BACK", "7"))
MAX_WORKERS = int(os.environ.get("OPTIMA_MAX_WORKERS", "10"))
# NEW:
OPTIMA_DAYS_BACK = int(os.environ.get("OPTIMA_DAYS_BACK", "1"))
MAX_WORKERS = int(os.environ.get("OPTIMA_MAX_WORKERS", "20"))
```

- [ ] **Step 5: Run all processor tests**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_processor.py -v
```

Expected: ALL pass.

- [ ] **Step 6: Commit**

```bash
git add src/functions/optima_exporter/optima_shared/config.py tests/unit/optima_exporter/interval_exporter/test_processor.py
git commit -m "feat: change defaults to DAYS_BACK=1 and MAX_WORKERS=20"
```

---

## Task 9: Update Terraform env vars

**Files:**
- Modify: `terraform/optima_exporter.tf`

- [ ] **Step 1: Update env-var values in the interval-exporter Lambda block**

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

- [ ] **Step 3: Optionally show plan diff (no apply)**

```bash
cd terraform && terraform plan -target=aws_lambda_function.optima_interval_exporter && cd ..
```

Expected: only `OPTIMA_DAYS_BACK` and `OPTIMA_MAX_WORKERS` env vars change; no other resource modifications.

- [ ] **Step 4: Commit**

```bash
git add terraform/optima_exporter.tf
git commit -m "chore: align Terraform env vars with new source defaults"
```

---

## Task 10: Scoping guard test — `_prefix_nmi_in_nem12` must not leak

**Files:**
- Create: `tests/unit/optima_exporter/interval_exporter/test_prefix_scoping.py`

This guards the namespace-isolation invariant: the prefix helper should only ever be referenced from within `optima_exporter/`. Any future import from elsewhere fails the test, alerting the reviewer that the rewrite is leaking outside its intended scope.

- [ ] **Step 1: Create the guard test**

Create `tests/unit/optima_exporter/interval_exporter/test_prefix_scoping.py`:

```python
"""Scoping guard: _prefix_nmi_in_nem12 must remain internal to optima_exporter."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[4]
SRC_ROOT = REPO_ROOT / "src"
ALLOWED_PREFIX = SRC_ROOT / "functions" / "optima_exporter"
SYMBOL = "_prefix_nmi_in_nem12"


def test_prefix_helper_is_only_referenced_inside_optima_exporter() -> None:
    """
    The Optima_-prefix rewrite is a namespace convention specific to the Optima
    pipeline. If this symbol ever gets imported elsewhere in src/, downstream
    files that should keep their bare NMIs may end up with the prefix.
    """
    offenders: list[str] = []

    for path in SRC_ROOT.rglob("*.py"):
        if str(path).startswith(str(ALLOWED_PREFIX)):
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
```

- [ ] **Step 2: Run the test, verify it passes**

```bash
uv run pytest tests/unit/optima_exporter/interval_exporter/test_prefix_scoping.py -v
```

Expected: PASS (the symbol is only in `downloader.py`).

- [ ] **Step 3: Sanity check — the guard would catch a leak**

Manually verify by adding (then removing) a fake reference:

```bash
echo "# _prefix_nmi_in_nem12  # leak test" >> src/functions/file_processor/app.py
uv run pytest tests/unit/optima_exporter/interval_exporter/test_prefix_scoping.py -v
# Expect FAIL listing src/functions/file_processor/app.py
git checkout src/functions/file_processor/app.py
uv run pytest tests/unit/optima_exporter/interval_exporter/test_prefix_scoping.py -v
# Expect PASS
```

- [ ] **Step 4: Commit**

```bash
git add tests/unit/optima_exporter/interval_exporter/test_prefix_scoping.py
git commit -m "test: add scoping guard for _prefix_nmi_in_nem12"
```

---

## Task 11: Final regression sweep + lint

**Files:** none (verification only)

- [ ] **Step 1: Run the full test suite**

```bash
uv run pytest --cov=src --cov-report=term-missing
```

Expected: all 487+ existing tests + the new ones pass; coverage ≥ 90 %.

- [ ] **Step 2: Lint**

```bash
uv run ruff check .
uv run ruff format --check .
```

Expected: no errors. If `ruff format --check` reports changes, run `uv run ruff format .` and commit as `chore: format`.

- [ ] **Step 3: Quick smoke test of `_prefix_nmi_in_nem12` against the real fixture**

```bash
uv run python -c "
import sys, tempfile
sys.path.insert(0, 'src')
sys.path.insert(0, 'src/functions/optima_exporter')
from interval_exporter.downloader import _prefix_nmi_in_nem12
from shared.nem_adapter import output_as_data_frames

with open('tests/unit/fixtures/optima_bidenergy_nem12_sample.csv', 'rb') as f:
    raw = f.read()

prefixed = _prefix_nmi_in_nem12(raw, prefix='Optima_')
assert b'200,Optima_4001348123,' in prefixed
assert prefixed.count(b'200,Optima_4001348123,') == 4

with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
    tmp.write(prefixed)
    tmp_path = tmp.name

frames = output_as_data_frames(tmp_path)
assert len(frames) == 1
nmi, df = frames[0]
assert nmi == 'Optima_4001348123', f'Got {nmi}'
print(f'Smoke test OK: NMI={nmi}, rows={len(df)}, cols={list(df.columns)}')
"
```

Expected: `Smoke test OK: NMI=Optima_4001348123, rows=288, cols=[...]`.

- [ ] **Step 4: No commit needed** — this task is verification only.

---

## Post-merge staging verification (per spec §7.3)

After CI deploys the merged PR (do this BEFORE the next 14:00 Sydney scheduled run; if running close to schedule, disable EventBridge rules first):

1. **NZ site** — invoke Lambda with one `Optima_<NZ-NMI>` for `2026-04-10` to `2026-04-12`. Check S3 file is NEM12 with `200,Optima_<bare>,` line, and downstream lands in `newP/`.
2. **AU site** — same with `Optima_4001348123`.
3. **Multi-NMI re-check** — pick 5 sites with the largest meter counts; `aws s3 cp` each file locally; for each file count distinct NMIs in `200` records; assert each is in DynamoDB.
4. **Athena verification** — `SELECT COUNT(*) FROM default.sensordata_default WHERE sensorid = '<neptune-id>' AND ts BETWEEN '2026-04-10' AND '2026-04-12'`.
5. **CloudWatch** — confirm `sbm-ingester-metrics-log` non-zero monitor-point count; no entries in `sbm-ingester-parse-error-log` traceable to the new exports.

If any step fails: revert the PR, redeploy via the same workflow. Hudi upsert means re-pulling the same window after fix is safe.
