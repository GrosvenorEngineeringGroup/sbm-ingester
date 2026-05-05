# Non-NEM Parsers Modularization Design

**Date:** 2026-05-05
**Status:** Draft
**Owner:** zeyu

## Background

`src/shared/non_nem_parsers.py` (211 lines) currently bundles 7 parsers plus the `get_non_nem_df` dispatcher into a single file. Two siblings — `noosa_solar_parser.py` and `billing_parser.py` — already live in their own modules at `src/shared/`, so the "one parser = one file" pattern partially exists but is inconsistent.

This refactor splits the bundled parsers into a domain-organised `parsers/` subpackage, renames three functions whose current names are misleading, and reorganises the test suite to mirror the new structure. No parser behaviour changes.

## Motivation

1. **Single-file bloat.** `non_nem_parsers.py` mixes parsers from 4 distinct platforms (Envizi, Optima/BidEnergy, RACV, Green Square). Adding a new parser means editing a shared file with multiple responsibilities.
2. **Misleading names.** Three function names no longer reflect what the code actually does:
   - `optima_parser` — the file it parses contains both Usage AND Generation data, not just usage. The Optima platform also has other parsers (billing), so "optima" is not specific.
   - `optima_usage_and_spend_to_s3` — name says Optima but the function only accepts files matching `"RACV-Usage and Spend Report"`. Belongs to RACV behaviourally.
   - `bunnings_usage_and_spend_parser` — file is "Usage and Spend Report" which contains both usage AND spend data; `_usage_and_spend` is verbose and `_usage` alone would be misleading. `_billing` captures the file's purpose.
3. **Inconsistent module layout.** Two parsers are already in their own modules; the rest are bundled. Either commit to bundling or commit to splitting — half-and-half breeds confusion.
4. **Test discovery.** Tests for the bundled parsers are spread across `test_non_nem_parsers.py`, `test_non_nem_parsers_edge_cases.py`, plus the dedicated `test_billing_parser.py` and `test_noosa_solar_parser.py`. Mirroring source structure makes "where is the test for X" trivially answerable.

## Non-Goals

- **No behaviour changes.** Every parser's input/output contract, dispatch order, and side effects stay identical. This is a pure structural refactor.
- **No new parsers.** The forthcoming `interval_exporter` Lambda is a separate work item; this spec only ensures the renamed `interval_parser` will correctly persist both `E1_kWh` (Usage) and `B1_kWh` (Generation) channels for that future pipeline.
- **No changes to `nem_adapter.py`** (NEM12 path is unrelated), `file_processor`, Hudi schema, Glue, Terraform, or IAM.
- **No name change for `green_square_private_wire_schneider_comx_parser`.** The function is verbose but unique enough; renaming risks bigger diff for marginal benefit.
- **No name change for the 3 envizi parsers or `racv_elec_parser` or `noosa_solar_parser`.** They already describe what they do.

## Final Structure

### Source

```
src/shared/
├── parsers/                            # NEW subpackage, all non-NEM parsers
│   ├── __init__.py                    # Exposes `ParserResult` type alias only
│   ├── optima/                        # Optima/BidEnergy platform parsers (3)
│   │   ├── __init__.py
│   │   ├── interval.py                # interval_parser
│   │   ├── bunnings_billing.py        # bunnings_billing_parser
│   │   └── racv_billing.py            # racv_billing_parser
│   ├── racv/                          # RACV-internal parsers (NOT via BidEnergy)
│   │   ├── __init__.py
│   │   ├── elec.py                    # racv_elec_parser
│   │   └── noosa_solar.py             # noosa_solar_parser
│   ├── envizi/                        # Envizi platform parsers (3)
│   │   ├── __init__.py
│   │   ├── vertical_water.py          # envizi_vertical_parser_water
│   │   ├── vertical_water_bulk.py     # envizi_vertical_parser_water_bulk
│   │   └── vertical_electricity.py    # envizi_vertical_parser_electricity
│   └── green_square/                  # Green Square ComX
│       ├── __init__.py
│       └── comx.py                    # green_square_private_wire_schneider_comx_parser
├── nem_adapter.py                      # UNCHANGED
├── common.py                           # UNCHANGED
└── non_nem_parsers.py                  # SHRINKS to ~30 lines (dispatcher only)
```

**Deleted source files** (content moved into the subpackage):

- `src/shared/billing_parser.py` → `src/shared/parsers/optima/bunnings_billing.py`
- `src/shared/noosa_solar_parser.py` → `src/shared/parsers/racv/noosa_solar.py`

Use `git mv` so history follows.

### Tests

```
tests/unit/parsers/                     # NEW, mirrors source structure
├── __init__.py
├── conftest.py                         # Optional shared fixtures
├── optima/
│   ├── __init__.py
│   ├── test_interval.py
│   ├── test_bunnings_billing.py
│   └── test_racv_billing.py
├── racv/
│   ├── __init__.py
│   ├── test_elec.py
│   └── test_noosa_solar.py
├── envizi/
│   ├── __init__.py
│   ├── test_vertical_water.py
│   ├── test_vertical_water_bulk.py
│   └── test_vertical_electricity.py
├── green_square/
│   ├── __init__.py
│   └── test_comx.py
└── test_dispatcher.py                  # get_non_nem_df routing tests
```

**Test file migrations:**

| Source | Destination |
|---|---|
| `tests/unit/test_non_nem_parsers.py` (parser-specific tests) | Split into per-parser files under `tests/unit/parsers/` |
| `tests/unit/test_non_nem_parsers.py` (`get_non_nem_df` tests) | `tests/unit/parsers/test_dispatcher.py` |
| `tests/unit/test_non_nem_parsers_edge_cases.py` | Split into per-parser files under `tests/unit/parsers/` |
| `tests/unit/test_billing_parser.py` | `tests/unit/parsers/optima/test_bunnings_billing.py` |
| `tests/unit/test_noosa_solar_parser.py` | `tests/unit/parsers/racv/test_noosa_solar.py` |

After the move the four flat-level test files (`test_non_nem_parsers.py`, `test_non_nem_parsers_edge_cases.py`, `test_billing_parser.py`, `test_noosa_solar_parser.py`) are deleted.

## Function Rename Map

Three renames; the other six function names stay unchanged.

| Old name (old path) | New name (new path) |
|---|---|
| `optima_parser` (`shared.non_nem_parsers`) | `interval_parser` (`shared.parsers.optima.interval`) |
| `bunnings_usage_and_spend_parser` (`shared.billing_parser`) | `bunnings_billing_parser` (`shared.parsers.optima.bunnings_billing`) |
| `optima_usage_and_spend_to_s3` (`shared.non_nem_parsers`) | `racv_billing_parser` (`shared.parsers.optima.racv_billing`) |

Rationale recap:

- `interval_parser` — the file is "Interval Usage Csv" from BidEnergy UI but contains both Usage and Generation columns. `interval_usage_parser` would be misleading (implies usage only); `interval_parser` is honest.
- `bunnings_billing_parser` — file is "Usage and Spend Report" billing output; `_billing_` captures purpose without false-narrowing to either side.
- `racv_billing_parser` — same file format as Bunnings billing but RACV-specific behaviour (archive to a different S3 bucket only, no Hudi write). Vendor prefix preserved because behaviour differs from the Bunnings sibling.

## Final Dispatcher (`non_nem_parsers.py`)

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

**Dispatch order is preserved verbatim from the existing implementation** — no behavioural change.

## Import Strategy

**All consumers update their import paths to point at the new module locations.** No backward-compat shims in `non_nem_parsers.py`. The dispatcher (`get_non_nem_df`) remains importable from `shared.non_nem_parsers` because that file is kept (with reduced contents) — no consumer of the dispatcher needs to change.

Affected import statements (35 total, mostly in tests):

| Old import | New import |
|---|---|
| `from shared.non_nem_parsers import optima_parser` | `from shared.parsers.optima.interval import interval_parser` |
| `from shared.non_nem_parsers import optima_usage_and_spend_to_s3` | `from shared.parsers.optima.racv_billing import racv_billing_parser` |
| `from shared.non_nem_parsers import racv_elec_parser` | `from shared.parsers.racv.elec import racv_elec_parser` |
| `from shared.non_nem_parsers import green_square_private_wire_schneider_comx_parser` | `from shared.parsers.green_square.comx import green_square_private_wire_schneider_comx_parser` |
| `from shared.non_nem_parsers import envizi_vertical_parser_water` | `from shared.parsers.envizi.vertical_water import envizi_vertical_parser_water` |
| `from shared.non_nem_parsers import envizi_vertical_parser_water_bulk` | `from shared.parsers.envizi.vertical_water_bulk import envizi_vertical_parser_water_bulk` |
| `from shared.non_nem_parsers import envizi_vertical_parser_electricity` | `from shared.parsers.envizi.vertical_electricity import envizi_vertical_parser_electricity` |
| `from shared.non_nem_parsers import get_non_nem_df` | (unchanged — dispatcher stays at this path) |
| `from shared.billing_parser import bunnings_usage_and_spend_parser` | `from shared.parsers.optima.bunnings_billing import bunnings_billing_parser` |
| `from shared.noosa_solar_parser import noosa_solar_parser` | `from shared.parsers.racv.noosa_solar import noosa_solar_parser` |

## Subpackage `__init__.py` Conventions

- `parsers/__init__.py` exposes only the `ParserResult` type alias (`list[tuple[str, pd.DataFrame]]`). No re-exports of parser functions, to keep import paths one-true-way.
- Per-platform `__init__.py` files (`optima/__init__.py`, `racv/__init__.py`, `envizi/__init__.py`, `green_square/__init__.py`) are empty package markers. No re-exports.

This means consumers always import via the deepest path (e.g., `shared.parsers.optima.interval`), which makes `git grep` for "who uses parser X" reliable.

## Verification: Usage + Generation Both Persist

The downstream goal that motivated this refactor is the upcoming `interval_exporter` Lambda, whose downloaded CSVs will flow through `file_processor` → `interval_parser` → Hudi. Both `Usage` and `Generation` columns must end up as separate channels (`E1_kWh` and `B1_kWh`) in the data lake.

The existing `interval_parser` (formerly `optima_parser`) already implements this:

```python
if "Usage" in raw_df.columns:
    output_df["E1_kWh"] = base_df["Usage"].values
if "Generation" in raw_df.columns:
    output_df["B1_kWh"] = base_df["Generation"].values
```

Add a regression test in `tests/unit/parsers/optima/test_interval.py` to lock this contract:

```python
def test_interval_parser_persists_both_usage_and_generation():
    """Both Usage→E1_kWh and Generation→B1_kWh must be produced when present."""
    # Use a real-shape sample CSV with non-zero Usage AND Generation columns
    result = interval_parser(SAMPLE_CSV_WITH_GENERATION, error_path)
    assert len(result) == 1
    nmi_key, df = result[0]
    assert "E1_kWh" in df.columns
    assert "B1_kWh" in df.columns
    assert df["E1_kWh"].sum() > 0
    assert df["B1_kWh"].sum() > 0   # critical: Generation persists
```

If a future change accidentally drops one channel, this test breaks.

## Migration Steps (high-level — execution plan goes in writing-plans)

1. Create `src/shared/parsers/` subpackage skeleton (empty `__init__.py` files).
2. `git mv` the 7 bundled parsers from `non_nem_parsers.py` into their new files (manual extraction from a single source file, but use `git mv` for `billing_parser.py` and `noosa_solar_parser.py` to preserve history).
3. Apply the 3 function renames in their new files.
4. Reduce `non_nem_parsers.py` to the dispatcher-only form shown above.
5. Update all 35 import statements across `src/`, `tests/`, and any scripts.
6. Reorganise tests under `tests/unit/parsers/` per the structure above.
7. Add the `test_interval_parser_persists_both_usage_and_generation` regression test.
8. Run `uv run ruff check . --fix && uv run ruff format . && uv run pytest --cov=src` — verify all 525+ tests pass and coverage stays ≥90%.

## Risk & Rollback

- **Risk: missed import path.** Mitigation: `ruff check` will surface unresolved imports; `pytest` will surface mock-path mismatches in tests.
- **Risk: dispatch order accidentally changes.** Mitigation: the dispatcher snippet above is the contract; reviewers compare against the original ordering line-by-line.
- **Risk: git history harder to follow.** Mitigation: `git mv` for the two already-separate files preserves history. The 7 parsers extracted from `non_nem_parsers.py` will have their pre-refactor history attached to that source file's commits — `git log --follow` won't reach them, but `git log --all -S '<function name>'` still works.
- **Rollback:** single revert commit reverses the entire refactor since behaviour is unchanged.

## Affected Surface (summary)

| Layer | Touched? |
|---|---|
| `src/shared/parsers/` (new subpackage) | ✅ created |
| `src/shared/non_nem_parsers.py` | ✅ shrinks to dispatcher |
| `src/shared/billing_parser.py` | ❌ deleted (moved) |
| `src/shared/noosa_solar_parser.py` | ❌ deleted (moved) |
| Other `src/shared/*` (`nem_adapter.py`, `common.py`) | ⚪ unchanged |
| `src/functions/file_processor/` | ⚪ unchanged |
| `src/functions/optima_exporter/` | ⚪ unchanged |
| `src/glue/`, Terraform, IAM, CI | ⚪ unchanged |
| `tests/unit/parsers/` (new) | ✅ created |
| `tests/unit/test_non_nem_parsers*.py` / `test_billing_parser.py` / `test_noosa_solar_parser.py` | ❌ deleted (split + moved) |
