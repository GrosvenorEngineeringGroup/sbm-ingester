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
└── green_square/
    ├── __init__.py
    └── test_comx.py

tests/unit/test_dispatcher.py           # NEW, sibling of tests/unit/parsers/ (because dispatcher
                                        # lives at shared.non_nem_parsers, not under shared.parsers)
```

**Why `test_dispatcher.py` is a sibling, not nested under `parsers/`:** the source it tests (`get_non_nem_df`) lives at `src/shared/non_nem_parsers.py`, not under `src/shared/parsers/`. Source/test mirror principle puts it next to (not inside) the parsers test directory.

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

**Acknowledged naming asymmetry:** `interval_parser` drops its platform prefix (was `optima_parser`) while `bunnings_billing_parser` and `racv_billing_parser` keep their vendor prefixes — even though all three sit under `parsers/optima/`. The asymmetry reflects a structural difference, not an accident:

- **One vendor-agnostic parser** handles all interval files (it dispatches on the `BuyerShortName` column inside the CSV — Bunnings and RACV go through the same function). Path `parsers/optima/interval.py` already disambiguates from the bulk endpoint variant; no vendor prefix needed.
- **Two vendor-specific functions** for billing because the behaviours diverge sharply: Bunnings billing → full parse → Hudi sensor rows; RACV billing → archive-only to a different S3 bucket. They cannot be merged without an `if vendor == ...` switch, so they remain distinct functions; the vendor prefix on each is necessary to tell them apart in the dispatcher list and at the import site.

If a future change unifies billing behaviour, the rename to a single `billing_parser` becomes natural — but that is out of scope for this refactor.

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

**Affected import statements: 60 total** across `src/` and `tests/` (verified by `grep -rn "from shared.non_nem_parsers\|from shared.billing_parser\|from shared.noosa_solar_parser" --include="*.py" src/ tests/ | wc -l`). Of these, the rename-affected sites are concentrated in `tests/unit/test_non_nem_parsers*.py`, `tests/unit/test_billing_parser.py`, and `tests/unit/test_noosa_solar_parser.py`; the dispatcher-only import (`from shared.non_nem_parsers import get_non_nem_df`) appears 6 times and stays unchanged.

| Old import | New import |
|---|---|
| `from shared.non_nem_parsers import optima_parser` | `from shared.parsers.optima.interval import interval_parser` |
| `from shared.non_nem_parsers import optima_usage_and_spend_to_s3` | `from shared.parsers.optima.racv_billing import racv_billing_parser` |
| `from shared.non_nem_parsers import racv_elec_parser` | `from shared.parsers.racv.elec import racv_elec_parser` |
| `from shared.non_nem_parsers import green_square_private_wire_schneider_comx_parser` | `from shared.parsers.green_square.comx import green_square_private_wire_schneider_comx_parser` |
| `from shared.non_nem_parsers import envizi_vertical_parser_water` | `from shared.parsers.envizi.vertical_water import envizi_vertical_parser_water` |
| `from shared.non_nem_parsers import envizi_vertical_parser_water_bulk` | `from shared.parsers.envizi.vertical_water_bulk import envizi_vertical_parser_water_bulk` |
| `from shared.non_nem_parsers import envizi_vertical_parser_electricity` | `from shared.parsers.envizi.vertical_electricity import envizi_vertical_parser_electricity` |
| `from shared.non_nem_parsers import get_non_nem_df` | (unchanged — dispatcher stays at this path; ~6 sites) |
| `from shared.billing_parser import bunnings_usage_and_spend_parser` | `from shared.parsers.optima.bunnings_billing import bunnings_billing_parser` |
| `from shared.noosa_solar_parser import noosa_solar_parser` | `from shared.parsers.racv.noosa_solar import noosa_solar_parser` |

## Mock Path Migration

When a module moves, every `unittest.mock.patch("<old.module.path>.<symbol>")` targeting that module silently breaks: `patch()` raises `AttributeError` only when the patched name doesn't exist in the target module, but if the OLD module still exists (as `non_nem_parsers.py` does, just smaller), the patch may attach to a different `logger` than the one the parser code is actually using — leading to silent test failures (mocks don't intercept anything, but assertions still pass).

**Patches affected:**

| Old patch path | New patch path | Sites |
|---|---|---|
| `patch("shared.non_nem_parsers.logger")` for tests calling parsers DIRECTLY | `patch("shared.parsers.<platform>.<file>.logger")` (each parser's own logger) | ~36 in `test_non_nem_parsers.py` + `test_non_nem_parsers_edge_cases.py` |
| `patch("shared.non_nem_parsers.logger")` for tests calling `get_non_nem_df` (dispatcher) | (unchanged — dispatcher's logger stays at this path) | ~5 dispatcher test cases |
| `patch("shared.noosa_solar_parser.logger")` | `patch("shared.parsers.racv.noosa_solar.logger")` | 17 in `test_noosa_solar_parser.py` |
| `patch("shared.billing_parser.boto3.client", ...)` | `patch("shared.parsers.optima.bunnings_billing.boto3.client", ...)` | 1 in `test_billing_parser.py:124` |
| `monkeypatch.setattr(bp, ...)` where `bp = shared.billing_parser` | `monkeypatch.setattr(bp, ...)` where `bp = shared.parsers.optima.bunnings_billing` | 2 in `test_billing_parser.py` (alias rebinding) |

The migration must run a final sweep: `grep -rn 'patch("shared\.\(non_nem_parsers\|billing_parser\|noosa_solar_parser\)\.' tests/` should return zero matches when the refactor is complete (except the ~5 sites legitimately still patching the dispatcher's logger).

## Per-Module Logger Declarations

In the original `non_nem_parsers.py`, only `envizi_vertical_parser_water` (line 34) calls `logger.error` directly; the dispatcher uses the same module-level logger for `logger.debug` / `logger.error` on parse failures. The other 6 parsers in that file do not log — they just raise.

After the split, each parser file gets its own module-level logger declaration even if its functions don't currently log anything:

```python
# src/shared/parsers/envizi/vertical_water.py
from aws_lambda_powertools import Logger
logger = Logger(service="envizi-vertical-water-parser", child=True)

def envizi_vertical_parser_water(file_name: str, error_file_path: str) -> ParserResult:
    ...
    logger.error("envizi_vertical_parser_water: Multiple units", ...)
    ...
```

Why every file gets a logger (not just `vertical_water.py`):

1. **Mock patching consistency** — tests can always `patch("shared.parsers.<platform>.<file>.logger")` without first checking whether the file exposes one
2. **Future-proofing** — parsers will likely add logging as the codebase matures; having the logger ready avoids per-parser scaffolding later
3. **Mirrors existing siblings** — `noosa_solar_parser.py` and `billing_parser.py` already declare their own loggers (`service="noosa-solar-parser"` and `service="bunnings-billing-parser"`)

Service name convention: `"<platform>-<file-stem>-parser"` (e.g., `"envizi-vertical-water-parser"`, `"optima-interval-parser"`, `"green-square-comx-parser"`). This makes log filtering in CloudWatch trivial.

The dispatcher in `non_nem_parsers.py` keeps its existing logger declaration (`Logger(service="non-nem-parsers", child=True)`).

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
def test_interval_parser_persists_both_usage_and_generation(tmp_path):
    """Both Usage→E1_kWh and Generation→B1_kWh must be produced when present."""
    # Synthetic 12-column CSV matching the BidEnergy "Export Interval Usage Csv" output shape.
    # Two NMIs, one with non-zero Generation to confirm both channels persist.
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
    assert df["E1_kWh"].sum() == 3.2  # 1.5 + 1.7
    assert df["B1_kWh"].sum() == 1.7  # 0.8 + 0.9 — Generation persists
```

The fixture is inlined (synthetic CSV via `tmp_path`) rather than a file under `tests/unit/fixtures/` because: (a) the schema is small and self-documenting in the test, (b) the expected sums double as readability — anyone reading the test sees exactly what data is being asserted on, (c) avoids one more fixture file to maintain. If the test grows beyond ~15 lines of CSV literal, promote it to `tests/unit/fixtures/optima_interval_usage_with_generation.csv`.

If a future change accidentally drops one channel, this test breaks.

## Migration Steps (high-level — execution plan goes in writing-plans)

1. Create `src/shared/parsers/` subpackage skeleton (`__init__.py` files for `parsers/`, `parsers/optima/`, `parsers/racv/`, `parsers/envizi/`, `parsers/green_square/`).
2. **Extract** the 7 parser functions from `non_nem_parsers.py` into their new per-file homes (manual cut/paste, since they are functions inside a single source file — `git mv` does not apply here). **Use `git mv`** only for the two files that already exist as standalone modules: `billing_parser.py` → `parsers/optima/bunnings_billing.py` and `noosa_solar_parser.py` → `parsers/racv/noosa_solar.py` (preserves git history).
3. Apply the 3 function renames in their new files.
4. Reduce `non_nem_parsers.py` to the dispatcher-only form shown above.
5. Add per-module logger declarations to every parser file (see "Per-Module Logger Declarations" section).
6. Update all rename-affected import statements across `src/`, `tests/`, and any scripts.
7. Reorganise tests under `tests/unit/parsers/` per the structure above (and migrate mock patch paths per the "Mock Path Migration" section).
8. Add the `test_interval_parser_persists_both_usage_and_generation` regression test.
9. Run `uv run ruff check . --fix && uv run ruff format . && uv run pytest --cov=src` — verify all 525+ tests pass and coverage stays ≥90%.
10. Final sweep: `grep -rn 'patch("shared\.\(non_nem_parsers\|billing_parser\|noosa_solar_parser\)\.' tests/` should return only the ~5 dispatcher-logger patches.

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
