# Optima Demand Profile Parser Design

**Date:** 2026-05-05
**Status:** Draft
**Owner:** zeyu

## Background

BidEnergy's "Demand Profile" report (URL: `/BuyerReport/DemandProfile?...`) returns a 30-minute interval CSV containing real power (`kW`), apparent power (`kVa`), and `Power Factor` for each NMI. The data is currently not flowing into the Hudi data lake. This spec adds the persistence path: new Neptune monitor points + a parser that writes Hudi sensor rows directly.

Sample file: `Bunnings demand profile.csv` (4233 rows, 1 NMI, Feb–Apr 2026). Format:

```
Commodities:,"Electricity"
Sites (NMIs):,"4001260599"
Status:,"Active"
Country:, Australia
Start:,01-Feb-2026
End:,30-Apr-2026


Business Unit,Identifier,Identifier Type,ReadingDateTime,E,kW,kVa,Power Factor,Site Name
Bunnings Australia,4001260599,NMI,01-Feb-2026 00:00:00,5.2400,10.4800,10.4800,1.0000,BUN AUS Forbes
...
```

The parser persists three of the columns: `kW`, `kVa`, `Power Factor`. The `E` (energy), `Business Unit`, and `Site Name` columns are discarded.

## Why This Cannot Use the Standard Channel-Suffix Path

The `file_processor` Lambda inspects each parser's output DataFrame columns and gates by `suffix in NMI_DATA_STREAM_COMBINED` (where suffixes are NEM12 channel codes like `E1`, `B1`, `K1`). It then resolves Neptune IDs via `f"{nmi}-{suffix}"`. Demand metrics (`kw`, `kva`, `pf`) are not NEM12 channel codes and would be silently dropped.

The `bunnings_billing_parser` solves the same problem by **writing Hudi rows directly to `s3://hudibucketsrc/sensorDataFiles/`** and returning `[]` to the dispatcher. The dispatcher treats the empty list as "parser handled this file, no DataFrame to flow through file_processor". This spec adopts the same pattern.

## Scope

This spec covers Steps 1, 2, and 5 of the larger demand-profile pipeline:

| Step | Inclusion | Deliverable |
|---|---|---|
| 1 | ✅ in scope | `data/demand_points.csv` (1431 rows): NMI/field/nem12_id/label/category/meter_vertex_id |
| 2 | ✅ in scope | `scripts/generate_demand_points.py` + `scripts/import_demand_points.py` |
| 3 | ❌ out of scope | `nem12_mappings.json` is auto-exported hourly by the `sbm-files-ingester-nem12-mappings-to-s3` Lambda — no change needed |
| 4 | ❌ out of scope | `demand_exporter` Lambda (separate PR; will mirror the in-flight `interval_exporter` pattern) |
| 5 | ✅ in scope | `src/shared/parsers/optima/demand.py` parser + dispatcher wire-up + tests |

After this PR, the only blocker for end-to-end demand data flowing into Hudi is Step 4 (the exporter Lambda). Until that exists, demand CSVs can be manually downloaded from BidEnergy and uploaded to `sbm-file-ingester/newTBP/` to exercise the parser.

## Naming Conventions

### Neptune monitor point identifiers

```
sensor key (nem12Id)              Hudi unit
Optima_<NMI>-demand-kw            kw
Optima_<NMI>-demand-kva           kva
Optima_<NMI>-demand-pf            ""  (empty string)
```

**Rationale:**
- **`Optima_` prefix:** matches existing `Optima_<NMI>-E1` / `-B1` interval-data convention. Bunnings billing uses bare NMI (no `Optima_`) for historical reasons; new mappings should follow the prefixed Optima convention.
- **⚠️ Acknowledged long-term inconsistency.** After this PR, two Optima sensor families coexist under different prefixes in `nem12_mappings.json`: billing (bare `<NMI>-billing-...`) and demand/interval (`Optima_<NMI>-...`). Retrofitting billing is **explicitly out of scope** — billing's existing Hudi rows are keyed by the current sensor IDs, and changing the prefix would require coordinated Neptune rename + Hudi backfill. Future Optima parsers SHOULD use the `Optima_` prefix; billing stays as-is.
- **`demand-` namespace:** mirrors the `billing-` namespace (`<NMI>-billing-peak-usage`). Lets a grep on `demand-` find every demand-related point.
- **All-lowercase `kw`/`kva`/`pf`:** consistent with existing Hudi unit values (`kwh`, `aud`).
- **Empty unit for PF:** Power Factor is a dimensionless ratio; empty string is technically correct. Trade-off: `WHERE unit = ''` queries are slightly awkward, but acceptable.

### Scope: which NMIs get demand points

**All Bunnings Optima sites (project=bunnings, nmi LIKE `Optima_%`) — 477 NMIs** (AU 413 + NZ 64).

NZ sites likely have no demand data (BidEnergy URL filters `countrystr=AU`; NZ uses ICP not NMI), but we create points anyway to keep the inventory complete. The Hudi rows simply won't appear for NZ NMIs because the demand exporter will only fetch AU sites in Step 4.

RACV sites (~55) are excluded from this PR. RACV's demand requirements can be addressed in a follow-up.

**Total new Neptune points: 477 × 3 = 1431.**

### 9 NMIs without existing E1/B1 mappings

DynamoDB has 9 Bunnings Optima sites whose `Optima_<NMI>-E1` and `-B1` mappings are not yet in `nem12_mappings.json` (newer sites that haven't received interval data yet). For these:

- The generator script tries to find their `meter_vertex_id` via the E1 walk first, then via B1.
- If neither exists in Neptune, the generator writes the row with empty `meter_vertex_id` and the import script skips and logs it. These NMIs need manual investigation (likely no Neptune meter vertex yet — they need to be created via the meter-importer flow first).

## Step 1+2: Neptune Point Creation

### File 1: `data/demand_points.csv` (NEW, generated)

Schema mirrors `data/billing_points.csv`:

```csv
identifier,field,nem12_id,label,point_category,meter_vertex_id
4001260599,kw,Optima_4001260599-demand-kw,4001260599 Demand kW,demand,p:bunnings:xxx
4001260599,kva,Optima_4001260599-demand-kva,4001260599 Demand kVA,demand,p:bunnings:xxx
4001260599,pf,Optima_4001260599-demand-pf,4001260599 Demand Power Factor,demand,p:bunnings:xxx
...
```

| Column | Value pattern | Notes |
|---|---|---|
| `identifier` | bare NMI | Matches BidEnergy CSV's `Identifier` column and billing convention |
| `field` | `kw` / `kva` / `pf` | Short measurement name; `pointCategory=demand` already supplies the namespace |
| `nem12_id` | `Optima_<NMI>-demand-{field}` | Lookup key the parser will query |
| `label` | `<NMI> Demand kW` / `... Demand kVA` / `... Demand Power Factor` | Human-readable; bare NMI + descriptor; PF spelled out for clarity |
| `point_category` | `demand` | Mirrors billing's `billing` |
| `meter_vertex_id` | `p:bunnings:xxx` | Resolved via Neptune walk |

Total rows: 1431 (data) + 1 (header).

### Script 1: `scripts/generate_demand_points.py` (NEW)

```bash
PYTHONPATH=src uv run scripts/generate_demand_points.py \
    --output data/demand_points.csv \
    [--project bunnings] \
    [--dry-run]
```

**Input:** none (pulls from DynamoDB and Neptune).

**Logic:**

1. Scan DynamoDB `sbm-optima-config` for items where `project = bunnings` AND `nmi` starts with `Optima_`. Expect 477 items.
2. For each NMI, find `meter_vertex_id` in Neptune:
   - **Strategy A (primary):** walk from existing E1 point.
     ```
     g.V().has('nem12Id', 'Optima_<NMI>-E1').in('equipRef').id().limit(1)
     ```
     Works for the 468 NMIs that already have E1 mappings.
   - **Strategy B (fallback for the 9 unmapped):** walk from B1 point with the same query swapped to `-B1`. Some unmapped NMIs may have B1 even without E1.
   - **Strategy C:** if neither A nor B yields a vertex, leave `meter_vertex_id` empty and add the NMI to the "missing" report.
3. Write `data/demand_points.csv` with 1431 rows (3 per NMI), regardless of whether `meter_vertex_id` was found. Empty `meter_vertex_id` cells are intentional and handled downstream.
4. Print summary report:
   ```
   Total NMIs scanned: 477
   meter_vertex_id found via E1: <N>
   meter_vertex_id found via B1: <N>
   meter_vertex_id MISSING: <N> (listed by NMI)
   Rows written: 1431 (with <N>×3 missing meter_vertex_id)
   ```

**Reuses:** `scripts/billing_neptune_helper.gremlin_query` for Neptune access (already in repo, uses `gemsNeptuneExplorer` Lambda + `AWS_PROFILE=geg`).

### Script 2: `scripts/import_demand_points.py` (NEW, mirrors `import_billing_points.py`)

```bash
PYTHONPATH=src uv run scripts/import_demand_points.py \
    --csv data/demand_points.csv \
    [--dry-run] \
    [--workers 10] \
    [--output data/demand_point_ids.csv]
```

**Logic:** copied from `import_billing_points.py` with three changes:

1. `pointCategory` property changes from `'billing'` to `'demand'`.
2. Skip rows with empty `meter_vertex_id` and log them (`"orphan: NMI=<X>, field=<Y>, no meter_vertex_id"`).
3. Output filename defaults to `data/demand_point_ids.csv` instead of `data/billing_point_ids.csv`.

**Idempotency:** existence check by `nem12Id` (same pattern as billing) — re-running the script after partial completion is safe. **Caveat:** if a meter vertex is deleted in Neptune *after* a previous run created points pointing at it, the next run sees the `nem12Id` already exists and skips, leaving an orphaned point with a dangling `equipRef`. For Bunnings static infrastructure this is unlikely; if it happens, manual cleanup is via `meter-importer/scripts/delete_points.py`.

**Point ID generation:** reuses `import_billing_points.py`'s `generate_point_id()` helper (`p:bunnings:<hex_ts>-<hex_rand>`). May extract to a shared module later, but inline copy is fine for now (YAGNI).

### After Step 2

Once `import_demand_points.py` completes successfully, the next hourly run of the `sbm-files-ingester-nem12-mappings-to-s3` Lambda exports the new 1431 points to `nem12_mappings.json`. The parser (Step 5) can then resolve them.

**Operational sequencing:** the parser must NOT be deployed before the import script is run, otherwise it will look up missing mappings and skip every row.

## Step 5: `demand_parser`

### File: `src/shared/parsers/optima/demand.py` (NEW)

**Pattern: mirrors `bunnings_billing_parser`** — read CSV, look up sensor IDs from `nem12_mappings.json`, write Hudi rows directly to S3, return `[]` to the dispatcher.

### Function signature

```python
def demand_parser(file_name: str, error_file_path: str) -> ParserResult:
    """Parse a BidEnergy Demand Profile CSV and write Hudi sensor rows directly.

    Persists three columns per interval per NMI:
      - kW           → sensor Optima_<NMI>-demand-kw,  unit "kw"
      - kVa          → sensor Optima_<NMI>-demand-kva, unit "kva"
      - Power Factor → sensor Optima_<NMI>-demand-pf,  unit ""  (dimensionless)

    Like bunnings_billing_parser, this writes directly to
    s3://hudibucketsrc/sensorDataFiles/ and returns [] to the dispatcher;
    the file_processor's channel-suffix gate would otherwise drop
    non-NEM12 column names like "kw"/"kva"/"pf".
    """
```

### Field mapping constant

```python
CSV_FIELD_MAPPING: list[tuple[str, str, str]] = [
    # (CSV column name, demand suffix, Hudi unit)
    ("kW", "kw", "kw"),
    ("kVa", "kva", "kva"),    # BidEnergy's actual capitalisation, not standard kVA
    ("Power Factor", "pf", ""),  # Dimensionless ratio
]
```

### Filename + content gate (defence in depth)

```python
# 1. Fast filename reject (no I/O) — CASE-INSENSITIVE
if "demand profile" not in Path(file_name).name.lower():
    raise Exception("Not a Demand Profile file (filename mismatch)")

# 2. Content sniff (read first line only)
with open(file_name) as f:
    first_line = f.readline()
if not first_line.startswith("Commodities:"):
    raise Exception("Not a Demand Profile file (missing metadata header)")
```

Case-insensitive on purpose — the user's manual download is `Bunnings demand profile.csv` (lowercase), but a future automated exporter (Step 4) might use a different casing. Both must accept. The substring `"demand profile"` (with space) is unique enough that case-insensitive match won't false-positive.

### CSV parsing

The file has 6 metadata rows + ~2 blank rows + 1 header row + N data rows. Use `csv.DictReader` (matches `bunnings_billing_parser` style; lower memory than pandas DataFrame for what is row-by-row processing):

```python
def _parse_demand_rows(file_path: str) -> list[dict[str, str]]:
    """Skip metadata rows, return data rows as DictReader dicts.

    Layout:
      Row 1-6: metadata key:value pairs (Commodities/Sites/Status/Country/Start/End)
      Row 7-8: blank
      Row 9: column header
      Row 10+: data
    """
    with open(file_path, encoding="utf-8") as f:
        lines = f.read().splitlines()
    data_section = "\n".join(lines[8:])  # row 9 onward (0-indexed 8)
    reader = csv.DictReader(io.StringIO(data_section))
    return [row for row in reader if row.get("Identifier")]
```

### Per-row processing

```python
mappings = get_nem12_mappings()   # cached at module level (see "Helper sharing")

# locale note: %b (abbreviated month name) is locale-dependent. AWS Lambda
# Python runtime defaults to en_US.UTF-8 / C.UTF-8, where %b matches "Feb",
# "Mar", etc. Local dev environments using non-English locales would fail
# parsing — if this becomes a problem, switch to an explicit dict mapping.
ts = datetime.strptime(row["ReadingDateTime"], "%d-%b-%Y %H:%M:%S")
ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
nmi = row["Identifier"].strip()

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
```

### S3 write

```python
if rows_written == 0:
    logger.info("demand_no_rows_written", extra={"file": file_name, "unmapped": unmapped_count})
    return []   # don't write a header-only file (matches billing parser's behaviour)

ts_key = datetime.now(UTC).strftime("%Y%m%d%H%M%S%f")
key = f"sensorDataFiles/demand_export_{ts_key}.csv"
boto3.client("s3").put_object(
    Bucket="hudibucketsrc",
    Key=key,
    Body=buf.getvalue().encode(),
)
logger.info("demand_written", extra={"key": key, "rows": rows_written, "unmapped": unmapped_count})
return []
```

### Helper sharing: `_get_optima_mappings`

`bunnings_billing.py` already has `_get_nem12_mappings()` with module-level caching for `nem12_mappings.json`. Both parsers want the same cached dict.

**Decision: extract a shared helper at the `parsers/` level (not inside `optima/`).**

`nem12_mappings.json` is a global registry consumed by *any* parser that needs Neptune ID lookup; it isn't Optima-specific. Future envizi/racv parsers may want the same cache. Putting the loader at `parsers/_mappings.py` keeps the boundary correct.

Add `src/shared/parsers/_mappings.py`:

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
    """Lazy-load nem12_mappings.json from S3 once per Lambda container."""
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

**No `_reset_cache_for_tests()` helper.** Tests should reset the cache via `monkeypatch.setattr` against `_cache` directly — production code should not ship test-only public/semi-public APIs.

Then both `demand.py` and `bunnings_billing.py` import from it:

```python
from shared.parsers._mappings import get_nem12_mappings
```

**Side effect on `bunnings_billing.py`:** its existing module-level `_get_nem12_mappings()` and `_nem12_mappings_cache` are removed; the function call sites use `get_nem12_mappings()` from the shared module.

**Test patch sites in `tests/unit/parsers/optima/test_bunnings_billing.py` requiring update** (verify with `grep -nE '_get_nem12_mappings|_nem12_mappings_cache|_reset_mappings_cache' tests/unit/parsers/optima/test_bunnings_billing.py` — currently around lines 49, 99-103, 107, 125):

| Old | New |
|---|---|
| `monkeypatch.setattr(bp, "_get_nem12_mappings", lambda: {})` | `monkeypatch.setattr(mappings_mod, "get_nem12_mappings", lambda: {})` |
| `bp._nem12_mappings_cache = None` (in `_reset_mappings_cache` fixture) | `mappings_mod._cache = None` |
| Fixture `_reset_mappings_cache` (used by ~11 tests) | Renamed to reset the shared module's `_cache` |

Where `mappings_mod` is `from shared.parsers import _mappings as mappings_mod` (or similar import alias for the patch target). Each test that asserts on logger/mappings call counts must verify the new patch target after the change. Failure mode if not updated: tests still pass (patch targets a no-longer-imported name) but mocks are silently ineffective.

### ⚠️ Operational note: source file moves to `newIrrevFiles/`

After the parser returns `[]`, `file_processor` finds `file_neptune_ids` empty and routes the source CSV to `newIrrevFiles/` (not `newP/`). This is **correct and expected** — it matches `bunnings_billing_parser`'s behaviour. The Hudi rows are written *as a side effect* during parser execution, not via the standard NMI-mapped flow that determines the destination directory.

**Operator-facing implication:** demand CSVs **will appear in `newIrrevFiles/archived/<week>/`**, not `newP/`. Anyone auditing demand ingestion success should look at:
- CloudWatch logs for `service=optima-demand-parser` lines `"demand_written"` (rows count) and `"demand_no_rows_written"` (skip cases)
- Hudi `default.sensordata_default` table filtered on `sensorid LIKE 'p:bunnings:%'` AND `unit IN ('kw', 'kva', '')`

NOT at the source file's S3 destination directory.

### Dispatcher wire-up

In `src/shared/non_nem_parsers.py`:

```python
from shared.parsers.optima.demand import demand_parser
# (alphabetic position, so between bunnings_billing and interval)

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

The exact position doesn't affect behaviour because each parser fast-fails on filename/content mismatch, but grouping demand near billing/interval keeps the BidEnergy-report parsers visually together.

## Tests

### `tests/unit/parsers/optima/test_demand.py` (NEW)

At minimum 7 tests; mirror the structure of `tests/unit/parsers/optima/test_bunnings_billing.py`:

| Test | Verifies |
|---|---|
| `test_filename_gate_rejects_non_demand_files` | Filename without "Demand Profile" → raises |
| `test_content_gate_rejects_files_without_commodities_header` | Filename matches but first line wrong → raises |
| `test_parses_kw_kva_pf_to_correct_sensor_ids` | All three measurements written with correct sensor IDs and units |
| `test_unmapped_nmis_skipped_with_log` | Mapping returns None for some sensors → those rows skipped, others written |
| `test_empty_data_skips_s3_put` | Header-only file → no S3 PUT, returns `[]` |
| `test_pf_unit_is_empty_string` | Power Factor row's unit field is `""` |
| `test_dispatcher_routes_demand_file` | End-to-end through `get_non_nem_df` |

### Test fixture

Put the CSV builder helper in `tests/unit/parsers/optima/conftest.py` so all 7 tests can share it (rather than inlining in `test_demand.py`). This matches the existing pattern in `tests/unit/optima_exporter/conftest.py`.

```python
# tests/unit/parsers/optima/conftest.py
import pytest


@pytest.fixture
def write_demand_csv(tmp_path):
    """Factory fixture: write a synthetic Demand Profile CSV, return path."""
    def _write(filename="Bunnings_Demand_Profile.csv", rows=None):
        csv_path = tmp_path / filename
        rows = rows or [
            ("4001260599", "01-Feb-2026 00:00:00", "5.24", "10.48", "10.48", "1.0000"),
            ("4001260599", "01-Feb-2026 00:30:00", "5.21", "10.42", "10.42", "1.0000"),
            ("4001260599", "01-Feb-2026 05:30:00", "29.56", "59.12", "67.18", "0.8800"),
        ]
        body_lines = [
            'Commodities:,"Electricity"',
            'Sites (NMIs):,"4001260599"',
            'Status:,"Active"',
            'Country:, Australia',
            'Start:,01-Feb-2026',
            'End:,30-Apr-2026',
            '',
            '',
            'Business Unit,Identifier,Identifier Type,ReadingDateTime,E,kW,kVa,Power Factor,Site Name',
        ]
        for nmi, ts, e, kw, kva, pf in rows:
            body_lines.append(f"Bunnings Australia,{nmi},NMI,{ts},{e},{kw},{kva},{pf},BUN AUS Forbes")
        csv_path.write_text("\n".join(body_lines))
        return csv_path
    return _write
```

Promote to `tests/unit/fixtures/` only if a real-shape file is needed for richer scenarios.

## Open Questions / Filed for Step 4

1. **Exact downloaded filename pattern.** When the demand_exporter Lambda is built (Step 4), confirm the filename it produces and tighten the parser's filename gate accordingly (e.g., `"Bunnings-AU-Demand_Profile-<NMI>.csv"`).
2. **Header tolerance for filename casing.** `"Demand Profile"` substring works for `Bunnings demand profile.csv` if we lowercase before matching. Decide whether to match case-insensitively or require the exporter to use a specific casing.
3. **DemandKva NEM ambiguity.** The interval parser already discards `DemandKva` from the *interval* CSV. After demand_parser is in place, the `DemandKva` column in interval data could in principle be promoted to the same `Optima_<NMI>-demand-kva` sensor — but this would create dual write paths for the same sensor. Out of scope for this spec; flagged for future consideration.
4. **RACV demand support.** This PR creates points for Bunnings only. RACV's 55 AU NMIs would need a parallel data preparation step (not just adding to the same script — RACV has its own DynamoDB project, BidEnergy account, and Neptune namespace conventions).

## Affected Surface (summary)

| Layer | Touched? |
|---|---|
| `data/demand_points.csv` (new, generated artifact) | ✅ created |
| `scripts/generate_demand_points.py` | ✅ created |
| `scripts/import_demand_points.py` | ✅ created |
| `src/shared/parsers/optima/demand.py` | ✅ created |
| `src/shared/parsers/_mappings.py` (new shared helper at parsers/ level — broader than Optima) | ✅ created |
| `src/shared/parsers/optima/bunnings_billing.py` | ⚠️ small refactor (use shared `get_nem12_mappings`) |
| `src/shared/non_nem_parsers.py` | ⚠️ add demand_parser to dispatcher |
| `tests/unit/parsers/optima/test_demand.py` | ✅ created |
| `tests/unit/parsers/optima/test_bunnings_billing.py` | ⚠️ patch target update for the moved `get_nem12_mappings` |
| Neptune (live) | ⚠️ 1431 new point vertices created via import script |
| Hudi schema | ⚪ unchanged (just new sensorIds in existing rows) |
| Glue, Terraform, IAM, EventBridge | ⚪ unchanged |

## Risk & Rollback

- **Risk: 9 unmapped NMIs have no Neptune meter vertex.** Mitigation: `generate_demand_points.py` writes those rows with empty `meter_vertex_id`; `import_demand_points.py` skips and logs them. Operator follows up via meter-importer to create meters first, then re-runs the demand import (idempotent).
- **Risk: parser deployed before import script runs.** All sensor lookups miss → parser writes zero rows and logs `unmapped` for every input row. Mitigation: deploy order documented; the parser logs are the visible signal.
- **Risk: BidEnergy changes column names (e.g., `kVa` → `kVA`).** Parser silently produces zero rows (no exception). Mitigation: tests assert specific column-name strings; a real production miss would show in CloudWatch logs as `unmapped` count = 100% of rows.
- **Rollback:** revert the dispatcher commit + delete the per-parser file. The Neptune points can stay (orphaned but harmless) or be cleaned up with `meter-importer/scripts/delete_points.py` filtered by `pointCategory='demand'`.
