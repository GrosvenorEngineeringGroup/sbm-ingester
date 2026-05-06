# Optima Interval Exporter — Design Spec

**Status:** Revised 2026-05-06 (truth-check pass against 8 real BidEnergy CSVs + production mappings)
**Date:** 2026-05-06
**Owner:** zeyu
**Related:**
- [`2026-05-05-optima-demand-exporter-design.md`](2026-05-05-optima-demand-exporter-design.md)
- [`2026-04-13-optima-interval-exporter-nem12-migration-design.md`](2026-04-13-optima-interval-exporter-nem12-migration-design.md) — the *previous* migration that this spec partially reverses

## Problem

`optima-nem12-exporter` (deployed since April 2026) downloads NEM12 CSV files from BidEnergy's `/BuyerReport/ExportIntervalUsageProfileNem12` endpoint and feeds them through `nem_adapter` to populate `Optima_<NMI>-E1` / `Optima_<NMI>-B1` Hudi sensors. The pipeline works, but operational experience has surfaced a preference for the simpler, flatter CSV format produced by the BidEnergy SiteUsage page's "Export Interval Usage Csv" button (`POST /BuyerReport/exportdailyusagecsv`). That endpoint returns a ZIP wrapping a single 12-column per-NMI CSV, which is easier to inspect, debug, and reason about than NEM12's 100/200/300/900 record format.

We need a new Lambda `optima-interval-exporter` that becomes the **primary** interval data source while keeping the existing `optima-nem12-exporter` Lambda code intact (its EventBridge schedules will be disabled, but the function remains invocable for ad-hoc backups, debugging, or future re-enablement).

## Discoveries (truth-check pass: 8 real BidEnergy downloads + production mappings, 2026-05-06)

### Existing parser, dispatcher, and tests are already wired

- [`src/shared/parsers/optima/interval.py`](../../../src/shared/parsers/optima/interval.py) — `interval_parser()` reads the 12-column CSV and returns `[(f"Optima_{Identifier}", DataFrame[E1_kWh, B1_kWh])]`
- [`src/shared/non_nem_parsers.py:12,29`](../../../src/shared/non_nem_parsers.py) — already imported and registered in the dispatcher
- [`tests/unit/parsers/optima/test_interval.py`](../../../tests/unit/parsers/optima/test_interval.py) — full test suite already in place
- [`src/functions/file_processor/app.py:457-475`](../../../src/functions/file_processor/app.py) — channel-mapping (`E1_kWh` → `Optima_<NMI>-E1`) already handles the parser output

### Verified by downloading real BidEnergy responses for 8 sites + 3 single-day samples

Auth + POST /BuyerReport/exportdailyusagecsv against 4 AU + 4 NZ Bunnings sites for 4-month range (2025-04-01 → 2025-07-31), plus 3 single-day samples (May 1, May 15, Jun 15). Findings:

| Truth | What spec previously assumed | What real data shows |
|---|---|---|
| Line endings | (unspecified) | **CRLF (`\r\n`)** — every line including last |
| BOM | (unspecified) | **No BOM** — bytes start with `B` of `BuyerShortName` |
| NZ Identifier type | "NMI" | **`ICP`** (e.g. `0000010008MQCB6`, alphanumeric). Parser already handles via `Identifier` column. |
| Filename pattern | partial | `Bunnings-{COUNTRY}-Electricity-{IDENT}-{NMI\|ICP}-{DistributorId}.csv` |
| Empty-data response | "22-byte EOCD-only ZIP" | **322B ZIP wrapping 148B CSV containing literal text `No data is available\r\n`** — never a truly empty ZIP |

### Phantom bug: the `%b` / `%B` date-format crash does NOT exist

The previous spec revision claimed `pd.to_datetime` would auto-infer `%B` from "May", then crash on "Jun". **Re-tested on real data — it does not happen**:

| Test | Result |
|---|---|
| Real AU CSV, 5856 rows spanning Apr→May→Jun→Jul | ✅ Parses cleanly; index covers months `[4, 5, 6, 7]` |
| Real NZ ICP CSV, same 4-month range | ✅ Parses cleanly |
| Real single-day CSVs for 2025-05-01, 2025-05-15, 2025-06-15 | ✅ All parse cleanly |

Pandas auto-inference correctly resolves `%b` from BidEnergy's "01 Apr 2025" / "01 Jun 2025" strings. **Drop the entire 1-line `format=` fix, conftest fixture realignment, and multi-month regression test from scope** — they were chasing a bug that does not reproduce on real input.

### Real bug: parser crashes on the "No data is available" CSV

Two of the 8 test sites returned the empty-data sentinel CSV. Running `interval_parser` on it:

```
UFuncTypeError: ufunc 'add' did not contain a loop with signature matching
                types (dtype('float64'), dtype('<U1')) -> None
```

Root cause: pandas reads the `No data is available` row as a single-cell value in `BuyerShortName`, with all other 11 columns becoming float64 NaN. Then [`interval.py:24`](../../../src/shared/parsers/optima/interval.py)'s `raw_df["Date"] + " " + raw_df["Start Time"]` tries to add a string `" "` to a float64 NaN column → ufunc crash.

**Today** these CSVs would be misrouted to `newParseErr/`, generating CloudWatch parse-error noise. **Going forward** with interval as the primary source, an estimated ≥ 25% of daily site responses will hit this code path (for sites without data on a given day) — must be fixed.

### NZ Neptune mappings are 100% present

Earlier draft flagged "NZ may lack `Optima_<ICP>` mappings" as a cutover risk. Verified against `s3://sbm-file-ingester/nem12_mappings.json` (1.0 MB, 14,608 entries):

| Project | DynamoDB sites | Missing `-E1` | Missing `-B1` |
|---|---|---|---|
| Bunnings AU | 413 | 0 | 9 (all on sites without solar — expected) |
| **Bunnings NZ** | **64** | **0** | **0** |
| RACV | 55 | 3 | 1 |

**The NZ mapping risk is removed from the risk table.** The 9 AU + 4 RACV gaps are pre-existing reality (sites without solar / un-mapped meters) and would route to `newIrrevFiles/` — same behaviour as today's NEM12 path; no regression.

### Therefore this spec now covers

1. The new EXPORTER Lambda (~85% of the work).
2. A small fix to `interval_parser` to handle the `No data is available` sentinel by returning `[]`.
3. Adding 4 real BidEnergy CSV samples (committed under `tests/unit/fixtures/optima_interval/`) + a regression test that runs `interval_parser` against each.

**Out of scope (removed from previous revision):** `format=` argument, `conftest.py:create_optima_csv` realignment, multi-month regression test.

## Goal

Add `optima-interval-exporter` Lambda + Terraform/CI/CD wiring, mirroring the structure of the recently-deployed `optima-demand-exporter`. The new Lambda downloads ZIP-wrapped CSVs from `POST /BuyerReport/exportdailyusagecsv`, extracts the inner CSV, and uploads to `s3://sbm-file-ingester/newTBP/` — where the existing `interval_parser` (after a small empty-data-sentinel fix) consumes them via the standard `file_processor` pandas/DataFrame path.

## Non-Goals

- Modifying `nem12_exporter` code, IAM, log group, alarm, or Lambda function (all preserved; only the 2 EventBridge schedules are disabled).
- Modifying `non_nem_parsers.py` dispatcher or `file_processor` (already wired).
- Replacing `interval_parser` — only the empty-data-sentinel fix is in scope; pandas/DataFrame architecture unchanged.
- Persisting `DemandKva` (already collected by `optima-demand-exporter`) or `Reactive` (no Neptune mapping; YAGNI).
- Real-time / on-demand exports (this is a scheduled batch job).
- Changing `tests/unit/conftest.py:create_optima_csv` (still emits ISO dates — that's fine; pandas handles both formats correctly, see Discoveries above).

## Constraints

- Must reuse `optima_shared/` modules (`auth.py`, `config.py`, `dynamodb.py`).
- Must mirror packaging — bundled into the same `optima_exporter.zip` artefact (already shared by nem12, billing, demand).
- Must update the manually-managed `sbm-ingester-cicd-policy` IAM whitelist (otherwise GitHub Actions deploy fails with `AccessDeniedException`).
- Must keep `nem12_exporter` Lambda function and code intact (only its 2 EventBridge schedules are removed via Terraform).
- **Must remove pre-existing Terraform `moved` blocks** for `optima_interval_exporter` (lines 468-491 of `terraform/optima_exporter.tf`) — left over from the April rename. Otherwise creating new resources with the same name will conflict.

## Verified Facts (cross-checked against running code & live BidEnergy)

1. **Endpoint** (verified 2026-05-06 via `agent-browser` + 8 real script-driven downloads):
   ```
   POST https://app.bidenergy.com/BuyerReport/exportdailyusagecsv
   Content-Type: application/x-www-form-urlencoded
   Cookie: <session cookies>
   Body: siteId=<UUID>&start=<DD MMM YYYY>&end=<DD MMM YYYY>

   Response (always 200 on auth success):
     Content-Type: application/zip
     Body: ZIP wrapping exactly 1 CSV (header-only "No data is available" if site has no data)
   ```
   - Verified for **4 Bunnings AU + 4 Bunnings NZ sites** for 2025-04-01 → 2025-07-31 range (8/8 successful 200 responses, all valid ZIPs).
   - **POST is required**; GET returns `text/html` error page.
   - **`Content-Type: application/x-www-form-urlencoded` header is required**; without it, even POST returns HTML.
   - Empty-data response: ZIP **always wraps a CSV** (header-only or with literal `No data is available\r\n` body). No truly empty (EOCD-only) ZIPs observed in any sample.

2. **CSV format inside ZIP** (verified byte-for-byte across 8 downloads):
   ```
   BuyerShortName,Country,Commodity,Identifier,IdentifierType,DistributorId,Date,Start Time,Usage,Generation,DemandKva,Reactive\r\n
   "Bunnings","AU","Electricity","2002105104","NMI","UMPLP",01 Apr 2025,00:00,13.0600,0.00,27.57,4.41\r\n
   ```
   - **CRLF line endings**, no UTF-8 BOM.
   - 30-minute intervals (48 rows per NMI per day; 5856 rows for 4 months).
   - AU uses `IdentifierType=NMI` (numeric `2002105104`); NZ uses `IdentifierType=ICP` (alphanumeric `0000010008MQCB6`). Parser already handles both via `Identifier` column.
   - Empty-data sentinel CSV (148 bytes total):
     ```
     BuyerShortName,Country,Commodity,Identifier,IdentifierType,DistributorId,Date,Start Time,Usage,Generation,DemandKva,Reactive\r\n
     No data is available\r\n
     ```

3. **Existing `interval_parser` consumes happy-path CSVs verbatim** with no date-format issues. Tested on real 4-month spans (Apr/May/Jun/Jul) and single-day samples (May 1 / May 15 / Jun 15) — pandas auto-inference handles all `DD MMM YYYY` strings correctly. Only fix needed: graceful handling of the `No data is available` sentinel (currently raises `UFuncTypeError` because pandas reads it into a single row with NaN-typed `Date`/`Start Time` columns, and `pd.to_datetime` then chokes on float64 + str concatenation).

4. **DynamoDB site uniqueness** (verified via scan): all 532 NMIs (Bunnings 477 + RACV 55) have **unique `siteIdStr`**. Each POST returns exactly one CSV inside the ZIP.

5. **Neptune mapping coverage** (verified against live `nem12_mappings.json`, 14,608 entries):
   - Bunnings AU 413 sites: 0 missing E1, 9 missing B1 (no-solar sites — pre-existing)
   - Bunnings NZ 64 sites: 0 missing E1, 0 missing B1 — fully covered
   - RACV 55 sites: 3 missing E1, 1 missing B1 (pre-existing)

   Net: 519/532 sites (97.6%) have full E1+B1 mappings; 13 partial gaps are pre-existing reality, not a cutover risk.

6. **`file_processor` already handles the parser's `(f"Optima_{Identifier}", DataFrame[E1_kWh, B1_kWh])` output.** Lines 457-475 of `file_processor/app.py` extract channel suffix (`E1_kWh` → `E1`), build lookup key (`f"{nmi}-{suffix}"` = `Optima_<NMI>-E1`), and write Hudi rows with unit derived from `_kWh` suffix.

7. **Pre-existing Terraform `moved` blocks** at `terraform/optima_exporter.tf:468-491` reference resources named `aws_*.optima_interval_exporter` (left over from April's rename to nem12_exporter). These will conflict with new resources of the same name unless removed.

## Architecture

```
EventBridge Scheduler (cron 14:00 Sydney, per project — taking the slot vacated by nem12)
   ├── optima-bunnings-interval-daily   → input {"project":"bunnings"}
   └── optima-racv-interval-daily        → input {"project":"racv"}
              ↓
      optima-interval-exporter Lambda (NEW)
        (Python 3.13, 256 MB, 900s, shared `getIdFromNem12Id-role-153b7a0a` IAM role)
              ↓
        1. config = optima_shared.config.get_project_config(project)
        2. sites  = optima_shared.dynamodb.get_sites_for_project(project)
        3. cookies = optima_shared.auth.login_bidenergy(...)
        4. ThreadPoolExecutor(max_workers=OPTIMA_MAX_WORKERS=20):
              for each site (siteIdStr, nmi):
                zip_bytes = downloader.download_interval_zip(cookies, siteIdStr, start, end)
                csv_bytes = downloader.extract_first_csv(zip_bytes)   # 5-line zipfile op
                # csv_bytes is uploaded EVEN if the ZIP was empty (audit retention)
                uploader.upload_to_s3(csv_bytes, filename)
              ↓
      s3://sbm-file-ingester/newTBP/optima_<project>_interval_NMI#<NMI>_<start>_<end>_<ts>.csv
              ↓
      ┌──────────── Existing pipeline (no changes) ────────────┐
      │ S3 → SQS → sbm-files-ingester → non_nem_parsers       │
      │   → interval_parser (pd.read_csv → returns            │
      │     [(f"Optima_{NMI}", DataFrame[E1_kWh, B1_kWh])])   │
      │   → file_processor splits column name → suffix=E1     │
      │   → mappings.get(f"Optima_{NMI}-E1") → Hudi sensorId  │
      │   → write_row(sensor_id, ts, val, unit="kwh")         │
      │   → file moves to newP/ (file_neptune_ids non-empty)  │
      └────────────────────────────────────────────────────────┘
              ↓
      Existing Glue job hourly picks up Hudi CSV → sensordata_default Athena table

Parallel (unchanged after this change):
  ├── optima-nem12-exporter Lambda  — code/Lambda preserved, 2 EventBridge schedules DISABLED
  └── optima-demand-exporter Lambda + 14:30 schedules — unchanged
```

## Components

```
src/functions/optima_exporter/interval_exporter/        # reuses pre-rename empty directory
├── __init__.py
├── app.py              # Lambda handler — same shape as demand_exporter/app.py
├── downloader.py       # download_interval_zip() — POST exportdailyusagecsv; extract_first_csv()
├── processor.py        # process_export() — orchestrates per-project export
└── uploader.py         # upload_to_s3() — verbatim copy from demand_exporter (logger renamed)

src/shared/parsers/optima/interval.py
                        # MODIFY: add early-return when CSV is the "No data is available"
                        #   sentinel (parser currently crashes with UFuncTypeError)

tests/unit/fixtures/optima_interval/
                        # CREATE (commit 4 real BidEnergy CSV samples):
                        # - interval_au_single_day.csv         (~5 KB, AU NMI Apr 1 2025)
                        # - interval_nz_single_day.csv         (~5 KB, NZ ICP Apr 1 2025)
                        # - interval_au_4month.csv             (~580 KB, AU 5856 rows Apr-Jul)
                        # - interval_empty.csv                 (148 B, "No data is available")

tests/unit/parsers/optima/test_interval.py
                        # MODIFY: add 4 regression tests using the real fixtures above
                        #   (happy AU, happy NZ-ICP, multi-month, empty-data sentinel)

terraform/optima_exporter.tf
                        # MODIFY:
                        # - REMOVE 5 stale `moved` blocks (lines 468-491) — leftover from April rename
                        # - Comment out 2 nem12 schedule resources (with reason + revival instructions)
                        # - Add: 1 log group + 1 Lambda + 2 schedulers + 1 alarm (interval)
                        # - Update optima_scheduler_invoke_lambda Resource list (add 4th ARN)

.github/workflows/main.yml
                        # MODIFY:
                        # - Build step: 1 new `cp -r` for interval_exporter (5th line; currently 4)
                        # - Deploy step: 1 new `update-function-code` block (4th block; currently 3)
```

**Reused unchanged (no code changes):**
- `optima_shared/auth.py` — works for Bunnings & RACV (verified)
- `optima_shared/config.py` — env vars + `get_project_config()`
- `optima_shared/dynamodb.py` — `get_sites_for_project()`, `get_site_by_nmi()`
- **`shared/non_nem_parsers.py`** — already imports + registers `interval_parser`
- **`file_processor/app.py`** — channel-mapping logic (lines 457-475) already handles `E1_kWh` / `B1_kWh` columns

### Differences from `demand_exporter`

| Aspect | `demand_exporter` | `interval_exporter` |
|---|---|---|
| HTTP method | GET | **POST** |
| Body / params | URL query string | `application/x-www-form-urlencoded` body |
| Required fields | isCsv, start, end, filter.SiteIdStr, filter.SiteStatus, filter.commodities, filter.countrystr | **siteId, start, end** (only 3) |
| Response | Plain CSV | **ZIP wrapping single CSV** |
| Body validation | Body sniff `b"Commodities:"` | First 2 bytes `b"PK"` (ZIP magic) |
| Filename prefix | `optima_<proj>_demand_profile_NMI#` | `optima_<proj>_interval_NMI#` |
| Parser path | Custom `demand_parser` (writes Hudi directly, returns `[]`) | **Existing `interval_parser` (pandas DataFrame path through file_processor)** |
| Source-file destination | `newIrrevFiles/` (parser returns `[]`) | **`newP/` for sites with data; `newIrrevFiles/` for "No data is available" sentinel** |
| Schedule | 14:30 Sydney | 14:00 Sydney (taking nem12's vacated slot) |

## Detailed Component Behaviour

### `app.py` — Lambda handler

Pure pass-through to `process_export(project, nmi, start_date, end_date)`. Same event contract as nem12/demand:

```python
event = {
    "project": "bunnings" | "racv",   # required
    "nmi": "Optima_4102026418",        # optional — single-NMI mode
    "startDate": "2026-04-29",         # optional ISO
    "endDate": "2026-04-30",           # optional ISO
}
```

Returns `{"statusCode": 400, "body": "Missing required parameter: project"}` if `project` missing. Otherwise delegates to `processor.process_export`.

### `processor.py`

Implements `process_export(project, nmi=None, start_date=None, end_date=None) -> dict`. Mirrors `demand_exporter/processor.py` exactly (same date-range resolution, same DynamoDB fetch, same login, same `ThreadPoolExecutor`, same per-site result accumulation, same statusCode 200/207/4xx return shape). Only differences vs demand:

1. Calls `download_interval_zip` then `extract_first_csv` (two-step flow).
2. Empty-data sentinel handling: `extract_first_csv` returns the BidEnergy CSV bytes verbatim — no synthesis, no special casing. If BidEnergy returns the 148-byte "No data is available" CSV, those exact bytes are uploaded to S3 for audit retention. The `interval_parser` (see fix below) detects the sentinel and returns `[]`, causing `file_processor` to route the source file to `newIrrevFiles/` with zero `file_neptune_ids`. Processor sets `result["empty_data"] = True` (analogous to demand's `result["no_data"]`) so the return body includes `empty_data_count` for operational visibility.

### `downloader.py`

```python
def download_interval_zip(
    cookies: str,
    site_id_str: str,
    start_date: str,           # ISO YYYY-MM-DD
    end_date: str,             # ISO YYYY-MM-DD
    project: str,
    nmi: str,
) -> bytes | None:
    """POST /BuyerReport/exportdailyusagecsv, return raw ZIP bytes (or None on failure).

    Body validation: accepts only responses whose first two bytes are b"PK"
    (ZIP local file header magic). HTML responses (auth lost, wrong Content-Type)
    → None. No truly empty (EOCD-only) ZIPs observed in 8/8 real sample
    downloads — BidEnergy always wraps a CSV (even the "No data is available"
    sentinel CSV).
    """

def extract_first_csv(zip_bytes: bytes) -> bytes:
    """Open the ZIP and return the bytes of the single inner CSV verbatim.

    No synthesis, no special casing. The "No data is available" sentinel CSV
    (148 bytes) is returned as-is for audit retention; the parser detects and
    handles the sentinel downstream.

    Raises BadZipFile if input is not a valid ZIP, or ValueError if the ZIP
    contains zero entries (defensive — never observed in production).
    """

def format_date_for_url(date_str: str) -> str:
    """ISO YYYY-MM-DD → 'DD MMM YYYY' (e.g., '06 May 2025').

    Note: %b is locale-dependent. AWS Lambda Python 3.13 uses C.UTF-8 where %b
    matches Apr/Jun/etc., as does CI. Non-English dev locales would produce
    different output and break local testing.
    """
```

Differences vs `demand_exporter/downloader.py`:

- URL: `f"{BIDENERGY_BASE_URL}/BuyerReport/exportdailyusagecsv"`
- HTTP method: `requests.post(url, data={"siteId": ..., "start": ..., "end": ...}, headers={"Cookie": cookies}, timeout=300)` — `data=` (dict) auto-sets `Content-Type: application/x-www-form-urlencoded` and URL-encodes the body.
- Body validation: first 2 bytes `b"PK"` instead of `Commodities:` header sniff. HTML rejection identical to demand pattern.
- **No** `nmi` URL parameter (`nmi` arg used only for output filename construction).
- Filename: `f"optima_{project.lower()}_interval_NMI#{nmi.upper()}_{start_date}_{end_date}_{timestamp}.csv"`
- Two-step flow: `download_interval_zip` returns raw ZIP bytes; `extract_first_csv` returns the inner CSV bytes verbatim (including the 148-byte "No data is available" sentinel — handled downstream by the parser, not by synthesis here).

### `uploader.py`

**Copy `demand_exporter/uploader.py` verbatim** (same `upload_to_s3()` signature including optional `bucket` and `prefix` overrides, same `_s3_client` singleton with `region_name="ap-southeast-2"`, same `ContentType="text/csv"`, same logger formatting). Only change: rename `Logger(service="optima-demand-exporter")` → `Logger(service="optima-interval-exporter")` and adjust the module docstring.

### `shared/parsers/optima/interval.py` — empty-data sentinel fix

Add an early return when BidEnergy returns the "No data is available" sentinel CSV. After `raw_df = pd.read_csv(file_name)` on line 23, insert:

```python
# BidEnergy returns a 148-byte sentinel CSV when a site has no data for the
# requested range. Pandas reads "No data is available" as a single row with
# NaN-typed Date/Start Time columns, which would crash the str+str datetime
# concat below with UFuncTypeError. Detect and short-circuit to [].
if len(raw_df) == 1 and raw_df["Date"].isna().all():
    logger.info("interval_no_data_sentinel", extra={"file": file_name})
    return []
```

The `[]` return signals to `file_processor` that no Hudi rows should be written; the source file is then routed to `newIrrevFiles/` (matching the convention used elsewhere for files that parse cleanly but contain no usable data).

No other parser logic changes. Tests confirm pandas auto-inference handles `DD MMM YYYY` correctly across all 12 months, so no `format=` argument is needed.

### `tests/unit/fixtures/optima_interval/` — real BidEnergy samples

Commit 4 verbatim downloads from BidEnergy production (already captured under `/tmp/real_intervals/` during truth-check on 2026-05-06):

| Fixture | Size | Source | Purpose |
|---|---|---|---|
| `interval_au_single_day.csv` | 4.9 KB | AU NMI 2002105104, 2025-05-01 | Daily-run happy path |
| `interval_nz_single_day.csv` | 4.9 KB | NZ ICP 0000010008MQCB6, 2025-05-01 | NZ ICP coverage |
| `interval_au_4month.csv` | 573 KB | AU NMI 2002105104, 2025-04-01 → 2025-07-31 | Multi-month spanning Apr/May/Jun/Jul (catches any future date-format regression) |
| `interval_empty.csv` | 148 B | NZ ICP 0000005438UN02B, 2025-04-01 → 2025-07-31 | "No data is available" sentinel |

Files are CRLF, no BOM, double-quoted string columns, exactly as BidEnergy emits them.

### `tests/unit/parsers/optima/test_interval.py` — fixture-driven regression tests

Add 4 tests reading the real fixtures above:

```python
FIXTURE_DIR = Path(__file__).parent.parent.parent / "fixtures" / "optima_interval"

class TestIntervalParserOnRealFixtures:
    def test_au_single_day(self) -> None:
        path = str(FIXTURE_DIR / "interval_au_single_day.csv")
        result = interval_parser(path, "error_log")
        assert len(result) == 1
        sensor_id, df = result[0]
        assert sensor_id == "Optima_2002105104"
        assert list(df.columns) == ["E1_kWh", "B1_kWh"]
        assert len(df) == 48  # 30-min intervals × 24 h
        assert df.index.min() == pd.Timestamp("2025-05-01 00:00:00")

    def test_nz_icp_single_day(self) -> None:
        path = str(FIXTURE_DIR / "interval_nz_single_day.csv")
        result = interval_parser(path, "error_log")
        assert len(result) == 1
        sensor_id, df = result[0]
        # NZ uses ICP — alphanumeric — parser must not assume numeric NMI.
        assert sensor_id == "Optima_0000010008MQCB6"
        assert len(df) == 48

    def test_au_four_months_spans_distinct_months(self) -> None:
        path = str(FIXTURE_DIR / "interval_au_4month.csv")
        result = interval_parser(path, "error_log")
        sensor_id, df = result[0]
        assert sensor_id == "Optima_2002105104"
        # 4 months × ~30 days × 48 intervals ≈ 5856 rows
        assert len(df) > 5000
        assert sorted(df.index.month.unique().tolist()) == [4, 5, 6, 7]

    def test_empty_data_sentinel_returns_empty_list(self) -> None:
        """Regression: BidEnergy returns 148-byte 'No data is available' CSV when
        site has no data; parser must return [] (not raise UFuncTypeError)."""
        path = str(FIXTURE_DIR / "interval_empty.csv")
        result = interval_parser(path, "error_log")
        assert result == []
```

These 4 tests replace the synthetic multi-month regression test from the previous spec revision. Real fixtures catch the full set of byte-level quirks (CRLF, quoting, ICP vs NMI) that synthetic data would miss.

## Equivalence with current NEM12 flow (much stronger now)

After this change, **the parsing/Hudi-write path for interval data is byte-identical to the NEM12 path** that nem12_exporter currently feeds. Both routes:

| What | NEM12 (before) | Interval (after) |
|---|---|---|
| Source endpoint | `/ExportIntervalUsageProfileNem12` | `POST /exportdailyusagecsv` |
| Source file format | NEM12 (100/200/300/900 records) | 12-column flat CSV in ZIP |
| Parser | `nem_adapter` | `interval_parser` (pandas) |
| Parser output | `[(NMI, DataFrame[E1_kWh, B1_kWh, ...])]` | `[(f"Optima_{NMI}", DataFrame[E1_kWh, B1_kWh])]` |
| file_processor channel mapping | `<col>.split("_")[0]` → `<NMI>-<suffix>` | **same** |
| Hudi sensor IDs | `Optima_<NMI>-E1`, `Optima_<NMI>-B1` (from mappings) | **same** |
| Source-file destination after success (with data) | `newP/` | **same** |
| Source-file destination after success (no data) | `newIrrevFiles/` | **same** (parser returns `[]` for sentinel) |
| Hudi unit | `kwh` | **same** |
| Timestamp resolution | 30 min | **same** |
| Hudi record key | `sensorId + ts` | **same** (upserts cleanly during cutover overlap) |

Athena queries, SkySpark mappings, and downstream dashboards require **no changes**.

## Error Handling

| Scenario | Behaviour |
|---|---|
| `project` missing in event | Return `{"statusCode": 400, "body": "Missing required parameter: project"}`. No retry. |
| `get_project_config(project)` returns None | Return `{"statusCode": 400, "body": "No credentials configured for project: <p>"}`. |
| `get_sites_for_project(project)` returns `[]` | Return `{"statusCode": 404, "body": "No sites found for project <p>"}`. |
| `login_bidenergy(...)` returns None | Return `{"statusCode": 401, "body": "Failed to authenticate with BidEnergy"}`. EventBridge default retry policy (max 1 retry, 60s delay) will re-attempt. |
| `start_date > end_date` (after resolution) | Return `{"statusCode": 400, ...}`. Defense-in-depth assertion. |
| Per-site download HTTP 401/403/404/timeout/connection error | Per-site fail; counted; does not abort the run. |
| **Per-site response is HTML (POST/Content-Type missed)** | Per-site fail; first 2 bytes ≠ `b"PK"` OR `Content-Type` contains `text/html` → reject; logged with response preview. |
| **Per-site response is "No data is available" CSV** (148 B, ~25% of sites observed empirically) | **Treated as success.** `extract_first_csv` returns the bytes verbatim; processor uploads to S3. `result["success"] = True`, `result["empty_data"] = True`. Parser detects sentinel (`len==1` and `Date` is NaN), returns `[]`, file_processor routes source to `newIrrevFiles/` (zero `file_neptune_ids`). |
| **ZIP parse failure** (corrupted, unexpected format — never observed in 8 samples) | Per-site fail; `result["error"] = "zip parse"`; logged. |
| **CSV header mismatch downstream** (parser pandas raises) | file_processor moves source to `newParseErr/`; surfaced via existing CloudWatch parse-error log group. |
| S3 PUT failure for individual site | Per-site fail; `result["error"] = "s3"`. Does not abort the run. |
| Final tally has any errors | Return `statusCode 207` (Multi-Status), nem12/demand convention. |

EventBridge sees 200/207/4xx all as successful invocations. Operational signal comes from existing CloudWatch alarm template (`optima-interval-exporter-errors` → SNS `sbm-ingester-alerts`).

## Configuration Surface

### Environment variables (set in Terraform)

```
# From local.optima_common_env (already defined for nem12/billing/demand):
BIDENERGY_BASE_URL              = https://app.bidenergy.com
OPTIMA_CONFIG_TABLE             = sbm-optima-config
OPTIMA_BUNNINGS_USERNAME/PASSWORD/CLIENT_ID
OPTIMA_RACV_USERNAME/PASSWORD/CLIENT_ID

# Specific to optima-interval-exporter (mirrors demand block):
POWERTOOLS_SERVICE_NAME = optima-interval-exporter
S3_UPLOAD_BUCKET        = sbm-file-ingester
S3_UPLOAD_PREFIX        = newTBP/
OPTIMA_DAYS_BACK        = 1
OPTIMA_MAX_WORKERS      = 20
```

`OPTIMA_DAYS_BACK` and `OPTIMA_MAX_WORKERS` are set explicitly even though `optima_shared/config.py` already defaults them to `"1"` and `"20"` — kept for parity with nem12/demand and IaC visibility.

**Explicitly NOT set:** `OPTIMA_<PROJECT>_COUNTRIES` (per-site `country` from DynamoDB instead, mirroring nem12/demand).

### DynamoDB schema (no change)

`sbm-optima-config` shared with all four exporters (nem12, billing, demand, interval).

### EventBridge schedules (changes)

**New**:
```
optima-bunnings-interval-daily   cron(0 14 * * ? *)   tz Australia/Sydney
optima-racv-interval-daily       cron(0 14 * * ? *)   tz Australia/Sydney
```

**Disabled** (commented out in `optima_exporter.tf` with revival instructions):
```
optima-bunnings-nem12-daily      DISABLED 2026-05-06
optima-racv-nem12-daily          DISABLED 2026-05-06
```

The 14:00 slot used by the disabled NEM12 schedules is reused by the new interval schedules. `optima-demand-exporter` continues at 14:30 (unchanged), giving 30-minute stagger between the two daily exporters.

## Testing

Unit tests for the new exporter mirror `tests/unit/optima_exporter/demand_exporter/`:

| Test file | Coverage |
|---|---|
| `test_app.py` | Lambda handler routes `event["project"]` → `process_export`; rejects missing project with 400; defaults forward correctly. |
| `test_downloader.py` | `download_interval_zip` POST URL + Content-Type + body construction; 200 ZIP happy path; 200 ZIP wrapping "No data is available" CSV is uploaded verbatim; 200 HTML response → None; 200 ZIP parse failure → None; 401/403/404/500/timeout/connection-error all return None; date format `DD MMM YYYY`; cookie header. |
| `test_processor.py` | `get_date_range`; `process_site` (success / empty data / download fail / S3 fail); `process_export` happy path with mocked DynamoDB+S3+login; inverted dates → 400; auth fail → 401; missing config → 400; missing sites → 404; partial failure → 207; single-NMI mode. |
| `test_uploader.py` | 4 tests copied verbatim from `test_demand_exporter` uploader (logger.service = `optima-interval-exporter`). |

**Parser tests** (`tests/unit/parsers/optima/test_interval.py`):
- Existing tests continue to use `create_optima_csv` synthetic ISO-date fixtures — no changes needed.
- Add 4 new tests in a `TestIntervalParserOnRealFixtures` class that read the real BidEnergy CSV samples committed under `tests/unit/fixtures/optima_interval/` (see Detailed Component Behaviour above). These cover: AU happy path, NZ ICP, multi-month spanning Apr/May/Jun/Jul, and the "No data is available" empty-data sentinel.

**Coverage target:** ≥90% per lefthook pre-push gate (actual aim ~95%).

**Manual smoke test post-deploy:**

1. `aws lambda invoke --function-name optima-interval-exporter --payload '{"project":"bunnings","nmi":"Optima_4102026418"}' --cli-binary-format raw-in-base64-out --region ap-southeast-2 /tmp/out.json` — expect `success_count: 1`.
2. `aws s3 ls s3://sbm-file-ingester/newTBP/ | grep "interval_NMI#OPTIMA_4102026418"` — exactly one file.
3. `aws logs tail /aws/lambda/sbm-files-ingester --since 2m --region ap-southeast-2` — expect normal pandas-path processing log lines (no `demand_written`-style direct-Hudi log; instead expect channel mapping + write_row entries).
4. `aws s3 ls s3://sbm-file-ingester/newP/ s3://sbm-file-ingester/newIrrevFiles/ | grep "interval_NMI#OPTIMA_4102026418"` — file routed to `newP/` (site has data) or `newIrrevFiles/` (empty-data sentinel).
5. After Glue job runs: Athena query for `Optima_4102026418-E1` sensor_id with new timestamps — expect ~48 new rows for 1-day interval (or upserts of existing rows if NEM12 wrote earlier same day).

## Infrastructure (Terraform)

### Step 1: Remove 5 stale `moved` blocks (lines 468-491)

These were generated during April's rename of `interval_exporter` → `nem12_exporter`. Now that we are creating new resources with the original `optima_interval_exporter` names, the moved blocks must be removed first or `terraform plan` errors with duplicate-address conflicts.

Delete entirely from `terraform/optima_exporter.tf`:

```hcl
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

### Step 2: Add new resources

```hcl
# ================================
# Lambda 4: Interval Exporter (NEW primary interval data source)
# ================================

resource "aws_cloudwatch_log_group" "optima_interval_exporter" {
  name              = "/aws/lambda/optima-interval-exporter"
  retention_in_days = var.log_retention_days
  tags              = local.common_tags
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

  tracing_config { mode = "PassThrough" }

  depends_on = [aws_cloudwatch_log_group.optima_interval_exporter]
  tags       = local.common_tags
}

resource "aws_scheduler_schedule" "optima_bunnings_interval" {
  name       = "optima-bunnings-interval-daily"
  group_name = "default"
  flexible_time_window { mode = "OFF" }
  schedule_expression          = "cron(0 14 * * ? *)"
  schedule_expression_timezone = "Australia/Sydney"
  target {
    arn      = aws_lambda_function.optima_interval_exporter.arn
    role_arn = aws_iam_role.optima_scheduler_role.arn
    input    = jsonencode({ project = "bunnings" })
  }
}

resource "aws_scheduler_schedule" "optima_racv_interval" {
  name       = "optima-racv-interval-daily"
  group_name = "default"
  flexible_time_window { mode = "OFF" }
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
  period              = 3600
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Optima interval exporter Lambda errors"
  dimensions = {
    FunctionName = aws_lambda_function.optima_interval_exporter.function_name
  }
  alarm_actions = [data.aws_sns_topic.sbm_alerts.arn]
  ok_actions    = [data.aws_sns_topic.sbm_alerts.arn]
  tags          = local.common_tags
}
```

### Step 3: Update `optima_scheduler_invoke_lambda` policy

Replace the existing `Resource` list (currently 3 ARNs) with the 4-Lambda version:

```hcl
Resource = [
  aws_lambda_function.optima_nem12_exporter.arn,
  aws_lambda_function.optima_billing_exporter.arn,
  aws_lambda_function.optima_demand_exporter.arn,
  aws_lambda_function.optima_interval_exporter.arn,   # NEW
]
```

### Step 4: Disable NEM12 schedules

Replace the existing `aws_scheduler_schedule.optima_bunnings_nem12` and `aws_scheduler_schedule.optima_racv_nem12` resource blocks with commented-out versions:

```hcl
# === DISABLED 2026-05-06 ===
# Replaced by optima-interval-exporter (uses POST /BuyerReport/exportdailyusagecsv).
# Lambda function + log group + alarm intentionally kept for manual invoke / backup / debug.
# To re-enable: uncomment these two resource blocks + run terraform apply.
#
# resource "aws_scheduler_schedule" "optima_bunnings_nem12" {
#   name       = "optima-bunnings-nem12-daily"
#   group_name = "default"
#   flexible_time_window { mode = "OFF" }
#   schedule_expression          = "cron(0 14 * * ? *)"
#   schedule_expression_timezone = "Australia/Sydney"
#   target {
#     arn      = aws_lambda_function.optima_nem12_exporter.arn
#     role_arn = aws_iam_role.optima_scheduler_role.arn
#     input    = jsonencode({ project = "bunnings" })
#   }
# }
# resource "aws_scheduler_schedule" "optima_racv_nem12" {
#   ... (mirror of above with project = "racv") ...
# }
```

### Expected `terraform plan` summary

```
Plan: 5 to add, 1 to change, 2 to destroy.
  + aws_cloudwatch_log_group.optima_interval_exporter
  + aws_lambda_function.optima_interval_exporter
  + aws_scheduler_schedule.optima_bunnings_interval
  + aws_scheduler_schedule.optima_racv_interval
  + aws_cloudwatch_metric_alarm.optima_interval_errors
  ~ aws_iam_role_policy.optima_scheduler_invoke_lambda  (in-place: add 4th ARN)
  - aws_scheduler_schedule.optima_bunnings_nem12       (commented out — intentional)
  - aws_scheduler_schedule.optima_racv_nem12           (commented out — intentional)
```

The 2 destroys remove only EventBridge schedules; the `optima-nem12-exporter` Lambda function, log group, and error alarm remain for manual invoke / backup / debug. The `moved` blocks are also gone (Step 1) — Terraform should not reference them anywhere in plan output.

### CI/CD policy update (manual step)

Add `arn:aws:lambda:ap-southeast-2:318396632821:function:optima-interval-exporter` to `sbm-ingester-cicd-policy` v10 `LambdaUpdateFunctions` Resource list (current default version is v9 after demand exporter was added). Procedure documented in `sbm-ingester/CLAUDE.md` ("Manual Sync: CI/CD IAM Policy"). Failure mode if skipped: deploy fails with `AccessDeniedException: lambda:UpdateFunctionCode`.

### GitHub Actions workflow update

`.github/workflows/main.yml`:

1. In the `Build Optima Exporter Lambda` step (currently has 4 `cp -r` lines: `optima_shared`, `nem12_exporter`, `billing_exporter`, `demand_exporter`), add a 5th `cp -r` line:
   ```yaml
   cp -r src/functions/optima_exporter/interval_exporter build/optima_exporter/
   ```
2. In the `Upload Optima Exporter & Refresh` step (currently has 3 `update-function-code` blocks: nem12, billing, demand), add a 4th block:
   ```yaml
   aws lambda update-function-code \
     --function-name optima-interval-exporter \
     --s3-bucket gega-code-deployment-bucket \
     --s3-key sbm-files-ingester/optima_exporter.zip \
     --publish
   ```

The `optima_exporter.zip` artefact is shared by all four Optima Lambdas (nem12, billing, demand, interval) — single build, four function updates.

## Risk & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Cutover gap: nem12 disabled, interval not yet running | Low | Low (1-day data gap at most) | Apply Terraform AFTER GitHub Actions deploy succeeds — schedules remain in old state until both deploy + apply complete. |
| Cutover overlap: both nem12 and interval write same Hudi sensors during the window between code deploy and Terraform apply | High (intentional) | None (Hudi upserts by sensorId+ts) | Acceptable; the second writer's value wins, both are effectively the same data. |
| Empty-data CSVs (~25% of daily site responses) flood `newIrrevFiles/` | High (intentional) | None | Weekly archiver moves them to `archived/<week>/` after 7 days. 148 bytes × 532 sites × 365 days × 0.25 = ~7 MB/year — negligible. |
| BidEnergy session timeout mid-run for large project (Bunnings 477 sites) | Low | Medium | 900s Lambda timeout vs ~50s expected runtime (20 workers) leaves 17× headroom. Single-NMI re-invoke supported via `event.nmi`. Demand exporter has run for 7 days at this volume without rate-limit responses — carry-over assumption. |
| Forgetting to update `sbm-ingester-cicd-policy` v10 whitelist | Medium | High (deploy blocked) | Pre-merge checklist + this spec explicitly calls it out + `CLAUDE.md` documents the procedure. |
| Forgetting to remove the 5 stale `moved` blocks before adding new resources | Medium | High (terraform plan errors out) | Spec Step 1 explicitly calls this out as the FIRST Terraform action. Plan should be inspected for "duplicate resource" errors and aborted if found. |
| Existing `interval_parser` lacks a filename gate | Low | Low | The dispatcher (`non_nem_parsers.py`) tries parsers in order and catches exceptions — wrong-format files raise inside `pd.read_csv` and the dispatcher continues. Adding a defensive filename gate is a separate cleanup spec (out of scope here). |
| `%b` is locale-dependent for `format_date_for_url` URL building | Low | Low (only impacts non-English dev locales) | Lambda runs on `C.UTF-8` (verified); CI runners same. If a developer with a non-English `LC_TIME` runs tests locally they may see e.g. "Mai" — would need locale override in test setup. Documented in downloader docstring. |
| DST transition day (annual, early Oct in Sydney) | Annual | Low (1-hour gap or duplicate slot per site/year) | Hudi upserts by `sensorId+ts` so duplicates merge cleanly. Behaviour will be observed on first October run; no proactive handling needed. |
| 13 pre-existing partial mapping gaps (9 AU no-solar B1 + 3 RACV E1 + 1 RACV B1) | Existing | None | Same behaviour as today's NEM12 path — those rows route to `newIrrevFiles/`. No regression. |

## Open Questions

1. **Should `format_date_for_url` be promoted to `optima_shared/`?** Currently duplicated in nem12, demand, and now interval downloaders. Recommendation: keep duplicated for this spec, **add a follow-up task** to promote into `optima_shared/dates.py` once the interval exporter is live and stable.

2. **Should `uploader.py` be promoted to `optima_shared/`?** Same trade-off. Recommendation: keep duplicated; cleanup is a separate spec.

## Out-of-Scope Follow-ups

- Promote shared helpers (`format_date_for_url`, `upload_to_s3`) into `optima_shared/`.
- Add a defensive filename gate to `interval_parser` to make it self-protecting against format mismatches.
- Eventually delete `optima-nem12-exporter` Lambda function and code (after several weeks of confidence in the new interval source) — separate spec.
- Migrate Bunnings billing parser to use the `Optima_<NMI>` prefix convention (long-standing inconsistency, separate spec).
- Consider unified `optima-exporter` Lambda with subcommands (`nem12 | billing | demand | interval`) — defer until operational pain warrants it.
