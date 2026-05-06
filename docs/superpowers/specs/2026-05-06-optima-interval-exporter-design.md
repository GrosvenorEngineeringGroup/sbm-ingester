# Optima Interval Exporter — Design Spec

**Status:** Approved (revised after subagent review)
**Date:** 2026-05-06
**Owner:** zeyu
**Related:**
- [`2026-05-05-optima-demand-exporter-design.md`](2026-05-05-optima-demand-exporter-design.md)
- [`2026-04-13-optima-interval-exporter-nem12-migration-design.md`](2026-04-13-optima-interval-exporter-nem12-migration-design.md) — the *previous* migration that this spec partially reverses

## Problem

`optima-nem12-exporter` (deployed since April 2026) downloads NEM12 CSV files from BidEnergy's `/BuyerReport/ExportIntervalUsageProfileNem12` endpoint and feeds them through `nem_adapter` to populate `Optima_<NMI>-E1` / `Optima_<NMI>-B1` Hudi sensors. The pipeline works, but operational experience has surfaced a preference for the simpler, flatter CSV format produced by the BidEnergy SiteUsage page's "Export Interval Usage Csv" button (`POST /BuyerReport/exportdailyusagecsv`). That endpoint returns a ZIP wrapping a single 12-column per-NMI CSV, which is easier to inspect, debug, and reason about than NEM12's 100/200/300/900 record format.

We need a new Lambda `optima-interval-exporter` that becomes the **primary** interval data source while keeping the existing `optima-nem12-exporter` Lambda code intact (its EventBridge schedules will be disabled, but the function remains invocable for ad-hoc backups, debugging, or future re-enablement).

## Discoveries (during reviewer pass + end-to-end test)

### Existing parser, dispatcher, and tests are mostly in place

- [`src/shared/parsers/optima/interval.py`](../../../src/shared/parsers/optima/interval.py) — `interval_parser()` already implemented using pandas; reads the 12-column CSV (`Identifier`, `Date`, `Start Time`, `Usage`, `Generation`, ...) and returns `[(f"Optima_{NMI}", DataFrame[E1_kWh, B1_kWh])]`
- [`src/shared/non_nem_parsers.py:12,29`](../../../src/shared/non_nem_parsers.py) — `interval_parser` already imported and registered in dispatcher
- [`tests/unit/parsers/optima/test_interval.py`](../../../tests/unit/parsers/optima/test_interval.py) — test suite already exists (but uses ISO date fixtures — see bug below)

The `file_processor` channel-mapping path at [`src/functions/file_processor/app.py:457-475`](../../../src/functions/file_processor/app.py) already handles the parser's output:

```python
for col in df.columns:                            # e.g. col = "E1_kWh"
    suffix = col.split("_")[0]                    # → "E1"
    if suffix not in NMI_DATA_STREAM_COMBINED:    # passes (E1, B1, etc. allowed)
        continue
    monitor_point_name = f"{nmi}-{suffix}"        # → "Optima_<NMI>-E1"
    neptune_id = nem12_mappings.get(monitor_point_name)
    # ...write Hudi rows...
```

And `nem12_mappings.json` confirmed (verified 2026-05-06) contains keys in the exact format `Optima_<NMI>-E1` and `Optima_<NMI>-B1`.

### Latent bug discovered: existing parser fails on multi-month real data

End-to-end test against the real BidEnergy CSV `Bunnings-AU-Electricity-4102026418-NMI-ENERGYAP.csv` (year-long range, multiple months) **fails** with:

```
ValueError: time data "01 Jun 2025 00:00" doesn't match format "%d %B %Y %H:%M",
            at position 1248
```

Root cause at [`interval.py:24`](../../../src/shared/parsers/optima/interval.py):

```python
raw_df["Interval_Start"] = pd.to_datetime(
    raw_df["Date"] + " " + raw_df["Start Time"]
)   # No format= → pandas auto-infers
```

When the file's first month is "May" (which is identical for `%b` and `%B`), pandas infers `format="%d %B %Y %H:%M"` (full-month-name). It then chokes on subsequent rows like `"01 Jun 2025"` because abbreviated `Jun` ≠ full-name `June`. The bug is invisible to the existing tests because the test fixture (`create_optima_csv` in `tests/unit/conftest.py:326`) generates dates in ISO format (`2024-01-01`), not in BidEnergy's `DD Mmm YYYY` format.

**Therefore this spec covers**:
1. The new EXPORTER Lambda (~80% of the work).
2. A 1-line fix to the existing parser to pass `format="%d %b %Y %H:%M"` explicitly.
3. Updating `create_optima_csv` to produce CSV in the actual BidEnergy date format so this class of bug surfaces in CI.
4. A regression test that spans multiple months with abbreviation-only month names (`Jun`, `Jul`, `Aug`, `Sep`, `Oct`, `Nov`, `Dec`).

## Goal

Add `optima-interval-exporter` Lambda + Terraform/CI/CD wiring, mirroring the structure of the recently-deployed `optima-demand-exporter`. The new Lambda downloads ZIP-wrapped CSVs from `POST /BuyerReport/exportdailyusagecsv`, extracts the inner CSV, and uploads to `s3://sbm-file-ingester/newTBP/` — where the existing `interval_parser` (after a 1-line date-format fix) consumes them via the standard `file_processor` pandas/DataFrame path.

## Non-Goals

- Modifying `nem12_exporter` code, IAM, log group, alarm, or Lambda function (all preserved; only the 2 EventBridge schedules are disabled).
- Modifying `non_nem_parsers.py` dispatcher or `file_processor` (already wired).
- Replacing `interval_parser` — only the 1-line date-format fix is in scope; pandas/DataFrame architecture unchanged.
- Persisting `DemandKva` (already collected by `optima-demand-exporter`) or `Reactive` (no Neptune mapping; YAGNI).
- Real-time / on-demand exports (this is a scheduled batch job).

## Constraints

- Must reuse `optima_shared/` modules (`auth.py`, `config.py`, `dynamodb.py`).
- Must mirror packaging — bundled into the same `optima_exporter.zip` artefact (already shared by nem12, billing, demand).
- Must update the manually-managed `sbm-ingester-cicd-policy` IAM whitelist (otherwise GitHub Actions deploy fails with `AccessDeniedException`).
- Must keep `nem12_exporter` Lambda function and code intact (only its 2 EventBridge schedules are removed via Terraform).
- **Must remove pre-existing Terraform `moved` blocks** for `optima_interval_exporter` (lines 468-491 of `terraform/optima_exporter.tf`) — left over from the April rename. Otherwise creating new resources with the same name will conflict.

## Verified Facts (cross-checked against running code & live BidEnergy)

1. **Endpoint** (verified 2026-05-06 via `agent-browser`):
   ```
   POST https://app.bidenergy.com/BuyerReport/exportdailyusagecsv
   Content-Type: application/x-www-form-urlencoded
   Cookie: <session cookies>
   Body: siteId=<UUID>&start=<DD MMM YYYY>&end=<DD MMM YYYY>

   Response (success):
     Content-Type: application/zip
     Content-Disposition: attachment; filename="<Buyer>-<Country>-Electricity-<NMI>-<NMI|ICP>-<DistributorId> interval data.zip"
     Body: ZIP wrapping 1 CSV
   ```
   - Verified for **5 Bunnings AU + 5 Bunnings NZ + 1 RACV AU site** (12 successful 200 responses, all `application/zip`).
   - **POST is required**; GET returns `text/html` (BidEnergy 14669-byte error page).
   - **`Content-Type: application/x-www-form-urlencoded` header is required**; without it, even POST returns the same HTML error page.
   - Empty result (no data for site/range): returns 22-byte ZIP (EOCD only, no CSV inside).

2. **CSV format inside ZIP** (verified, 100% byte-match against user-provided sample):
   ```
   BuyerShortName,Country,Commodity,Identifier,IdentifierType,DistributorId,Date,Start Time,Usage,Generation,DemandKva,Reactive
   "Bunnings","AU","Electricity","4102026418","NMI","ENERGYAP",06 May 2025,00:00,1.6250,0.00,3.25,0.00
   ```
   - 30-minute intervals (48 rows per NMI per day).
   - NZ uses `IdentifierType=ICP`, AU uses `NMI` — `interval_parser` doesn't care (treats `Identifier` as opaque string).

3. **Existing `interval_parser` consumes this format with one fix.** Reading `src/shared/parsers/optima/interval.py` shows it ingests the 12-column BidEnergy CSV correctly except for the date column: `pd.to_datetime` is called without a `format=` argument, which causes failure on year-long real data spanning multiple months (see Discoveries above). After adding `format="%d %b %Y %H:%M"`, the parser handles real BidEnergy CSVs verbatim.

4. **DynamoDB site uniqueness** (verified via scan): all 532 NMIs (Bunnings 477 + RACV 55) have **unique `siteIdStr`**. There are no multi-NMI-per-site cases. Each POST returns exactly one CSV inside the ZIP.

5. **Existing Neptune mappings** (verified via current `nem12_mappings.json`): every Bunnings NMI has both `Optima_<NMI>-E1` and `Optima_<NMI>-B1` keys. Sample for NMI `4102026418`: `Optima_4102026418-E1` and `Optima_4102026418-B1` both present.

6. **`file_processor` already handles the parser's `(NMI, DataFrame[E1_kWh, B1_kWh])` output.** Lines 457-475 of `file_processor/app.py` extract the channel suffix from the column name (`E1_kWh` → `E1`), build the lookup key (`f"{nmi}-{suffix}"` = `Optima_<NMI>-E1`), and write Hudi rows with unit derived from the column's `_kWh` suffix.

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
                        # MODIFY (1 line): add format="%d %b %Y %H:%M" to pd.to_datetime call

tests/unit/conftest.py
                        # MODIFY: update create_optima_csv to emit BidEnergy date format
                        #   (DD Mmm YYYY) instead of ISO (YYYY-MM-DD)

tests/unit/parsers/optima/test_interval.py
                        # MODIFY: add regression test for multi-month data spanning
                        #   abbreviation-only months (Jun/Jul/.../Dec)

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
| Body validation | Body sniff `b"Commodities:"` | Body sniff `b"PK\x03\x04"` (or `b"PK\x05\x06"` for empty ZIP) |
| Filename prefix | `optima_<proj>_demand_profile_NMI#` | `optima_<proj>_interval_NMI#` |
| Parser path | Custom `demand_parser` (writes Hudi directly, returns `[]`) | **Existing `interval_parser` (pandas DataFrame path through file_processor)** |
| Source-file destination | `newIrrevFiles/` (parser returns `[]`) | **`newP/` (parser returns DataFrames; file_processor maps to neptune_ids)** |
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
2. Empty ZIP handling lives in `extract_first_csv` (see downloader). Processor sets `result["empty_zip"] = True` (analogous to demand's `result["no_data"]`) so the return body can include `empty_zip_count`. The synthesised header-only CSV is uploaded to S3 normally; the parser will then read the header and produce an empty DataFrame, and `file_processor` will route the source to `newP/` with zero `file_neptune_ids` — same harmless terminal state.

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

    Performs body validation: accepts only responses whose body starts with the
    ZIP local file header magic (PK\\x03\\x04) or the EOCD-only empty-ZIP magic
    (PK\\x05\\x06). HTML responses (auth lost, missing Content-Type, etc.) → None.
    """

def extract_first_csv(zip_bytes: bytes) -> bytes:
    """Return the bytes of the single CSV inside the ZIP. If the ZIP is empty
    (EOCD only, 22 bytes — no CSV entries), return a synthesised header-only
    CSV so the upload + parser path stays uniform.

    Raises BadZipFile if the input is not a valid ZIP.
    """

def format_date_for_url(date_str: str) -> str:
    """ISO YYYY-MM-DD → 'DD MMM YYYY' (e.g., '06 May 2025'). Locale-dependent (%b)."""
```

Differences vs `demand_exporter/downloader.py`:

- URL: `f"{BIDENERGY_BASE_URL}/BuyerReport/exportdailyusagecsv"`
- HTTP method: `requests.post(url, data={"siteId": ..., "start": ..., "end": ...}, headers={"Cookie": cookies}, timeout=300)` — `data=` (dict) auto-sets `Content-Type: application/x-www-form-urlencoded` and URL-encodes the body.
- Body validation: ZIP magic bytes (`PK\x03\x04` or `PK\x05\x06`) instead of `Commodities:` header sniff. HTML rejection identical to demand pattern.
- **No** `nmi` URL parameter (`nmi` arg used only for output filename construction).
- Filename: `f"optima_{project.lower()}_interval_NMI#{nmi.upper()}_{start_date}_{end_date}_{timestamp}.csv"`
- Empty-ZIP synthesis: `extract_first_csv` is the single point of responsibility. When `zipfile.ZipFile(BytesIO(zip_bytes)).namelist() == []`, log INFO `interval_no_data_in_zip` and return the bytes of a header-only CSV: `b"BuyerShortName,Country,Commodity,Identifier,IdentifierType,DistributorId,Date,Start Time,Usage,Generation,DemandKva,Reactive\n"`.

### `uploader.py`

**Copy `demand_exporter/uploader.py` verbatim** (same `upload_to_s3()` signature including optional `bucket` and `prefix` overrides, same `_s3_client` singleton with `region_name="ap-southeast-2"`, same `ContentType="text/csv"`, same logger formatting). Only change: rename `Logger(service="optima-demand-exporter")` → `Logger(service="optima-interval-exporter")` and adjust the module docstring.

### `shared/parsers/optima/interval.py` — 1-line bug fix

Find:
```python
raw_df["Interval_Start"] = pd.to_datetime(raw_df["Date"] + " " + raw_df["Start Time"])
```

Replace with:
```python
raw_df["Interval_Start"] = pd.to_datetime(
    raw_df["Date"] + " " + raw_df["Start Time"],
    format="%d %b %Y %H:%M",
)
```

The explicit `format=` ensures pandas uses abbreviated month names (`%b` = `Jan`, `Feb`, ..., `Dec`) consistently, eliminating the auto-inference branch that crashed on multi-month data. No other parser logic changes.

### `tests/unit/conftest.py:create_optima_csv` — fixture format alignment

Find (current ISO date emission, lines ~352-356):
```python
row: dict = {
    "Identifier": identifier,
    "Date": t.strftime("%Y-%m-%d"),
    "Start Time": t.strftime("%H:%M"),
}
```

Replace with (BidEnergy production format):
```python
row: dict = {
    "Identifier": identifier,
    "Date": t.strftime("%d %b %Y"),     # e.g. "01 Jan 2024"
    "Start Time": t.strftime("%H:%M"),
}
```

This causes the existing test suite to exercise the same date format the production parser will see. After this change + the parser format fix, all existing `test_interval.py` tests should still pass (they assert structural properties like column presence and value extraction, not date string formats). Verify by running the full test suite as part of implementation.

### `tests/unit/parsers/optima/test_interval.py` — multi-month regression test

Add a new test inside `class TestIntervalParser` that exercises the bug we found:

```python
def test_handles_multiple_months_with_abbreviated_names(self, temp_directory: str) -> None:
    """Regression: parser must not auto-infer date format from a single-month preamble.

    The first BidEnergy data we tested (year-long range starting in May) crashed at
    the May→Jun boundary because pandas inferred %B (full month name, "May" matches)
    from the preamble and then failed on "Jun" (which is only %b).
    """
    from datetime import datetime, timedelta
    from pathlib import Path

    # Span 8 months starting from May → covers May, Jun, Jul, Aug, Sep, Oct, Nov, Dec.
    # All months except May are abbreviation-only (full name differs).
    rows = []
    base = datetime(2025, 5, 1, 0, 0, 0)
    for i in range(8 * 30 * 48):  # 8 months × ~30 days × 48 half-hour intervals
        t = base + timedelta(minutes=30 * i)
        rows.append({
            "BuyerShortName": "Bunnings",
            "Country": "AU",
            "Commodity": "Electricity",
            "Identifier": "TEST_NMI",
            "IdentifierType": "NMI",
            "DistributorId": "TEST",
            "Date": t.strftime("%d %b %Y"),
            "Start Time": t.strftime("%H:%M"),
            "Usage": 1.0,
            "Generation": 0.0,
            "DemandKva": 2.0,
            "Reactive": 0.1,
        })
    filepath = str(Path(temp_directory) / "multi_month.csv")
    pd.DataFrame(rows).to_csv(filepath, index=False)

    with patch("shared.non_nem_parsers.logger"):
        from shared.parsers.optima.interval import interval_parser
        result = interval_parser(filepath, "error_log")

    assert len(result) == 1
    nmi, df = result[0]
    assert nmi == "Optima_TEST_NMI"
    # Must include rows from BOTH May (start) and December (end-ish).
    assert df.index.min().month == 5
    assert df.index.max().month >= 11
```

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
| Source-file destination after success | `newP/` | **same** |
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
| **Per-site response is HTML (POST/Content-Type missed)** | Per-site fail; sniff `content-type=text/html` OR body not `b"PK\x03\x04"` / `b"PK\x05\x06"` → reject; logged with response preview. |
| **Per-site response is empty ZIP** (22 bytes, EOCD only) | **Treated as success.** `extract_first_csv` returns synthesised header-only CSV bytes; processor uploads to S3. Log INFO `interval_no_data_in_zip`. `result["success"] = True`, `result["empty_zip"] = True`. Parser reads header, produces empty DataFrame, file_processor routes source to `newP/` with zero `file_neptune_ids` (still counted as processed). |
| **ZIP parse failure** (corrupted, unexpected format) | Per-site fail; `result["error"] = "zip parse"`; logged. |
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
| `test_downloader.py` | `download_interval_zip` POST URL + Content-Type + body construction; 200 ZIP happy path; 200 empty ZIP returns synthesised header CSV; 200 HTML response → None; 200 ZIP parse failure → None; 401/403/404/500/timeout/connection-error all return None; date format `DD MMM YYYY`; cookie header. |
| `test_processor.py` | `get_date_range`; `process_site` (success / empty ZIP / download fail / S3 fail); `process_export` happy path with mocked DynamoDB+S3+login; inverted dates → 400; auth fail → 401; missing config → 400; missing sites → 404; partial failure → 207; single-NMI mode. |
| `test_uploader.py` | 4 tests copied verbatim from `test_demand_exporter` uploader (logger.service = `optima-interval-exporter`). |

**Parser tests already exist** (`tests/unit/parsers/optima/test_interval.py`); changes required:
- Verify all existing tests still pass after the `create_optima_csv` fixture is updated to emit BidEnergy date format (no test should be format-dependent — they assert structural properties).
- Add `test_handles_multiple_months_with_abbreviated_names` regression test (see section above) to prevent re-introduction of the auto-inference date bug.

**Coverage target:** ≥90% per lefthook pre-push gate (actual aim ~95%).

**Manual smoke test post-deploy:**

1. `aws lambda invoke --function-name optima-interval-exporter --payload '{"project":"bunnings","nmi":"Optima_4102026418"}' --cli-binary-format raw-in-base64-out --region ap-southeast-2 /tmp/out.json` — expect `success_count: 1`.
2. `aws s3 ls s3://sbm-file-ingester/newTBP/ | grep "interval_NMI#OPTIMA_4102026418"` — exactly one file.
3. `aws logs tail /aws/lambda/sbm-files-ingester --since 2m --region ap-southeast-2` — expect normal pandas-path processing log lines (no `demand_written`-style direct-Hudi log; instead expect channel mapping + write_row entries).
4. `aws s3 ls s3://sbm-file-ingester/newP/ | grep "interval_NMI#OPTIMA_4102026418"` — file routed to `newP/` (parser returned non-empty `file_neptune_ids`).
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
| Empty ZIPs flood `newP/` | Low | Low | Same weekly archiver moves them to `archived/<week>/` after 7 days. Header-only CSV is < 200 bytes. 532 sites × 365 days × 200 bytes = ~39 MB/year — negligible. |
| BidEnergy session timeout mid-run for large project (Bunnings 477 sites) | Low | Medium | 900s Lambda timeout vs ~50s expected runtime (20 workers) leaves 17× headroom. Single-NMI re-invoke supported via `event.nmi`. |
| Forgetting to update `sbm-ingester-cicd-policy` v10 whitelist | Medium | High (deploy blocked) | Pre-merge checklist + this spec explicitly calls it out + `CLAUDE.md` documents the procedure. |
| Forgetting to remove the 5 stale `moved` blocks before adding new resources | Medium | High (terraform plan errors out) | Spec Step 1 explicitly calls this out as the FIRST Terraform action. Plan should be inspected for "duplicate resource" errors and aborted if found. |
| Existing `interval_parser` lacks a filename gate | Low | Low | The dispatcher (`non_nem_parsers.py`) tries parsers in order and catches exceptions — wrong-format files raise inside `pd.read_csv` and the dispatcher continues. Adding a defensive filename gate is a separate cleanup spec (out of scope here). |
| Date-format fix breaks an existing test that assumes ISO dates | Medium | Low | Run full test suite after the fixture + parser change; any failing tests are using brittle date assertions and should be updated to assert structural properties instead. The end-to-end CI gate catches this before deploy. |
| Updated fixture changes test data shape and exposes other latent bugs | Low | Medium | Same mitigation: full test suite must pass before commit. Treat any new failures as legitimate bugs found by improved fixture realism. |

## Open Questions

1. **Should `format_date_for_url` be promoted to `optima_shared/`?** Currently duplicated in nem12, demand, and now interval downloaders. Recommendation: keep duplicated for this spec, **add a follow-up task** to promote into `optima_shared/dates.py` once the interval exporter is live and stable.

2. **Should `uploader.py` be promoted to `optima_shared/`?** Same trade-off. Recommendation: keep duplicated; cleanup is a separate spec.

## Out-of-Scope Follow-ups

- Promote shared helpers (`format_date_for_url`, `upload_to_s3`) into `optima_shared/`.
- Add a defensive filename gate to `interval_parser` to make it self-protecting against format mismatches.
- Eventually delete `optima-nem12-exporter` Lambda function and code (after several weeks of confidence in the new interval source) — separate spec.
- Migrate Bunnings billing parser to use the `Optima_<NMI>` prefix convention (long-standing inconsistency, separate spec).
- Consider unified `optima-exporter` Lambda with subcommands (`nem12 | billing | demand | interval`) — defer until operational pain warrants it.
