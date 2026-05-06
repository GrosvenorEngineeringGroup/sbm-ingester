# Optima Interval Exporter — Design Spec

**Status:** Approved
**Date:** 2026-05-06
**Owner:** zeyu
**Related:**
- [`2026-05-05-optima-demand-profile-parser-design.md`](2026-05-05-optima-demand-profile-parser-design.md)
- [`2026-05-05-optima-demand-exporter-design.md`](2026-05-05-optima-demand-exporter-design.md)
- [`2026-04-13-optima-interval-exporter-nem12-migration-design.md`](2026-04-13-optima-interval-exporter-nem12-migration-design.md) — the *previous* migration that this spec partially reverses

## Problem

`optima-nem12-exporter` (deployed since April 2026) downloads NEM12 CSV files from BidEnergy's `/BuyerReport/ExportIntervalUsageProfileNem12` endpoint and feeds them through `nem_adapter` to populate `Optima_<NMI>-E1` / `Optima_<NMI>-B1` Hudi sensors. The pipeline works, but operational experience has surfaced a preference for the simpler, flatter CSV format produced by the BidEnergy SiteUsage page's "Export Interval Usage Csv" button (`POST /BuyerReport/exportdailyusagecsv`). That endpoint returns a ZIP wrapping a single 12-column per-NMI CSV, which is easier to inspect, debug, and reason about than NEM12's 100/200/300/900 record format.

We need a new Lambda `optima-interval-exporter` that becomes the **primary** interval data source while keeping the existing `optima-nem12-exporter` Lambda code intact (its EventBridge schedules will be disabled, but the function remains invocable for ad-hoc backups, debugging, or future re-enablement).

## Goal

Add `optima-interval-exporter` Lambda + supporting `interval_parser` module + Terraform/CI/CD wiring, mirroring the structure of the recently-deployed `optima-demand-exporter`. New parser persists `Usage` and `Generation` columns to the same `Optima_<NMI>-E1` and `Optima_<NMI>-B1` Hudi sensors that NEM12 was writing to (so downstream Athena queries / dashboards continue working without change).

## Non-Goals

- Modifying `nem12_exporter` code, IAM, log group, alarm, or Lambda function (all preserved; only the 2 EventBridge schedules are disabled).
- Persisting `DemandKva` (already collected by `optima-demand-exporter` as `Optima_<NMI>-demand-kva`).
- Persisting `Reactive` (no current Neptune sensor mapping; YAGNI).
- Real-time / on-demand exports (this is a scheduled batch job).
- Changing Hudi sensor IDs or adding new sensor categories — the goal is **transparent source switch**, not a new data product.

## Constraints

- Must reuse `optima_shared/` modules (`auth.py`, `config.py`, `dynamodb.py`) — single source of truth.
- Must mirror packaging — bundled into the same `optima_exporter.zip` artefact (already shared by nem12, billing, demand).
- Must update the manually-managed `sbm-ingester-cicd-policy` IAM whitelist (otherwise GitHub Actions deploy fails with `AccessDeniedException`).
- Must keep `nem12_exporter` Lambda function and code intact (only its 2 EventBridge schedules are removed via Terraform).

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
     Body: ZIP wrapping 1 CSV (file inside named without " interval data" suffix)
   ```
   - Verified for **5 Bunnings AU sites + 5 Bunnings NZ sites + 1 RACV AU site** (12 successful 200 responses, all `application/zip`).
   - **POST is required**; GET returns `text/html` (BidEnergy 14669-byte error page).
   - **`Content-Type: application/x-www-form-urlencoded` header is required**; without it, even POST returns the same HTML error page.
   - Empty result (no data for site/range): returns 22-byte ZIP (EOCD only, no CSV inside).

2. **CSV format inside ZIP** (verified, 100% byte-match against user-provided sample `Bunnings-AU-Electricity-4102026418-NMI-ENERGYAP.csv`):
   ```
   BuyerShortName,Country,Commodity,Identifier,IdentifierType,DistributorId,Date,Start Time,Usage,Generation,DemandKva,Reactive
   "Bunnings","AU","Electricity","4102026418","NMI","ENERGYAP",06 May 2025,00:00,1.6250,0.00,3.25,0.00
   ```
   - Quoted strings, `06 May 2025` space-separated date, `1.6250` 4-decimal precision.
   - NZ uses `IdentifierType=ICP` instead of `NMI` and 15-char alphanumeric identifiers.
   - 30-minute intervals (48 rows per NMI per day).

3. **DynamoDB site uniqueness** (verified via scan): all 532 NMIs (Bunnings 477 + RACV 55) have **unique `siteIdStr`**. There are no multi-NMI-per-site cases. Therefore each POST returns exactly one CSV inside the ZIP.

4. **Existing Neptune mappings** (verified via current nem12_mappings.json): every Bunnings NMI has both `Optima_<NMI>-E1` and `Optima_<NMI>-B1` keys. RACV mappings exist for known NMIs. The new parser can map directly without any Neptune backfill.

5. **`nem12_exporter` schedule disablement does not affect Lambda function**. AWS EventBridge Schedules are independent of the Lambda function they target; deleting the schedule resource leaves the function callable via `aws lambda invoke` (verified pattern from `optima-billing-exporter` which has separate weekly + monthly schedules).

## Architecture

```
EventBridge Scheduler (cron 14:00 Sydney, per project — taking the slot vacated by nem12)
   ├── optima-bunnings-interval-daily   → input {"project":"bunnings"}
   └── optima-racv-interval-daily        → input {"project":"racv"}
              ↓
      optima-interval-exporter Lambda
        (Python 3.13, 256 MB, 900s, shared `getIdFromNem12Id-role-153b7a0a` IAM role)
              ↓
        1. config = optima_shared.config.get_project_config(project)
        2. sites  = optima_shared.dynamodb.get_sites_for_project(project)
              # 532 unique siteIdStr (verified 1 NMI = 1 site)
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
      (existing pipeline) S3 → SQS → sbm-files-ingester
              ↓ filename gate "interval" + content sniff "BuyerShortName,"
      shared.parsers.optima.interval.interval_parser
              ↓ writes Hudi rows directly to s3://hudibucketsrc/sensorDataFiles/
              ↓ returns [] (signals dispatcher to NOT stream channel data)
      file moves to newIrrevFiles/  (parser returned [] — file_neptune_ids empty)
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

src/shared/parsers/optima/
└── interval.py         # NEW — interval_parser() — writes Hudi for E1+B1 directly

src/shared/non_nem_parsers.py
                        # MODIFY — register interval_parser in dispatcher list
                        # (between bunnings_billing_parser and demand_parser)

terraform/optima_exporter.tf
                        # MODIFY:
                        # - Comment out 2 nem12 schedule resources (with reason + revival instructions)
                        # - Add: 1 log group + 1 Lambda + 2 schedulers + 1 alarm (interval)
                        # - Update optima_scheduler_invoke_lambda Resource list (add 4th ARN)

.github/workflows/main.yml
                        # MODIFY:
                        # - Build step: 1 new `cp -r` for interval_exporter
                        # - Deploy step: 1 new `update-function-code` block
```

**Reused unchanged:**
- `optima_shared/auth.py` — works for Bunnings & RACV (verified)
- `optima_shared/config.py` — `get_project_config()`, `BIDENERGY_BASE_URL`, `S3_UPLOAD_BUCKET`, `S3_UPLOAD_PREFIX`, `OPTIMA_DAYS_BACK`, `MAX_WORKERS`
- `optima_shared/dynamodb.py` — `get_sites_for_project()`, `get_site_by_nmi()`

### Differences from `demand_exporter`

| Aspect | `demand_exporter` | `interval_exporter` |
|---|---|---|
| HTTP method | GET | **POST** |
| Body / params | URL query string | `application/x-www-form-urlencoded` body |
| Required fields | isCsv, start, end, filter.SiteIdStr, filter.SiteStatus, filter.commodities, filter.countrystr | **siteId, start, end** (only 3) |
| Response | Plain CSV | **ZIP wrapping single CSV** |
| Body validation | Body sniff `b"Commodities:"` | Body sniff `b"PK\x03\x04"` (ZIP magic) |
| Filename prefix | `optima_<proj>_demand_profile_NMI#` | `optima_<proj>_interval_NMI#` |
| Hudi sensors | `Optima_<NMI>-demand-{kw,kva,pf}` | `Optima_<NMI>-E1` + `Optima_<NMI>-B1` (existing) |
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
2. Empty ZIP (no CSV inside) is treated as a special case: synthesise a header-only CSV (just the `BuyerShortName,...,Reactive` line) and upload that to S3 for audit. Set `result["empty_zip"] = True`. The downstream parser will read the header, find 0 data rows, and return `[]` cleanly — file_processor routes the source to `newIrrevFiles/`.

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
    """POST /BuyerReport/exportdailyusagecsv, return raw ZIP bytes (or None on failure)."""

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
- Body validation: primary check is `body.startswith(b"PK\x03\x04")` (ZIP local file header magic) **OR** `body[:4] == b"PK\x05\x06"` (EOCD-only empty ZIP). HTML response detection identical to demand pattern.
- **No** `nmi` URL parameter (`nmi` arg used only for output filename construction).
- Filename: `f"optima_{project.lower()}_interval_NMI#{nmi.upper()}_{start_date}_{end_date}_{timestamp}.csv"`
- Empty ZIP handling: log INFO `interval_no_data_in_zip`, then `extract_first_csv` returns header-only synthesised bytes (caller still uploads).

`format_date_for_url` is identical to demand's; duplicated for module independence (deferred shared-helper extraction is in *Out-of-Scope Follow-ups*).

### `uploader.py`

**Copy `demand_exporter/uploader.py` verbatim**. Only change: rename `Logger(service="optima-demand-exporter")` → `Logger(service="optima-interval-exporter")` and adjust the module docstring.

### `interval.py` (parser)

```python
def interval_parser(file_path: str, error_file_path: str) -> ParserResult:
    """Parse Optima interval CSV; persist Usage→E1 and Generation→B1 to Hudi.

    Returns [] always (caller treats as 'no NEM12 mappings' → file moves to newIrrevFiles).
    """
```

**Filename gate**: `"interval" in Path(file_name).name.lower() and "demand" not in Path(file_name).name.lower()`. The "and not demand" guard prevents collision with future filenames that combine words.

**Content gate**: first non-empty line must equal `BuyerShortName,Country,Commodity,Identifier,IdentifierType,DistributorId,Date,Start Time,Usage,Generation,DemandKva,Reactive` (verbatim, with no quotes — verified from raw response). Reject otherwise.

**Per-row processing**:
1. Parse `Date` + `Start Time` → `datetime.strptime(f"{date} {start_time}", "%d %b %Y %H:%M")` → format as `"%Y-%m-%d %H:%M:%S"` for Hudi.
2. `nmi = row["Identifier"]` (bare NMI, e.g. `4102026418` for AU or `0000005438UN02B` for NZ).
3. **E1 (Usage)**:
   - `sensor_id_e1 = mappings.get(f"Optima_{nmi}-E1")`
   - if `sensor_id_e1` and `row["Usage"]` parses as float (validate via `float()` — but write the **raw string** to Hudi to preserve precision, matching `demand_parser`/`bunnings_billing_parser` pattern):
     - `buf.write(f"{sensor_id_e1},{ts_str},{raw_usage},kwh,{ts_str},\n")`
4. **B1 (Generation)**:
   - `sensor_id_b1 = mappings.get(f"Optima_{nmi}-B1")`
   - if `sensor_id_b1` and `float(row["Generation"]) > 0` (skip zero-export intervals to avoid noise):
     - `buf.write(f"{sensor_id_b1},{ts_str},{raw_generation},kwh,{ts_str},\n")`
5. **Skip silently** if `sensor_id` is None for either channel (some NMIs may only have E1 mapped, no B1).

**Final S3 upload**: same as `demand_parser` — direct PUT to `s3://hudibucketsrc/sensorDataFiles/interval_export_<ts>.csv`.

## Data Flow

```
14:00 Sydney    EventBridge fires schedule for one project
                → Lambda invocation event = {"project": "bunnings"}
                ↓
                process_export("bunnings"):
                  date_range  = yesterday..yesterday          (OPTIMA_DAYS_BACK=1)
                  config      = OPTIMA_BUNNINGS_USERNAME/PASSWORD/CLIENT_ID env
                  sites       = DynamoDB query → 477 sites (Bunnings AU 64 + NZ 413)
                  cookies     = login_bidenergy(...)              # 1 HTTP call
                  ↓
                  ThreadPoolExecutor(max_workers=20):              # ~24 batches
                    for each site (siteIdStr, nmi, country):
                      ┌─ download_interval_zip(cookies, siteIdStr, start, end) ──┐
                      │   POST /BuyerReport/exportdailyusagecsv                  │
                      │   Content-Type: application/x-www-form-urlencoded        │
                      │   Body: siteId=<UUID>&start=DD MMM YYYY&end=DD MMM YYYY  │
                      │   → application/zip (or HTML on auth failure)            │
                      └──────────────────────────────────────────────────────────┘
                      validate_zip(zip_bytes)                       # check PK\x03\x04 OR EOCD magic
                      csv_bytes = extract_first_csv(zip_bytes)      # synthesised header if empty
                      filename  = f"optima_bunnings_interval_NMI#{nmi.upper()}_{start}_{end}_{ts}.csv"
                      upload_to_s3(csv_bytes, filename)             # 1 PUT per site
                  ↓
                  return {"statusCode": 200|207, "body": {success_count, error_count, empty_zip_count, ...}}
                ↓
                S3 ObjectCreated event on newTBP/optima_*_interval_NMI#*.csv
                → SQS sbm-files-ingester-queue
                → sbm-files-ingester Lambda
                → shared.non_nem_parsers.get_non_nem_df(...) picks interval_parser via filename gate
                → interval_parser:
                    1. Filename gate passes ("interval" + not "demand")
                    2. Content gate: first row == "BuyerShortName,Country,..."
                    3. For each data row: write Optima_<NMI>-E1 (Usage) + Optima_<NMI>-B1 (Generation>0) to Hudi
                    4. Return []
                → file moves to newIrrevFiles/  (parser returned [] — file_neptune_ids empty)
                → existing Glue job (hourly) picks up Hudi CSV
```

For NMIs with no demand interval data on the date range (empty ZIP), the synthesised header-only CSV is still uploaded; `interval_parser` reads the header, sees zero data rows, and returns `[]` — the source file lands in `newIrrevFiles/` as audit retention, mirroring `demand_parser`'s "No data found" sentinel handling.

## Equivalence with current NEM12 flow

After this change, all sites that NEM12 was writing to continue receiving data at the same Hudi sensorIds and timestamps:

| What | NEM12 (before) | Interval (after) |
|---|---|---|
| Sensor: consumption | `Optima_<NMI>-E1` | `Optima_<NMI>-E1` (same) |
| Sensor: export | `Optima_<NMI>-B1` | `Optima_<NMI>-B1` (same) |
| Timestamp resolution | 30 min | 30 min (same) |
| Unit | kWh | kWh (same) |
| Hudi record key | `sensorId + ts` | `sensorId + ts` (same → upserts cleanly during cutover overlap) |

Athena queries, SkySpark mappings, and downstream dashboards require **no changes**.

## Error Handling

| Scenario | Behaviour |
|---|---|
| `project` missing in event | Return `{"statusCode": 400, "body": "Missing required parameter: project"}`. No retry. |
| `get_project_config(project)` returns None (missing env vars) | Return `{"statusCode": 400, "body": "No credentials configured for project: <p>"}`. |
| `get_sites_for_project(project)` returns `[]` | Return `{"statusCode": 404, "body": "No sites found for project <p>"}`. |
| `login_bidenergy(...)` returns None | Return `{"statusCode": 401, "body": "Failed to authenticate with BidEnergy"}`. EventBridge default retry policy (max 1 retry, 60s delay) will re-attempt. |
| `start_date > end_date` (after resolution) | Return `{"statusCode": 400, ...}`. Defense-in-depth assertion. |
| Per-site download HTTP 401/403 | Per-site fail; counted; does not abort the run. |
| Per-site download HTTP 404 | Per-site fail; counted; continues. |
| Per-site download timeout / connection error | Per-site fail; counted; continues. |
| **Per-site response is HTML (POST/Content-Type missed)** | Per-site fail; sniff `content-type=text/html` OR body not `b"PK\x03\x04"` / `b"PK\x05\x06"` → reject; logged with response preview. |
| **Per-site response is empty ZIP** (22 bytes, EOCD only) | **Treated as success.** `extract_first_csv` returns synthesised header-only CSV; upload to S3. Log INFO `interval_no_data_in_zip`. `result["success"] = True`, `result["empty_zip"] = True`. |
| **ZIP parse failure** (corrupted, unexpected format) | Per-site fail; `result["error"] = "zip parse"`; logged. |
| **CSV header mismatch** (parser-side gate fails) | file_processor moves source to `newParseErr/`; surfaced via existing CloudWatch parse-error log group. |
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

`sbm-optima-config` shared with all four exporters (nem12, billing, demand, interval). Uses `nmi`, `siteIdStr`, `country`, `siteName` fields — no schema additions.

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

Mirror the directory structure under `tests/unit/optima_exporter/interval_exporter/`:

| Test file | Coverage |
|---|---|
| `test_app.py` | Lambda handler routes `event["project"]` → `process_export`; rejects missing project with 400; defaults forward correctly. |
| `test_downloader.py` | `download_interval_zip` POST URL + Content-Type + body construction; 200 ZIP happy path; 200 empty ZIP returns synthesised header CSV; 200 HTML response → None; 200 ZIP parse failure → None; 401/403/404/500/timeout/connection-error all return None; date format `DD MMM YYYY`; cookie header. |
| `test_processor.py` | `get_date_range`; `process_site` (success / empty ZIP / download fail / S3 fail / country propagation); `process_export` happy path with mocked DynamoDB+S3+login; inverted dates → 400; auth fail → 401; missing config → 400; missing sites → 404; partial failure → 207; single-NMI mode. |
| `test_uploader.py` | 4 tests copied verbatim from `test_demand_exporter` uploader (logger.service = optima-interval-exporter). |

Plus parser tests under `tests/unit/parsers/optima/test_interval.py`:

| Test class | Coverage |
|---|---|
| `TestFilenameGate` | Accepts `optima_bunnings_interval_NMI#...csv`; rejects `optima_bunnings_demand_profile_NMI#...csv`; case-insensitive. |
| `TestContentGate` | Accepts the verbatim `BuyerShortName,Country,...` header; rejects unrelated headers. |
| `TestEmptyData` | Header-only CSV → returns `[]` cleanly. |
| `TestE1Persistence` | Usage > 0 → writes `Optima_<NMI>-E1` row with `unit=kwh`; preserves raw value precision (`14.3600` not `14.36`). |
| `TestB1Persistence` | Generation > 0 → writes `Optima_<NMI>-B1` row; Generation == 0 skips B1; missing B1 mapping silently skipped. |
| `TestNZIcpHandling` | NZ 15-char ICP identifier (e.g. `0000010008MQCB6`) → `Optima_<ICP>-E1` lookup works. |
| `TestUnmappedSilent` | Unmapped NMI → `unmapped_count++`, no Hudi write, no error raised. |
| `TestQuotedFields` | csv.DictReader handles `"Bunnings","AU","Electricity",...` (quoted strings) correctly. |

Existing tests reused unchanged: `optima_shared/test_*.py`.

**Coverage target:** ≥90% per lefthook pre-push gate (actual aim ~95% given the parser/exporter precedent).

**Manual smoke test post-deploy:**

1. `aws lambda invoke --function-name optima-interval-exporter --payload '{"project":"bunnings","nmi":"Optima_4102026418"}' --cli-binary-format raw-in-base64-out --region ap-southeast-2 /tmp/out.json` — expect `success_count: 1`.
2. `aws s3 ls s3://sbm-file-ingester/newTBP/ | grep "interval_NMI#OPTIMA_4102026418"` — exactly one file.
3. `aws logs tail /aws/lambda/sbm-files-ingester --since 2m --region ap-southeast-2 | grep -E "interval_written|interval_no_rows"` — expect `interval_written`.
4. `aws s3 ls s3://sbm-file-ingester/newIrrevFiles/ | grep "interval_NMI#OPTIMA_4102026418"` — file routed correctly (parser returned `[]`).
5. After Glue job runs: Athena query for `Optima_4102026418-E1` sensor_id — expect ~48 new rows (1 day × 48 half-hour intervals).

## Infrastructure (Terraform)

Add to `terraform/optima_exporter.tf`:

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

### Update `optima_scheduler_invoke_lambda` policy

Replace the existing `Resource` list with the 4-Lambda version:

```hcl
Resource = [
  aws_lambda_function.optima_nem12_exporter.arn,
  aws_lambda_function.optima_billing_exporter.arn,
  aws_lambda_function.optima_demand_exporter.arn,
  aws_lambda_function.optima_interval_exporter.arn,   # NEW
]
```

### Disable NEM12 schedules (Q1 = A)

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

The 2 destroys are expected and correct — they remove only EventBridge schedules; the `optima-nem12-exporter` Lambda function and its CloudWatch log group + error alarm remain for manual invoke / backup / debug.

### CI/CD policy update (manual step)

Add `arn:aws:lambda:ap-southeast-2:318396632821:function:optima-interval-exporter` to `sbm-ingester-cicd-policy` v10 `LambdaUpdateFunctions` Resource list. Procedure documented in `sbm-ingester/CLAUDE.md` ("Manual Sync: CI/CD IAM Policy"). Failure mode if skipped: deploy fails with `AccessDeniedException: lambda:UpdateFunctionCode`.

### GitHub Actions workflow update

`.github/workflows/main.yml`:

1. In the `Build Optima Exporter Lambda` step (search for `mkdir -p build/optima_exporter`), add a new `cp -r` line:
   ```yaml
   cp -r src/functions/optima_exporter/interval_exporter build/optima_exporter/
   ```
2. In the `Upload Optima Exporter & Refresh` step (search for `update-function-code --function-name optima-demand-exporter`), add a fourth `update-function-code` block:
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
| Empty ZIPs flood `newIrrevFiles/` | Low | Low | Same weekly archiver moves them to `archived/<week>/` after 7 days. Header-only CSV is < 200 bytes. 532 sites × 365 days × 200 bytes = ~39 MB/year — negligible. |
| BidEnergy session timeout mid-run for large project (Bunnings 477 sites) | Low | Medium | 900s Lambda timeout vs ~50s expected runtime (20 workers) leaves 17× headroom. Single-NMI re-invoke supported via `event.nmi`. |
| Forgetting to update `sbm-ingester-cicd-policy` v10 whitelist | Medium | High (deploy blocked) | Pre-merge checklist + this spec explicitly calls it out + `CLAUDE.md` documents the procedure. |
| `interval_parser` filename gate collision with future filenames | Low | Medium | Gate uses both `"interval" in name` AND `"demand" not in name` — robust against demand-related filenames. New file types should add their own positive gate; if a future filename includes both "interval" and "demand", deliberate disambiguation is required. |

## Open Questions

1. **Should `format_date_for_url` be promoted to `optima_shared/`?** Currently duplicated in nem12, demand, and now interval downloaders. Recommendation: keep duplicated for this spec, **add a follow-up task** to promote into `optima_shared/dates.py` once the interval exporter is live and stable (3 of 4 exporters then share the helper, justifying extraction).

2. **Should `uploader.py` be promoted to `optima_shared/`?** Same trade-off. Recommendation: keep duplicated; cleanup is a separate spec.

## Out-of-Scope Follow-ups

- Promote shared helpers (`format_date_for_url`, `upload_to_s3`) into `optima_shared/` once interval exporter is live and stable.
- Eventually delete `optima-nem12-exporter` Lambda function and code (after several weeks of confidence in the new interval source) — separate spec.
- Migrate Bunnings billing parser to use the `Optima_<NMI>` prefix convention (long-standing inconsistency, separate spec).
- Consider unified `optima-exporter` Lambda with subcommands (`nem12 | billing | demand | interval`) — defer until operational pain warrants it.
