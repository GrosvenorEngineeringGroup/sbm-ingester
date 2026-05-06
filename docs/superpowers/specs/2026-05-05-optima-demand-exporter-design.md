# Optima Demand Exporter — Design Spec

**Status:** Draft
**Date:** 2026-05-05
**Owner:** zeyu
**Related:** [`2026-05-05-optima-demand-profile-parser-design.md`](2026-05-05-optima-demand-profile-parser-design.md)

## Problem

The newly deployed `demand_parser` (in `shared/parsers/optima/demand.py`) consumes BidEnergy "Demand Profile" CSV files and writes `kW`/`kVa`/`Power Factor` per-NMI Hudi rows. But there is currently no automated mechanism to drop those CSVs into `s3://sbm-file-ingester/newTBP/`. Operators have been triggering manual exports through the BidEnergy UI.

We need a daily automated exporter, scheduled the same way as `optima-nem12-exporter` and `optima-billing-exporter`, that downloads the Demand Profile CSV for every site in `sbm-optima-config` and uploads the file to `newTBP/` for the existing ingestion pipeline to pick up.

## Goal

Add a new Lambda `optima-demand-exporter` that mirrors `optima-nem12-exporter` in structure, packaging, and operational behavior, but hits the BidEnergy Demand Profile endpoint and produces files the existing `demand_parser` already accepts.

## Non-Goals

- Modifying `demand_parser` (already in production, accepts the format the new exporter produces).
- Migrating Bunnings billing parser to use the `Optima_<NMI>` prefix (separate spec).
- Real-time / on-demand exports (this is a scheduled batch job).
- Email notifications on success or failure (reuses existing CloudWatch alarm pattern).

## Constraints

- Must reuse `optima_shared/` modules (`auth.py`, `config.py`, `dynamodb.py`) — single source of truth for credentials, DynamoDB lookups, BidEnergy login.
- Must follow `optima-nem12-exporter` patterns (return `statusCode 200/207/4xx`, never raise on per-site failures, parallel via `ThreadPoolExecutor`).
- Must mirror `optima-nem12-exporter` packaging — bundled into the same `optima_exporter.zip` build artifact (already shared by `optima-nem12-exporter` and `optima-billing-exporter`).
- Must update the manually-managed `sbm-ingester-cicd-policy` IAM whitelist to add the new Lambda ARN (otherwise GitHub Actions deploy will fail with `AccessDeniedException`).

## Verified Facts (cross-checked against running code & live BidEnergy)

1. **Endpoint** (verified 2026-05-05 via `agent-browser` against RACV Noosa Resort, NMI `Optima_3117512760`):
   ```
   GET https://app.bidenergy.com/BuyerReport/DemandProfilePartial
       ?isCsv=true
       &start=01 Apr 2026                  (URL-encoded space; format "DD MMM YYYY")
       &end=30 Apr 2026
       &filter.SiteIdStr=<UUID>
       &filter.SiteStatus=Active
       &filter.commodities=Electricity
       &filter.countrystr=AU                (or NZ)
   Response:
     Content-Type: application/vnd.csv
     Content-Disposition: attachment; filename="<Project> demand profile.csv"
     Body (UTF-8): 7 metadata rows + blank + header + interval rows
                   Header: Business Unit, Identifier, Identifier Type, ReadingDateTime,
                           E, kW, kVa, Power Factor, Site Name
   ```
   Returned 143,088 interval rows for one RACV site spanning 2018-03-07 onward.

2. **Existing `demand_parser` accepts this CSV unchanged.** The header `Identifier, ReadingDateTime, kW, kVa, Power Factor` columns it consumes are all present. No rewrite of CSV body required (unlike the NEM12 200-record `Optima_` prefix rewrite — `demand_parser` itself prepends `Optima_` when looking up Neptune mappings).

3. **Country support is per-site, sourced from DynamoDB.** `nem12_exporter/processor.py:222` uses `site.get("country", "AU")`. There is **no** `OPTIMA_<PROJECT>_COUNTRIES` env var filter on nem12 (that env var exists only for `billing_exporter`). The new demand exporter must follow the same pattern.

4. **Filename gate compatibility verified.** `demand_parser` filename gate is `"demand profile" in path.name.lower().replace("_", " ")`. The new filename `optima_bunnings_demand_profile_NMI#<NMI>_<start>_<end>_<ts>.csv` lowercases + underscore-to-space transforms to `optima bunnings demand profile nmi#...csv` — contains substring `"demand profile"` ✅.

5. **Some sites have no demand data.** RACV site `e8e007ae-9d9b-4f19-91e7-b3bb006790b3` returned the page without data; with `isCsv=true` BidEnergy returns a CSV body containing the literal sentinel `No data found`. The user explicitly requires these CSVs to also be uploaded to S3 for audit retention.

## Architecture

```
EventBridge Scheduler (cron 14:30 Sydney, per project)
   ├── optima-bunnings-demand-daily       → input {"project":"bunnings"}
   └── optima-racv-demand-daily            → input {"project":"racv"}
              ↓
      optima-demand-exporter Lambda
        (Python 3.13, 256 MB, 900s, shared `getIdFromNem12Id-role-153b7a0a` IAM role)
              ↓
        1. config = optima_shared.config.get_project_config(project)
        2. sites  = optima_shared.dynamodb.get_sites_for_project(project)
              # returns ALL sites; per-site `country` field used (no env var filter)
        3. cookies = optima_shared.auth.login_bidenergy(...)
              # if None → return {"statusCode": 401, ...}
        4. ThreadPoolExecutor(max_workers=OPTIMA_MAX_WORKERS=20):
              for each site:
                csv_bytes = downloader.download_demand_csv(cookies, site, start, end)
                # csv_bytes is uploaded EVEN if it contains "No data found" (audit)
                uploader.upload_to_s3(csv_bytes, filename)
              ↓
      s3://sbm-file-ingester/newTBP/optima_<project>_demand_profile_NMI#<NMI>_<start>_<end>_<ts>.csv
              ↓
      (existing pipeline) S3 → SQS → sbm-files-ingester → demand_parser → Hudi
```

## Components

```
src/functions/optima_exporter/demand_exporter/
├── __init__.py
├── app.py              # Lambda handler; same shape as nem12_exporter/app.py
├── downloader.py       # download_demand_csv() — hits DemandProfilePartial?isCsv=true
├── processor.py        # process_export() — orchestrates per-project export
└── uploader.py         # upload_to_s3() — same shape as nem12_exporter/uploader.py
```

**Reused unchanged:**

- `optima_shared/auth.py` — `login_bidenergy()` cookie session
- `optima_shared/config.py` — `get_project_config()`, `BIDENERGY_BASE_URL`, `S3_UPLOAD_BUCKET`, `S3_UPLOAD_PREFIX`, `OPTIMA_DAYS_BACK`, `MAX_WORKERS`
- `optima_shared/dynamodb.py` — `get_sites_for_project()`, `get_site_by_nmi()`

### Differences from `nem12_exporter`

| Aspect | `nem12_exporter` | `demand_exporter` |
|---|---|---|
| BidEnergy endpoint | `/BuyerReport/ExportIntervalUsageProfileNem12` | `/BuyerReport/DemandProfilePartial` |
| Required query param | `nmi=` (empty) | (none extra) |
| Date format in URL | `DD Mmm YYYY` (e.g. `01 Apr 2026`) | `DD Mmm YYYY` (same) |
| Response body | NEM12 (header `100,...`) | Plain CSV (`Commodities:` metadata header) |
| NMI rewrite in body | Yes — `_prefix_nmi_in_nem12()` adds `Optima_` to 200 records | **No** — parser computes lookup key itself |
| File prefix | `optima_<proj>_NMI#` | `optima_<proj>_demand_profile_NMI#` |
| Upload-on-no-data | N/A (NEM12 always has structure) | **Yes** (sentinel CSV uploaded for audit) |
| Schedule (Sydney) | 14:00 daily | 14:30 daily (staggered to avoid concurrent BidEnergy load) |

## Detailed Component Behaviour

### `app.py` — Lambda handler

Pure pass-through to `process_export(project, nmi, start_date, end_date)`. Same event contract as `nem12_exporter`:

```python
event = {
    "project": "bunnings" | "racv",   # required
    "nmi": "Optima_4001260599",        # optional — single-NMI mode
    "startDate": "2026-04-29",         # optional ISO
    "endDate": "2026-04-30",           # optional ISO
}
```

Returns `{"statusCode": 400, "body": "Missing required parameter: project"}` if `project` is missing. Otherwise delegates to `processor.process_export`.

### `processor.py`

Implements `process_export(project, nmi=None, start_date=None, end_date=None) -> dict`. Mirrors `nem12_exporter/processor.py` with three behavioural deltas:

1. **No NMI prefix rewrite.** The `nmi_prefix` argument from `nem12_exporter.downloader.download_csv` is removed; demand CSVs pass through unchanged.

2. **Always upload, even on "No data found".** Contains the sentinel-detection logic; logs `demand_no_data_found` at INFO with `{project, nmi, country}` and tags the upload with a metric so audits can filter empty exports. The S3 upload still happens.

3. **Per-site `country` from DynamoDB.** Uses `site.get("country", "AU")` exactly like `nem12_exporter/processor.py:222`. No `COUNTRIES` env-var filter, no env-driven country list.

Date-range resolution logic (default = yesterday only, governed by `OPTIMA_DAYS_BACK`) is copied verbatim from `nem12_exporter/processor.py:22-40,166-189` (same defaults, same defense-in-depth assertions, same 4xx return on inverted ranges).

### `downloader.py`

```python
def download_demand_csv(
    cookies: str,
    site_id_str: str,
    start_date: str,           # ISO YYYY-MM-DD
    end_date: str,             # ISO YYYY-MM-DD
    project: str,
    nmi: str,
    *,
    country: str = "AU",
) -> tuple[bytes, str] | None:
    ...
```

Differences vs `nem12_exporter/downloader.py:download_csv`:

- URL: `f"{BIDENERGY_BASE_URL}/BuyerReport/DemandProfilePartial"`
- Query params: drops the URL `nmi=""` parameter (the demand endpoint has no per-NMI URL param). The Python `nmi` argument is **kept** in the function signature — used only to construct the output filename, never sent in the URL.
- Body validation: primary check is `body.lstrip(b"\xef\xbb\xbf \t\r\n").startswith(b"Commodities:")` (BOM-tolerant content sniff). Falls back to `Content-Type` containing `csv` only when the sniff is inconclusive. Rejects HTML error pages exactly the same way nem12 does — `b"<!doctype" in content_start or b"<html" in content_start`.
- **No** `_prefix_nmi_in_nem12` rewrite branch.
- Filename: `f"optima_{project.lower()}_demand_profile_NMI#{nmi.upper()}_{start_date}_{end_date}_{timestamp}.csv"`
- **No-data handling:** if body contains `b"No data found"`, log `demand_no_data_in_response` at INFO and **return the bytes anyway** (caller uploads them; sentinel CSV is the audit artefact).

Date-format helper `format_date_for_url` is identical to nem12's; can be either imported from `nem12_exporter.downloader` or duplicated for module independence (see *Open Questions* below). Carries the same locale caveat as `_build_hudi_csv` in `demand.py:69` — `%b` is locale-dependent and assumes the Lambda runtime locale resolves to `en_US.UTF-8` / `C.UTF-8` (it does on AWS Lambda).

### `uploader.py`

**Copy `src/functions/optima_exporter/nem12_exporter/uploader.py` verbatim** — same `upload_to_s3()` signature, same lazy `_s3_client` singleton with `region_name="ap-southeast-2"`, same `ContentType="text/csv"` header, same logger formatting, same return type. Only change: rename the `Logger(service="optima-nem12-exporter")` to `Logger(service="optima-demand-exporter")` so log lines are routed to the correct CloudWatch log group.

Promoting this to `optima_shared/` is a follow-up cleanup task — keeping a copy keeps each Lambda module self-contained, matching nem12's current pattern.

## Data Flow

```
14:30 Sydney    EventBridge fires schedule for one project
                → Lambda invocation event = {"project": "bunnings"}
                ↓
                process_export("bunnings"):
                  date_range  = yesterday..yesterday      (OPTIMA_DAYS_BACK=1)
                  config      = OPTIMA_BUNNINGS_USERNAME/PASSWORD/CLIENT_ID env
                  sites       = DynamoDB query (project=bunnings) → ~477 sites (AU+NZ mixed)
                  cookies     = login_bidenergy(...)                    # 1 HTTP call
                  ↓
                  ThreadPoolExecutor(max_workers=20):                    # ~24 batches
                    for each site (siteIdStr, nmi, country):
                      csv_bytes = download_demand_csv(...)               # 1 HTTP per site
                      filename  = optima_bunnings_demand_profile_NMI#<NMI>_<start>_<end>_<ts>.csv
                      upload_to_s3(csv_bytes, filename)                  # 1 PUT per site
                  ↓
                  return {"statusCode": 200|207, "body": {success_count, error_count, ...}}
                ↓
                S3 ObjectCreated event on newTBP/optima_*_demand_profile_NMI#*.csv
                → SQS sbm-files-ingester-queue
                → sbm-files-ingester Lambda
                → shared.non_nem_parsers.get_non_nem_outcome(...)  picks demand_parser via filename gate
                → demand_parser writes Hudi rows to s3://hudibucketsrc/sensorDataFiles/
                → file_processor moves the source by ParserOutcome.status
                → existing Glue job picks up Hudi CSV
```

For sites with no demand meter installed, the CSV body is the BidEnergy "No data found" sentinel form. `demand_parser` detects this sentinel and returns `ParserOutcome(status="processed_empty", reason="no_data_sentinel")` without raising or writing Hudi rows. `file_processor` routes `processed_empty` source CSVs to `newP/`; valid candidate data with no mapped demand points remains `unmapped` and routes to `newIrrevFiles/`. Audit retention is provided by the file's presence in `newP/` (and its weekly `archived/<ISO-week>/` rollup by `sbm-weekly-archiver`).

## Error Handling

| Scenario | Behaviour |
|---|---|
| `project` missing in event | Return `{"statusCode": 400, "body": "Missing required parameter: project"}`. No retry. |
| `get_project_config(project)` returns None (missing env vars) | Return `{"statusCode": 400, "body": "No credentials configured for project: <p>"}`. |
| `get_sites_for_project(project)` returns `[]` | Return `{"statusCode": 404, "body": "No sites found for project <p>"}`. |
| `login_bidenergy(...)` returns None | Return `{"statusCode": 401, "body": "Failed to authenticate with BidEnergy"}`. EventBridge default retry policy (max 1 retry, 60s delay) will re-attempt. |
| `start_date > end_date` (after resolution) | Return `{"statusCode": 400, ...}`. Defense-in-depth assertion mirrored from nem12. |
| Per-site download HTTP 401/403 | Per-site fail; `result["error"] = "auth"`; counted into `error_count` but does not abort the run. (If session truly expired, all subsequent sites also fail — visible in logs.) |
| Per-site download HTTP 404 | Per-site fail; `result["error"] = "site not found"`. |
| Per-site download timeout / connection error | Per-site fail; counted; continues with next site. |
| Per-site response is HTML error page | Per-site fail; logged with response preview (matches nem12 behaviour). |
| Per-site response body contains `"No data found"` | **Treated as success.** Upload to S3. Log `demand_no_data_in_response` at INFO. `result["success"] = True`, `result["no_data"] = True`. |
| S3 PUT failure for an individual site | Per-site fail; `result["error"] = "s3"`. Does not abort the run. |
| Final tally has any errors | Return `statusCode 207` (Multi-Status), nem12 convention. |

EventBridge sees 200/207/4xx all as successful invocations (no further retries). Operational signal comes from the existing CloudWatch alarm template in `optima_exporter.tf` (added per-Lambda) and the per-Lambda CloudWatch log group.

## Configuration Surface

### Environment variables (set in Terraform)

```
# From local.optima_common_env (already defined for nem12_exporter):
BIDENERGY_BASE_URL              = https://app.bidenergy.com
OPTIMA_CONFIG_TABLE             = sbm-optima-config
OPTIMA_BUNNINGS_USERNAME/PASSWORD/CLIENT_ID
OPTIMA_RACV_USERNAME/PASSWORD/CLIENT_ID

# Specific to optima-demand-exporter (mirrors nem12 block):
POWERTOOLS_SERVICE_NAME = optima-demand-exporter
S3_UPLOAD_BUCKET        = sbm-file-ingester
S3_UPLOAD_PREFIX        = newTBP/
OPTIMA_DAYS_BACK        = 1
OPTIMA_MAX_WORKERS      = 20
```

`OPTIMA_DAYS_BACK` and `OPTIMA_MAX_WORKERS` are set explicitly even though `optima_shared/config.py` already defaults them to `"1"` and `"20"` respectively — kept in the Terraform block for parity with nem12 and to make per-Lambda tuning visible in IaC without editing source.

**Explicitly NOT set:** `OPTIMA_<PROJECT>_COUNTRIES` (per-site `country` from DynamoDB instead, mirroring `nem12_exporter`).

### DynamoDB schema (no change)

`sbm-optima-config` table is shared with `nem12_exporter` and `billing_exporter`. Existing fields used:

| Field | Type | Used by demand exporter? |
|---|---|---|
| `project` (PK) | string | yes (filter) |
| `nmi` (SK) | string | yes (filename + logging) |
| `siteIdStr` | string | yes (URL `filter.SiteIdStr`) |
| `country` | string (default `AU`) | yes (URL `filter.countrystr`) |
| `siteName` | string | not used (logged only if present) |

No schema changes — the same DynamoDB items already populated for nem12 + billing serve demand exports without any backfill.

### EventBridge schedules (new)

```
optima-bunnings-demand-daily   cron(30 14 * * ? *)   tz Australia/Sydney
optima-racv-demand-daily       cron(30 14 * * ? *)   tz Australia/Sydney
```

Both fire at 14:30 Sydney, 30 minutes after the existing `optima-*-nem12-daily` schedules (14:00). Staggering reduces concurrent load on BidEnergy and keeps log search by time-of-day clean.

## Testing

Mirror the directory structure under `tests/unit/optima_exporter/demand_exporter/`:

| Test file | Coverage |
|---|---|
| `test_app.py` | Lambda handler routes `event["project"]` → `process_export`; rejects missing project with 400. |
| `test_downloader.py` | `download_demand_csv` URL construction (incl. `isCsv=true`, `DD Mmm YYYY` date format, country switch); 200 CSV happy path; 200 with `"No data found"` body returns bytes (not None); 200 HTML error page returns None; 401/403/404/timeout/connection-error all return None. |
| `test_processor.py` | `process_export` happy path with mocked downloader/uploader; per-site `country` propagation from DynamoDB; ThreadPoolExecutor failure isolation (one site fails → others succeed); inverted date range → 400; auth failure → 401; missing config → 400; missing sites → 404. |
| `test_uploader.py` | `upload_to_s3` builds correct `Bucket`/`Key`/`Body`; returns False on ClientError. |

Existing tests reused unchanged: `optima_shared/test_auth.py`, `optima_shared/test_config.py`, `optima_shared/test_dynamodb.py` — these cover the shared modules the new exporter depends on.

**Coverage target:** ≥90% per `lefthook` pre-push gate.

**Manual smoke test post-deploy:**

1. Trigger one ad-hoc invoke: `aws lambda invoke --function-name optima-demand-exporter --payload '{"project":"racv","nmi":"Optima_3117512760"}' /tmp/out.json`
2. Verify CloudWatch log shows `demand_csv_download_successful` and `demand_uploaded_to_s3`.
3. Verify the file lands at `s3://sbm-file-ingester/newTBP/optima_racv_demand_profile_NMI#OPTIMA_3117512760_*.csv`. (Note: the DynamoDB `nmi` field is stored mixed-case as `Optima_3117512760`; the filename pattern applies `nmi.upper()` → `OPTIMA_3117512760`. The event payload uses the mixed-case DynamoDB key.)
4. Wait for `sbm-files-ingester` to consume → look for `demand_written` CloudWatch log line in `sbm-files-ingester` log group.
5. After Glue job runs: Athena query `SELECT sensorid, COUNT(*) FROM sensordata_default WHERE sensorid IN ('<kw>','<kva>','<pf>') GROUP BY sensorid` using the three sensor IDs from `data/demand_points.csv` for that NMI; expect ~48 rows each (1 day × 48 half-hour intervals).

## Infrastructure (Terraform)

Add to `terraform/optima_exporter.tf`:

```hcl
resource "aws_cloudwatch_log_group" "optima_demand_exporter" {
  name              = "/aws/lambda/optima-demand-exporter"
  retention_in_days = var.log_retention_days
  tags              = local.common_tags
}

resource "aws_lambda_function" "optima_demand_exporter" {
  function_name = "optima-demand-exporter"
  description   = "Exports Optima Demand Profile CSVs to S3 for ingestion pipeline"
  role          = data.aws_iam_role.ingester_role.arn       # shared with nem12 + billing
  handler       = "demand_exporter.app.lambda_handler"
  runtime       = "python3.13"
  timeout       = 900
  memory_size   = 256
  s3_bucket     = var.deployment_bucket
  s3_key        = "${local.lambda_s3_prefix}/optima_exporter.zip"

  environment {
    variables = merge(local.optima_common_env, {
      POWERTOOLS_SERVICE_NAME = "optima-demand-exporter"
      S3_UPLOAD_BUCKET        = "sbm-file-ingester"
      S3_UPLOAD_PREFIX        = "newTBP/"
      OPTIMA_DAYS_BACK        = "1"
      OPTIMA_MAX_WORKERS      = "20"
    })
  }

  tracing_config { mode = "PassThrough" }

  depends_on = [aws_cloudwatch_log_group.optima_demand_exporter]
  tags       = local.common_tags
}

resource "aws_scheduler_schedule" "optima_bunnings_demand" {
  name       = "optima-bunnings-demand-daily"
  group_name = "default"

  flexible_time_window { mode = "OFF" }

  schedule_expression          = "cron(30 14 * * ? *)"
  schedule_expression_timezone = "Australia/Sydney"

  target {
    arn      = aws_lambda_function.optima_demand_exporter.arn
    role_arn = aws_iam_role.optima_scheduler_role.arn
    input    = jsonencode({ project = "bunnings" })
  }
}

resource "aws_scheduler_schedule" "optima_racv_demand" {
  name       = "optima-racv-demand-daily"
  group_name = "default"

  flexible_time_window { mode = "OFF" }

  schedule_expression          = "cron(30 14 * * ? *)"
  schedule_expression_timezone = "Australia/Sydney"

  target {
    arn      = aws_lambda_function.optima_demand_exporter.arn
    role_arn = aws_iam_role.optima_scheduler_role.arn
    input    = jsonencode({ project = "racv" })
  }
}

# CloudWatch alarm — mirror existing optima_nem12_errors alarm exactly
# (period=3600, both alarm_actions and ok_actions wired to the shared SNS topic).
resource "aws_cloudwatch_metric_alarm" "optima_demand_errors" {
  alarm_name          = "optima-demand-exporter-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 3600 # 1 hour — matches optima_nem12_errors
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Optima demand exporter Lambda errors"

  dimensions = {
    FunctionName = aws_lambda_function.optima_demand_exporter.function_name
  }

  alarm_actions = [data.aws_sns_topic.sbm_alerts.arn]
  ok_actions    = [data.aws_sns_topic.sbm_alerts.arn]

  tags = local.common_tags
}
```

Update `aws_iam_role_policy.optima_scheduler_invoke_lambda` (currently at `terraform/optima_exporter.tf:290-305`) to include the new Lambda ARN. Replace the existing `Resource` list:

```hcl
# Before:
Resource = [
  aws_lambda_function.optima_nem12_exporter.arn,
  aws_lambda_function.optima_billing_exporter.arn,
]

# After:
Resource = [
  aws_lambda_function.optima_nem12_exporter.arn,
  aws_lambda_function.optima_billing_exporter.arn,
  aws_lambda_function.optima_demand_exporter.arn,
]
```

Failure mode if skipped: EventBridge fires the schedule but `lambda:InvokeFunction` returns AccessDenied; CloudWatch shows 100% scheduler invocation failures with zero Lambda invocations — silent (no alarms unless one is added on scheduler `InvocationsFailedToBeSentToDeadLetterCount`).

### CI/CD policy update (manual step)

Add `arn:aws:lambda:ap-southeast-2:318396632821:function:optima-demand-exporter` to `sbm-ingester-cicd-policy` v9 `LambdaUpdateFunctions` Resource list. Procedure documented in `sbm-ingester/CLAUDE.md` ("Manual Sync: CI/CD IAM Policy"). Failure mode if skipped: deploy fails with `AccessDeniedException: lambda:UpdateFunctionCode`.

### GitHub Actions workflow update

The single workflow file is `.github/workflows/main.yml` (not `deploy.yml`). Use string-search anchors (line numbers drift):

1. In the `optima_exporter` build step (search for `mkdir -p build/optima_exporter`), add a `cp -r` line for the new module alongside the existing `optima_shared`, `nem12_exporter`, and `billing_exporter` copies:
   ```yaml
   cp -r src/functions/optima_exporter/demand_exporter build/optima_exporter/
   ```
2. In the `optima_exporter` deploy step (search for `update-function-code --function-name optima-nem12-exporter`), add a third `update-function-code` block alongside the existing nem12 and billing ones:
   ```yaml
   aws lambda update-function-code \
     --function-name optima-demand-exporter \
     --s3-bucket gega-code-deployment-bucket \
     --s3-key sbm-files-ingester/optima_exporter.zip \
     --publish
   ```

The `optima_exporter.zip` artefact is shared by all three Optima Lambdas (nem12, billing, demand) — single build, three function updates.

## Risk & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Many sites have no demand meter → S3 fills with sentinel CSVs | High | Low (audit-required) | Parser returns `processed_empty`, so weekly archiver moves them from `newP/` to `newP/archived/<week>/`. Sentinel CSVs are < 500 bytes each; 477 × 365 = ~84 MB/year per project — negligible. |
| BidEnergy session timeout mid-run for large project (Bunnings ~477 sites × ~2s = ~16 min serial; 24 parallel batches ~50s) | Low | Medium | 900s Lambda timeout vs ~50s expected runtime (with 20 workers) leaves 17× headroom. If timeouts surface, single-NMI re-invoke supported via `event.nmi`. |
| BidEnergy rate-limits / throttles | Low | Medium | 14:30 stagger keeps demand exporter off-peak from nem12 (14:00). `OPTIMA_MAX_WORKERS` env-tunable without redeploy. |
| Forgetting to update `sbm-ingester-cicd-policy` whitelist | Medium | High (deploy blocked) | Pre-merge checklist + this spec explicitly calls it out + `CLAUDE.md` documents the procedure. |

## Open Questions

1. **Should `format_date_for_url` be promoted to `optima_shared/`?** Currently only used by `nem12_exporter/downloader.py`. Demand exporter needs the identical helper. Either:
   - **(a)** Promote to `optima_shared/dates.py` (DRY; touches nem12 imports).
   - **(b)** Duplicate the 5-line helper into `demand_exporter/downloader.py` (module independence; tiny duplication).

   Recommendation: **(b)** for this spec, **(a)** as a follow-up cleanup task — keeps the demand_exporter PR self-contained.

2. **Should `uploader.py` be promoted to `optima_shared/`?** Same trade-off as Q1. Currently nem12 keeps its own copy. Recommendation: **mirror nem12's pattern** — keep a copy in `demand_exporter/uploader.py` for now.

## Out-of-Scope Follow-ups

- Promote shared helpers (`format_date_for_url`, `upload_to_s3`) into `optima_shared/` once the demand exporter is live and stable.
- Add a Bunnings billing parser migration to use the `Optima_<NMI>` prefix convention (currently uses bare NMI — historical inconsistency).
- Consider a unified `optima-exporter` Lambda with subcommands (`nem12 | billing | demand`) instead of three separate Lambdas — defer until operational pain warrants it.
