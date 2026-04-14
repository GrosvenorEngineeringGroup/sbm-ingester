# Bunnings Billing Parser Integration — Design Spec

**Date:** 2026-04-14
**Status:** Draft, pending review
**Author:** Claude (brainstorming session with user)
**Scope:** SBM Ingester — auto-process Bunnings BidEnergy "Usage and Spend Report" into Hudi data lake

---

## 1. Background and Problem

### 1.1 Current State

The `optima-billing-exporter` Lambda runs monthly on the 1st (7:00 AM Sydney) for both `bunnings` and `racv` projects. It logs into BidEnergy and triggers asynchronous "Monthly Usage and Spend" report generation. Reports are emailed to a registered mailbox, auto-forwarded, and uploaded to `s3://sbm-file-ingester/newTBP/`.

From there, the standard ingester pipeline (`newTBP/` → SQS → `sbm-files-ingester` Lambda → `non_nem_parsers.get_non_nem_df()`) dispatches to a parser based on filename/format.

### 1.2 Gaps

1. **No Bunnings parser.** `optima_usage_and_spend_to_s3()` (in `src/shared/non_nem_parsers.py`) only matches the RACV report (`RACV-Usage and Spend Report`) and performs a simple S3 copy to `s3://gegoptimareports/usageAndSpendReports/racvUsageAndSpend.csv` for downstream consumers (e.g., SkySpark). The Bunnings report (`Bunnings-Usage and Spend Report`) has no matching parser — no data flows to Hudi.

2. **Scheduler timing.** Running on the 1st of the month is too early for retailers to have finalized actual bills. Observed on 2026-04-14: the April 1 run for VCCCLG0019 returned only Estimated values (e.g., `Estimated Peak=26331.36`, `Peak=0`, Retailer=empty). A manual re-trigger on April 14 returned Actual values (`Peak=31105.09`, Retailer=`ZenEnergy`). The ~17% usage gap and ~$1,000 spend gap between estimated and actual is too large to ignore.

3. **Half-automated pipeline.** Even for RACV, there is no Lambda/job that converts billing CSV → Hudi format. Existing scripts (`scripts/billing_csv_to_hudi.py`, `scripts/import_billing_csv.py`) must be run manually. After the 4/1 run, the March estimated data was written to Hudi manually; the 4/14 actual data will not be auto-reconciled.

4. **Aurora PostgreSQL path deprecated.** `scripts/import_billing_csv.py` targets Aurora (`sites`, `meters`, `bills` tables). The user has decided this path is no longer needed; it should be removed.

### 1.3 Goals

- Change the `optima-bunnings-billing-monthly` scheduler from the 1st to the **7th of the month** (Sydney 07:00), giving retailers more time to finalize bills. RACV scheduler remains unchanged.
- Add a **new Bunnings-only parser** that converts the BidEnergy billing CSV into Hudi-format sensor rows and writes directly to `s3://hudibucketsrc/sensorDataFiles/`, without modifying any upstream (`file_processor`) or downstream (Glue ETL) components.
- Remove the Aurora PostgreSQL billing import code paths.
- Reuse the existing `nem12_mappings.json` for NMI+field → Neptune point ID lookup (no new mapping store).

### 1.4 Non-Goals

- No change to the RACV billing path (`optima_usage_and_spend_to_s3`); SkySpark and other consumers still read from `gegoptimareports/usageAndSpendReports/racvUsageAndSpend.csv`.
- No change to the Glue ETL script (`src/glue/hudi_import/script.py`) or the Hudi table schema.
- No change to the `file_processor` Lambda's dispatch logic (`get_non_nem_df` signature unchanged).
- No re-processing of historical reports. First run under the new logic begins with the 2026-05-07 Bunnings scheduler trigger.

---

## 2. High-Level Design

### 2.1 Architecture

```
                  EventBridge Scheduler (cron 0 7 7 * ? * Australia/Sydney)
                                    │
                                    ▼
                     optima-billing-exporter Lambda
                      (existing, unchanged logic)
                                    │
                                    ▼
                  BidEnergy async report generation
                                    │
                                    ▼
           Email → optimaBunningsEnergy@verdeos.com
                 → auto-forward → S3 uploader tool
                                    │
                                    ▼
                 s3://sbm-file-ingester/newTBP/
                    *.Bunnings-Usage and Spend Report.csv
                                    │
                                    ▼
               S3 ObjectCreated → SQS → sbm-files-ingester Lambda
                     (existing, unchanged)
                                    │
                                    ▼
                NEM12 stream/batch parsers fail (not NEM12 format)
                                    │
                                    ▼
                non_nem_parsers.get_non_nem_df() dispatcher
                                    │
                                    ▼
               bunnings_usage_and_spend_parser()  ← NEW
                                    │
                ┌───────────────────┼───────────────────┐
                ▼                   ▼                   ▼
      Read UTF-16 LE CSV    Load nem12_mappings   Write sensor CSV
      Skip 7 metadata rows  cached lazy load      to hudibucketsrc/
      Parse DictReader      from existing         sensorDataFiles/
                            s3://sbm-file-         billing_export_*.csv
                            ingester/
                            nem12_mappings.json
                                    │
                                    ▼
                              return []
                                    │
                                    ▼
         file_processor moves original file to newIrrevFiles/
         (existing behaviour for parsers that return empty data)
                                    │
                                    ▼
                Glue job DataImportIntoLake (hourly trigger)
                          (existing, unchanged)
                                    │
                                    ▼
                   Hudi table: default.sensordata_default
```

### 2.2 Key Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Parser approach | Side-effect + return `[]` | Mirrors existing `optima_usage_and_spend_to_s3`; avoids hacking the `{letter}{digit}` suffix convention in `file_processor`; zero upstream changes. |
| Scope | Bunnings only; RACV untouched | User explicit: RACV has special downstream usage (SkySpark) and must not be modified. |
| Point ID lookup | Reuse `nem12_mappings.json` (10,971 billing keys already present) | Zero new infrastructure. Already synced hourly by `sbm-files-ingester-nem12-mappings-to-s3`. Key format `{NMI}-billing-{field}` already matches. |
| Mapping access in parser | Module-level lazy cache, loads from S3 on first use | Parser signature `(file_name, error_file_path)` cannot be changed without touching upstream. One extra S3 GET per Lambda cold start is acceptable (~1MB). |
| Output format | Hudi sensor CSV with header `sensorId,ts,val,unit,its,quality` | Same schema `file_processor.DirectCSVWriter` already writes; Glue ETL already consumes this. |
| Scheduler change | Bunnings only: `cron(0 7 1 * ? *)` → `cron(0 7 7 * ? *)` | Gives retailer ~7 days to finalize bills. Observation (VCCCLG0019 March 2026) shows estimated→actual transition happens within that window. RACV scheduler unchanged (out of scope). |

### 2.3 Rejected Alternatives

1. **Change parser to return 23 separate (p:point_id, df) tuples with fake `E1_kWh`-style column names.** Works mechanically (column suffix check at `file_processor:459` would pass), but hacky: `billing-peak-usage` semantically is not an "E1" channel, and the convention would become misleading for future readers. Side-effect approach is honest about what the parser does.
2. **DynamoDB table for billing point ID mapping.** Rejected. The data is static reference data (~2,300 rows for Bunnings), accessed in bulk per run, and already exists in `nem12_mappings.json`. DynamoDB adds infrastructure without benefit.
3. **New dedicated S3 CSV (`billing_point_ids.csv`) on S3.** Rejected for the same reason — duplicates data already in `nem12_mappings.json`.
4. **New Lambda with S3 event trigger on `newIrrevFiles/`.** Rejected. Violates the principle of one ingestion entry point (`newTBP/`) and breaks `newIrrevFiles/` semantics (it is the "no Neptune mapping" archive bucket).
5. **Change `get_non_nem_df` signature to accept `nem12_mappings`.** Rejected per user constraint: do not modify upstream.

---

## 3. Detailed Design

### 3.1 New Parser: `bunnings_usage_and_spend_parser`

**Location:** `src/shared/non_nem_parsers.py`

**Signature:** matches existing parser contract

```python
def bunnings_usage_and_spend_parser(
    file_name: str, error_file_path: str
) -> ParserResult:
```

**Behaviour:**

1. **Filename guard.** If `"Bunnings-Usage and Spend Report"` is not in `file_name`, raise `Exception("Not Bunnings Usage and Spend File")`. The dispatcher will try the next parser.
2. **Decode UTF-16 LE.** Read the file as bytes, decode `utf-16-le`, strip BOM. Normalize line endings (`\r\n` and `\r` → `\n`).
3. **Skip 7 metadata rows.** BidEnergy CSV header structure:
   ```
   Row 1: Commodities:,"Electricity"
   Row 2: Status:,"Active"
   Row 3: Country:, Australia
   Row 4: Start:,01 May 2025
   Row 5: End:,30 Apr 2026
   Row 6: (blank)
   Row 7: (blank)
   Row 8: BuyerShortName,Country,Commodity,Identifier,IdentifierType,...  ← header
   Row 9+: data rows
   ```
   Use `csv.DictReader(lines[7:])`.
4. **Lazy-load `nem12_mappings.json`** via module-level cache (see §3.2).
5. **For each data row:**
   - Extract `Identifier` (NMI), `Date` (e.g., `"Mar 2026"`), `Usage Measurement Unit`, `Spend Currency`.
   - Convert date: `datetime.strptime(date_str, "%b %Y")` → first-of-month timestamp `"YYYY-MM-01 00:00:00"`.
   - For each of the 23 billing fields in `CSV_FIELD_MAPPING`:
     - Build lookup key: `f"{nmi}-{billing_suffix}"` (e.g., `"VCCCLG0019-billing-peak-usage"`).
     - Look up `sensor_id` in `nem12_mappings`. If missing, skip silently (mapping not yet synced).
     - Read raw CSV value. If blank or whitespace-only, skip.
     - Pick unit: usage fields → `usage_unit` (from `Usage Measurement Unit`, lowercased); spend fields → `spend_unit` (from `Spend Currency`, lowercased).
     - Append row: `sensor_id,ts,val,unit,ts,<blank quality>\n`
6. **Write CSV to Hudi source bucket.**
   - Bucket: `hudibucketsrc` (already in Lambda IAM role)
   - Key: `sensorDataFiles/billing_export_{YYYYMMDDHHMMSSffffff}.csv` (microsecond-precision UTC timestamp via `datetime.utcnow().strftime("%Y%m%d%H%M%S%f")`). Microsecond precision eliminates collision risk when multiple billing files are processed in the same Lambda invocation; filename remains human-readable and monotonically sortable.
   - Body: buffer content with the `sensorId,ts,val,unit,its,quality` header
   - Uses the Lambda's default boto3 credentials (no profile).
7. **Return `[]`.** Tells `file_processor` there is no interval data for this file. `file_processor` will move the original CSV to `newIrrevFiles/` (existing behaviour).

**Error handling:**

- If `nem12_mappings.json` load fails (S3 404, network), re-raise — the file will be moved to `newParseErr/`. A structured log is emitted.
- If no rows match (all NMIs missing mapping), **skip the S3 PUT** and log at WARN: `"Bunnings billing: zero mapped rows produced"`. Glue should not see a header-only CSV (avoids empty-file edge cases in upsert).
- If date parsing fails on any row, skip that row and log at WARN level. Do not abort the whole file.
- If S3 PUT to `hudibucketsrc` fails, raise — file moves to `newParseErr/` and the next scheduled run can retry (by re-triggering the exporter manually).

### 3.2 Mapping Cache: `_get_nem12_mappings`

**Location:** `src/shared/non_nem_parsers.py`

```python
_nem12_mappings_cache: dict | None = None

def _get_nem12_mappings() -> dict:
    """
    Lazily load nem12_mappings.json from S3 and cache at module level.
    Populated on first non-NEM parser invocation that needs it; reused
    for the life of the Lambda container (cold start to shutdown).
    """
    global _nem12_mappings_cache
    if _nem12_mappings_cache is None:
        obj = boto3.client("s3").get_object(
            Bucket="sbm-file-ingester",
            Key="nem12_mappings.json",
        )
        _nem12_mappings_cache = json.loads(obj["Body"].read())
    return _nem12_mappings_cache
```

**Why module-level cache:**

- Parser signature cannot be extended without upstream changes.
- Lambda warm starts reuse the module, so cache survives across invocations until the container dies.
- Only ~1 MB JSON, one GET per cold start. Trivial cost and latency.

**Cache invalidation:**

- Not needed. New mappings synced hourly by `sbm-files-ingester-nem12-mappings-to-s3`. Worst case, a new Bunnings store opened in the last hour is missing its mapping — its billing fields are skipped (unmatched NMI lookup returns None). The next monthly billing run will catch it up automatically.

### 3.3 CSV Field Mapping

Hardcoded in `non_nem_parsers.py` alongside the parser:

```python
# (CSV column name, billing suffix in nem12_mappings key, unit source)
CSV_FIELD_MAPPING: list[tuple[str, str, str]] = [
    ("Peak",                              "billing-peak-usage",                        "usage"),
    ("OffPeak",                           "billing-off-peak-usage",                    "usage"),
    ("Shoulder",                          "billing-shoulder-usage",                    "usage"),
    ("Total Usage",                       "billing-total-usage",                       "usage"),
    ("Total GreenPower",                  "billing-total-greenpower-usage",            "usage"),
    ("Estimated Peak",                    "billing-estimated-peak-usage",              "usage"),
    ("Estimated OffPeak",                 "billing-estimated-off-peak-usage",          "usage"),
    ("Estimated Shoulder",                "billing-estimated-shoulder-usage",          "usage"),
    ("Total Estimated Usage",             "billing-total-estimated-usage",             "usage"),
    ("Total Estimated GreenPower",        "billing-total-estimated-greenpower-usage",  "usage"),
    ("Energy Charge",                     "billing-energy-charge",                     "spend"),
    ("Total Network Charge",              "billing-network-charge",                    "spend"),
    ("Environmental Charge",              "billing-environmental-charge",              "spend"),
    ("Metering Charge",                   "billing-metering-charge",                   "spend"),
    ("Other Charge",                      "billing-other-charge",                      "spend"),
    ("Total Spend",                       "billing-total-spend",                       "spend"),
    ("GreenPower Spend",                  "billing-greenpower-spend",                  "spend"),
    ("Estimated Energy Charge",           "billing-estimated-energy-charge",           "spend"),
    ("Estimated Network Charge",          "billing-estimated-network-charge",          "spend"),
    ("Estimated Environmental Charge",    "billing-estimated-environmental-charge",    "spend"),
    ("Estimated Metering Charge",         "billing-estimated-metering-charge",         "spend"),
    ("Estimated Other Charge",            "billing-estimated-other-charge",            "spend"),
    ("Total Estimated Spend",             "billing-total-estimated-spend",             "spend"),
]
```

Total: 23 fields. Suffix values must match `nem12_mappings.json` keys exactly — verified against the current JSON (`VAAA000266-billing-peak-usage` style).

### 3.4 Dispatcher Registration

**Location:** `src/shared/non_nem_parsers.py:190` (within `get_non_nem_df`)

```python
parsers = [
    noosa_solar_parser,
    envizi_vertical_parser_water,
    envizi_vertical_parser_electricity,
    racv_elec_parser,
    optima_usage_and_spend_to_s3,          # RACV — unchanged
    bunnings_usage_and_spend_parser,       # NEW — Bunnings
    optima_parser,
    envizi_vertical_parser_water_bulk,
    green_square_private_wire_schneider_comx_parser,
]
```

The new parser is placed immediately after the RACV parser. Order matters only for performance (faster-rejecting parsers first); both Bunnings and RACV parsers reject by filename within a few bytes of comparison, so position is flexible.

### 3.5 Scheduler Change

**File:** `terraform/optima_exporter.tf`

Change **only** `optima-bunnings-billing-monthly`:

```hcl
resource "aws_scheduler_schedule" "optima_bunnings_billing" {
  name       = "optima-bunnings-billing-monthly"
  group_name = "default"

  flexible_time_window { mode = "OFF" }

  # OLD: schedule_expression = "cron(0 7 1 * ? *)"
  schedule_expression = "cron(0 7 7 * ? *)"   # 7th of month, 07:00 Sydney
  schedule_expression_timezone = "Australia/Sydney"
  state = "ENABLED"

  target {
    arn      = aws_lambda_function.optima_billing_exporter.arn
    role_arn = aws_iam_role.optima_scheduler.arn
    input    = jsonencode({ project = "bunnings" })
    retry_policy {
      maximum_event_age_in_seconds = 86400
      maximum_retry_attempts       = 185
    }
  }
}
```

`optima-racv-billing-monthly` is **not** modified — it stays on the 1st.

### 3.6 Deletions

| Path | Reason |
|---|---|
| `scripts/import_billing_csv.py` | Aurora path deprecated. |
| `scripts/billing_csv_to_hudi.py` | Superseded by parser. |
| `scripts/fetch_billing_point_ids.py` | Redundant: `nem12_mappings.json` already contains billing point IDs. |
| `scripts/generate_billing_points_csv.py` | Redundant (same reason). |
| `scripts/import_billing_points.py` | Aurora-related; remove if it targets Aurora only. Inspect before delete. |
| `data/billing_point_ids.csv` | Unused after refactor. |
| `data/billing_points.csv` | Unused after refactor. |
| `data/billing_hudi_preview.csv` | Unused. |
| `tests/unit/test_billing_csv_to_hudi.py` | Script removed; parser tests replace it. |
| `tests/unit/test_import_billing_csv.py` | Script removed. |
| Aurora SQLModel definitions (`sites`, `meters`, `bills`) | Aurora path deprecated. |
| `docs/plans/2026-02-23-billing-csv-import-design.md` | Superseded — can be archived or deleted. |
| `docs/plans/2026-02-23-billing-csv-import.md` | Superseded. |
| `docs/plans/2026-02-23-billing-neptune-hudi-design.md` | Superseded. |
| `docs/plans/2026-02-23-billing-points-import-design.md` | Superseded. |

Before deletion, each script is inspected for imports from outside (Terraform, GitHub Actions, CLAUDE.md) to confirm no external dependency.

### 3.7 Testing

**New unit tests** in `tests/unit/test_billing_parser.py` (or extend `test_non_nem_parsers.py`):

| Test | Input | Expected |
|---|---|---|
| `test_filename_mismatch_raises` | `RACV-Usage and Spend Report.csv` | Raises "Not Bunnings Usage and Spend File" |
| `test_utf16_decoding` | UTF-16 LE bytes with BOM | Parser decodes successfully |
| `test_happy_path_single_nmi_single_month` | Mock CSV with 1 NMI × 1 month + mock mappings | Writes CSV to mock S3 with 23 rows; returns `[]` |
| `test_actual_vs_estimated_distinction` | Row with actual Peak=100 and Estimated Peak=0 | Two rows written: one for `billing-peak-usage`, one for `billing-estimated-peak-usage` with val=0 |
| `test_missing_mapping_skipped` | NMI not in `nem12_mappings` | Row silently skipped; no error |
| `test_blank_value_skipped` | Cell value empty | Row not written |
| `test_unit_selection` | Row with `kWh` usage unit and `AUD` spend currency | Usage rows get `unit=kwh`, spend rows get `unit=aud` |
| `test_date_conversion` | `Date="Mar 2026"` | `ts="2026-03-01 00:00:00"` |
| `test_invalid_date_skipped` | `Date="bogus"` | Row skipped, WARN log emitted |
| `test_cache_single_s3_get` | Parser invoked twice | S3 `get_object` called once |
| `test_s3_write_target` | Happy path | Upload goes to `Bucket=hudibucketsrc, Key=sensorDataFiles/billing_export_*.csv` |

Use `moto` for S3 mocking (existing test pattern in repo). Fixtures: trimmed 2-NMI × 3-month UTF-16 CSV under `tests/unit/fixtures/bunnings_billing_sample.csv`.

**Integration test** (optional, separate PR): extend `test_integration.py` with an end-to-end flow dropping a Bunnings billing CSV into a mock `newTBP/` and asserting a `sensorDataFiles/billing_export_*.csv` appears on mocked Hudi bucket.

### 3.8 Deployment & Rollout

1. Merge PR. GitHub Actions auto-deploys `sbm-files-ingester` Lambda with the new parser.
2. Apply Terraform (`cd terraform && terraform apply`) to update the Bunnings scheduler cron.
3. Verification: manually invoke `aws lambda invoke --function-name optima-billing-exporter --payload '{"project":"bunnings","country":"AU"}'`. Within ~30 minutes, confirm:
   - New file in `s3://sbm-file-ingester/newTBP/*Bunnings-Usage and Spend Report*.csv`
   - New file in `s3://hudibucketsrc/sensorDataFiles/billing_export_*.csv`
   - Original file moved to `s3://sbm-file-ingester/newIrrevFiles/`
   - CloudWatch logs for `sbm-files-ingester` show parser execution without errors
4. Wait for next Glue run (hourly) and query Athena to confirm actual data now populates `billing-peak-usage` etc. for a recent NMI.
5. Run the 2026-05-07 monthly schedule with no intervention; confirm the same end-state.
6. Optional backfill: manually re-invoke the exporter for past months one-off, then let the pipeline catch up.

### 3.9 Observability

- Log structured entries from the parser with keys: `event=bunnings_billing_parsed`, `file`, `nmi_count`, `rows_written`, `unmapped_nmi_count`.
- CloudWatch metric: existing `SBM/Ingester` namespace — no new metrics required (use existing `monitor_points_count` via file_processor counting).
- On `unmapped_nmi_count > 0`, log at WARN — prompts ops to check `nem12_mappings.json` freshness.

### 3.10 Security & IAM

No changes. `sbm-files-ingester` Lambda role (`getIdFromNem12Id-role-153b7a0a`) already has:
- `AmazonS3FullAccess` (`s3:*`) — covers both `s3:GetObject` on `sbm-file-ingester/nem12_mappings.json` and `s3:PutObject` on `hudibucketsrc/sensorDataFiles/billing_export_*.csv`. Verified 2026-04-14.

No new resources, no policy changes.

---

## 4. Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| BidEnergy changes the CSV format (column names, encoding) | Low | High | Parser fails → file to `newParseErr/`. Unit tests cover current format; on format change, update `CSV_FIELD_MAPPING` and re-deploy. |
| `nem12_mappings.json` missing billing entries for a new NMI | Medium | Low | Silent skip with WARN log; next hourly sync picks it up. Next monthly run reconciles. |
| Dual write (4/1 estimated then 4/14 actual) creates row conflicts in Hudi | Low | Low | Hudi upsert key is `sensorId + ts`; 4/14 row overwrites 4/1 row for the same month. This is the *desired* behaviour. |
| Auto-forward tool stops forwarding Bunnings mails (like the gap observed before 4/14) | Low | High | Not in scope for this spec. Log monitoring by ops: if no Bunnings billing CSV arrives within 24h of scheduler trigger, page oncall. |
| 7th of month still too early for some retailers | Low | Medium | If observed, bump scheduler to 10th or 14th in a follow-up Terraform change. Design explicitly documents the 7th is a heuristic, not a contract. |
| Parser raises unexpected exception after partial rows written | Very Low | Low | Glue ETL idempotency: partial file is either fully upserted (Hudi re-runs) or overwritten on next trigger. Data consistency unaffected. |

---

## 5. Open Questions (resolved)

- **Q1.** `scripts/import_billing_points.py`: inspect at implementation time — delete if it targets Aurora, keep if it refreshes Neptune points. Not a design blocker; the verdict affects the deletion list only.
- **Q2.** Old billing docs under `docs/plans/2026-02-23-billing-*.md`: deleted in **this** PR (Section 3.6) since they describe the now-replaced architecture. No need for a separate cleanup PR.
- **Q3.** CloudWatch alarm on unmapped NMIs: deferred. Start with WARN-level structured logs; add metric + alarm in a follow-up if the signal proves useful in ops.

---

## 6. Success Criteria

- ✅ 2026-05-07 Bunnings scheduler fires automatically; report arrives in `newTBP/` via auto-forward.
- ✅ `sbm-files-ingester` Lambda processes file without error; parser writes `billing_export_*.csv` to `hudibucketsrc/sensorDataFiles/`.
- ✅ Next Glue run loads billing rows into Hudi. Athena query on VCCCLG0019 for Mar/Apr 2026 returns actual values (not 0) on the `billing-peak-usage` and `billing-total-spend` sensors.
- ✅ RACV billing flow unchanged — `gegoptimareports/usageAndSpendReports/racvUsageAndSpend.csv` continues to update monthly on the 1st.
- ✅ Aurora-related billing scripts and models removed from repo; no remaining references in Terraform, GitHub Actions, or other code.
- ✅ All new unit tests pass; overall coverage ≥ 90% (repo standard).

---

## 7. Implementation Sequence (to be expanded by writing-plans)

**Delivery model:** single PR on branch `feat/bunnings-billing-parser`. User explicitly requested one PR over two, accepting the slightly larger review surface in exchange for a single atomic change.

1. **Step 1:** Add `bunnings_usage_and_spend_parser` + `_get_nem12_mappings` cache in `non_nem_parsers.py`; register in dispatcher.
2. **Step 2:** Write unit tests for the parser and cache, including UTF-16 decoding fixture (all 11 tests from Section 3.7).
3. **Step 3:** Terraform: update Bunnings scheduler cron to 7th.
4. **Step 4:** Delete deprecated Aurora + old billing scripts, data files, tests, and doc plans. Verify no external references.
5. **Step 5:** Update `CLAUDE.md` (module-specific) to document the new parser and removed scripts.
6. **Step 6:** PR, code review, merge → auto-deploy. Apply Terraform.
7. **Step 7:** Manual verification run; confirm end-to-end behaviour.

---

## 8. Appendix

### 8.1 Example CSV Row (for reference)

From `/Users/zeyu/Downloads/20260414.155519-Bunnings-Usage and Spend Report.csv` (UTF-16 LE), NMI VCCCLG0019, Mar 2026:

```
"Bunnings","AU","Electricity","VCCCLG0019","NMI","POWCP","BUN AUS Sunshine WH","390","Bunnings VIC","6083","4231","","Bunnings Australia","480 Ballarat Road","Sunshine","AU:VIC","3020","","","6083;bun-aus;location;retail;retail-site;vic;warehouse","01 Jan 2000","Active","","",Mar 2026,ZenEnergy,31105.09,31218.55,0.00,62323.64,0.00,0.00,0.00,0.00,0.00,0.00,kWh,2925.16,8374.03,3158.90,46.81,61.69,14566.59,0.00,0.00,0.00,0.00,0.00,0.00,0.00,AUD
```

### 8.2 Expected Parser Output (23 Hudi rows for that single source row)

```
p:bunnings:19c88bf11c8-76959f,2026-03-01 00:00:00,31105.09,kwh,2026-03-01 00:00:00,
p:bunnings:19c88bf11ca-38fd75,2026-03-01 00:00:00,31218.55,kwh,2026-03-01 00:00:00,
p:bunnings:19c88bf11cc-78f6f0,2026-03-01 00:00:00,0.00,kwh,2026-03-01 00:00:00,
p:bunnings:19c88bf11d1-860458,2026-03-01 00:00:00,62323.64,kwh,2026-03-01 00:00:00,
... (19 more) ...
p:bunnings:19c88bf1236-c657f1,2026-03-01 00:00:00,0.00,aud,2026-03-01 00:00:00,
```
