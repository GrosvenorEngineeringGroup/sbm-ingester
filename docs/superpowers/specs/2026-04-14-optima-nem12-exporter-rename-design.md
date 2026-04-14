# Optima NEM12 Exporter Rename — Design

**Date:** 2026-04-14
**Status:** Approved (pending implementation)

## Goal

Rename the existing `interval_exporter` component in `optima_exporter` to `nem12_exporter`. The rename makes the naming **format-based** (NEM12) rather than data-shape-based (interval), so future sibling exporters that handle Optima interval data in other formats (CSV / JSON / XML / etc.) can coexist under distinct, unambiguous names.

## Rationale

Optima interval reports are available in multiple export formats. The current `interval_exporter` name describes the data, not the format. Using `nem12_exporter` reserves a clean naming space for future format-specific exporters (e.g. `csv_exporter`, `xml_exporter`) under the same `optima_exporter` umbrella.

## Non-Goals

- Not changing the pipeline's runtime behavior.
- Not migrating DynamoDB configuration data (it's decoupled from Lambda/module names).
- Not changing the file naming convention produced by the Lambda (still `optima_<project>_NMI#...csv`).
- Not renaming shared concepts that describe **data properties** (e.g. "30-minute interval readings" in NEM12 parsers) — those are physically accurate and unrelated to this component.
- Not migrating historical CloudWatch log data — old log group will be destroyed.
- Not updating historical plan/spec files in `docs/superpowers/plans/` or `docs/superpowers/specs/` that reference the old name (they are historical records).

## Scope

### 1. Python Module Rename

| Old | New |
|-----|-----|
| `src/functions/optima_exporter/interval_exporter/` | `src/functions/optima_exporter/nem12_exporter/` |
| `tests/unit/optima_exporter/interval_exporter/` | `tests/unit/optima_exporter/nem12_exporter/` |

- Internal file names (`app.py`, `downloader.py`, `processor.py`, `uploader.py`) remain unchanged — they're generic.
- All `from interval_exporter.X import ...` statements → `from nem12_exporter.X import ...`.
- All `patch("interval_exporter.processor.Y")` in tests → `patch("nem12_exporter.processor.Y")`.
- Directory `__init__.py` docstring updated.

### 2. AWS Resources (Terraform)

Terraform will **destroy and recreate** the Lambda, log group, schedulers, and alarm (AWS resource names are immutable identifiers). User has confirmed no external callers exist and historical log loss is acceptable.

| Old Name | New Name |
|----------|----------|
| Lambda `optima-interval-exporter` | `optima-nem12-exporter` |
| Log group `/aws/lambda/optima-interval-exporter` | `/aws/lambda/optima-nem12-exporter` |
| Scheduler `optima-bunnings-interval-daily` | `optima-bunnings-nem12-daily` |
| Scheduler `optima-racv-interval-daily` | `optima-racv-nem12-daily` |
| Alarm `optima-interval-exporter-errors` | `optima-nem12-exporter-errors` |
| Handler `interval_exporter.app.lambda_handler` | `nem12_exporter.app.lambda_handler` |
| `POWERTOOLS_SERVICE_NAME=optima-interval-exporter` | `optima-nem12-exporter` |

**Terraform address renames** (use `moved {}` blocks to preserve state continuity where possible):
- `aws_lambda_function.optima_interval_exporter` → `aws_lambda_function.optima_nem12_exporter`
- `aws_cloudwatch_log_group.optima_interval_exporter` → `aws_cloudwatch_log_group.optima_nem12_exporter`
- `aws_scheduler_schedule.optima_bunnings_interval` → `aws_scheduler_schedule.optima_bunnings_nem12`
- `aws_scheduler_schedule.optima_racv_interval` → `aws_scheduler_schedule.optima_racv_nem12`
- `aws_cloudwatch_metric_alarm.optima_interval_errors` → `aws_cloudwatch_metric_alarm.optima_nem12_errors`

**Note:** Although `moved {}` blocks preserve TF state addresses, changing the underlying `name` / `function_name` / `alarm_name` attributes forces AWS-side replacement. The `moved {}` blocks still help Terraform avoid confusion about disappearing resources.

**Commented-out weekly schedule** in `terraform/optima_exporter.tf:248-260` — also updated for consistency (still commented).

### 3. CI/Deployment

| File | Changes |
|------|---------|
| `.github/workflows/main.yml` | Build path `src/functions/optima_exporter/interval_exporter` → `.../nem12_exporter`; `aws lambda update-function-code --function-name optima-interval-exporter` → `optima-nem12-exporter` |
| `scripts/deploy-lambda.sh` | Same two changes (build path + function name) |

### 4. Documentation

| File | Action |
|------|--------|
| Root `README.md` | Update Lambda table row (`optima-interval-exporter` → `optima-nem12-exporter`) and any interval-named references to the component |
| `src/functions/optima_exporter/README.md` | Full update: architecture diagram, directory tree, command examples, scheduler names, paths, operational runbook |
| `sbm-ingester/CLAUDE.md` | 3 locations: Lambda table row, module description bullet, directory tree |

### 5. In-Code Semantic Updates (Option B — format-first)

When "interval" refers to **this module or its output**, replace with "NEM12". When it refers to **the underlying data's physical property** (e.g. 30-minute readings), leave it.

Specific replacements:

| Location | Old | New |
|----------|-----|-----|
| `app.py` module docstring | "Exports interval usage data from BidEnergy..." | "Exports NEM12 files from BidEnergy..." |
| `app.py` logger | `service="optima-interval-exporter"` | `service="optima-nem12-exporter"` |
| `app.py` handler docstring | "Lambda handler for interval data export." | "Lambda handler for NEM12 export." |
| `downloader.py` module docstring | "CSV download utilities for interval data export." | "CSV download utilities for NEM12 export." |
| `downloader.py` logger | `service="optima-interval-exporter"` | `service="optima-nem12-exporter"` |
| `downloader.py` `download_csv` docstring | "Download CSV interval usage data from BidEnergy." | Retain (describes BidEnergy's endpoint semantics, which remains interval-oriented) |
| `processor.py` module docstring | "Processing logic for interval data export." | "Processing logic for NEM12 export." |
| `processor.py` logger | `service="optima-interval-exporter"` | `service="optima-nem12-exporter"` |
| `processor.py` `process_export` docstring | "Process interval data export for a project." | "Process NEM12 export for a project." |
| `processor.py` log message | `logger.info("Starting interval export", ...)` | `logger.info("Starting NEM12 export", ...)` |
| `uploader.py` module docstring | "S3 upload utilities for interval data export." | "S3 upload utilities for NEM12 export." |
| `uploader.py` logger | `service="optima-interval-exporter"` | `service="optima-nem12-exporter"` |

### 6. Tests

| File | Changes |
|------|---------|
| `tests/unit/optima_exporter/conftest.py` | `import interval_exporter.uploader as uploader_module` → `import nem12_exporter.uploader as uploader_module`; same for `processor_module`; fixture comment "Create mock Lambda context for interval exporter" → "NEM12 exporter"; mock `function_name = "optima-interval-exporter"` → `"optima-nem12-exporter"`; mock `invoked_function_arn` update |
| `tests/unit/optima_exporter/test_e2e_full_chain.py` | Top docstring "End-to-end ... optima-interval-exporter" → "optima-nem12-exporter"; import path comments; 2 imports; mock `function_name` and `invoked_function_arn` |
| `tests/unit/optima_exporter/interval_exporter/__init__.py` | Moved to `.../nem12_exporter/__init__.py`; docstring updated |
| `tests/unit/optima_exporter/interval_exporter/test_app.py` | Renamed to `test_app.py` in new dir; all `interval_exporter.app` imports/patches → `nem12_exporter.app` |
| `tests/unit/optima_exporter/interval_exporter/test_downloader.py` | Same pattern |
| `tests/unit/optima_exporter/interval_exporter/test_processor.py` | Same pattern; also `logger = Logger(service=...)` references in assertions (if any) |
| `tests/unit/optima_exporter/interval_exporter/test_uploader.py` | Same pattern |
| `tests/unit/conftest.py:11` | Comment `# ... shared, interval_exporter, billing_exporter` → `# ... shared, nem12_exporter, billing_exporter` |

### 7. Explicitly Not Changed

- `tests/unit/test_nem12_streaming.py`, `tests/unit/test_nem_adapter_edge_cases.py` — "interval" here describes the data's 30-min/15-min/5-min granularity, which is format-agnostic and physically accurate.
- `tests/unit/conftest.py` fixtures `nem12_15min_interval_file`, `nem12_5min_interval_file` — describe NEM12 file variants by their sampling interval; unrelated to the component name.
- `terraform/aurora.tf` — `monitoring_interval` for RDS, unrelated to this change.
- `docs/superpowers/plans/2026-04-13-optima-interval-exporter-nem12-migration.md` — historical implementation plan for the *previous* NEM12 migration; filename preserved as historical record.
- `docs/superpowers/specs/2026-04-13-optima-interval-exporter-nem12-migration-design.md` — same, historical spec.

## Safety Assessment

### Runtime Correctness — Unaffected

No runtime code path depends on the string "interval" as an identifier:

- **S3 upload path**: controlled by `S3_UPLOAD_PREFIX` env var, decoupled from Lambda name.
- **Generated filename**: `optima_<project>_NMI#<nmi>_<start>_<end>_<ts>.csv` — no "interval" token.
- **DynamoDB config (`sbm-optima-config`)**: partitioned by `(project, nmi)` — Lambda-name-independent.
- **NEM12 mappings JSON (`nem12_mappings.json`)**: keys are `Optima_<nmi>-<channel>`, decoupled.
- **Downstream `sbm-files-ingester`**: consumes S3 events, dispatches on file prefix `optima_` — unchanged.
- **Constants** (`OPTIMA_NMI_PREFIX = "Optima_"`): unaffected.

### Data Flow — Unaffected

Full path: EventBridge → **optima-nem12-exporter** (new Lambda) → BidEnergy → S3 (`sbm-file-ingester/newTBP/`) → SQS → `sbm-files-ingester` → Hudi via Glue → Athena. Every downstream hop is blind to the exporter Lambda's name.

### External Callers — None

User has confirmed no external callers of the Lambda. Internal callers (EventBridge schedulers) are renamed in lockstep.

### Deployment Ordering (One Risk Area)

The Lambda rename creates a **chicken-and-egg** between GitHub Actions and Terraform:

- GHA tries `aws lambda update-function-code --function-name optima-nem12-exporter` — fails on first deploy because the TF resource hasn't been created yet under the new name.
- Terraform creates the new Lambda but needs the deployment zip to exist in S3 at the expected key.

**Resolution** (to be detailed in the implementation plan):
1. GHA builds and uploads the new zip to S3 (zip name unchanged — still `optima_exporter.zip`) — succeeds.
2. GHA `aws lambda update-function-code ... --function-name optima-nem12-exporter` — **fails** on first run (new Lambda doesn't exist yet). Acceptable one-time failure.
3. Human runs `terraform apply`:
   - `moved {}` blocks reconcile the TF state address changes.
   - AWS destroys `optima-interval-exporter` Lambda, log group, schedulers, alarm.
   - AWS creates `optima-nem12-exporter` Lambda, log group, schedulers, alarm — pulling the new zip from S3 (which already has the new handler path).
4. Subsequent GHA runs succeed against the new Lambda name.

Between steps 3a and 3b there is a ~1-minute window with no Lambda. Daily scheduler fires at 13:00 UTC — deploy outside this window.

## Verification After Deployment

1. `terraform plan` — confirm only expected resources are destroyed/created.
2. `uv run pytest` — 487+ tests pass with new module name.
3. Invoke new Lambda manually: `aws lambda invoke --function-name optima-nem12-exporter --payload '{"project":"bunnings","nmi":"Optima_XXX","startDate":"2026-04-13","endDate":"2026-04-13"}' /tmp/out.json` — returns `statusCode: 200`.
4. Check new log group `/aws/lambda/optima-nem12-exporter` has entries with `service=optima-nem12-exporter`.
5. Confirm EventBridge Scheduler console shows `optima-bunnings-nem12-daily` and `optima-racv-nem12-daily` targeting the new Lambda.
6. Wait 24h → confirm scheduled invocation succeeded → verify Athena has fresh data for the day.

## Rollback Plan

If the rename causes unexpected breakage:
- `git revert` the PR.
- `terraform apply` — recreates old-named resources from reverted state.
- Old log history is already lost; new log history from the brief new-name window is lost on rollback.
- Pipeline returns to original state in ~2-3 minutes.

## File Inventory (Complete List of Touchpoints)

**Code files to rename/modify:**
- `src/functions/optima_exporter/interval_exporter/` → `nem12_exporter/` (4 files: `app.py`, `downloader.py`, `processor.py`, `uploader.py`)
- `tests/unit/optima_exporter/interval_exporter/` → `nem12_exporter/` (5 files: `__init__.py`, `test_app.py`, `test_downloader.py`, `test_processor.py`, `test_uploader.py`)
- `tests/unit/optima_exporter/conftest.py`
- `tests/unit/optima_exporter/test_e2e_full_chain.py`
- `tests/unit/conftest.py`

**Infra:**
- `terraform/optima_exporter.tf`

**CI/Deploy:**
- `.github/workflows/main.yml`
- `scripts/deploy-lambda.sh`

**Docs:**
- `README.md` (repo root)
- `src/functions/optima_exporter/README.md`
- `sbm-ingester/CLAUDE.md`

Total: ~13 files modified, 2 directories renamed (each containing 4-5 files).
