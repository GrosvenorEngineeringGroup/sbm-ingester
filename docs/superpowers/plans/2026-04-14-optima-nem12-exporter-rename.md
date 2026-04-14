# Optima NEM12 Exporter Rename Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rename `interval_exporter` → `nem12_exporter` across source, tests, Terraform, CI, and docs, giving the module a format-based name so future sibling exporters (CSV/JSON/XML) can coexist under `optima_exporter/`.

**Architecture:** Pure rename. No runtime behavior change. Python module and test package move to `nem12_exporter/`; all imports update; AWS Lambda + log group + 2 schedulers + 1 alarm change AWS identity (Terraform destroy+recreate, with `moved {}` blocks for TF state continuity); CI deploy paths and `--function-name` args update; docs reflect the new name.

**Tech Stack:** Python 3.13, uv, pytest, AWS Lambda, CloudWatch, EventBridge Scheduler, Terraform, GitHub Actions

**Spec:** `docs/superpowers/specs/2026-04-14-optima-nem12-exporter-rename-design.md`

---

## File Inventory

**Directories to rename (git mv):**
- `src/functions/optima_exporter/interval_exporter/` → `nem12_exporter/`
- `tests/unit/optima_exporter/interval_exporter/` → `nem12_exporter/`

**Files to modify (no rename):**
- `src/functions/optima_exporter/nem12_exporter/app.py` (after rename)
- `src/functions/optima_exporter/nem12_exporter/downloader.py` (after rename)
- `src/functions/optima_exporter/nem12_exporter/processor.py` (after rename)
- `src/functions/optima_exporter/nem12_exporter/uploader.py` (after rename)
- `tests/unit/optima_exporter/nem12_exporter/__init__.py` (after rename)
- `tests/unit/optima_exporter/nem12_exporter/test_app.py` (after rename)
- `tests/unit/optima_exporter/nem12_exporter/test_downloader.py` (after rename)
- `tests/unit/optima_exporter/nem12_exporter/test_processor.py` (after rename)
- `tests/unit/optima_exporter/nem12_exporter/test_uploader.py` (after rename)
- `tests/unit/optima_exporter/conftest.py`
- `tests/unit/optima_exporter/test_e2e_full_chain.py`
- `tests/unit/conftest.py`
- `terraform/optima_exporter.tf`
- `.github/workflows/main.yml`
- `scripts/deploy-lambda.sh`
- `README.md`
- `src/functions/optima_exporter/README.md`
- `CLAUDE.md`

---

### Task 1: Rename Python source directory

**Files:**
- Rename: `src/functions/optima_exporter/interval_exporter/` → `src/functions/optima_exporter/nem12_exporter/`

- [ ] **Step 1: Rename directory with git mv**

Run: `git mv src/functions/optima_exporter/interval_exporter src/functions/optima_exporter/nem12_exporter`

Expected: no output, `git status` shows 4 renamed files (`app.py`, `downloader.py`, `processor.py`, `uploader.py`).

- [ ] **Step 2: Verify rename**

Run: `ls src/functions/optima_exporter/`
Expected output contains: `nem12_exporter` (not `interval_exporter`).

---

### Task 2: Update internal imports inside `nem12_exporter/`

**Files:**
- Modify: `src/functions/optima_exporter/nem12_exporter/app.py`
- Modify: `src/functions/optima_exporter/nem12_exporter/processor.py`

- [ ] **Step 1: Update import in `app.py`**

File: `src/functions/optima_exporter/nem12_exporter/app.py`

Old:
```python
from interval_exporter.processor import process_export
```
New:
```python
from nem12_exporter.processor import process_export
```

- [ ] **Step 2: Update imports in `processor.py`**

File: `src/functions/optima_exporter/nem12_exporter/processor.py`

Old:
```python
from interval_exporter.downloader import download_csv
from interval_exporter.uploader import upload_to_s3
```
New:
```python
from nem12_exporter.downloader import download_csv
from nem12_exporter.uploader import upload_to_s3
```

- [ ] **Step 3: Verify no stale imports remain in source**

Run: `rg 'interval_exporter' src/functions/optima_exporter/`
Expected: no output (zero matches).

---

### Task 3: Update docstrings, logger service names, and log messages in source

**Files:**
- Modify: `src/functions/optima_exporter/nem12_exporter/app.py`
- Modify: `src/functions/optima_exporter/nem12_exporter/downloader.py`
- Modify: `src/functions/optima_exporter/nem12_exporter/processor.py`
- Modify: `src/functions/optima_exporter/nem12_exporter/uploader.py`

- [ ] **Step 1: Update `app.py` — module docstring, logger, handler docstring**

File: `src/functions/optima_exporter/nem12_exporter/app.py`

Replacements (use exact string match; apply each):

| Old | New |
|-----|-----|
| `"""Lambda handler for Optima interval exporter.` (or whichever phrasing currently exists at top) — effectively: **module-level docstring and any comment describing this module** | Replace "interval exporter" / "interval data export" with "NEM12 exporter" / "NEM12 export" where the subject is this module |
| `logger = Logger(service="optima-interval-exporter")` | `logger = Logger(service="optima-nem12-exporter")` |
| `Lambda handler for interval data export.` (handler docstring line) | `Lambda handler for NEM12 export.` |
| `Exports interval usage data from BidEnergy by downloading CSV reports` (module docstring) | `Exports NEM12 files from BidEnergy by downloading CSV reports` |

- [ ] **Step 2: Update `downloader.py`**

File: `src/functions/optima_exporter/nem12_exporter/downloader.py`

| Old | New |
|-----|-----|
| `"""CSV download utilities for interval data export."""` | `"""CSV download utilities for NEM12 export."""` |
| `logger = Logger(service="optima-interval-exporter")` | `logger = Logger(service="optima-nem12-exporter")` |

**Keep unchanged:** `Download CSV interval usage data from BidEnergy.` in `download_csv` docstring (describes BidEnergy's endpoint semantics, which is about interval data regardless of our module name).

- [ ] **Step 3: Update `processor.py`**

File: `src/functions/optima_exporter/nem12_exporter/processor.py`

| Old | New |
|-----|-----|
| `"""Processing logic for interval data export."""` | `"""Processing logic for NEM12 export."""` |
| `logger = Logger(service="optima-interval-exporter")` | `logger = Logger(service="optima-nem12-exporter")` |
| `Process interval data export for a project.` (in `process_export` docstring) | `Process NEM12 export for a project.` |
| `logger.info("Starting interval export", ...)` | `logger.info("Starting NEM12 export", ...)` |

- [ ] **Step 4: Update `uploader.py`**

File: `src/functions/optima_exporter/nem12_exporter/uploader.py`

| Old | New |
|-----|-----|
| `"""S3 upload utilities for interval data export."""` | `"""S3 upload utilities for NEM12 export."""` |
| `logger = Logger(service="optima-interval-exporter")` | `logger = Logger(service="optima-nem12-exporter")` |

- [ ] **Step 5: Verify no `optima-interval-exporter` service strings remain in source**

Run: `rg 'optima-interval-exporter' src/functions/optima_exporter/`
Expected: no output.

---

### Task 4: Rename Python test directory

**Files:**
- Rename: `tests/unit/optima_exporter/interval_exporter/` → `tests/unit/optima_exporter/nem12_exporter/`

- [ ] **Step 1: Rename test directory with git mv**

Run: `git mv tests/unit/optima_exporter/interval_exporter tests/unit/optima_exporter/nem12_exporter`

Expected: 5 files renamed (`__init__.py`, `test_app.py`, `test_app.py`, `test_downloader.py`, `test_processor.py`, `test_uploader.py`).

- [ ] **Step 2: Verify**

Run: `ls tests/unit/optima_exporter/`
Expected: contains `nem12_exporter` (not `interval_exporter`).

---

### Task 5: Update imports/patches inside renamed test files

**Files:**
- Modify: `tests/unit/optima_exporter/nem12_exporter/__init__.py`
- Modify: `tests/unit/optima_exporter/nem12_exporter/test_app.py`
- Modify: `tests/unit/optima_exporter/nem12_exporter/test_downloader.py`
- Modify: `tests/unit/optima_exporter/nem12_exporter/test_processor.py`
- Modify: `tests/unit/optima_exporter/nem12_exporter/test_uploader.py`

- [ ] **Step 1: Update `__init__.py` docstring**

File: `tests/unit/optima_exporter/nem12_exporter/__init__.py`

Old: `"""Tests for interval_exporter modules."""`
New: `"""Tests for nem12_exporter modules."""`

- [ ] **Step 2: Replace-all `interval_exporter` → `nem12_exporter` in each test file**

For each of the 4 test files below, perform a **literal substring replace-all** of `interval_exporter` → `nem12_exporter` (this covers both import statements like `from interval_exporter.processor import ...` and `patch("interval_exporter.xxx.yyy")` calls):

- `tests/unit/optima_exporter/nem12_exporter/test_app.py`
- `tests/unit/optima_exporter/nem12_exporter/test_downloader.py`
- `tests/unit/optima_exporter/nem12_exporter/test_processor.py`
- `tests/unit/optima_exporter/nem12_exporter/test_uploader.py`

Additionally, update docstrings at the top of each file if they say `"""Unit tests for interval_exporter/X.py module."""` → `"""Unit tests for nem12_exporter/X.py module."""`.

And replace `Tests the Lambda handler entry point for interval data export.` → `Tests the Lambda handler entry point for NEM12 export.` (in `test_app.py`).

- [ ] **Step 3: Verify**

Run: `rg 'interval_exporter' tests/unit/optima_exporter/nem12_exporter/`
Expected: no output.

---

### Task 6: Update `tests/unit/optima_exporter/conftest.py`

**Files:**
- Modify: `tests/unit/optima_exporter/conftest.py`

- [ ] **Step 1: Update imports**

| Old | New |
|-----|-----|
| `import interval_exporter.uploader as uploader_module` | `import nem12_exporter.uploader as uploader_module` |
| `import interval_exporter.processor as processor_module` | `import nem12_exporter.processor as processor_module` |

- [ ] **Step 2: Update mock Lambda context fixture**

| Old | New |
|-----|-----|
| `"""Create mock Lambda context for interval exporter."""` | `"""Create mock Lambda context for NEM12 exporter."""` |
| `context.function_name = "optima-interval-exporter"` | `context.function_name = "optima-nem12-exporter"` |
| `context.invoked_function_arn = "arn:aws:lambda:ap-southeast-2:123456789012:function:optima-interval-exporter"` | `context.invoked_function_arn = "arn:aws:lambda:ap-southeast-2:123456789012:function:optima-nem12-exporter"` |

- [ ] **Step 3: Verify**

Run: `rg 'interval_exporter|optima-interval-exporter' tests/unit/optima_exporter/conftest.py`
Expected: no output.

---

### Task 7: Update e2e test file

**Files:**
- Modify: `tests/unit/optima_exporter/test_e2e_full_chain.py`

- [ ] **Step 1: Update top docstring and path comments**

| Old | New |
|-----|-----|
| `"""End-to-end integration test spanning optima-interval-exporter → sbm-files-ingester.` | `"""End-to-end integration test spanning optima-nem12-exporter → sbm-files-ingester.` |
| `  → optima_exporter.interval_exporter.app.lambda_handler` (in path comment) | `  → optima_exporter.nem12_exporter.app.lambda_handler` |

- [ ] **Step 2: Update imports**

| Old | New |
|-----|-----|
| `import interval_exporter.processor as processor_module` | `import nem12_exporter.processor as processor_module` |
| `import interval_exporter.uploader as uploader_module` | `import nem12_exporter.uploader as uploader_module` |
| `from interval_exporter.app import lambda_handler as optima_lambda_handler` | `from nem12_exporter.app import lambda_handler as optima_lambda_handler` |

- [ ] **Step 3: Update inline comment and mock context**

| Old | New |
|-----|-----|
| `# STEP 1: invoke optima interval exporter lambda_handler` | `# STEP 1: invoke optima NEM12 exporter lambda_handler` |
| `mock_context.function_name = "optima-interval-exporter"` | `mock_context.function_name = "optima-nem12-exporter"` |
| `"arn:aws:lambda:ap-southeast-2:123456789012:function:optima-interval-exporter"` | `"arn:aws:lambda:ap-southeast-2:123456789012:function:optima-nem12-exporter"` |

- [ ] **Step 4: Verify**

Run: `rg 'interval_exporter|optima-interval-exporter' tests/unit/optima_exporter/test_e2e_full_chain.py`
Expected: no output.

---

### Task 8: Update `tests/unit/conftest.py` path comment

**Files:**
- Modify: `tests/unit/conftest.py`

- [ ] **Step 1: Update comment**

File: `tests/unit/conftest.py` (around line 11)

Old: `# Add optima_exporter to path for Lambda-style imports (shared, interval_exporter, billing_exporter)`
New: `# Add optima_exporter to path for Lambda-style imports (shared, nem12_exporter, billing_exporter)`

- [ ] **Step 2: Verify full test suite is free of stale references**

Run: `rg 'interval_exporter|optima-interval-exporter' tests/`
Expected: no output.

---

### Task 9: Run test suite to verify all Python changes

- [ ] **Step 1: Run pytest**

Run: `uv run pytest`
Expected: all 487+ tests pass. Exit code 0.

If any test fails due to the rename, stop and diagnose before proceeding. Common issues: a missed `patch("interval_exporter...")` call, missed logger assertion, or missed docstring update that tests inspect.

- [ ] **Step 2: Run linter**

Run: `uv run ruff check .`
Expected: exit code 0 (no new lint errors introduced).

---

### Task 10: Update Terraform — rename resources and add `moved {}` blocks

**Files:**
- Modify: `terraform/optima_exporter.tf`

- [ ] **Step 1: Update file header comment**

File: `terraform/optima_exporter.tf` (line 5)

Old: `# - Interval Exporter: Downloads CSV interval data, uploads to S3`
New: `# - NEM12 Exporter: Downloads NEM12 CSV files from BidEnergy, uploads to S3`

- [ ] **Step 2: Rename `aws_cloudwatch_log_group.optima_interval_exporter`**

File: `terraform/optima_exporter.tf`

Old block (lines ~76-81):
```hcl
resource "aws_cloudwatch_log_group" "optima_interval_exporter" {
  name              = "/aws/lambda/optima-interval-exporter"
  retention_in_days = var.log_retention_days

  tags = local.common_tags
}
```
New block:
```hcl
resource "aws_cloudwatch_log_group" "optima_nem12_exporter" {
  name              = "/aws/lambda/optima-nem12-exporter"
  retention_in_days = var.log_retention_days

  tags = local.common_tags
}
```

- [ ] **Step 3: Rename `aws_lambda_function.optima_interval_exporter`**

File: `terraform/optima_exporter.tf`

Old block (lines ~83-115):
```hcl
resource "aws_lambda_function" "optima_interval_exporter" {
  function_name = "optima-interval-exporter"
  description   = "Exports Optima interval data to S3 for ingestion pipeline"
  role          = data.aws_iam_role.ingester_role.arn
  handler       = "interval_exporter.app.lambda_handler"
  ...
  environment {
    variables = merge(local.optima_common_env, {
      POWERTOOLS_SERVICE_NAME = "optima-interval-exporter"

      # S3 upload configuration
      S3_UPLOAD_BUCKET = "sbm-file-ingester"
      S3_UPLOAD_PREFIX = "newTBP/"

      # Interval export configuration
      OPTIMA_DAYS_BACK   = "1"
      OPTIMA_MAX_WORKERS = "20"
    })
  }
  ...
  depends_on = [aws_cloudwatch_log_group.optima_interval_exporter]

  tags = local.common_tags
}
```
New block (keep structure identical; change only marked fields):
```hcl
resource "aws_lambda_function" "optima_nem12_exporter" {
  function_name = "optima-nem12-exporter"
  description   = "Exports Optima NEM12 files to S3 for ingestion pipeline"
  role          = data.aws_iam_role.ingester_role.arn
  handler       = "nem12_exporter.app.lambda_handler"
  ...
  environment {
    variables = merge(local.optima_common_env, {
      POWERTOOLS_SERVICE_NAME = "optima-nem12-exporter"

      # S3 upload configuration
      S3_UPLOAD_BUCKET = "sbm-file-ingester"
      S3_UPLOAD_PREFIX = "newTBP/"

      # NEM12 export configuration
      OPTIMA_DAYS_BACK   = "1"
      OPTIMA_MAX_WORKERS = "20"
    })
  }
  ...
  depends_on = [aws_cloudwatch_log_group.optima_nem12_exporter]

  tags = local.common_tags
}
```

(Preserve `runtime`, `timeout`, `memory_size`, `s3_bucket`, `s3_key`, `tracing_config` lines verbatim.)

- [ ] **Step 4: Update section header comment**

File: `terraform/optima_exporter.tf` (line ~72-74)

Old: `# Lambda 1: Interval Exporter`
New: `# Lambda 1: NEM12 Exporter`

- [ ] **Step 5: Rename `aws_scheduler_schedule.optima_bunnings_interval`**

File: `terraform/optima_exporter.tf`

Old (lines ~164-180):
```hcl
# Bunnings Interval - Daily 2:00 PM Sydney
resource "aws_scheduler_schedule" "optima_bunnings_interval" {
  name       = "optima-bunnings-interval-daily"
  ...
  target {
    arn      = aws_lambda_function.optima_interval_exporter.arn
    ...
  }
}
```
New:
```hcl
# Bunnings NEM12 - Daily 2:00 PM Sydney
resource "aws_scheduler_schedule" "optima_bunnings_nem12" {
  name       = "optima-bunnings-nem12-daily"
  ...
  target {
    arn      = aws_lambda_function.optima_nem12_exporter.arn
    ...
  }
}
```

- [ ] **Step 6: Rename `aws_scheduler_schedule.optima_racv_interval`**

File: `terraform/optima_exporter.tf`

Old (lines ~182-199):
```hcl
# RACV Interval - Daily 2:00 PM Sydney
resource "aws_scheduler_schedule" "optima_racv_interval" {
  name       = "optima-racv-interval-daily"
  ...
  target {
    arn      = aws_lambda_function.optima_interval_exporter.arn
    ...
  }
}
```
New:
```hcl
# RACV NEM12 - Daily 2:00 PM Sydney
resource "aws_scheduler_schedule" "optima_racv_nem12" {
  name       = "optima-racv-nem12-daily"
  ...
  target {
    arn      = aws_lambda_function.optima_nem12_exporter.arn
    ...
  }
}
```

- [ ] **Step 7: Update section header for schedulers (line ~159-161)**

Old: `# EventBridge Scheduler: Interval (Daily)`
New: `# EventBridge Scheduler: NEM12 (Daily)`

- [ ] **Step 8: Update commented-out weekly schedule block (lines ~243-267)**

File: `terraform/optima_exporter.tf`

In the commented block:
- `# EventBridge Scheduler: Interval (Weekly)` → `# EventBridge Scheduler: NEM12 (Weekly)`
- `# Bunnings Interval Weekly - SUSPENDED` → `# Bunnings NEM12 Weekly - SUSPENDED`
- `# resource "aws_scheduler_schedule" "optima_bunnings_interval_weekly" {` → `# resource "aws_scheduler_schedule" "optima_bunnings_nem12_weekly" {`
- `#   name       = "optima-bunnings-interval-weekly"` → `#   name       = "optima-bunnings-nem12-weekly"`
- `#     arn      = aws_lambda_function.optima_interval_exporter.arn` → `#     arn      = aws_lambda_function.optima_nem12_exporter.arn`

- [ ] **Step 9: Update IAM role policy reference to Lambda**

File: `terraform/optima_exporter.tf` (line ~300)

Old: `aws_lambda_function.optima_interval_exporter.arn,`
New: `aws_lambda_function.optima_nem12_exporter.arn,`

- [ ] **Step 10: Rename `aws_cloudwatch_metric_alarm.optima_interval_errors`**

File: `terraform/optima_exporter.tf` (lines ~311-330)

Old block:
```hcl
resource "aws_cloudwatch_metric_alarm" "optima_interval_errors" {
  alarm_name          = "optima-interval-exporter-errors"
  ...
  alarm_description   = "Optima interval exporter Lambda errors"

  dimensions = {
    FunctionName = aws_lambda_function.optima_interval_exporter.function_name
  }
  ...
}
```
New block:
```hcl
resource "aws_cloudwatch_metric_alarm" "optima_nem12_errors" {
  alarm_name          = "optima-nem12-exporter-errors"
  ...
  alarm_description   = "Optima NEM12 exporter Lambda errors"

  dimensions = {
    FunctionName = aws_lambda_function.optima_nem12_exporter.function_name
  }
  ...
}
```

- [ ] **Step 11: Add `moved {}` blocks at end of file**

File: `terraform/optima_exporter.tf` (append at end of file)

```hcl
# ================================
# Terraform state moves (rename interval_exporter → nem12_exporter)
# ================================
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

Note: `moved {}` preserves state address continuity, but the underlying AWS resource **will still be replaced** because the `name` / `function_name` / `alarm_name` attributes are immutable identifiers. The blocks just prevent Terraform from showing the rename as delete-then-create at the state-address level.

- [ ] **Step 12: Verify no `interval` references remain in active (non-commented) Terraform**

Run: `rg 'optima_interval|optima-interval' terraform/optima_exporter.tf`
Expected: only matches inside the `moved {}` blocks (the `from = ...` side), and any commented-out lines in the weekly block.

---

### Task 11: Update CI workflow

**Files:**
- Modify: `.github/workflows/main.yml`

- [ ] **Step 1: Update build-step copy path**

File: `.github/workflows/main.yml` (line ~176)

Old: `cp -r src/functions/optima_exporter/interval_exporter build/optima_exporter/`
New: `cp -r src/functions/optima_exporter/nem12_exporter build/optima_exporter/`

- [ ] **Step 2: Update build-step comment**

File: `.github/workflows/main.yml` (line ~168)

Old: `# Build Optima Exporter Lambda (shared zip for interval + billing exporters)`
New: `# Build Optima Exporter Lambda (shared zip for nem12 + billing exporters)`

- [ ] **Step 3: Update `--function-name` argument**

File: `.github/workflows/main.yml` (line ~249)

Old:
```yaml
aws lambda update-function-code \
  --function-name optima-interval-exporter \
```
New:
```yaml
aws lambda update-function-code \
  --function-name optima-nem12-exporter \
```

- [ ] **Step 4: Verify**

Run: `rg 'interval_exporter|optima-interval-exporter' .github/workflows/main.yml`
Expected: no output.

---

### Task 12: Update deploy script

**Files:**
- Modify: `scripts/deploy-lambda.sh`

- [ ] **Step 1: Update build path**

File: `scripts/deploy-lambda.sh` (line ~145)

Old: `cp -r src/functions/optima_exporter/interval_exporter /tmp/lambda_build/`
New: `cp -r src/functions/optima_exporter/nem12_exporter /tmp/lambda_build/`

- [ ] **Step 2: Update for-loop function names**

File: `scripts/deploy-lambda.sh` (line ~206)

Old: `for func in optima-interval-exporter optima-billing-exporter; do`
New: `for func in optima-nem12-exporter optima-billing-exporter; do`

- [ ] **Step 3: Verify**

Run: `rg 'interval_exporter|optima-interval-exporter' scripts/deploy-lambda.sh`
Expected: no output.

---

### Task 13: Update documentation

**Files:**
- Modify: `README.md`
- Modify: `src/functions/optima_exporter/README.md`
- Modify: `CLAUDE.md`

- [ ] **Step 1: Update root `README.md`**

File: `README.md` (line ~162)

Old: `| `optima-interval-exporter` | Python 3.13 | 256 MB | 900s | Daily export - downloads BidEnergy interval CSV data to S3 |`
New: `| `optima-nem12-exporter` | Python 3.13 | 256 MB | 900s | Daily export - downloads BidEnergy NEM12 files to S3 |`

(Also scan this file for any other `interval-exporter` or `interval_exporter` mentions and replace with the NEM12 equivalent; leave statements describing "interval meter data" or "30-minute intervals" if they refer to data properties.)

- [ ] **Step 2: Update `src/functions/optima_exporter/README.md`**

File: `src/functions/optima_exporter/README.md`

Perform a replace-all of:
- `interval_exporter` → `nem12_exporter`
- `optima-interval-exporter` → `optima-nem12-exporter`
- `Interval Exporter` → `NEM12 Exporter` (title case occurrences referring to the component)

Specifically the following known occurrences must be updated:
- Line 5: `- **Interval Exporter** - Downloads interval usage CSV data...` → `- **NEM12 Exporter** - Downloads NEM12 CSV files...`
- Line 28: `├── interval_exporter/       # Lambda 1: Interval data export` → `├── nem12_exporter/          # Lambda 1: NEM12 export`
- Line 49: `Lambda (optima-interval-exporter)` → `Lambda (optima-nem12-exporter)`
- Line 98: table row for Lambda
- Line 205: scheduler mapping table
- Line 211: comment in invoke example
- Line 213: `--function-name optima-interval-exporter` → `--function-name optima-nem12-exporter`
- Line 235: directory tree
- Line 252: `uv run pytest tests/unit/optima_exporter/interval_exporter/ -v` → `uv run pytest tests/unit/optima_exporter/nem12_exporter/ -v`

- [ ] **Step 3: Update `CLAUDE.md` (repo root of sbm-ingester)**

File: `CLAUDE.md`

Update 3 known occurrences:
- Line 86 (Lambda table row): `| `optima-interval-exporter` | Python 3.13 | 256 MB | 900s | Daily export - downloads BidEnergy interval CSV data, uploads to S3 (X-Ray disabled) |` → `| `optima-nem12-exporter` | Python 3.13 | 256 MB | 900s | Daily export - downloads BidEnergy NEM12 files, uploads to S3 (X-Ray disabled) |`
- Line 142 (module description): `- `interval_exporter/` - Lambda 1: Downloads CSV interval data to S3` → `- `nem12_exporter/` - Lambda 1: Downloads NEM12 CSV files to S3`
- Line 293 (directory tree): `├── interval_exporter/` → `├── nem12_exporter/`

- [ ] **Step 4: Verify**

Run: `rg 'interval_exporter|optima-interval-exporter' README.md src/functions/optima_exporter/README.md CLAUDE.md`
Expected: no output.

---

### Task 14: Full-repo verification sweep

- [ ] **Step 1: Final grep across entire repo**

Run: `rg 'interval_exporter|optima-interval-exporter' --glob '!docs/superpowers/plans/2026-04-13-*' --glob '!docs/superpowers/specs/2026-04-13-*' --glob '!docs/superpowers/plans/2026-04-14-*' --glob '!docs/superpowers/specs/2026-04-14-*' --glob '!terraform/optima_exporter.tf'`

Expected: no output. Historical specs/plans and Terraform `moved {}` blocks are excluded.

- [ ] **Step 2: Re-run tests + linter**

Run: `uv run pytest && uv run ruff check .`
Expected: all tests pass, no lint errors.

- [ ] **Step 3: Terraform format check**

Run: `cd terraform && terraform fmt -check && cd ..`
Expected: no files need formatting.

---

### Task 15: Terraform plan review (no apply)

- [ ] **Step 1: Initialize and plan**

Run: `cd terraform && terraform init -upgrade && terraform plan -out=/tmp/rename.tfplan 2>&1 | tee /tmp/rename.tfplan.txt && cd ..`

- [ ] **Step 2: Verify expected changes**

Inspect `/tmp/rename.tfplan.txt`. Expected:
- 5 `moved {}` state moves applied (no resource impact from these)
- `aws_lambda_function.optima_nem12_exporter`: **replacement** (destroy → create) due to `function_name` change
- `aws_cloudwatch_log_group.optima_nem12_exporter`: **replacement** due to `name` change
- `aws_scheduler_schedule.optima_bunnings_nem12`: **replacement** due to `name` change
- `aws_scheduler_schedule.optima_racv_nem12`: **replacement** due to `name` change
- `aws_cloudwatch_metric_alarm.optima_nem12_errors`: **replacement** due to `alarm_name` change

Total: 5 replacements + 5 state moves. Zero net creates/deletes of distinct logical resources.

If any unexpected resources appear (additions, destroys without corresponding creates, unrelated changes), stop and diagnose.

**Do not run `terraform apply` in this task** — deployment is coordinated separately (see Task 17).

---

### Task 16: Commit everything

- [ ] **Step 1: Stage all changes**

Run: `git add -A`

- [ ] **Step 2: Verify staged changes look correct**

Run: `git status && git diff --cached --stat`

Expected to see:
- Renames: `src/functions/optima_exporter/interval_exporter/*` → `nem12_exporter/*` (4 files)
- Renames: `tests/unit/optima_exporter/interval_exporter/*` → `nem12_exporter/*` (5 files)
- Modified: `tests/unit/optima_exporter/conftest.py`, `tests/unit/optima_exporter/test_e2e_full_chain.py`, `tests/unit/conftest.py`
- Modified: `terraform/optima_exporter.tf`
- Modified: `.github/workflows/main.yml`, `scripts/deploy-lambda.sh`
- Modified: `README.md`, `src/functions/optima_exporter/README.md`, `CLAUDE.md`

- [ ] **Step 3: Commit**

Run:
```bash
git commit -m "$(cat <<'EOF'
refactor: rename optima interval_exporter to nem12_exporter

Format-based naming (NEM12) replaces data-shape naming (interval),
reserving a clean naming space for future sibling exporters that
handle Optima interval data in other formats.

Covers:
- Python module + test directory renames
- Terraform resource renames with moved {} blocks (Lambda, log group,
  2 schedulers, 1 alarm force-replaced due to immutable AWS names)
- CI workflow and deploy script function-name + build-path updates
- Docs (README, module README, CLAUDE.md)

No external callers. No runtime behavior change.
EOF
)"
```

---

### Task 17: Deployment coordination (manual, post-merge)

> This task captures operational steps to perform AFTER the commit is pushed to `main`. It is the **only** task that changes live AWS state. It is listed here so the plan is complete, but the executing subagent should mark this task and stop, handing control back to the human.

- [ ] **Step 1: Ensure push happens outside the 13:00-15:00 UTC daily scheduler window**

The EventBridge schedulers fire at `cron(0 14 * * ? *)` in `Australia/Sydney` (= 04:00 UTC in AEDT / 03:00 UTC in AEST). Avoid deploying near that time. Preferred deploy window: any time outside 03:00-05:00 UTC.

- [ ] **Step 2: Push to `main`, observe GitHub Actions**

Run: `git push origin main`

GHA will:
1. Build and upload `optima_exporter.zip` (with new `nem12_exporter/` dir inside) to `s3://gega-code-deployment-bucket/sbm-files-ingester/optima_exporter.zip` — **succeeds**.
2. Attempt `aws lambda update-function-code --function-name optima-nem12-exporter` — **fails** on first run (the new Lambda does not exist yet in AWS).

This is the expected one-time failure.

- [ ] **Step 3: Apply Terraform**

Run: `cd terraform && terraform apply /tmp/rename.tfplan` (using the plan file captured in Task 15; if stale, re-run `terraform plan` first).

Terraform will:
1. Destroy the 5 old-named AWS resources (old log group's historical logs lost).
2. Create 5 new-named AWS resources, with the new Lambda pulling its code from the already-uploaded `optima_exporter.zip` (which has the correct new handler path `nem12_exporter.app.lambda_handler`).

~1-minute window with no Lambda present in AWS. No scheduled invocations during this window (see Step 1).

- [ ] **Step 4: Verify deployment**

Run:
```bash
aws lambda invoke \
  --function-name optima-nem12-exporter \
  --payload "$(echo -n '{"project":"bunnings","nmi":"Optima_QB13041223","startDate":"2026-04-13","endDate":"2026-04-13"}' | base64)" \
  --region ap-southeast-2 \
  /tmp/out.json && cat /tmp/out.json
```

Expected: `{"statusCode": 200, ...}` with non-zero success_count.

Check log group:
```bash
aws logs describe-log-streams --log-group-name /aws/lambda/optima-nem12-exporter --region ap-southeast-2 --max-items 1
```
Expected: at least one log stream exists, containing entries with `"service":"optima-nem12-exporter"`.

Check schedulers:
```bash
aws scheduler list-schedules --region ap-southeast-2 --query 'Schedules[?contains(Name, `nem12`)].Name' --output text
```
Expected: `optima-bunnings-nem12-daily optima-racv-nem12-daily`.

- [ ] **Step 5: Wait 24h for first scheduled run**

After the next 14:00 Sydney fire, confirm:
- A fresh NEM12 CSV file lands in `s3://sbm-file-ingester/newTBP/optima_bunnings_NMI#...csv` for a recent date.
- Athena query confirms data ingested to Hudi table.

---

## Execution Notes

- **Order matters within Task 1-15**: code/tests are self-contained and can run in any order AFTER Task 1 completes (rename source dir first). Terraform (Task 10) and docs (Task 13) are independent.
- **Gate on tests**: do not proceed past Task 9 if pytest fails.
- **Do not skip Task 15**: `terraform plan` is the last chance to catch misconfiguration before live AWS changes.
- **Task 17 is out of scope for automated subagent execution**: stop after Task 16 and hand back to human for deployment.
