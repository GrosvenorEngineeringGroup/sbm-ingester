# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Repository Overview

SBM Ingester is a serverless file ingestion pipeline for building energy data. It processes meter data files (NEM12 and non-NEM formats) uploaded to S3, transforms them into a standard format, and writes the output to a data lake.

## Build & Deployment

Deployment is fully automated via GitHub Actions on push to `main`. The workflow:
1. Sets up Python 3.13 and uv package manager
2. Exports dependencies via `uv export --no-dev --no-hashes`
3. Builds Lambda packages (ingester, redrive, nem12-mappings, weekly-archiver, glue-trigger, optima-exporter, cim-exporter)
4. Uploads zips to `gega-code-deployment-bucket` S3
5. Updates Lambda function code with `aws lambda update-function-code`
6. Uploads Glue ETL script to `aws-glue-assets-*` S3
7. Builds and pushes CIM Exporter Docker image to ECR (incremental, only when `src/functions/cim_exporter/**` changes)

**Development setup:**
```bash
uv sync --all-extras        # Install all dependencies including dev
./scripts/setup-lefthook.sh # Setup git hooks (optional)
uv run ruff check .         # Lint
uv run ruff format .        # Format
uv run pytest               # Run tests
```

**Git Hooks (lefthook):**
- Pre-commit: ruff check + format, trailing whitespace, YAML validation
- Pre-push: pytest + coverage check (≥90%)
- See `docs/LEFTHOOK.md` for configuration details

**Run tests:**
```bash
uv run pytest                           # Run all tests
uv run pytest tests/unit/ -v            # Verbose output
uv run pytest --cov=src                 # With coverage
uv run pytest -k "test_name"            # Run specific test
```

**Add dependencies:**
```bash
uv add <package>                 # Runtime dependency
uv add --optional dev <package>  # Dev dependency
uv sync --all-extras             # Install with dev dependencies
```

## Code Quality

Linting is enforced via ruff with these rule sets:
- `E`, `F`, `W` - pycodestyle and Pyflakes
- `I` - isort import sorting
- `B` - flake8-bugbear (common bugs)
- `C4` - flake8-comprehensions
- `UP` - pyupgrade (Python 3.13+ syntax)
- `SIM` - flake8-simplify
- `RUF` - Ruff-specific rules

**Commands:**
```bash
uv run ruff check .        # Lint
uv run ruff check --fix .  # Auto-fix issues
uv run ruff format .       # Format code
```

## Architecture

### Data Flow
```
S3 (sbm-file-ingester/newTBP/)
    → SQS (sbm-files-ingester-queue)
    → Lambda (sbm-files-ingester)
    → S3 (hudibucketsrc/sensorDataFiles/)
```

### Lambda Functions

| Function | Runtime | Memory | Timeout | Purpose |
|----------|---------|--------|---------|---------|
| `sbm-files-ingester` | Python 3.13 | 512 MB | 900s | Main processor - parses files, maps NMIs, writes to data lake |
| `sbm-files-ingester-redrive` | Python 3.13 | 128 MB | 600s | Re-triggers processing for stuck files in `newTBP/` |
| `sbm-files-ingester-nem12-mappings-to-s3` | Python 3.13 | 128 MB | 60s | Hourly scheduled job - exports NEM12→Neptune ID mappings to S3 |
| `sbm-weekly-archiver` | Python 3.13 | 1024 MB | 600s | Weekly scheduled job (Monday UTC 00:00) - archives processed files with 50 concurrent workers |
| `sbm-glue-trigger` | Python 3.13 | 128 MB | 30s | Hourly scheduled job - triggers Glue ETL when file count ≥ threshold |
| `optima-nem12-exporter` | Python 3.13 | 256 MB | 900s | Daily export - downloads BidEnergy NEM12 files, uploads to S3 (X-Ray disabled) |
| `optima-billing-exporter` | Python 3.13 | 128 MB | 120s | Weekly export (Saturday 7 AM Sydney) - triggers BidEnergy billing report (email delivery) |
| `optima-demand-exporter` | Python 3.13 | 256 MB | 900s | Daily export (2:30 PM Sydney) - downloads BidEnergy Demand Profile CSVs (kW/kVa/PF), uploads to S3 (X-Ray disabled) |
| `cim-report-exporter` | Python 3.13 | 1024 MB | 300s | Daily (8 AM Sydney) - Playwright browser automation for CIM AFDD reports via Docker/ECR |

### Glue ETL Job

| Job | Workers | Timeout | Purpose |
|-----|---------|---------|---------|
| `DataImportIntoLake` | 5 (G.2X) | 24h | Imports CSVs from `sensorDataFiles/` into Apache Hudi data lake |

**src/functions/glue_trigger/app.py** - Glue trigger Lambda
- `lambda_handler()` - Checks file count in S3, triggers Glue job if ≥ threshold
- `count_files_in_prefix()` - Counts files excluding directory markers
- `start_glue_job()` - Starts Glue job run, handles `ConcurrentRunsExceededException`

**src/glue/hudi_import/script.py** - Glue ETL script (PySpark)
- Reads CSVs from `hudibucketsrc/sensorDataFiles/`
- Upserts to Hudi table with record key `sensorId + ts`
- Archives processed files to `sensorDataFilesArchived/`
- Configuration: `BATCH_SIZE=400`, `MAX_RUNTIME=4h`, `ARCHIVE_WORKERS=10`

### Key Components

**src/functions/file_processor/app.py** - Main entry point
- `lambda_handler()` - Processes SQS events from S3 notifications
- `parse_and_write_data()` - Downloads files, parses with appropriate parser, maps NMIs to Neptune IDs, writes CSVs to data lake
- Batch S3 writes with configurable `BATCH_SIZE` (default 50000 rows per CSV)
- File stability check for streaming uploads

**src/functions/weekly_archiver/app.py** - Weekly archiver Lambda
- `lambda_handler()` - Archives files from previous week to ISO week directories
- `archive_files_for_prefix()` - Concurrent processing with 50 workers
- Supports manual invocation with `target_week` parameter (e.g., `2026-W03`)

**src/shared/nem_adapter.py** - NEM12/NEM13 parser adapter (uses nemreader library)
- `output_as_data_frames()` - Returns list of (NMI, DataFrame) tuples
- Column naming: `{suffix}_{unit}` format (e.g., `E1_kWh`, `B1_kWh`)
- `split_days` parameter for splitting multi-day readings

**src/shared/non_nem_parsers.py** - Alternative format parsers
- `get_non_nem_df()` - Dispatcher that tries parsers in order
- Supports: RACV Noosa Solar, Envizi water/electricity, RACV, Optima generation, Green Square ComX

**src/shared/noosa_solar_parser.py** - RACV Noosa Solar CSV parser
- `noosa_solar_parser()` - Parses Fronius inverter data with SkySpark point IDs (`p:racv:r:xxx`) as column headers
- Handles numeric (kWh) and string status values via `FRONIUS_MODE_MAP` (13 inverter operating modes)
- Point IDs bypass Neptune mapping — written directly to data lake via `p:` prefix check in file processor

**src/shared/common.py** - S3 constants and utilities
- S3 path constants: `PARSE_ERR_DIR`, `IRREVFILES_DIR`, `PROCESSED_DIR`

**src/functions/optima_exporter/** - Optima/BidEnergy data exporter (modular structure)
- `optima_shared/` - Common utilities (auth, config, dynamodb)
  - `auth.py` - `login_bidenergy()` web login with cookie extraction
  - `config.py` - Environment variable management
  - `dynamodb.py` - Site configuration queries
- `nem12_exporter/` - Lambda 1: Downloads NEM12 CSV files to S3
  - `app.py` - Lambda handler for NEM12 exports
  - `downloader.py` - `download_csv()` from BidEnergy
  - `uploader.py` - `upload_to_s3()` for ingestion pipeline
  - `processor.py` - Parallel site processing with ThreadPoolExecutor
- `billing_exporter/` - Lambda 2: Triggers billing report (email delivery)
  - `app.py` - Lambda handler for billing exports
  - `trigger.py` - `trigger_monthly_usage_report()` API call
- Configuration stored in DynamoDB (`sbm-optima-config` table)
- **X-Ray tracing disabled** to avoid "Message too long" errors

**src/functions/cim_exporter/** - CIM AFDD report exporter (Docker container)
- `Dockerfile` - Based on `mcr.microsoft.com/playwright/python:v1.50.0-noble`
- `requirements.txt` - Container dependencies (aws-lambda-powertools, playwright)
- `cim_shared/` - Configuration utilities
  - `config.py` - Environment variable management (CIM credentials, SMTP settings)
- `report_exporter/` - Report export logic
  - `app.py` - Lambda handler for daily report export
  - `browser.py` - Playwright browser automation (login, navigate, download)
  - `emailer.py` - SES SMTP email with CSV attachment
- **Deployment:** Docker image → ECR → Lambda (container image type)
- **Trigger:** EventBridge Scheduler daily at 8:00 AM Sydney time

### File Movement After Processing

| Outcome | Destination |
|---------|-------------|
| Successful parse + Neptune ID found | `newP/` (processed) |
| Parse succeeded but no Neptune mapping | `newIrrevFiles/` (irrelevant) |
| Parse failed | `newParseErr/` (parse error) |

### S3 Archive Structure

Files are archived weekly by the `sbm-weekly-archiver` Lambda using ISO week format:

```
sbm-file-ingester/
├── newP/
│   ├── (active files)
│   └── archived/
│       ├── 2026-W01/
│       ├── 2026-W02/
│       └── ...
├── newIrrevFiles/
│   ├── (active files)
│   └── archived/
│       └── 2026-WXX/
└── newParseErr/
    ├── (active files)
    └── archived/
        └── 2026-WXX/
```

The archiver runs every Monday at UTC 00:00 (AEST 11:00) and moves files from the previous week to the corresponding `archived/YYYY-WXX/` directory.

### Custom CloudWatch Log Groups
- `sbm-ingester-execution-log` - Processing start/end timestamps
- `sbm-ingester-error-log` - Application errors
- `sbm-ingester-parse-error-log` - File parsing failures
- `sbm-ingester-runtime-error-log` - Non-parse runtime issues
- `sbm-ingester-metrics-log` - Daily metrics (file counts, monitor points)

## Infrastructure (Terraform)

Infrastructure is managed via Terraform in `terraform/` directory (split into multiple files).

**Files:**
| File | Purpose |
|------|---------|
| `ingester.tf` | Main ingester, redrive, NEM12 mappings Lambdas |
| `weekly_archiver.tf` | Weekly archiver Lambda |
| `glue.tf` | Glue job, trigger Lambda, EventBridge rule |
| `logs.tf` | CloudWatch Log Groups |
| `monitoring.tf` | CloudWatch Alarms, SNS topic |
| `nem12_mappings.tf` | API Gateway for NEM12 mappings |
| `locals.tf` | Shared variables and constants |
| `outputs.tf` | Terraform outputs |
| `optima_exporter.tf` | Optima exporter Lambda, DynamoDB table, EventBridge Scheduler, CloudWatch Alarm |
| `cim_exporter.tf` | CIM exporter Lambda (container image), ECR repository, EventBridge Scheduler, CloudWatch Alarm |

**Commands:**
```bash
cd terraform
terraform init      # Initialize providers
terraform plan      # Preview changes
terraform apply     # Apply changes
```

**Managed Resources:**
- Lambda functions (8: 7 zip + 1 Docker container)
- ECR repository (cim-exporter)
- Glue job (1)
- SQS queues (main + DLQ with redrive policy)
- S3 event notifications
- CloudWatch Log Groups (9)
- CloudWatch Alarms (DLQ messages, Lambda errors)
- DynamoDB table (idempotency)
- SNS topic (alerts)
- API Gateway + API Key (manual trigger endpoint)
- EventBridge rules (hourly NEM12 mappings, hourly Glue trigger, weekly archiver, daily Optima export)

**Outputs:**
- `sbm_api_invoke_url` - API endpoint for manual NEM12 mapping refresh
- `sbm_api_key_value` - API key (sensitive, stored in `terraform/.env`)
- `sbm_dlq_url` - Dead Letter Queue URL
- `sbm_alerts_topic_arn` - SNS topic for subscribing to alerts

## ⚠️ Manual Sync: CI/CD IAM Policy

The managed IAM policy `sbm-ingester-cicd-policy` (attached to IAM user `sbm-ingester-github-actions`) hard-codes a Lambda ARN whitelist for `lambda:UpdateFunctionCode`. **This policy is NOT managed by Terraform** — it is edited manually via `aws iam create-policy-version` to avoid keeping long-lived secrets in TF state and the 5-version cap on managed policies.

**Whenever you rename, add, or remove a Lambda function**, you MUST also update this policy. Otherwise the next GitHub Actions deploy will fail with `AccessDeniedException: lambda:UpdateFunctionCode`.

Current whitelisted Lambdas (as of last verified sync, 2026-05-06):
- `sbm-files-ingester`
- `sbm-files-ingester-redrive`
- `sbm-files-ingester-nem12-mappings-to-s3`
- `sbm-weekly-archiver`
- `sbm-glue-trigger`
- `optima-nem12-exporter`
- `optima-billing-exporter`
- `optima-demand-exporter`
- `cim-report-exporter`

**To update:**
```bash
# 1. Fetch current policy
aws iam get-policy-version \
    --policy-arn arn:aws:iam::318396632821:policy/sbm-ingester-cicd-policy \
    --version-id $(aws iam get-policy --policy-arn arn:aws:iam::318396632821:policy/sbm-ingester-cicd-policy --query 'Policy.DefaultVersionId' --output text) \
    --query 'PolicyVersion.Document' > /tmp/policy.json

# 2. Edit /tmp/policy.json (Statement with Sid "LambdaUpdateFunctions" → Resource list)

# 3. If already at 5 versions, delete the oldest non-default version:
aws iam delete-policy-version --policy-arn arn:aws:iam::318396632821:policy/sbm-ingester-cicd-policy --version-id <vN>

# 4. Create new default version
aws iam create-policy-version \
    --policy-arn arn:aws:iam::318396632821:policy/sbm-ingester-cicd-policy \
    --policy-document file:///tmp/policy.json --set-as-default
```

## AWS Resources

- **Region:** ap-southeast-2
- **S3 Buckets:** `sbm-file-ingester` (input), `hudibucketsrc` (output), `gega-code-deployment-bucket` (code)
- **SQS:** `sbm-files-ingester-queue` (900s visibility), `sbm-files-ingester-dlq` (14 day retention)
- **DynamoDB:** `sbm-ingester-idempotency` (PAY_PER_REQUEST, TTL enabled)
- **Neptune:** Stores NEM12 ID → sensor ID mappings
- **SNS:** `sbm-ingester-alerts` - Subscribe for DLQ/error notifications
- **API Gateway:** `/nem12-mappings` endpoint (API key required, 500 req/day limit)
- **ECR:** `cim-exporter` repository for Docker container Lambda

## Testing

Tests are located in `tests/unit/` using pytest with moto for AWS mocking. **Total: 525 tests.**

| Test File | Tests | Description |
|-----------|-------|-------------|
| `test_nem_adapter.py` | 22 | NEM12/NEM13 parsing, column naming, split_days |
| `test_nem12_streaming.py` | 36 | Streaming NEM12 parser, fallback paths |
| `test_batch_s3_writes.py` | 16 | Buffer flushing, batch triggers, CSV format |
| `test_non_nem_parsers.py` | 20 | Envizi, RACV, Optima, ComX parsers |
| `test_common.py` | 2 | Constants and utilities |
| `test_integration.py` | 11 | End-to-end pipeline, file movement |
| `test_edge_cases.py` | 15 | File processor edge cases, batch flush, lambda handler |
| `test_nem_adapter_edge_cases.py` | 15 | NMI processing errors, empty channels, unit handling |
| `test_non_nem_parsers_edge_cases.py` | 15 | Bulk water, S3 upload, RACV zeros, ComX edge cases |
| `test_file_stability.py` | 14 | Streaming file stability checks, requeue logic |
| `test_weekly_archiver.py` | 26 | Weekly archiver Lambda, concurrent processing, error handling |
| `test_glue_hudi_import.py` | 63 | Glue ETL script, Hudi config, batch processing, archiving |
| `test_glue_trigger.py` | 24 | Glue trigger Lambda, file counting, job start |
| `test_process_nem12_locally.py` | 16 | Local NEM12 processing script |
| `test_import_optima_config_to_dynamodb.py` | 36 | DynamoDB config import script |
| `test_billing_csv_to_hudi.py` | 34 | Billing CSV → Hudi conversion |
| `test_noosa_solar_parser.py` | 20 | Noosa Solar parser, Fronius mode mapping, p: prefix bypass |

### Optima Exporter Tests (122 tests)

Tests for the optima_exporter module are organized in a directory structure mirroring the source code:

```
tests/unit/optima_exporter/
├── conftest.py                        # Shared fixtures (reset_env, reload_* functions)
├── test_e2e_full_chain.py             # 1 test - E2E: BidEnergy → Hudi source
├── optima_shared/
│   ├── test_auth.py                   # 6 tests - BidEnergy login, cookie extraction
│   ├── test_config.py                 # 13 tests - Environment config, project credentials
│   └── test_dynamodb.py               # 14 tests - DynamoDB queries, site lookup
├── nem12_exporter/
│   ├── test_app.py                    # 2 tests - Lambda handler
│   ├── test_downloader.py             # 38 tests - CSV download, date formatting, NEM12 prefix rewrite
│   ├── test_processor.py              # 29 tests - Export orchestration, parallel processing
│   ├── test_prefix_scoping.py         # 2 tests - NMI prefix scoping
│   └── test_uploader.py               # 7 tests - S3 upload
└── billing_exporter/
    ├── test_app.py                    # 2 tests - Lambda handler
    └── test_trigger.py                # 8 tests - Billing report trigger
```

Test fixtures in `tests/unit/fixtures/`:
- `nem12_sample.csv` - NEM12 interval meter data
- `nem13_sample.csv` - NEM13 accumulation meter data
- `nem12_multiple_meters.csv` - Multi-NMI test file
- `optima_interval/` - Real BidEnergy "Export Interval Usage Csv" downloads (verbatim production responses for spec verification + regression tests):
  - `interval_au_single_day.csv` (4.8 KB) - AU NMI 2002105104, single-day happy path
  - `interval_nz_single_day.csv` (4.9 KB) - NZ ICP 0000010008MQCB6, alphanumeric identifier coverage
  - `interval_au_4month.csv` (573 KB) - AU 5856 rows spanning Apr/May/Jun/Jul (multi-month)
  - `interval_empty.csv` (148 B) - "No data is available" sentinel CSV

## In-Progress Work

- **`optima-interval-exporter`** Lambda — being added as the new primary interval data source (replacing NEM12 export). The existing `optima-nem12-exporter` Lambda code, IAM, log group, and alarm are kept intact for backup; only its 2 EventBridge daily schedules will be disabled in favour of new `optima-bunnings-interval-daily` / `optima-racv-interval-daily` schedules at the same 14:00 Sydney slot. Includes a 4-line fix to `interval_parser` to handle the BidEnergy "No data is available" sentinel gracefully (currently crashes ~25% of daily site responses with `UFuncTypeError`).
  - Spec: [`docs/superpowers/specs/2026-05-06-optima-interval-exporter-design.md`](docs/superpowers/specs/2026-05-06-optima-interval-exporter-design.md)
  - Plan: [`docs/superpowers/plans/2026-05-06-optima-interval-exporter.md`](docs/superpowers/plans/2026-05-06-optima-interval-exporter.md) (13 tasks, including `terraform apply` user gate at Task 10a → 10b and a single push gate at Task 12)
  - Until execution begins, `optima_exporter.zip` artefact still bundles only 3 modules (nem12, billing, demand) — `interval_exporter/` will be added as a 4th in plan Task 9. CI/CD policy will need to bump from v9 → v10 to whitelist the new Lambda ARN.
