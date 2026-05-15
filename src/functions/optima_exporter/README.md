# Optima Exporter

Exports energy data from BidEnergy/Optima platform. This module contains four Lambda functions that operate independently:

- **Interval Exporter** - Downloads ZIP-wrapped interval CSV files and uploads to S3 for ingestion
- **NEM12 Exporter** - Downloads NEM12 CSV files and uploads to S3 for ingestion (backup/manual invoke)
- **Demand Exporter** - Downloads demand profile CSV files and uploads to S3 for ingestion
- **Billing Exporter** - Triggers monthly billing report generation (delivered via email)

## Table of Contents

- [Module Structure](#module-structure)
- [Data Flow](#data-flow)
- [Lambda Functions](#lambda-functions)
- [Event Parameters](#event-parameters)
- [Environment Variables](#environment-variables)
- [DynamoDB Configuration](#dynamodb-configuration)
- [Scheduling](#scheduling)
- [Testing](#testing)

## Module Structure

```
optima_exporter/
├── optima_shared/           # Shared utilities
│   ├── __init__.py
│   ├── auth.py              # BidEnergy login (cookie-based authentication)
│   ├── config.py            # Environment variable management
│   └── dynamodb.py          # Site configuration queries from DynamoDB
├── nem12_exporter/          # Lambda 1: NEM12 export
│   ├── __init__.py
│   ├── app.py               # Lambda handler
│   ├── downloader.py        # CSV download from BidEnergy API
│   ├── processor.py         # Parallel site processing (ThreadPoolExecutor)
│   └── uploader.py          # S3 upload to ingestion bucket
├── billing_exporter/        # Lambda 2: Billing report trigger
│   ├── __init__.py
│   ├── app.py               # Lambda handler
│   └── trigger.py           # Monthly Usage and Spend report API
├── demand_exporter/         # Lambda 3: Demand profile export
│   ├── __init__.py
│   ├── app.py
│   ├── downloader.py
│   ├── processor.py
│   └── uploader.py
├── interval_exporter/       # Lambda 4: Interval CSV export
│   ├── __init__.py
│   ├── app.py
│   ├── downloader.py
│   ├── processor.py
│   └── uploader.py
└── tmp/                     # Local development test files (not deployed)
```

## Data Flow

### Interval Exporter

```
EventBridge (daily)
    │
    ▼
Lambda (optima-interval-exporter)
    │
    ├─→ DynamoDB (sbm-optima-config) ─→ Get site list by project
    │
    ├─→ BidEnergy Login ─→ Get session cookie
    │
    ├─→ For each site (parallel, max 20 workers):
    │       │
    │       ├─→ Download ZIP from BidEnergy API
    │       │       (POST /BuyerReport/exportdailyusagecsv)
    │       │
    │       ├─→ Extract inner CSV
    │       │
    │       └─→ Upload CSV to S3 (sbm-file-ingester/newTBP/)
    │
    └─→ Files processed by sbm-files-ingester pipeline
```

### Billing Exporter

```
EventBridge (1st of month)
    │
    ▼
Lambda (optima-billing-exporter)
    │
    ├─→ BidEnergy Login ─→ Get session cookie
    │
    └─→ Trigger report for each country (AU, NZ):
            │
            └─→ GET /BuyerReportRead/Usage
                    │
                    ▼
            BidEnergy generates report (async)
                    │
                    ▼
            Email sent to registered account
                    │
                    ▼
            Forwarded to client_ec_data@gegroup.com.au
                    │
                    ▼
            Attachment uploaded to S3 (sbm-file-ingester/newTBP/)
                    │
                    └─→ Files processed by sbm-files-ingester pipeline
```

## Lambda Functions

| Function | Memory | Timeout | Purpose |
|----------|--------|---------|---------|
| `optima-interval-exporter` | 256 MB | 900s (15min) | Downloads interval CSV files for all sites, uploads to S3 |
| `optima-nem12-exporter` | 256 MB | 900s (15min) | Downloads NEM12 CSV files for all sites, uploads to S3 (backup/manual invoke) |
| `optima-demand-exporter` | 256 MB | 900s (15min) | Downloads demand profile CSV files for all sites, uploads to S3 |
| `optima-billing-exporter` | 128 MB | 120s | Triggers billing report generation (async, email delivery) |

**Note:** X-Ray tracing is disabled to avoid "Message too long" errors from large parallel operations.

## Event Parameters

### Interval/NEM12/Demand Exporters

```json
{
  "project": "bunnings",       // Required: "bunnings" or "racv"
  "nmi": "NMI001",            // Optional: Single NMI to export (default: all)
  "startDate": "2026-01-01",  // Optional: ISO date format YYYY-MM-DD
  "endDate": "2026-01-07",    // Optional: ISO date format YYYY-MM-DD
  "mode": "previous_month"     // Optional: re-ingest the previous calendar month
                               // (overrides startDate/endDate and OPTIMA_DAYS_BACK).
                               // Interval and Demand exporters only.
}
```

If `startDate`/`endDate`/`mode` are not provided, defaults to the last `OPTIMA_DAYS_BACK` days ending yesterday (interval and demand: 3 days; NEM12: 1 day). The interval exporter accepts AU NMIs and NZ ICP identifiers.

### Billing Exporter

```json
{
  "project": "bunnings",       // Required: "bunnings" or "racv"
  "country": "AU",            // Optional: "AU" or "NZ" (default: all configured)
  "startDate": "Feb 2025",    // Optional: "Mmm YYYY" format
  "endDate": "Jan 2026"       // Optional: "Mmm YYYY" format
}
```

If `startDate`/`endDate` are not provided, defaults to the past 12 months (configurable via `OPTIMA_BILLING_MONTHS`).

## Environment Variables

### Common Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `BIDENERGY_BASE_URL` | BidEnergy API base URL | `https://app.bidenergy.com` |
| `OPTIMA_CONFIG_TABLE` | DynamoDB table for site config | `sbm-optima-config` |
| `S3_UPLOAD_BUCKET` | S3 bucket for uploads | `sbm-file-ingester` |
| `S3_UPLOAD_PREFIX` | S3 key prefix | `newTBP/` |

### Interval/NEM12/Demand Exporter Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `OPTIMA_DAYS_BACK` | Days of data to export (end_date - days + 1 through end_date). Interval/Demand Lambdas deployed with `3`; NEM12 with `1`. | `1` |
| `OPTIMA_MAX_WORKERS` | Parallel download threads | `20` |

### Billing Exporter Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `OPTIMA_BILLING_MONTHS` | Months of billing data to request | `12` |

### Project Credentials (per project)

| Variable | Description |
|----------|-------------|
| `OPTIMA_BUNNINGS_USERNAME` | BidEnergy username for Bunnings |
| `OPTIMA_BUNNINGS_PASSWORD` | BidEnergy password for Bunnings |
| `OPTIMA_BUNNINGS_CLIENT_ID` | BidEnergy client ID for Bunnings |
| `OPTIMA_BUNNINGS_COUNTRIES` | Comma-separated countries (e.g., `AU,NZ`) |
| `OPTIMA_RACV_USERNAME` | BidEnergy username for RACV |
| `OPTIMA_RACV_PASSWORD` | BidEnergy password for RACV |
| `OPTIMA_RACV_CLIENT_ID` | BidEnergy client ID for RACV |
| `OPTIMA_RACV_COUNTRIES` | Comma-separated countries (default: `AU`) |

## DynamoDB Configuration

Site mappings are stored in `sbm-optima-config` table:

| Attribute | Type | Description |
|-----------|------|-------------|
| `project` | String | Partition key (project name, e.g., "bunnings") |
| `nmi` | String | Sort key (National Meter Identifier) |
| `siteIdStr` | String | BidEnergy site GUID |
| `siteName` | String | Human-readable site name |
| `country` | String | Country code (`AU` or `NZ`) - optional |

**Example query:**
```python
# Get all sites for bunnings
get_sites_for_project("bunnings")

# Get specific site by NMI
get_site_by_nmi("bunnings", "NMI001")
```

### Importing Site Configuration

Use the script to import site mappings from CSV:

```bash
uv run scripts/import_optima_config_to_dynamodb.py sites.csv --project bunnings
```

CSV format: `nmi,siteIdStr,siteName,country`

## Scheduling

Configured via EventBridge Scheduler in Terraform (`terraform/optima_exporter.tf`):

| Schedule | Lambda | Description |
|----------|--------|-------------|
| `cron(0 14 * * ? *)` (Australia/Sydney) | interval-exporter | Daily at 14:00 Sydney time |
| Manual invoke only | nem12-exporter | Backup interval export path |
| `cron(30 14 * * ? *)` (Australia/Sydney) | demand-exporter | Daily at 14:30 Sydney time |
| `cron(0 7 1 * ? *)` (Australia/Sydney) | billing-exporter | 1st of each month at 07:00 Sydney time |

### Manual Invocation

```bash
# Invoke interval exporter for specific NMI/ICP
aws lambda invoke \
  --function-name optima-interval-exporter \
  --payload '{"project":"bunnings","nmi":"NMI001"}' \
  response.json

# Invoke NEM12 exporter backup path for specific NMI
aws lambda invoke \
  --function-name optima-nem12-exporter \
  --payload '{"project":"bunnings","nmi":"NMI001"}' \
  response.json

# Invoke billing exporter for specific country
aws lambda invoke \
  --function-name optima-billing-exporter \
  --payload '{"project":"bunnings","country":"AU"}' \
  response.json
```

## Testing

Tests are located in `tests/unit/optima_exporter/` covering all modules:

```
tests/unit/optima_exporter/
├── conftest.py                    # Shared fixtures
├── test_e2e_full_chain.py         # 1 test - E2E: BidEnergy → Hudi source
├── optima_shared/
│   ├── test_auth.py               # 6 tests
│   ├── test_config.py             # 13 tests
│   └── test_dynamodb.py           # 14 tests
├── nem12_exporter/
│   ├── test_app.py                # 2 tests
│   ├── test_downloader.py         # 38 tests - CSV download, date format, NEM12 prefix rewrite
│   ├── test_processor.py          # 29 tests
│   ├── test_prefix_scoping.py     # 2 tests - NMI prefix scoping
│   └── test_uploader.py           # 7 tests
├── interval_exporter/
│   ├── test_app.py
│   ├── test_downloader.py
│   ├── test_processor.py
│   └── test_uploader.py
└── billing_exporter/
    ├── test_app.py                # 2 tests
    └── test_trigger.py            # 8 tests
```

**Run tests:**

```bash
# Run all optima exporter tests
uv run pytest tests/unit/optima_exporter/ -v

# Run specific module tests
uv run pytest tests/unit/optima_exporter/nem12_exporter/ -v

# Run with coverage
uv run pytest tests/unit/optima_exporter/ --cov=src/functions/optima_exporter
```

## Related Files

- **Terraform:** `terraform/optima_exporter.tf` - Lambda, DynamoDB, EventBridge Scheduler
- **Deployment:** `.github/workflows/main.yml` - CI/CD pipeline (builds `optima_exporter.zip`)
- **Config Import:** `scripts/import_optima_config_to_dynamodb.py` - Site configuration loader
