# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.0] - 2026-01-15

### Added
- Glue ETL job (`DataImportIntoLake`) for Apache Hudi data lake integration
  - PySpark script for batch CSV import to Hudi table
  - Record key: `sensorId + ts`, partition by year
  - Concurrent file archiving with 10 workers
  - Configurable batch size (400 files) and max runtime (4 hours)
  - Testing parameters: `MAX_FILES` and `DRY_RUN` for safe testing
- Glue Trigger Lambda (`sbm-glue-trigger`) for automated job scheduling
  - Hourly EventBridge trigger
  - Configurable file count threshold (default 10)
  - Handles `ConcurrentRunsExceededException` gracefully
- Local NEM12 processing script (`scripts/process_nem12_locally.py`)
  - Process NEM12 files locally and upload to S3
  - Dry-run mode for preview
- CI/CD pipeline now deploys Glue script to S3

### Changed
- Terraform infrastructure split into multiple files for better organization:
  - `ingester.tf` - Main Lambda functions
  - `glue.tf` - Glue job and trigger
  - `logs.tf` - CloudWatch Log Groups
  - `monitoring.tf` - Alarms and SNS
  - `nem12_mappings.tf` - API Gateway
  - `weekly_archiver.tf` - Weekly archiver Lambda
- Test suite expanded to 255 tests (was 157)

## [0.4.0] - 2026-01-14

### Added
- Weekly Archiver Lambda (`sbm-weekly-archiver`) for automated S3 file archiving
  - Archives files to ISO week directories (e.g., `2026-W03/`)
  - Triggered by EventBridge every Monday at UTC 00:00 (AEST 11:00)
  - AWS Lambda Powertools integration (Logger, Tracer, Metrics)
  - Concurrent processing with 50 workers for high throughput
  - Manual invocation support with `target_week` parameter
- Migration script (`scripts/migrate_archives_to_weekly.py`) for converting monthly archives to weekly
  - Multi-threaded processing with configurable workers (default 50)
  - Progress bar with ETA display
  - Dry-run mode for preview
- Local Lambda deployment script (`scripts/deploy-lambda.sh`) for quick iterations
- File stability check for streaming uploads to prevent partial file processing
  - Configurable poll interval and max retries
  - Automatic requeue for unstable files

### Changed
- S3 archive structure changed from monthly (`2025-08/`) to weekly (`2025-W32/`) format
- Weekly Archiver Lambda configuration optimized:
  - Memory increased from 256MB to 1024MB
  - Timeout increased from 300s to 600s
- Main ingester Lambda memory reduced from 1024MB to 512MB (sufficient for workload)
- Improved error handling in Weekly Archiver:
  - `NoSuchKey` errors treated as SKIPPED (file already moved)
  - Returns HTTP 207 for partial success (some files errored)
  - Returns HTTP 400 for invalid `target_week` format
  - Enhanced logging with full context for debugging

### Fixed
- URL encoding bug for files with spaces in filename
- Race condition in archiver when files are moved by concurrent processes

## [0.3.0] - 2025-01-13

### Added
- AWS Lambda Powertools integration (Logger, Tracer, Metrics)
- Structured JSON logging with CloudWatch Logs Insights support
- X-Ray distributed tracing for performance visibility
- Powertools Idempotency with DynamoDB persistence layer
- Package-level exports via `__init__.py` for cleaner imports

### Changed
- Reorganized project structure to standard Lambda layout:
  - `src/functions/` for Lambda handlers
  - `src/shared/` for common modules
  - `tests/unit/` for test files
- Renamed main handler from `gemsDataParseAndWrite.py` to `app.py`
- Unified naming conventions to PEP 8 snake_case:
  - `nonNemParserFuncs.py` → `non_nem_parsers.py`
  - All function/variable names converted to snake_case
- Simplified imports using `from shared import ...` pattern
- Replaced custom CloudWatchLogger with Powertools Logger
- Replaced manual metrics dict with Powertools Metrics

### Removed
- Custom CloudWatchLogger class (replaced by Powertools Logger)
- Manual metrics management functions
- Unnecessary empty `__init__.py` files in function directories

## [0.2.0] - 2025-01-13

### Added
- Comprehensive test suite with 115 tests achieving 100% code coverage
- Edge case tests for NEM adapter, non-NEM parsers, and main processing logic
- Test fixtures for NEM12, NEM13, and multi-meter data files
- Batch S3 write buffering with configurable `BATCH_SIZE` (default 50)
- Professional README following Standard-README specification
- Mermaid architecture diagram in documentation
- AWS mocking with moto for integration tests

### Changed
- Upgraded Python runtime from 3.9/3.12 to 3.13
- Migrated package management from pip to uv
- Improved CloudWatch logging with daily rotating streams
- Enhanced error handling in file parsing with fallback mechanism
- Updated documentation (README.md, CLAUDE.md) with current test metrics

### Fixed
- Lambda handler duplicate processing bug (moved `parseAndWriteData` outside loop)
- CloudWatchLogger.log() signature mismatch in nonNemParserFuncs
- move_s3_file() path parameter handling
- Metrics calculation using `max()` instead of `+=`

## [0.1.0] - 2025-07-31

### Added
- Initial serverless ingestion pipeline
- NEM12/NEM13 meter data file parsing via nemreader library
- Non-NEM parser support (Envizi water/electricity, RACV, Optima, Green Square ComX)
- S3 event-driven architecture with SQS queue
- Lambda functions: ingester, redrive, nem12-mappings-to-s3
- Terraform infrastructure as code
- GitHub Actions CI/CD pipeline
- CloudWatch custom log groups for monitoring
- API Gateway endpoint for manual NEM12 mapping refresh
- DynamoDB idempotency table
- SNS alerts topic for error notifications
