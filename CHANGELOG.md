# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
