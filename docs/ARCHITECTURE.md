# SBM Ingester Architecture

Reference document for the post-refactor (per-file ingest) architecture. Intended for engineers and LLM agents who need a structural map of the codebase. For setup, deploy, and command reference see `README.md` and the project-level `CLAUDE.md` (gitignored).

## Overview

SBM Ingester is a serverless file ingestion pipeline for building energy data. Meter data files (NEM12/NEM13 plus several vendor CSV formats) land in S3, are processed per-file by Lambda, and emerge as Hudi-source CSVs that a Glue ETL job upserts into an Apache Hudi table on the data lake.

The repo contains:

- **5 zip-package Lambdas**: `sbm-files-ingester` (main), `sbm-files-ingester-redrive`, `sbm-files-ingester-nem12-mappings-to-s3`, `sbm-weekly-archiver`, `sbm-glue-trigger`.
- **4 Optima/CIM exporter Lambdas**: `optima-nem12-exporter`, `optima-billing-exporter`, `optima-demand-exporter`, `optima-interval-exporter`, plus the container-based `cim-report-exporter`.
- **1 Glue ETL job**: `DataImportIntoLake` (PySpark, Hudi upserts).

This document focuses on the main file processor; the exporters and Glue job have their own design specs referenced at the bottom.

## File Processor Architecture

The main Lambda (`sbm-files-ingester`) lives under `src/functions/file_processor/` and is split across four modules so each layer has a single concern.

| Module | Role |
|--------|------|
| `app.py` | SQS adapter. Decodes SQS records, runs the file-stability check, requeues if unstable, calls `ingest_file`. |
| `pipeline.py` | Orchestrator. Hosts `ingest_file`, the idempotent boundary function. |
| `csv_writer.py` | `HudiSourceCsvWriter` — buffered + staged write to the Hudi-source S3 prefix with `commit` / `abort` lifecycle. |
| `persistence.py` | `InstrumentedDynamoDBPersistenceLayer` — subclass that emits an `idempotent_cache_hit` structured log on conflict, then re-raises so Powertools handles the cached response normally. |

### Entry point and idempotency boundary

`lambda_handler(event, context)` in `app.py` is a SQS event source. For each record:

1. Parse the SQS body, increment `_retry_count` tracking for requeue.
2. Call `check_file_stability(bucket, key)` (two consecutive HEAD-object calls with stable `ContentLength`).
3. If unstable and `retry_count < MAX_REQUEUE_RETRIES` (3, aligned with SQS `maxReceiveCount=3`), requeue with a `REQUEUE_DELAY_SECONDS=60` delay. Otherwise skip.
4. If stable: build a `SourceFile` and call `ingest_file(source_file=SourceFile(bucket=..., key=...))`.

`ingest_file` in `pipeline.py` is the idempotent boundary. The decorator order is load-bearing:

```python
@tracer.capture_method               # OUTER — keeps cache hits in X-Ray
@idempotent_function(                # INNER — closest to def
    data_keyword_argument="source_file",
    persistence_store=persistence_layer,
    config=idempotency_config,
    output_serializer=_parser_outcome_serializer,
)
def ingest_file(source_file: SourceFile) -> ParserOutcome: ...
```

`SourceFile` (`src/shared/source_file.py`) is a `@dataclass(frozen=True, slots=True)` with two fields: `bucket`, `key`. Powertools' `_prepare_data` detects `__dataclass_fields__` and feeds `dataclasses.asdict(data)` into the SHA-256 idempotency key — no custom serializer is needed for the input. The output (`ParserOutcome`) carries a `frozenset` and a `Counter` that Powertools' default JSON encoder cannot handle, so `_ParserOutcomeIdempotencySerializer` projects to and rehydrates from plain dicts.

### Boundary contract: what raises vs what returns

All side effects live INSIDE the boundary so that duplicate SQS deliveries hit the Powertools cache and replay no state-changing op. The exception policy follows from this:

- **Deterministic content failures** (broken file, unknown format, malformed shape) → caught and RETURNED as `ParserOutcome(status="parse_failed", reason="parser_error"|"processing_error")`. Cached for the 12 h TTL so retries do not keep replaying the broken file.
- **Transient infrastructure failures** (S3 5xx, DynamoDB throttle, etc.) → RAISED. Powertools deletes the in-progress record so SQS retry can re-execute. `csv_writer.abort()` is called first to roll back any staged Hudi-source objects.

A failed source-file move after a successful Hudi commit is treated as transient: the Hudi writer is aborted and `ProcessingError` is raised so SQS retry re-executes from scratch.

## ParserOutcome contract

`src/shared/parsers/outcome.py` defines the contract every parser path returns. It is a `@dataclass(frozen=True)`. The two relevant `Literal` enumerations:

**Statuses** (5):

- `processed` — at least one row written to the Hudi source.
- `processed_empty` — file was understood but yielded no usable rows (sentinel/empty/all-skipped/all-unknown-suffix).
- `unmapped` — file parsed and produced candidate rows, but none of the meter identifiers resolved to a Neptune ID.
- `processed_external` — parser wrote rows to an external destination (currently used by parsers that publish to a non-Hudi S3 path).
- `parse_failed` — caught `ParserError`/`ProcessingError`; only `ingest_file`'s exception handler produces this.

**Reasons** (9):

| Reason | Set by |
|--------|--------|
| `no_data_sentinel` | NEM envelope-only or vendor "no data available" sentinel. |
| `zero_rows` | Parser saw 0 rows in input. |
| `all_blank` | Every candidate cell was blank. |
| `all_zero_valid` | All values parsed but all equal zero (vendor placeholder). |
| `all_unknown_suffix` | No NMI suffix in input matched the known data-stream alphabet. |
| `all_skipped` | All rows hit a `SkipReason`. |
| `external_gegoptimareports` | Outcome belongs to an external destination flow. |
| `parser_error` | `ParserError` caught in `ingest_file`. |
| `processing_error` | `ProcessingError` caught in `ingest_file`. |

`derive_final(...)` is the disposition ladder. Given final accumulators (rows_written / candidate_row_count / unmapped_count / unsupported_suffixes / rows_skipped), it returns a new outcome with the correct `(status, reason)`:

1. `rows_written > 0` → `processed`.
2. all candidates unmapped → `unmapped`.
3. no candidates but unsupported suffixes seen → `processed_empty(all_unknown_suffix)`.
4. no candidates, rows skipped → `processed_empty(all_skipped)`.
5. else → `processed_empty(self.reason)`.

`derive_final` never produces `parse_failed`; that status only arises from caught exceptions in the boundary.

A test-only helper `tests/helpers/outcome_invariants.assert_parser_outcome_invariants` enforces the spec's cross-field invariants in CI without crashing production.

See `docs/superpowers/specs/2026-05-06-parser-outcome-semantics-design.md` for the full contract (cross-field invariants, skip-reason taxonomy, Counter vs row-count semantics).

## Data flow

```
S3 sbm-file-ingester/newTBP/   (input drop zone)
   │
   ▼
SQS sbm-files-ingester-queue   (batch_size=1, maxReceiveCount=3)
   │
   ▼
Lambda sbm-files-ingester
   │   ├─ idempotency check  ─► DynamoDB sbm-ingester-idempotency (TTL 12 h)
   │   ├─ parse              ─► NEM12 stream → non-NEM dispatcher fallback
   │   ├─ map identifiers    ─► S3 sbm-file-ingester/nem12_mappings.json
   │   └─ write              ─► S3 hudibucketsrc/sensorDataFilesStaging/
   │                            ─commit─► hudibucketsrc/sensorDataFiles/
   │
   ▼ source-file disposition
sbm-file-ingester/{newP/, newIrrevFiles/, newParseErr/}
```

Disposition table by final outcome status:

| Status | Destination prefix |
|--------|--------------------|
| `processed`, `processed_empty`, `processed_external` | `PROCESSED_DIR` (`newP/`) |
| `unmapped` | `UNMAPPED_DIR` (`newIrrevFiles/`) |
| `parse_failed` | `PARSE_ERR_DIR` (`newParseErr/`) |

Hudi-source CSVs in `hudibucketsrc/sensorDataFiles/` are picked up by the `DataImportIntoLake` Glue job (triggered hourly by `sbm-glue-trigger` when file count meets threshold) and upserted into the Hudi table by `(sensorid, ts)`.

Idempotency cache hits are observable through:

- Structured `idempotent_cache_hit` log lines emitted by `InstrumentedDynamoDBPersistenceLayer`.
- A CloudWatch alarm on DynamoDB's native `ConditionalCheckFailedRequests` metric for the idempotency table (`terraform/monitoring.tf`). No custom Lambda metric is published for cache hits.

## Constants and naming

Constants live in `src/shared/common.py`:

| Constant | Value | Purpose |
|----------|-------|---------|
| `INPUT_BUCKET` | `"sbm-file-ingester"` | Source bucket; all `newTBP/`/`newP/`/`newIrrevFiles/`/`newParseErr/` keys live here. |
| `HUDI_BUCKET` | `"hudibucketsrc"` | Hudi-source bucket — CSVs the Glue job reads. |
| `HUDI_FINAL_PREFIX` | `"sensorDataFiles"` | Committed Hudi-source key prefix. |
| `HUDI_STAGING_PREFIX` | `"sensorDataFilesStaging"` | Pre-commit staging prefix (per-writer-token subkey). |
| `PROCESSED_DIR` | `"newP/"` | |
| `UNMAPPED_DIR` | `"newIrrevFiles/"` | Historical S3 name preserved; logical concept is "unmapped". |
| `PARSE_ERR_DIR` | `"newParseErr/"` | |
| `*_LOG_GROUP` | various | Custom CloudWatch log groups (parse-error, runtime-error, error, execution, metrics). |

Naming conventions worth knowing:

- **`HudiSourceCsvWriter`** writes Hudi-**source** CSVs. It does NOT write Apache Hudi tables — the Glue job does. The class name is precise about which layer it operates on.
- **`dispatch_non_nem(file_path)`** in `src/shared/parsers/dispatcher.py` is the entry point for the non-NEM parser cascade. The legacy alias `get_non_nem_df` is retained for backwards compatibility but returns only `dataframes`.
- Parser subpackages are vendor-scoped: `envizi/`, `optima/`, `racv/`, `green_square/`. Each parser module is a single responsibility for one source format.

## Testing strategy

Tests live under `tests/unit/`. Two conftests cooperate:

- **`tests/conftest.py`** — top-level. Sets up `sys.path` for Lambda-style imports, seeds `os.environ["SQS_QUEUE_URL"]` for module-import-time reads, and defines the new shared moto fixtures: `mock_s3_buckets` (creates `INPUT_BUCKET` + `HUDI_BUCKET` + seeds a sample `nem12_mappings.json`), `mock_dynamodb_idempotency` (creates the idempotency table), and the `file_in_newtbp` factory.
- **`tests/unit/conftest.py`** — legacy compat. Patches `aws_lambda_powertools.utilities.idempotency.idempotent_function` to a passthrough so the bulk of tests do not have to set up DynamoDB. Keeps the parser/streaming/edge-case test suites cheap.

Tests that need REAL idempotency behaviour import the boundary via the underlying module (`from functions.file_processor import pipeline as _pipeline_mod`), bypassing the passthrough patch. As of HEAD this set is:

- `test_pipeline.py`
- `test_idempotency_boundary.py`
- `test_nem_envelope_short_circuit.py`
- `test_unmapped_disposition.py`
- `test_audit_sidecar_contract.py`

Cross-field outcome invariants are enforced in tests via `tests/helpers/outcome_invariants.assert_parser_outcome_invariants`. Production code intentionally does NOT enforce these in `__post_init__` to avoid a latent crash surface.

Test count baseline at HEAD: **773 collected** (20 deselected) via `uv run pytest --collect-only -q`.

## Open follow-ups and known limitations

- **Multi-site timezone handling is not yet generalised.** Only `racv/noosa_solar.py` explicitly detects AEDT and converts to AEST. Every other parser treats whatever the source produced as naive AEST. The implicit contract is "naive timestamps are AEST"; a follow-up spec is needed before any source produces explicit-tz data.
- **`optima/bunnings_billing.py` and `optima/demand.py` write directly to S3 outside the `HudiSourceCsvWriter` lifecycle** (each calls `boto3.client("s3").put_object` directly). The Hudi `(sensorid, ts)` upsert means duplicate retries are functionally idempotent, but these parsers do not participate in the writer's staging/commit/abort discipline. Reconciling them is a future refactor candidate.
- **DynamoDB idempotency hash format changed in the per-file refactor** (old: list-of-dicts batch key → new: `SourceFile` dataclass single-file key). Old and new records do not collide; the 12 h TTL means the transition window self-clears.

## Cross-references

Specs:

- `docs/superpowers/specs/2026-05-06-parser-outcome-semantics-design.md` — `ParserOutcome` contract, statuses, reasons, skip-reason taxonomy, cross-field invariants.
- `docs/superpowers/specs/2026-05-07-per-file-ingest-refactor-design.md` — per-file boundary refactor, idempotency contract, module split rationale.
- `docs/superpowers/specs/2026-05-06-optima-interval-exporter-design.md` — Optima interval exporter (out-of-band data source feeding the same ingestion pipeline).

Plans:

- `docs/superpowers/plans/2026-05-07-per-file-ingest-refactor.md` — the 17-task plan that landed this architecture.

Other module-level guides (kept up to date alongside the relevant modules):

- `src/functions/optima_exporter/` — Optima/BidEnergy exporters; see `docs/superpowers/specs/2026-05-06-optima-interval-exporter-design.md`.
- `src/functions/cim_exporter/` — CIM AFDD report exporter (Playwright + Docker/ECR).
- `src/glue/hudi_import/script.py` — Glue ETL script (PySpark, Hudi upsert).
- `terraform/` — IaC for Lambdas, Glue, SQS, S3 events, alarms.
