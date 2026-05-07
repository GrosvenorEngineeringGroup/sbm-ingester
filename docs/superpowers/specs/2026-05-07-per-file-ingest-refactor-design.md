# Per-File Ingest Refactor — Design Spec

**Status:** Draft for review
**Date:** 2026-05-07
**Owner:** zeyu

## Problem

`src/functions/file_processor/app.py` is a 1165-line module whose central function `parse_and_write_data(tbp_files: list[dict])` does parse, Hudi write, source-file movement, metrics, and audit in one body. The "batch" shape (`tbp_files: list`) is a vestige: SQS event source mapping is configured with `batch_size = 1` (`terraform/ingester.tf:190`), so the list always contains exactly one entry. The list-shape is not a runtime requirement; it is leaked plumbing that obscures the unit of work.

A post-implementation review of the parser-outcome-semantics branch identified additional issues that bundle naturally with a per-file refactor:

- Identifier names that fail semantic clarity (`tbp_files`, `BUCKET_NAME`, `IRREVFILES_DIR`, `BATCH_SIZE`, `outcome.dfs`, `_flush_buffer_to_s3`, `DirectCSVWriter`, `_candidate_values`, `_compute_dataframe_final_status`, `_looks_like_nem_envelope`, `error_file_path`).
- Two real production-correctness bugs:
  - `SQS_QUEUE_URL` falls back to a hard-coded production URL with hard-coded account ID (`app.py:51-54`); cross-account misconfiguration is silent.
  - SQS visibility timeout (900 s) equals Lambda timeout (900 s); a Lambda finishing at the timeout boundary races with SQS message re-delivery.
- Dead code (`_flush_buffer_to_s3` plus its 10 tests) that bypasses the staging/commit boundary and would re-introduce cross-writer collision risk if re-enabled.
- Observability gaps: idempotency cache hits emit no metric or log; no per-file `parser_outcome` structured log with full signal fields; no `FileProcessingDurationMs` metric.
- Helper duplication: `read_nem12_mappings` (`app.py:417`) parallels `shared.parsers._mappings.get_nem12_mappings`, both loading the same JSON.
- Cross-cutting helpers (`_compute_dataframe_final_status`, `_looks_like_nem_envelope`) live in `app.py` but belong with the contract.

## Goal

Refactor the file processor so that:

1. The per-file unit of work is a single, explicitly-named function `ingest_file(source_file: SourceFile) -> ParserOutcome` decorated with `@idempotent_function`.
2. The Lambda handler is a thin SQS adapter: extract the record, call `ingest_file`, return.
3. All side effects (download, parse, Hudi write, source-file movement, metrics emission, audit sidecar) live INSIDE the idempotent boundary so that duplicate SQS deliveries cannot replay them.
4. Module organization, naming, and observability match what the code actually does.
5. Two production-correctness bugs (SQS_QUEUE_URL fallback, visibility-timeout race) are fixed in this work.

The goal is **production-grade clarity and correctness** under the constraint that no file currently producing Hudi rows on `origin/main` may stop producing them.

## Non-Goals

These are explicitly out of scope for this spec/PR:

### Intentionally NOT done

These were proposed by review but are over-engineered or out of scope for the per-file refactor's goal:

- DynamoDB Point-In-Time-Recovery (12 h TTL data does not benefit).
- `tenacity.retry` around `csv_writer.commit()` (conflicts with Powertools idempotency: SQS at-least-once + Powertools provides retry; adding inner retry obscures the "raise inside boundary to invalidate cache" contract).
- `csv.writer` escaping for Hudi CSV rows (current values — sensor IDs, ISO timestamps, floats, vendor codes — are escape-safe; defensive change without current bug).
- Vendor `quality` value regex validation (vendor codes are well-defined; over-engineering).
- Lambda Layer for pandas (depends on deployment artifact size; verify before splitting).
- Lambda memory tuning via `aws-lambda-power-tuning` (deferred to a separate optimization investigation).
- Six-module split inside `file_processor/` (three modules is sufficient and avoids navigation overhead for a Lambda-sized project).
- "Weak-agree" renames the user did not flag as priorities (`check_file_stability` → `wait_for_stable_file`, `requeue_message` → `redeliver_with_backoff`, `samples_sink` → `audit_samples`, `CSVUploadJob` → `StagedCsvUpload`, dropping `_` prefix from `_coerce.py` / `_mappings.py`, `NMI_DATA_STREAM_COMBINED` → `NMI_VALID_CHANNEL_CODES`).

If any Tier 3 item is needed later, it gets its own ticket and PR.

## Architecture

### Three-module split

`src/functions/file_processor/` is reorganized into three modules with clear responsibilities:

```
src/functions/file_processor/
├── app.py            # ~80 lines: lambda_handler (SQS adapter only)
├── pipeline.py       # ingest_file + @idempotent_function + side-effect orchestration
└── csv_writer.py     # HudiSourceCsvWriter + StagedCsvUpload
```

Cross-cutting helpers move to `src/shared/`:
- `_is_nem_envelope_only(file_path)` → `src/shared/nem_adapter.py` (alongside other NEM-format helpers).
- `_compute_dataframe_final_status(...)` → method on `ParserOutcome`: `outcome.derive_final(rows_written, candidate_row_count, unmapped_count, unsupported_suffixes, rows_skipped)`.

### Flow diagram

```
SQS message (batch_size=1)
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│ app.py: lambda_handler(event, context)                      │
│  • Extract Records[0] → bucket, key                         │
│  • File-stability check + redelivery (existing logic)       │
│  • Build SourceFile(bucket=..., key=...)                    │
│  • correlation_id = SQS messageId                           │
│  • Call ingest_file(source_file=src) → ParserOutcome        │
│    (or raises if transient infra error)                     │
│  • Return statusCode 200 with outcome summary               │
│                                                             │
│  No business decisions made here; SQS adapter only.         │
└─────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────┐
│ pipeline.py: ingest_file(source_file: SourceFile)           │
│ ────────────────────────────────────────────────────────────│
│ @tracer.capture_method                                      │
│ @idempotent_function(data_keyword_argument="source_file",   │
│                      persistence_store=...,                 │
│                      config=...)                            │
│ ────────────────────────────────────────────────────────────│
│  with tempfile.TemporaryDirectory() as tmp_dir:             │
│    • Download to tmp_dir                                    │
│    • Try NEM12 streaming parser → non-NEM dispatcher        │
│      (with NEM envelope short-circuit for empty files)      │
│      (batch-parser fallback removed — see Behavioral        │
│       Simplifications)                                      │
│    • Compute final outcome via                              │
│      ParserOutcome.derive_final(...) for DataFrame path     │
│    • Write Hudi rows via HudiSourceCsvWriter (commit/abort)       │
│    • Move source file by outcome.status                     │
│      (newP/ | newIrrevFiles/ | newParseErr/)                │
│    • Emit per-file CloudWatch metrics                       │
│    • Write audit sidecar if rows_skipped > 0 or             │
│      unmapped_count > 0 or unsupported_suffixes != ∅       │
│    • Emit per-file structured "parser_outcome" log          │
│  Return final ParserOutcome                                 │
│                                                             │
│  All side effects inside the boundary → safe under          │
│  Powertools cache-skip semantics.                           │
└─────────────────────────────────────────────────────────────┘
                                │
                                ▼
                  HudiSourceCsvWriter (csv_writer.py)
                  ParserOutcome contract (shared.parsers.outcome)
                  Existing parsers (shared.parsers.*)
                  shared.audit.write_audit_sidecar
                  shared.parsers.dispatcher.dispatch_non_nem
```

## Module Inventory

| Module | Responsibilities | Public surface |
|---|---|---|
| `functions/file_processor/app.py` | Decode SQS event, file-stability check, redelivery, call `ingest_file`, return statusCode. **No business decisions.** | `lambda_handler(event, context)` |
| `functions/file_processor/pipeline.py` | `ingest_file` orchestrating download → parse → Hudi → disposition → observability, all inside `@idempotent_function`. | `ingest_file(source_file)` |
| `functions/file_processor/csv_writer.py` | `HudiSourceCsvWriter` (formerly `DirectCSVWriter`) and `StagedCsvUpload` dataclass. Staging/commit/abort lifecycle. | `HudiSourceCsvWriter`, `StagedCsvUpload` |
| `shared/parsers/outcome.py` | `ParserOutcome` contract; gains `derive_final` method (formerly `_compute_dataframe_final_status`). | (existing + `derive_final`) |
| `shared/nem_adapter.py` | NEM12 streaming/batch wrappers; gains `_is_nem_envelope_only` (formerly `_looks_like_nem_envelope`). | (existing + envelope sniff) |
| `shared/parsers/dispatcher.py` | Renamed from `shared/non_nem_parsers.py`; `dispatch_non_nem` (formerly `get_non_nem_outcome`). | `dispatch_non_nem` |

## Data Model

### `SourceFile` frozen dataclass

A new domain type that replaces the ad-hoc `dict[str, str]` carrier:

```python
# src/shared/source_file.py
from __future__ import annotations
from dataclasses import dataclass

@dataclass(frozen=True, slots=True)
class SourceFile:
    """An S3 object reference identifying one input file."""
    bucket: str
    key: str
```

Used as the `data_keyword_argument` for Powertools idempotency. Powertools natively supports plain dataclasses as idempotency-key payloads — its `_prepare_data` (in `aws_lambda_powertools/utilities/idempotency/base.py`) detects `__dataclass_fields__` and calls `dataclasses.asdict(data)`, which works on `frozen=True, slots=True` instances because `asdict` iterates `__dataclass_fields__` rather than `__dict__`. Verified empirically: `json.dumps(asdict(SourceFile("a","b")))` produces `{"bucket":"a","key":"b"}`. **No custom `output_serializer` or `DataclassSerializer` is needed.** A frozen dataclass also serializes deterministically (field order from `__dataclass_fields__` is stable across constructions, unlike dict insertion order at distant call sites).

### `ParserOutcome.derive_final` method

The DataFrame-path final-status calc ladder moves from a free function in `app.py` to a method on the contract type:

```python
# src/shared/parsers/outcome.py (extension)
@dataclass(frozen=True)
class ParserOutcome:
    # ... existing fields ...

    def derive_final(
        self,
        *,
        rows_written: int,
        candidate_row_count: int,
        unmapped_count: int,
        unsupported_suffixes: frozenset[str],
        rows_skipped: int,
    ) -> ParserOutcome:
        """Return a new outcome with final (status, reason) per spec ladder.

        Ladder (in spec order):
          1. rows_written > 0                                       → processed
          2. candidate_row_count > 0 and all unmapped               → unmapped
          3. candidate_row_count == 0 and unsupported_suffixes      → processed_empty(all_unknown_suffix)
          4. rows_skipped > 0 and zero rows / candidates             → processed_empty(all_skipped)
          5. else                                                    → processed_empty(self.reason)
        """
```

This co-locates the ladder with the contract enums, single source of truth.

## Function Contracts

### `lambda_handler`

```python
# src/functions/file_processor/app.py
def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """SQS-triggered entry point. batch_size = 1 (terraform contract).

    Pre-conditions:
      - event["Records"] has exactly 1 message.
      - SQS_QUEUE_URL is set in env (no fallback).

    Flow:
      1. Decode the single SQS record → bucket, key.
      2. Set correlation_id = SQS messageId.
      3. Check file stability; redeliver with _retry_count++ if unstable.
      4. Build SourceFile(bucket, key).
      5. Call ingest_file(source_file).
         - On ParserError | ProcessingError: outcome describes failure;
           function already moved source to newParseErr/ inside boundary.
         - On unexpected exception: log at ERROR level, return 500;
           Powertools deletes in-progress record so SQS retry re-executes.
      6. Return {"statusCode": 200, "outcome": outcome_summary}.
    """
```

### `ingest_file`

**Decorator order is load-bearing.** Powertools idempotency docs require `@idempotent_function` to be the **innermost** decorator (closest to `def`). Source: <https://docs.aws.amazon.com/powertools/python/latest/utilities/idempotency/#tracer> — verbatim "Ensure that idempotency is the innermost decorator." The current `parse_and_write_data` in `app.py:643-644` violates this (idempotent outer, tracer inner) — on cache hit, Powertools returns the cached value before the tracer wrapper opens its X-Ray subsegment, so cached invocations are **invisible** in X-Ray. The order below corrects this regression as part of the refactor.

```python
# src/functions/file_processor/pipeline.py
@tracer.capture_method                          # OUTER — wraps everything, including cache lookup
@idempotent_function(                           # INNER — closest to def, per Powertools docs
    data_keyword_argument="source_file",
    persistence_store=persistence_layer,
    config=idempotency_config,
)
def ingest_file(source_file: SourceFile) -> ParserOutcome:
    """Process one source file end-to-end inside the idempotent boundary.

    Args:
      source_file: SourceFile(bucket, key) — frozen dataclass, hashable.

    Returns:
      ParserOutcome describing the final disposition. Returned outcomes are
      cached by Powertools for 12 h; on duplicate file_ref the cached
      outcome is returned without re-execution.

    Raises (transient — Powertools deletes in-progress record on these,
    SQS retry re-executes):
      - ProcessingError (ONLY for transient infrastructure failures:
        DynamoDB throttle, S3 5xx, etc.)
      - Any unexpected RuntimeError / AttributeError / etc.

    Does NOT raise (deterministic — outcome describes failure, cached):
      - File-content ParserError (after source moved to newParseErr/ and
        per-file metrics emitted; cached outcome.status = "parse_failed"
        with reason="parser_error" so the file is not re-processed).
        See "Contract evolution: parse_failed status" below.

    Side effects (all INSIDE the idempotent boundary):
      - Downloads source object from s3://<bucket>/<key> to tmp directory.
      - Parses via NEM12 streaming parser → non-NEM dispatcher
        (batch-parser fallback removed; see Behavioral Simplifications).
      - Writes Hudi rows to s3://hudibucketsrc/sensorDataFiles/ via
        HudiSourceCsvWriter (commit/abort transaction).
      - Writes external-sink artifacts (RACV billing → gegoptimareports).
      - Moves source to newP/ | newIrrevFiles/ | newParseErr/ by status.
      - Emits per-file CloudWatch metrics.
      - Writes audit sidecar if outcome carries skips/unmapped/unsupported.
      - Emits structured "parser_outcome" log with all signal fields.

    Cleanup:
      - tempfile.TemporaryDirectory ensures /tmp cleanup on exception.
    """
```

### `HudiSourceCsvWriter` (renamed from `DirectCSVWriter`)

Public surface unchanged. Rename only. Dataclass `StagedCsvUpload` (formerly `CSVUploadJob`) moves with it.

## Idempotency Semantics

### All-in-one inside boundary

Powertools `@idempotent_function` semantics:

1. On first call: function executes, return value is stored in DynamoDB keyed on the hash of `data_keyword_argument`.
2. On duplicate call (same hash, within TTL): cached return value is returned, **function body does NOT execute**.
3. If function raises, Powertools deletes the in-progress record so SQS retry can re-execute.

Implication: any side effect outside the decorator runs on every duplicate detection. To avoid this, ALL side effects relevant to "this file was processed" must live inside `ingest_file`. The handler is a thin adapter.

Critical observation: **Powertools does NOT cache exceptions**. If the function raises, the in-progress record is deleted so retry can re-execute. This means deterministic content failures must be RETURNED (not raised) for the cached-outcome contract to hold. See "Contract evolution: `parse_failed` status" below.

### Contract evolution: `parse_failed` status

The current `ParserOutcome` contract has 4 statuses (`processed`, `processed_empty`, `unmapped`, `processed_external`) and uses raised `ParserError` / `ProcessingError` exceptions for failure paths. Under the all-in-one-inside-boundary refactor, content failures must be RETURNED to be cacheable. We add ONE new status:

```python
ParserStatus = Literal[
    "processed",
    "processed_empty",
    "unmapped",
    "processed_external",
    "parse_failed",        # NEW
]
```

And TWO new reasons:

```python
ParserReason = Literal[
    "no_data_sentinel",
    "zero_rows",
    "all_blank",
    "all_zero_valid",
    "all_unknown_suffix",
    "all_skipped",
    "external_gegoptimareports",
    "parser_error",         # NEW: matched parser, structural failure (caught from ParserError exception)
    "processing_error",     # NEW: parsed OK but write failed in a non-transient way (rare; reserved for future use)
]
```

Disposition rule for the new status: `parse_failed` → source moved to `newParseErr/` (done internally by `ingest_file` before return). The status is purely informational at the handler layer.

`ParserOutcome.derive_final` (the DataFrame-path calc ladder) is **unchanged** — it never produces `parse_failed`; that status only arises from caught `ParserError` in `ingest_file`'s exception handler.

The test-only invariant helper (`tests/_outcome_invariants.py`) gains a branch:

```python
if outcome.status == "parse_failed":
    assert outcome.rows_written == 0
    assert outcome.reason in {"parser_error", "processing_error"}
    assert outcome.reason is not None
```

### When to raise vs return

A precise contract that maps exception class to caching behavior:

| Failure mode | Inside `ingest_file` | Caching behavior | Recovery |
|---|---|---|---|
| Vendor file is structurally broken (`ParserError` raised by parser) | Catch `ParserError`, move source to `newParseErr/`, emit metrics, **return** `ParserOutcome(status="parse_failed", reason="parser_error", ...)` | Cached. Re-delivery returns same outcome; no re-processing. | Manual: ops moves source from `newParseErr/` back to `newTBP/` after fix. |
| Hudi PUT 5xx / DynamoDB throttle / S3 transient error | **Raise** `ProcessingError` (or `ClientError` propagates) | Powertools deletes in-progress record. | SQS retries; idempotent cache miss; re-executes. |
| `RuntimeError` / `AttributeError` / unexpected | **Raise** as `ParserError` (per existing dispatch narrowing rule) | Powertools deletes in-progress record. | SQS retries until DLQ; visible to ops. |
| File not found in source bucket (404 on download) | **Raise** `ProcessingError` | Powertools deletes in-progress record. | SQS retries; if file is genuinely gone, DLQ catches it. |
| Source-move to destination prefix fails after Hudi commit | Roll back via `HudiSourceCsvWriter.abort()`, then **raise** `ProcessingError` | Powertools deletes in-progress record. | SQS retries; idempotent re-execution; transient error usually clears. |

The key invariant: **a deterministic content failure is cached; a transient infrastructure failure is raised**. Operators recover content failures by manual remediation; transient failures self-heal via SQS retry.

### Cache-hit observability

Two-layer approach — **AWS-native metric for the alarm, structured log for per-invocation debugging**. No custom CloudWatch metric (avoids dual-emission ambiguity with the underlying DynamoDB metric).

**Layer 1 — Alarm via DynamoDB native metric.**

CloudWatch automatically emits `AWS/DynamoDB::ConditionalCheckFailedRequests` for every conditional-write conflict on the idempotency table. Since `sbm-ingester-idempotency` is dedicated to this Lambda, that metric *is* the cache-hit count. The alarm definition lives in the Alarms section below, not in code.

**Layer 2 — Per-invocation structured log via persistence-layer subclass.**

Powertools' `_process_idempotency` (`aws_lambda_powertools/utilities/idempotency/base.py`) optimistically calls `save_inprogress`; on duplicates it raises `IdempotencyItemAlreadyExistsError`. Subclass the persistence layer to emit a structured log on that path — preserving Powertools' subsequent cached-response handling intact:

```python
# src/functions/file_processor/persistence.py (new)
from aws_lambda_powertools.utilities.idempotency import DynamoDBPersistenceLayer
from aws_lambda_powertools.utilities.idempotency.exceptions import (
    IdempotencyItemAlreadyExistsError,
)

class InstrumentedDynamoDBPersistenceLayer(DynamoDBPersistenceLayer):
    """DynamoDB persistence layer that logs cache hits.

    Powertools does not expose a native cache-hit hook. The cache-hit code
    path is the IdempotencyItemAlreadyExistsError raised by save_inprogress
    when a record already exists. We log here; the alarm uses DynamoDB's
    own ConditionalCheckFailedRequests metric (no custom metric needed).
    """

    def save_inprogress(self, data, remaining_time_in_millis=None):
        try:
            return super().save_inprogress(data, remaining_time_in_millis)
        except IdempotencyItemAlreadyExistsError:
            logger.info(
                "idempotent_cache_hit",
                extra={
                    "source_bucket": data.get("bucket"),
                    "source_key": data.get("key"),
                },
            )
            raise  # let Powertools handle the cached response
```

Used at module init in place of the bare `DynamoDBPersistenceLayer`. Logs are correlation-ID-tagged via Powertools logger context, so `idempotent_cache_hit` lines join the request trace alongside the rest of the invocation.

**What this is NOT**: no custom `IdempotentSkip` Lambda metric is emitted. The DynamoDB native metric is the alarm source of truth; the log is per-invocation diagnostic only.

## Pre-flight Infrastructure Changes

Production-correctness fixes and infrastructure hardening that must land in this PR.

### `SQS_QUEUE_URL` becomes required

Current (`app.py:48-52`):

```python
SQS_QUEUE_URL = os.environ.get(
    "SQS_QUEUE_URL",
    "https://sqs.ap-southeast-2.amazonaws.com/318396632821/sbm-files-ingester-queue",
)
```

Change to:

```python
SQS_QUEUE_URL = os.environ["SQS_QUEUE_URL"]   # KeyError on import = deploy fails fast
```

Rationale: a missing env var in a non-prod account would currently silently target the production SQS queue (different account ID, but boto3 would still attempt the call). Failing at import time is the correct behavior — Lambda's runtime fails to start, deploy fails immediately, no cross-account writes.

Terraform: the `aws_lambda_function.sbm_files_ingester` resource in `terraform/ingester.tf` (lines 16-30) currently has **no `environment {}` block at all** — verified at spec authoring time. The code change to `os.environ["SQS_QUEUE_URL"]` MUST land in the same PR as adding this block, otherwise Lambda fails at cold start with `KeyError`. Required addition:

```hcl
resource "aws_lambda_function" "sbm_files_ingester" {
  # ... existing fields ...
  tracing_config {
    mode = "Active"
  }
  environment {
    variables = {
      SQS_QUEUE_URL = aws_sqs_queue.sbm_files_ingester_queue.url
    }
  }
}
```

Test setup: a module-level `os.environ["SQS_QUEUE_URL"] = "https://sqs.test.local/queue"` (or equivalent autouse fixture) must be added to `tests/conftest.py` so test collection does not crash on import — every test that imports `functions.file_processor.app` would otherwise fail at collection time, including `tests/unit/test_file_stability.py:375-378` which already imports the module.

### SQS visibility timeout bump

Current: `visibility_timeout_seconds = 900` (matches Lambda timeout 900).

Change to: `visibility_timeout_seconds = 1080` (Lambda timeout + 180 s buffer).

Rationale: when a Lambda runs the full 900 s and SQS releases the message at exactly 900 s, a duplicate delivery races with the in-flight invocation completing. AWS guidance is `visibility >= function_timeout + buffer` for SQS-triggered Lambdas. 180 s buffer covers Lambda's hand-back-to-SQS latency.

Terraform: edit `aws_sqs_queue.sbm_files_ingester_queue.visibility_timeout_seconds`.

### S3 server-side encryption verification

Verify that the three S3 buckets have SSE configured (either via account-level default encryption OR via per-bucket terraform). Run:

```bash
aws s3api get-bucket-encryption --bucket sbm-file-ingester
aws s3api get-bucket-encryption --bucket hudibucketsrc
aws s3api get-bucket-encryption --bucket gegoptimareports
```

If any bucket returns `ServerSideEncryptionConfigurationNotFoundError`, add `aws_s3_bucket_server_side_encryption_configuration` to terraform with SSE-S3 (AES256). Cost is zero. The audit sidecar (`audit/<batch_ts>/<source>.skipped.json`) writes vendor row content that may include PII (NMIs, site addresses, customer numbers from RACV billing); SSE at rest is non-negotiable for that prefix.

If buckets are managed in a different repo, the verification step still happens here; remediation may land in the other repo's PR.

### DynamoDB idempotency table — `deletion_protection_enabled`

Set `deletion_protection_enabled = true` on the idempotency table in terraform (`aws_dynamodb_table.sbm_ingester_idempotency`). Cost: zero (it's a flag). Prevents accidental table deletion (e.g., misdirected `terraform destroy`) which would silently disable idempotency for the entire 12 h cache-rebuild window, leading to duplicate processing.

To delete the table intentionally requires two steps after this change: (1) disable protection, (2) delete. Compatible with normal operations.

### SQS retry budget alignment

Current state:
- SQS `maxReceiveCount = 3` on the source queue (after 3 receives, message goes to DLQ).
- Custom `_retry_count` budget = 5 in `requeue_message` for file-stability retries.

Issue: a file that fails stability check 3 times would already be DLQ'd by SQS before the custom retry budget of 5 is exhausted. The two budgets disagree and the DLQ effectively wins.

Fix: align the budgets. Two options, choose during implementation:

- **Option A (preferred)**: drop custom `MAX_REQUEUE_RETRIES` from 5 to 3. Custom retry exists only as a wrapper around the SQS retry; aligning the limits makes the contract clear: "3 attempts total, then DLQ".
- **Option B**: bump SQS `maxReceiveCount` to 7 or 8 (custom retry of 5 plus headroom). Acceptable if file-stability retries are common and the additional latency before DLQ is tolerable.

Option A is recommended unless ops history shows file-stability retries succeed past attempt 3.

### CloudWatch alarms

Create alarms in `terraform/monitoring.tf` for the file_processor Lambda:

| Alarm | Metric | Threshold | Period |
|---|---|---|---|
| `FileProcessor-DLQDepth` | SQS `ApproximateNumberOfMessagesVisible` on DLQ | `> 0` | 5 min |
| `FileProcessor-MaxRetriesExceeded` | Custom metric `MaxRetriesExceeded` | `> 0` per day | 1 day |
| `FileProcessor-ParseErrorSpike` | Custom metric `ParseErrorFiles` | `> 2× rolling 7-day avg` | 1 hour |
| `FileProcessor-ErrorRate` | `Errors / Invocations` | `> 1 %` | 5 min |
| `FileProcessor-IdempotentSkipSpike` | `AWS/DynamoDB::ConditionalCheckFailedRequests` (TableName=`sbm-ingester-idempotency`) | `> 5 % of Lambda invocations` over 1 h | 1 hour |

Each alarm publishes to an SNS topic (existing or new) for ops notification. Threshold values are starting points; tune after 1-2 weeks of baseline data.

## Naming Changes

Items the user explicitly prioritized for semantic clarity. **All applied in this PR.**

### Module / file renames

| From | To | Rationale |
|---|---|---|
| (delete) `parse_and_write_data` | (delete) | Function does far more than parse + write. Replaced by `ingest_file`. |
| `parse_and_write_data(tbp_files)` | `ingest_file(source_file)` | Domain verb; service is the "ingester". |
| `tbp_files: list[dict]` | `source_file: SourceFile` | `tbp` is opaque (S3-prefix artifact); `SourceFile` names the role. |
| (delete) `_flush_buffer_to_s3` | (delete) | Dead code (only called from tests). |
| `DirectCSVWriter` | `HudiSourceCsvWriter` | "Direct" was a "bypasses pandas" historical note. The class does **not** write Hudi tables — it writes CSV objects to `s3://hudibucketsrc/sensorDataFiles/` that the downstream `DataImportIntoLake` Glue job consumes into the Hudi table. The new name captures "source CSV for Hudi ingestion" rather than misleadingly suggesting direct Hudi writes. |
| `CSVUploadJob` | `StagedCsvUpload` | "Job" implies queued/scheduled; this is a staged upload record. |
| `_candidate_values` | `extract_valid_readings` | Function action, not return-shape. |
| `_compute_dataframe_final_status` | (moved to `ParserOutcome.derive_final`) | Belongs with the contract; co-locates ladder with statuses. |
| `_looks_like_nem_envelope` | `_is_nem_envelope_only` | Removes hedge language; matches deterministic check. |
| `get_non_nem_outcome` | `dispatch_non_nem` | "Get" implies lookup; this dispatches and may raise. |
| `non_nem_parsers.py` (module) | `parsers/dispatcher.py` | Co-locates with `shared.parsers` package. |
| `read_nem12_mappings` | (delete; use `shared.parsers._mappings.get_nem12_mappings`) | Eliminate duplicate JSON loader. |
| `move_s3_file` | (kept name, but body change) | Remove hard-coded `newTBP/` prefix rewrite at `app.py:460`; caller passes the full source key. |

### Variable / constant / parameter renames

| From | To | Rationale |
|---|---|---|
| `BUCKET_NAME` (`shared/common.py`) | `INPUT_BUCKET` | System has 3 buckets; this is the input one only. |
| `IRREVFILES_DIR` | `UNMAPPED_DIR` | "Irrev" reads as "irrevocable"; the routed status is `unmapped`. |
| `BATCH_SIZE` | `CSV_FLUSH_ROW_THRESHOLD` | Collides with SQS `batch_size` semantics; rename clarifies role. |
| `outcome.dfs` | `outcome.dataframes` | Saves nothing; obscures meaning. |
| `error_file_path` parameter on every parser | (delete) | Vestigial; never read in any parser body. |

### Constants moved to `shared/common.py`

The string literals `"hudibucketsrc"`, `"sensorDataFiles"`, `"sensorDataFilesStaging"` appear in 4+ places. Hoist to `shared/common.py`:

```python
HUDI_BUCKET = "hudibucketsrc"
HUDI_FINAL_PREFIX = "sensorDataFiles"
HUDI_STAGING_PREFIX = "sensorDataFilesStaging"
```

A future bucket rename becomes a one-line edit.

## Observability Completion

### Per-file structured `parser_outcome` log

Emitted once per file (inside `ingest_file`, after disposition):

```python
logger.info(
    "parser_outcome",
    extra={
        "bucket": source_file.bucket,
        "key": source_file.key,
        "correlation_id": correlation_id,    # SQS messageId
        "final_status": outcome.status,
        "final_reason": outcome.reason,
        "source_row_count": outcome.source_row_count,
        "candidate_row_count": outcome.candidate_row_count,
        "rows_written": outcome.rows_written,
        "rows_skipped": outcome.rows_skipped,
        "unmapped_count": outcome.unmapped_count,
        "skip_reasons": dict(outcome.skip_reasons),
        "unsupported_suffixes": sorted(outcome.unsupported_suffixes),
        "unmapped_identifiers_truncated": list(outcome.unmapped_identifiers[:50]),
        "destination_prefix": dest_prefix,
        "duration_ms": elapsed_ms,
        "_retry_count": retry_count,
    },
)
```

This is the operator's primary debug surface. Schema is stable; downstream log queries can rely on field names.

### New CloudWatch metrics

| Metric | Type | When emitted | Purpose |
|---|---|---|---|
| `FileProcessingDurationMs` | Milliseconds | Once per file at end of `ingest_file` | Latency / memory-tuning input. |

Cache-hit visibility uses DynamoDB's native `ConditionalCheckFailedRequests` metric (no custom Lambda metric) — see Cache-hit observability above.

Existing metrics (`ValidProcessedFiles`, `ParseErrorFiles`, `IrrelevantFiles`, `PartialMappedRatio`, `RowsSkippedRatio`, `MalformedValueCount`, `UnsupportedSuffixesFound`, `UnmappedIdentifierKind_<kind>`) emitted as today, but inside `ingest_file` (per-file rather than per-batch).

### Correlation ID propagation

SQS `messageId` is set as the Powertools logger `correlation_id` via `inject_lambda_context(correlation_id_path="Records[0].messageId")` or equivalent. All log lines emitted during this invocation carry the same correlation ID for downstream log search.

### X-Ray instrumentation

X-Ray tracing is already enabled (`tracing_config { mode = "Active" }` in terraform; Powertools `Tracer` instantiated in code). The refactor preserves and improves coverage:

- **Preserve**: `@tracer.capture_method` on `ingest_file` (replaces the existing decorator on `parse_and_write_data` so traces still cover the per-file flow).
- **Add**: `@tracer.capture_method` on `HudiSourceCsvWriter.commit` and `HudiSourceCsvWriter.abort` — these are S3 multi-object copy + delete operations that are common bottleneck candidates; without instrumentation, slow Hudi commits are invisible in the trace.
- **Remove**: the dead `@tracer.capture_method` on `_flush_buffer_to_s3` (deleted with the function).
- **Propagate trace context through `ThreadPoolExecutor`**: Powertools auto-instrumentation does not propagate to worker threads. Use `aws_xray_sdk.core.recorder.in_segment` (or pass segment id explicitly) so the 4 parallel S3 PUT subsegments are children of the parent `ingest_file` segment instead of orphans.

Cost: X-Ray is $5 per million traces + $0.50 per million retrieved. At ~700 traces/day this is < $0.05/month — negligible. Value: when investigating a slow file or planning future memory tuning, the trace timeline immediately answers "where did the time go?"

## Test Strategy

### Test layout

Tests reorganized to mirror the three-module split AND to break up `tests/unit/test_edge_cases.py` (currently 2000+ lines, a "god file" that bisecting failures takes longer than running):

```
tests/
├── conftest.py                              # shared moto fixtures (mock_s3_buckets, mock_dynamodb_idempotency, mock_cloudwatch_logs)
├── helpers/
│   └── outcome_invariants.py                # moved from tests/_outcome_invariants.py
├── unit/
│   ├── test_lambda_handler.py               # SQS adapter behavior; mocks ingest_file
│   ├── test_ingest_file.py                  # End-to-end per-file flow with moto
│   ├── test_csv_writer.py                   # HudiSourceCsvWriter staging/commit/abort
│   ├── test_idempotency_boundary.py         # Cache hit/miss, raise-vs-return contract
│   ├── test_nem_envelope_short_circuit.py   # 100/900-only handling
│   ├── test_dataframe_partial_skip.py       # row-skip + skip_reasons aggregation
│   ├── test_unmapped_disposition.py         # newIrrevFiles/ routing
│   ├── test_audit_sidecar_contract.py       # JSON schema + sample cap
│   ├── parsers/                             # (existing) per-parser tests, kept
│   └── ...
```

The following files are **deleted** in this PR:
- `tests/unit/test_batch_s3_writes.py` (440 lines, all testing `_flush_buffer_to_s3` dead code).
- `tests/unit/test_edge_cases.py` (2000+ lines): split into the focused files listed above. Each test moves to the file matching its behavior.

### Shared moto fixtures

`tests/conftest.py` defines fixtures so every test does NOT repeat boto3 monkeypatching boilerplate:

```python
@pytest.fixture
def mock_s3_buckets():
    """Yields a moto-mocked S3 with all three buckets created and SSE configured."""

@pytest.fixture
def mock_dynamodb_idempotency():
    """Yields a moto-mocked DynamoDB with the idempotency table created."""

@pytest.fixture
def file_in_newtbp(mock_s3_buckets):
    """Factory: places a CSV body at newTBP/<key> and returns SourceFile."""
```

Tests inject these instead of running `@mock_aws` + `s3 = boto3.resource(...)` + `s3.create_bucket(...)` per test.

### New tests

- **Idempotency-collision integration test** in `test_ingest_file.py`: call `ingest_file(source_file)` twice with the same `SourceFile`; assert second call returns identical `ParserOutcome` AND the source file is not moved twice (manually placed back in newTBP/ after first call → not moved on second; cache hit).
- **NEM12 RuntimeError propagation test** in `test_ingest_file.py`: mock `stream_as_data_frames` to raise `RuntimeError`; assert (a) `dispatch_non_nem` is NOT called (per `_NEM_FALLTHROUGH_ERRORS` narrowing), (b) the exception propagates out of `ingest_file`, (c) Powertools deletes the in-progress record (verify via DynamoDB scan).
- **Cache-hit log test**: with moto-mocked DynamoDB, call `ingest_file` twice with the same `SourceFile`; assert the second call emits exactly one `idempotent_cache_hit` structured log line carrying `source_bucket` and `source_key`. (No custom metric to assert — the alarm uses DynamoDB's native `ConditionalCheckFailedRequests`, which is exercised implicitly by the second call's conditional-write conflict.)
- **`SQS_QUEUE_URL` missing test**: import the module without the env var set; assert `KeyError`.
- **Visibility-timeout race regression test** (terraform plan output assertion only — no runtime test).

### Test patterns

- Tests assert behavior, not implementation. Names follow `test_<input>_yields_<status>_<reason>`.
- Tests use `moto` for S3 + DynamoDB; no per-test boto3 monkeypatching.
- Tests assert `ParserOutcome` field-by-field, the disposition prefix, AND the per-file structured log fields.

### Test counts

Today: 791 tests. After refactor: estimated 770-790 (loss from deleting `test_batch_s3_writes.py` partially offset by new `test_ingest_file.py` and `test_csv_writer.py` cases).

## Behavioral Changes from Current Code

The refactor is functionality-preserving in disposition + Hudi rows. Behavioral deltas:

| Change | Before | After |
|---|---|---|
| Function name | `parse_and_write_data(tbp_files)` | `ingest_file(source_file)` |
| Idempotency boundary | Whole `parse_and_write_data` (batch shape, but always 1 file) | `ingest_file` (explicitly per-file via `SourceFile` hash) |
| Side-effect locality | Mixed: function does S3, metrics, audit | All inside `ingest_file` (none in handler) |
| Handler line count | ~70 lines | ~30 lines |
| `app.py` total lines | 1165 | ~80 |
| Per-file structured log | Partial | Complete (`parser_outcome` with all signal fields) |
| Latency metric | Absent | `FileProcessingDurationMs` |
| Cache-hit visibility | Absent | `idempotent_cache_hit` structured log + DynamoDB `ConditionalCheckFailedRequests` alarm |
| `SQS_QUEUE_URL` fallback | Hard-coded prod URL | Required env var |
| SQS visibility timeout | 900 s | 1080 s |
| `_flush_buffer_to_s3` | Present (dead) | Deleted |
| `error_file_path` param | Present (unused) | Deleted from all parser signatures |
| `read_nem12_mappings` duplicate | Two loaders | One (`shared.parsers._mappings.get_nem12_mappings`) |
| `_compute_dataframe_final_status` location | `app.py` free function | `ParserOutcome.derive_final` method |
| `_looks_like_nem_envelope` location | `app.py` | `shared/nem_adapter.py` as `_is_nem_envelope_only` |
| NEM12 batch-parser fallback | `stream → batch → non-NEM` chain (3 hops) | `stream → non-NEM` chain (2 hops) — see Behavioral Simplifications |
| Decorator order (`@idempotent_function` + `@tracer.capture_method`) | `@idempotent_function` outer / `@tracer.capture_method` inner — violates Powertools docs, cache hits invisible in X-Ray | `@tracer.capture_method` outer / `@idempotent_function` inner — per Powertools docs, cache hits captured in X-Ray subsegment |
| Cache-hit observability mechanism | None | `idempotent_cache_hit` structured log via `InstrumentedDynamoDBPersistenceLayer.save_inprogress` override; alarm via DynamoDB native `ConditionalCheckFailedRequests` |
| `SQS_QUEUE_URL` Lambda env var | Hardcoded fallback in code; **no `environment {}` block in terraform** | Required env var (`os.environ[...]`); terraform adds `environment { variables = { SQS_QUEUE_URL = ... } }` |

The DynamoDB idempotency hash format changes (from `tbp_files` list-shape to `SourceFile` dataclass-shape). Old records and new records do not collide. For 12 h after deploy, a duplicate file processed under the old code may not be detected under the new code, leading to one of:

- Re-execution attempts to download `newTBP/<key>`, gets 404 (file moved on first run), function raises `ProcessingError`, source file already moved — net effect: file ends up in `newParseErr/` even though it was originally processed correctly.
- Or: re-execution proceeds and writes Hudi rows again, but Hudi upsert by `(sensorid, ts)` deduplicates, so the data layer is unaffected.

Estimated impact: 1 % of files in the 12 h window (SQS at-least-once redelivery rate) ≈ a few files out of ~350 over the transition.

### Behavioral Simplifications

#### Remove NEM12 batch-parser fallback in `app.py`

**Current (3-hop chain)**: streaming parser → on `_NEM_FALLTHROUGH_ERRORS` → batch parser (`output_as_data_frames`) → on same errors → non-NEM dispatcher.

**New (2-hop chain)**: streaming parser → on `_NEM_FALLTHROUGH_ERRORS` → non-NEM dispatcher.

**Rationale**:

- The batch parser fallback has been present since the initial commit (`bedbced`) but **no commit, test, or production fix** has ever exercised the "stream raised, batch returned data" recovery path.
- Standing equivalence tests (`tests/unit/test_nem12_streaming.py::TestStreamingVsBatchEquivalence`, `tests/unit/test_nem12_real_file_equivalence.py::TestFinalOutputEquivalence`) assert byte-for-byte equivalence between the two parsers on real NEM12 files. They are the safety net that makes single-parser operation safe.
- Keeping a defensive fallback that may never have fired has costs: it doubles read I/O on streaming failures, masks genuine bugs in `src/libs/nemreader/streaming.py` (since failures silently route to the batch path), and adds reasoning surface to error paths.
- All 13 mocks of `output_as_data_frames` in `tests/unit/test_edge_cases.py` (audited at spec authoring time) are paired with a `stream_as_data_frames` mock carrying the same side-effect or a semantically equivalent one. 12 of 13 pairs use identical side-effects (e.g. both `ValueError("not nem")`); the remaining pair (lines 1561-1562) uses `iter([])` for streaming and `[]` for batch — both meaning "no data parsed", so still the equivalent disposition. **Zero pairs assert "stream raised but batch returned data"**, so the recovery path is never exercised by tests. Removing those `output_as_data_frames` mock lines is mechanical cleanup.

**Scope**:

- Delete the `output_as_data_frames` import and the inner `try/except` block in `ingest_file` that calls it.
- Keep `output_as_data_frames` defined in `shared/nem_adapter.py` — it remains in use by the Optima NEM12 exporter (`tests/unit/optima_exporter/nem12_exporter/test_downloader.py`) and the equivalence tests.
- Remove the redundant `output_as_data_frames` mocks from the file-processor test suite during the test reorganization.

**Risk mitigation**: equivalence tests stay green as a precondition for deploy. If any real-world file fails streaming after deploy, it now lands in `newParseErr/` instead of being silently recovered — which is the **desired** signal: a streaming bug should be visible, not silently masked.

## Migration Plan

1. **Pre-flight**:
   - Verify `SQS_QUEUE_URL` is set in Lambda env via terraform.
   - Verify SQS visibility timeout is bumped to 1080 s in terraform plan.
2. **Code merge**: feature branch lands on `main` after review.
3. **Deploy**: standard CI/CD path. No DynamoDB migration; old idempotency records expire on their own (12 h TTL).
4. **Post-deploy monitoring (1 week)**:
   - `ConditionalCheckFailedRequests` on `sbm-ingester-idempotency` table: confirm SQS redelivery cache-hit rate is in expected range (< 1 % of Lambda invocations). Cross-reference `idempotent_cache_hit` log lines for per-file detail.
   - `FileProcessingDurationMs`: establish p50 / p99 baseline.
   - `ParseErrorFiles`: should NOT spike; if it does, investigate (likely transition-window 404s).
   - `newParseErr/` directory: ops periodically reviews, restores misclassified files to `newTBP/`.
5. **Tier 2 PRs land afterward**: SSE verification, deletion_protection, alarms, test split.

## Open Decisions

- Whether to add a `processed_with_partial_rollback` reason for the rare case where Hudi commit succeeded but source-move failed and was rolled back — defer until first occurrence in production.
- Whether the audit sidecar JSON should be versioned (`schema_version: 1`). Recommended yes; trivial to add and prevents future schema-drift breaking downstream consumers — may decide during implementation.
