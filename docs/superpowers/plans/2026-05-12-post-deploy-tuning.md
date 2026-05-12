# Post-Deploy Tuning Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix three production issues surfaced by post-deploy CloudWatch review of the per-file ingest refactor: (1) `MaxRetriesExceeded` alarm noise from `HeadObject` 404s on duplicate S3 events, (2) WA "No data found" sentinels misclassified as parse errors, (3) `idempotent_cache_hit` log missing structured fields.

**Architecture:** Three forward-only commits on `fix/post-deploy-tuning` on top of `f8282f4`. Each Task = one logical commit; **the commit is the LAST step of its Task and Tasks 1–3 each produce exactly one commit**. Tasks 4–5 are operational (no commits). Stability check and requeue logic remain in place — a known streaming-uploader producer requires them.

**Tech Stack:** Python 3.13, `aws-lambda-powertools >= 3.24.0`, `pytest`, `moto`, `boto3`, Terraform, pandas. Working directory: `/Users/zeyu/Desktop/GEG/sbm/sbm-ingester`.

**Spec:** `docs/superpowers/specs/2026-05-12-post-deploy-tuning-design.md`

**Branch:** `fix/post-deploy-tuning` (already created, off `main`, includes `f8282f4`)

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `src/functions/file_processor/app.py` | Modify | Add `StabilityResult` dataclass, refactor `check_file_stability` return type, handle `vanished` in `lambda_handler`, emit `S3DuplicateEvent` metric, revert `REQUEUE_DELAY_SECONDS` 90→60 |
| `tests/unit/test_file_stability.py` | Modify | Update existing tests for `StabilityResult`, add tests for `vanished` (HEAD 404) path |
| `tests/unit/test_lambda_handler.py` | Modify | Add test asserting `lambda_handler` skips requeue and emits `S3DuplicateEvent` metric when `vanished=True` |
| `terraform/monitoring.tf` | Modify | Add `FileProcessor-DuplicateEventSpike` alarm |
| `src/shared/parsers/synergy/__init__.py` | Create | Empty package marker |
| `src/shared/parsers/synergy/wa_meter_data.py` | Create | `synergy_wa_meter_data_parser` — strict header match, returns `processed_empty` |
| `tests/unit/fixtures/synergy/wa_no_data_found.csv` | Move | Sentinel fixture (moved from `tests/unit/fixtures/optima_interval/`) |
| `tests/unit/parsers/synergy/__init__.py` | Create | Empty test package marker |
| `tests/unit/parsers/synergy/test_wa_meter_data.py` | Create | Tests for Synergy WA parser |
| `tests/unit/parsers/optima/test_interval.py` | Modify | Remove `test_wa_no_data_found_fixture_returns_processed_empty` |
| `src/shared/parsers/optima/interval.py` | Modify | Remove WA branch from `_is_no_data_sentinel`, restore strict header gate |
| `src/shared/parsers/dispatcher.py` | Modify | Register `synergy_wa_meter_data_parser` at position 0 of `PARSERS` |
| `src/functions/file_processor/persistence.py` | Modify | Fix service alignment + DataRecord access (or delete entirely if Task 0 finds a native mechanism) |
| `tests/unit/test_persistence_cache_hit_log.py` | Rewrite | Use `capsys` + JSON parsing; pass a `DataRecord` instead of `dict` |
| `docs/superpowers/plans/2026-05-12-task-0-decision.md` | Create | One-line decision record written by Task 0 |

---

## Task 0: Investigation — Powertools cache-hit observability + DataRecord API

**Goal:** Determine two things and write a decision record:
1. Does Powertools provide a native cache-hit hook that gives us `source_bucket` + `source_key` + `idempotency_key`? (If yes → Task 3 path 6A.)
2. **What does `DataRecord.get_payload()` actually return at `save_inprogress` time?** This is critical: our planned Task 3 fix uses `data.get_payload()` to extract `bucket` / `key`. If the method does NOT return the input payload (e.g. returns the response, which doesn't exist yet at save_inprogress) → Task 3 falls back to logging only `idempotency_key` + an operational SOP to reverse-lookup the file via DynamoDB.

**Time budget:** 45 minutes (30 for investigation, 15 for writing the decision record).

**Files:**
- Read: `aws-lambda-powertools` source via local venv
- Create: `docs/superpowers/plans/2026-05-12-task-0-decision.md`

- [ ] **Step 1: Inspect Powertools version**

```bash
uv run python -c "
import aws_lambda_powertools as p
import os
print('Powertools:', p.__version__)
print('Path:', os.path.dirname(p.__file__))
"
```

Expected output: version `3.24.0` or later.

- [ ] **Step 2: Verify `DataRecord.get_payload()` semantics**

This determines whether Task 3's fix is reachable as designed.

```bash
uv run python -c "
from aws_lambda_powertools.utilities.idempotency.persistence.base import DataRecord
import inspect

print('=== DataRecord.__init__ signature ===')
print(inspect.signature(DataRecord.__init__))

print()
print('=== Public attributes/methods ===')
print([m for m in dir(DataRecord) if not m.startswith('_')])

print()
print('=== get_payload source (if any) ===')
if hasattr(DataRecord, 'get_payload'):
    print(inspect.getsource(DataRecord.get_payload))
else:
    print('NO get_payload METHOD — fallback path required')
"
```

Look for:
- Does `DataRecord` have a `get_payload()` method? What does it return?
- Does it read from `self.response_data` (the result — null at save_inprogress) or from a separate field (the input payload)?
- Is there a separate field like `self.payload`, `self.event`, or `self.original_payload` that holds the **input** dict?

**Note:** in some Powertools versions, `DataRecord.payload_hash` is the HASH of the payload, not the payload itself. If only the hash is reachable from a `DataRecord` constructed for `save_inprogress`, we cannot derive `bucket`/`key` directly. That is the trigger for the fallback path in Task 3.

- [ ] **Step 3: Check `@idempotent_function` for a `log_event` or hook parameter**

```bash
uv run python -c "
from aws_lambda_powertools.utilities.idempotency import idempotent_function
import inspect
print('=== idempotent_function signature ===')
print(inspect.signature(idempotent_function))
print()
print('=== source (first 4000 chars) ===')
print(inspect.getsource(idempotent_function)[:4000])
"
```

Look for: any parameter named `log_event`, `on_cache_hit`, `cache_hit_handler`, `event_logger`, or similar.

- [ ] **Step 4: Check `IdempotencyConfig` for flags**

```bash
uv run python -c "
from aws_lambda_powertools.utilities.idempotency.config import IdempotencyConfig
import inspect
print('=== IdempotencyConfig __init__ ===')
print(inspect.signature(IdempotencyConfig.__init__))
print()
print(IdempotencyConfig.__init__.__doc__ or '(no docstring)')
"
```

Look for: any `log_*` or `event_*` flag.

- [ ] **Step 5: Write the decision record**

Based on Steps 2–4, write `docs/superpowers/plans/2026-05-12-task-0-decision.md` with exactly one of these forms:

**Form A — `get_payload()` works AND a native cache-hit hook exists (Task 3 → 6A):**
```markdown
# Task 0 Decision

**Powertools version:** 3.X.Y

**DataRecord.get_payload() at save_inprogress time:** returns the input payload dict (verified).

**Native cache-hit hook:** `<name of mechanism>` — emits `<list of fields>`.

**Task 3 path:** 6A — replace `InstrumentedDynamoDBPersistenceLayer` with the native mechanism in `pipeline.py`. Delete `persistence.py` and `test_persistence_cache_hit_log.py`. Verify the native emission carries `source_bucket`, `source_key`, `idempotency_key`.
```

**Form B — `get_payload()` works but no native hook (Task 3 → 6B):**
```markdown
# Task 0 Decision

**Powertools version:** 3.X.Y

**DataRecord.get_payload() at save_inprogress time:** returns the input payload dict (verified).

**Native cache-hit hook:** none suitable.

**Task 3 path:** 6B — fix the subclass in place. Use `data.get_payload()` to extract `bucket` / `key` and `data.idempotency_key` for the hash.
```

**Form C — `get_payload()` does NOT return input payload at save_inprogress (Task 3 → 6C fallback):**
```markdown
# Task 0 Decision

**Powertools version:** 3.X.Y

**DataRecord.get_payload() at save_inprogress time:** does NOT return input payload. It returns `<actual return value>`.

**Reachable from DataRecord at save_inprogress:** `idempotency_key`, `payload_hash`, status. NOT the original `bucket` / `key`.

**Native cache-hit hook:** `<status — none, or one found in Step 3-4>`.

**Task 3 path:** 6C — fallback. Log `idempotency_key` only. Add a runbook entry: "To resolve bucket/key from a cache-hit log, query DynamoDB table `sbm-ingester-idempotency` with `id = <idempotency_key>` and read the `data` (or `response_data`) attribute; if also opaque, search SQS DLQ or recent S3 events around the log timestamp."
```

If after 45 minutes none of the three forms is conclusive, **default to Form B** (try the planned fix; the TDD red-state in Task 3 will catch a wrong assumption).

No commit. Task 0 produces only the decision record (which IS committed in Task 3).

---

## Task 1: Commit 1 — HEAD 404 fix + S3DuplicateEvent metric + alarm

This Task produces **exactly one commit**. All steps are internal; commit happens only at Step 13.

**Files:**
- Modify: `src/functions/file_processor/app.py` (lines 1-108 and 136-192)
- Modify: `tests/unit/test_file_stability.py`
- Modify: `tests/unit/test_lambda_handler.py`
- Modify: `terraform/monitoring.tf`

### Part A: Update tests for new return type (TDD red)

- [ ] **Step 1: Audit all existing call sites of `check_file_stability` that will break**

```bash
grep -rn "check_file_stability" src/ tests/
```

Expected hit list:
- `src/functions/file_processor/app.py` (definition + one call inside `lambda_handler`)
- `tests/unit/test_file_stability.py` (multiple tuple-unpack call sites)
- Possibly other test files

Note each tuple-unpack site (`is_stable, size = ...` or `is_stable, _ = ...`). All must be rewritten to use `StabilityResult` attributes when the new return type lands in Part B.

- [ ] **Step 2: Append failing tests for HEAD 404 to `test_file_stability.py`**

Append these three tests inside the existing `TestCheckFileStability` class:

```python
    def test_head_returns_404_marks_vanished(self, mock_s3_client: Any, mock_logger: Any) -> None:
        """HEAD returns 404 (not 'NoSuchKey') when a prior delivery already moved the file."""
        from botocore.exceptions import ClientError

        from functions.file_processor.app import check_file_stability

        mock_s3_client.head_object.side_effect = ClientError(
            error_response={
                "Error": {"Code": "404", "Message": "Not Found"},
                "ResponseMetadata": {"HTTPStatusCode": 404},
            },
            operation_name="HeadObject",
        )

        result = check_file_stability("test-bucket", "test-key")

        assert result.stable is False
        assert result.size == 0
        assert result.vanished is True

    def test_head_returns_nosuchkey_marks_vanished(self, mock_s3_client: Any, mock_logger: Any) -> None:
        """Defensive coverage for code paths that may surface NoSuchKey on HEAD."""
        from botocore.exceptions import ClientError

        from functions.file_processor.app import check_file_stability

        mock_s3_client.head_object.side_effect = ClientError(
            error_response={
                "Error": {"Code": "NoSuchKey", "Message": "The specified key does not exist."},
                "ResponseMetadata": {"HTTPStatusCode": 404},
            },
            operation_name="HeadObject",
        )

        result = check_file_stability("test-bucket", "test-key")

        assert result.stable is False
        assert result.size == 0
        assert result.vanished is True

    def test_head_returns_other_client_error_does_not_mark_vanished(
        self, mock_s3_client: Any, mock_logger: Any
    ) -> None:
        """Errors other than 404/NoSuchKey return vanished=False; caller may requeue."""
        from botocore.exceptions import ClientError

        from functions.file_processor.app import check_file_stability

        mock_s3_client.head_object.side_effect = ClientError(
            error_response={
                "Error": {"Code": "AccessDenied", "Message": "Denied"},
                "ResponseMetadata": {"HTTPStatusCode": 403},
            },
            operation_name="HeadObject",
        )

        result = check_file_stability("test-bucket", "test-key")

        assert result.stable is False
        assert result.size == 0
        assert result.vanished is False
```

- [ ] **Step 3: Rewrite every existing tuple-unpack call in `test_file_stability.py`**

For every test method using `is_stable, size = check_file_stability(...)` or `is_stable, _ = check_file_stability(...)`, rewrite to:

```python
result = check_file_stability("test-bucket", "test-key")
is_stable, size = result.stable, result.size
```

(Keep the variable names so existing assertions like `assert is_stable is True; assert size == 1000` continue to work.)

Verify all sites are updated:
```bash
grep -n "check_file_stability" tests/unit/test_file_stability.py
```

Expected: every call now uses `result = ...` form. No remaining `is_stable, size = check_file_stability(`.

- [ ] **Step 4: Add the duplicate-event handler test to `test_lambda_handler.py`**

First inspect the existing test class structure:
```bash
grep -n "^class \|def test_" tests/unit/test_lambda_handler.py | head -30
```

Append the following test. Place it inside an existing test class if one tests stability-check paths (look for a class name containing `Stability`, `Requeue`, or `Handler`); otherwise create a new class `TestLambdaHandlerDuplicateEvent` at the end of the file. The test imports its own dependencies so it is self-contained.

```python
class TestLambdaHandlerDuplicateEvent:
    """Verify that a vanished S3 object is treated as a duplicate event."""

    def test_vanished_file_skipped_silently_with_metric(self) -> None:
        """When stability check returns vanished=True, handler logs + emits
        S3DuplicateEvent metric and does NOT requeue or raise MaxRetriesExceeded.
        """
        import json
        from unittest.mock import patch

        from functions.file_processor.app import StabilityResult, lambda_handler

        sqs_record_body = {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": "sbm-file-ingester"},
                        "object": {"key": "newTBP/foo.csv"},
                    }
                }
            ]
        }
        event = {
            "Records": [
                {"body": json.dumps(sqs_record_body), "messageId": "msg-1"},
            ]
        }

        class _Ctx:
            function_name = "test"
            memory_limit_in_mb = 128
            invoked_function_arn = "arn:aws:lambda:ap-southeast-2:000:function:test"
            aws_request_id = "req-1"

        with patch(
            "functions.file_processor.app.check_file_stability",
            return_value=StabilityResult(stable=False, size=0, vanished=True),
        ) as mock_stab, patch(
            "functions.file_processor.app.requeue_message"
        ) as mock_requeue, patch(
            "functions.file_processor.app.ingest_file"
        ) as mock_ingest, patch(
            "functions.file_processor.app.metrics"
        ) as mock_metrics:
            response = lambda_handler(event, _Ctx())

        assert response["statusCode"] == 200
        assert response.get("duplicate") == 1
        assert response["requeued"] == 0
        assert response["skipped"] == 0
        mock_stab.assert_called_once()
        mock_requeue.assert_not_called()
        mock_ingest.assert_not_called()
        metric_names = [call.kwargs.get("name") for call in mock_metrics.add_metric.call_args_list]
        assert "S3DuplicateEvent" in metric_names
        assert "MaxRetriesExceeded" not in metric_names
```

- [ ] **Step 5: Audit existing `lambda_handler` tests for full-response-dict equality**

The new code adds a `duplicate` field to the handler response. If any existing test does `assert response == {...exact dict...}`, that assertion will break.

```bash
grep -n "response ==" tests/unit/test_lambda_handler.py
```

For every match, either change to attribute-by-attribute assertion (`assert response["processed"] == N`) or add the new `"duplicate": 0` key to the expected dict.

- [ ] **Step 6: Run tests to verify they fail**

```bash
uv run pytest tests/unit/test_file_stability.py tests/unit/test_lambda_handler.py -v
```

Expected: New tests FAIL (AttributeError / ImportError for `StabilityResult`, assertion fail on `duplicate` field, missing metric). Modified existing tests may fail too; that is expected until Part B lands.

### Part B: Implementation (TDD green)

- [ ] **Step 7: Replace `check_file_stability` and module preamble in `app.py`**

Replace the entire content from line 1 through the end of `check_file_stability` (around line 108) with:

```python
"""SQS-triggered Lambda handler for the SBM file ingester.

This module is a thin SQS adapter. All business logic lives in
functions.file_processor.pipeline.ingest_file.

Pre-conditions (enforced at import time):
  - SQS_QUEUE_URL env var is set; KeyError on import otherwise so deploy
    fails fast rather than silently targeting the production queue.

Per-record flow:
  1. Decode the SQS record → bucket, key.
  2. Check file stability (S3 size stable for 2 consecutive checks).
  3. If vanished (HEAD 404): emit S3DuplicateEvent metric, log, skip silently
     (a duplicate S3 event arrived after a prior delivery already moved the
     file). No requeue, no MaxRetriesExceeded.
  4. If unstable but present: requeue with backoff (up to MAX_REQUEUE_RETRIES).
  5. If stable: build SourceFile, call ingest_file (which is idempotent +
     traced + emits per-file structured log + metrics).
  6. Return statusCode 200.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote

import boto3
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.metrics import MetricUnit
from botocore.exceptions import ClientError

from functions.file_processor.pipeline import ingest_file
from shared.source_file import SourceFile

# Required env var — KeyError on import if missing.
SQS_QUEUE_URL = os.environ["SQS_QUEUE_URL"]

# File-stability tuning (preserved from previous shape).
FILE_STABILITY_CHECK_INTERVAL = 5  # seconds between checks
FILE_STABILITY_MAX_WAIT = 30  # max seconds to wait for stabilisation
FILE_STABILITY_REQUIRED_CHECKS = 2  # consecutive stable checks required
MAX_REQUEUE_RETRIES = 3  # aligned with SQS maxReceiveCount = 3 (per spec)
# Delay before a requeued message becomes visible. Reverted 90 -> 60
# (2026-05-12): the 90s bump in f8282f4 was based on the wrong root cause.
# The MaxRetriesExceeded alarms it tried to suppress were actually caused by
# HEAD 404 on duplicate S3 events (now handled via StabilityResult.vanished),
# not by slow stability convergence. Real slow uploads stabilise in <12s.
REQUEUE_DELAY_SECONDS = 60

logger = Logger(service="file-processor")
tracer = Tracer(service="file-processor")
metrics = Metrics(namespace="SBM/Ingester")

s3_client = boto3.client("s3")
sqs_client = boto3.client("sqs")


@dataclass(frozen=True, slots=True)
class StabilityResult:
    """Result of a file-stability probe.

    ``vanished`` distinguishes "S3 HEAD returned 404 — the key no longer
    exists" from "we tried but the file is not yet stable / unreadable for
    another reason". Vanished keys are the expected outcome for a duplicate
    S3 event that arrived after a prior delivery already moved the file out
    of newTBP/; callers should skip them silently (no requeue, no error).
    """

    stable: bool
    size: int
    vanished: bool = False


def _is_object_missing(err: ClientError) -> bool:
    """Return True iff a ClientError represents a missing S3 object.

    HeadObject returns ``Code="404"`` / ``Message="Not Found"`` (and
    ``HTTPStatusCode == 404``) when the key does not exist; older code paths
    and tests may surface ``NoSuchKey`` (which is GetObject semantics, but
    some moto / boto3 combinations still raise it on HEAD).
    """
    code = err.response.get("Error", {}).get("Code")
    status = err.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return code in {"NoSuchKey", "404"} or status == 404


@tracer.capture_method
def check_file_stability(bucket: str, key: str) -> StabilityResult:
    """Wait for an S3 object's size to stabilise across 2 consecutive HEADs."""
    last_size = -1
    stable_count = 0
    total_wait = 0

    while total_wait < FILE_STABILITY_MAX_WAIT:
        try:
            response = s3_client.head_object(Bucket=bucket, Key=key)
            current_size = response["ContentLength"]

            if current_size == 0:
                logger.debug(
                    "File is empty, waiting",
                    extra={"bucket": bucket, "key": key, "waited": total_wait},
                )
                time.sleep(FILE_STABILITY_CHECK_INTERVAL)
                total_wait += FILE_STABILITY_CHECK_INTERVAL
                continue

            if current_size == last_size:
                stable_count += 1
                if stable_count >= FILE_STABILITY_REQUIRED_CHECKS:
                    logger.info(
                        "File is stable",
                        extra={"bucket": bucket, "key": key, "size": current_size},
                    )
                    return StabilityResult(stable=True, size=current_size)
            else:
                stable_count = 0

            last_size = current_size
            time.sleep(FILE_STABILITY_CHECK_INTERVAL)
            total_wait += FILE_STABILITY_CHECK_INTERVAL
        except ClientError as e:
            if _is_object_missing(e):
                logger.info(
                    "s3_duplicate_event",
                    extra={"bucket": bucket, "key": key, "reason": "head_404"},
                )
                return StabilityResult(stable=False, size=0, vanished=True)
            logger.error(
                "Error checking file stability",
                exc_info=True,
                extra={"bucket": bucket, "key": key, "error": str(e)},
            )
            return StabilityResult(stable=False, size=0)
        except Exception as e:
            logger.error(
                "Error checking file stability",
                exc_info=True,
                extra={"bucket": bucket, "key": key, "error": str(e)},
            )
            return StabilityResult(stable=False, size=0)

    logger.warning(
        "File stability check timed out",
        extra={"bucket": bucket, "key": key, "last_size": last_size, "waited": total_wait},
    )
    return StabilityResult(stable=False, size=0)
```

- [ ] **Step 8: Replace `lambda_handler` to handle `vanished`**

Replace the entire `lambda_handler` function (currently lines 136-192, ending at the closing `}` of the return dict) with:

```python
@tracer.capture_lambda_handler
@metrics.log_metrics(capture_cold_start_metric=True)
@logger.inject_lambda_context(correlation_id_path="Records[0].messageId")
def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    requeued_count = 0
    skipped_count = 0
    processed_count = 0
    duplicate_count = 0

    for record in event["Records"]:
        try:
            message_body = json.loads(record["body"])
            retry_count = message_body.get("_retry_count", 0)
            # Bind retry_count to every log line emitted inside ingest_file
            # (including the parser_outcome structured log emitted by
            # pipeline._emit_parser_outcome_log). SQS batch_size=1 in
            # production so loop runs at most once per invocation —
            # remove_keys cleanup is omitted as moot.
            logger.append_keys(retry_count=retry_count)

            s3_event = message_body["Records"][0]
            bucket_name = s3_event["s3"]["bucket"]["name"]
            file_key = s3_event["s3"]["object"]["key"]
            decoded_key = unquote(file_key.replace("+", "%20"))

            logger.info(
                "Processing file",
                extra={"bucket": bucket_name, "key": decoded_key, "retry_count": retry_count},
            )

            stability = check_file_stability(bucket_name, decoded_key)

            if stability.vanished:
                # Duplicate S3 event: a prior delivery already moved this key
                # out of newTBP/. Silent skip — no requeue, no MaxRetriesExceeded.
                logger.info(
                    "s3_duplicate_event",
                    extra={
                        "source_bucket": bucket_name,
                        "source_key": decoded_key,
                        "retry_count": retry_count,
                    },
                )
                metrics.add_metric(name="S3DuplicateEvent", unit=MetricUnit.Count, value=1)
                duplicate_count += 1
                continue

            if not stability.stable:
                if retry_count >= MAX_REQUEUE_RETRIES:
                    logger.error(
                        "Max retries exceeded for unstable file",
                        extra={"bucket": bucket_name, "key": decoded_key, "retry_count": retry_count},
                    )
                    metrics.add_metric(name="MaxRetriesExceeded", unit=MetricUnit.Count, value=1)
                    skipped_count += 1
                    continue
                if requeue_message(message_body, retry_count):
                    requeued_count += 1
                    metrics.add_metric(name="MessagesRequeued", unit=MetricUnit.Count, value=1)
                continue

            ingest_file(source_file=SourceFile(bucket=bucket_name, key=decoded_key))
            processed_count += 1
        except Exception:
            logger.error("Error processing SQS record", exc_info=True)
            continue

    return {
        "statusCode": 200,
        "body": "Successfully processed files.",
        "processed": processed_count,
        "requeued": requeued_count,
        "skipped": skipped_count,
        "duplicate": duplicate_count,
    }
```

- [ ] **Step 9: Run tests, verify they all pass**

```bash
uv run pytest tests/unit/test_file_stability.py tests/unit/test_lambda_handler.py -v
```

Expected: All tests PASS, including the three new stability tests and the new duplicate-event handler test.

### Part C: Terraform alarm

- [ ] **Step 10: Append `FileProcessor-DuplicateEventSpike` alarm**

Append to the end of `terraform/monitoring.tf`:

```hcl
# Anomaly detector for the S3DuplicateEvent metric introduced 2026-05-12.
# Normal baseline expected: ~30 events/day (~5% of invocations) from
# S3's at-least-once ObjectCreated delivery. Alarm fires when the
# duplicates/invocations ratio crosses 50% over a 2-hour evaluation —
# that level indicates either an S3 misbehavior or that our
# move-after-process logic stopped working.
resource "aws_cloudwatch_metric_alarm" "file_processor_duplicate_event_spike" {
  alarm_name          = "FileProcessor-DuplicateEventSpike"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  threshold           = 0.5
  treat_missing_data  = "notBreaching"
  alarm_description   = "S3DuplicateEvent / Invocations ratio above 50% — investigate move-after-process logic or S3 event configuration"
  alarm_actions       = [aws_sns_topic.sbm_alerts.arn]

  metric_query {
    id          = "ratio"
    expression  = "duplicates / invocations"
    label       = "S3DuplicateEvent ratio"
    return_data = true
  }

  metric_query {
    id          = "duplicates"
    return_data = false
    metric {
      namespace   = "SBM/Ingester"
      metric_name = "S3DuplicateEvent"
      period      = 3600
      stat        = "Sum"
    }
  }

  metric_query {
    id          = "invocations"
    return_data = false
    metric {
      namespace   = "AWS/Lambda"
      metric_name = "Invocations"
      period      = 3600
      stat        = "Sum"
      dimensions  = {
        FunctionName = aws_lambda_function.sbm_files_ingester.function_name
      }
    }
  }

  tags = {
    Name = "FileProcessor-DuplicateEventSpike"
  }
}
```

- [ ] **Step 11: Validate Terraform plan**

```bash
cd terraform && terraform plan -out=/tmp/duplicate-event-spike.tfplan && cd ..
```

Expected output line: `Plan: 1 to add, 0 to change, 0 to destroy.` The single addition must be `aws_cloudwatch_metric_alarm.file_processor_duplicate_event_spike`. Anything else → stop and investigate.

### Part D: Lint, full regression, commit

- [ ] **Step 12: Repo-wide regression + lint**

```bash
uv run pytest -q
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
```

Expected: all pass (~770 tests + lint clean).

- [ ] **Step 13: Commit (Commit 1 of 3)**

```bash
git add src/functions/file_processor/app.py \
        tests/unit/test_file_stability.py \
        tests/unit/test_lambda_handler.py \
        terraform/monitoring.tf
git status  # verify nothing unintended is staged
git commit -m "fix: treat HEAD 404 as duplicate S3 event + revert REQUEUE_DELAY 90->60

Root cause for the FileProcessor-MaxRetriesExceeded alarm noise was not slow
uploads but HeadObject returning Code='404' / Message='Not Found' on duplicate
S3 events for already-moved keys (the existing NoSuchKey check never fired
because that's GetObject semantics, not HeadObject). The 90s REQUEUE_DELAY
bump in f8282f4 treated the wrong symptom.

Refactor check_file_stability to return a StabilityResult dataclass with a
vanished flag, and have lambda_handler skip vanished records silently with
a new S3DuplicateEvent metric and a structured s3_duplicate_event log line.
Reverts REQUEUE_DELAY_SECONDS 90 -> 60.

Adds FileProcessor-DuplicateEventSpike CloudWatch alarm (S3DuplicateEvent /
Invocations > 50% over 2h) to detect a future regression in the
move-after-process logic or anomalous S3 event behavior."
```

---

## Task 2: Commit 2 — Synergy WA parser + interval.py cleanup

This Task produces **exactly one commit**. Commit happens only at Step 12.

**Files:**
- Create: `src/shared/parsers/synergy/__init__.py` (empty)
- Create: `src/shared/parsers/synergy/wa_meter_data.py`
- Create: `tests/unit/parsers/synergy/__init__.py` (empty)
- Create: `tests/unit/parsers/synergy/test_wa_meter_data.py`
- Move: `tests/unit/fixtures/optima_interval/wa_no_data_found.csv` → `tests/unit/fixtures/synergy/wa_no_data_found.csv`
- Modify: `src/shared/parsers/dispatcher.py`
- Modify: `src/shared/parsers/optima/interval.py`
- Modify: `tests/unit/parsers/optima/test_interval.py`

### Part A: Create the new parser (TDD red)

- [ ] **Step 1: Create directory structure and move fixture**

```bash
mkdir -p src/shared/parsers/synergy tests/unit/parsers/synergy tests/unit/fixtures/synergy
touch src/shared/parsers/synergy/__init__.py tests/unit/parsers/synergy/__init__.py
git mv tests/unit/fixtures/optima_interval/wa_no_data_found.csv tests/unit/fixtures/synergy/wa_no_data_found.csv
wc -c tests/unit/fixtures/synergy/wa_no_data_found.csv
```

Expected: `56 tests/unit/fixtures/synergy/wa_no_data_found.csv`. The byte count proves the file content survived the move.

- [ ] **Step 2: Write the failing tests**

Create `tests/unit/parsers/synergy/test_wa_meter_data.py` with:

```python
"""Tests for the Synergy WA meter data parser."""

from __future__ import annotations

from pathlib import Path

import pytest

from shared.parsers import NotRelevantParser, ParserOutcome
from shared.parsers.synergy.wa_meter_data import synergy_wa_meter_data_parser

FIXTURE_DIR = Path(__file__).parent.parent.parent / "fixtures" / "synergy"


class TestSynergyWaMeterDataParser:
    def test_sentinel_fixture_returns_processed_empty(self) -> None:
        """The committed fixture (a 56-byte 'No data found' sentinel) returns processed_empty."""
        outcome = synergy_wa_meter_data_parser(str(FIXTURE_DIR / "wa_no_data_found.csv"))

        assert isinstance(outcome, ParserOutcome)
        assert outcome.status == "processed_empty"
        assert outcome.reason == "no_data_available"

    def test_rejects_files_without_synergy_wa_prefix(self, tmp_path: Path) -> None:
        """Any filename not starting with the Synergy WA prefix is NotRelevantParser."""
        f = tmp_path / "interval_au_single_day.csv"
        f.write_text("Date,Start Time,Identifier\n")

        with pytest.raises(NotRelevantParser, match="Not a Synergy WA"):
            synergy_wa_meter_data_parser(str(f))

    def test_falls_through_on_header_drift(self, tmp_path: Path) -> None:
        """A future format with a different header falls through to NotRelevantParser.

        Routes to newIrrevFiles/ rather than newParseErr/, so format drift surfaces
        as accumulation in newIrrevFiles/ instead of false-positive parse errors.
        """
        f = tmp_path / "Meter_Data_WA (AU)_Electricity_1778999999_2026051300000000.csv"
        f.write_text("Date,NMI,Usage\n2026-05-13,12345,1.23\n")

        with pytest.raises(NotRelevantParser, match="drifted"):
            synergy_wa_meter_data_parser(str(f))

    def test_production_filename_pattern_matches(self, tmp_path: Path) -> None:
        """Use the actual production filename pattern with the sentinel body."""
        prod_path = tmp_path / "Meter_Data_WA (AU)_Electricity_1778517074_2026051202315309.csv"
        prod_path.write_bytes((FIXTURE_DIR / "wa_no_data_found.csv").read_bytes())

        outcome = synergy_wa_meter_data_parser(str(prod_path))

        assert outcome.status == "processed_empty"
        assert outcome.reason == "no_data_available"
```

- [ ] **Step 3: Run tests, confirm they fail**

```bash
uv run pytest tests/unit/parsers/synergy/ -v
```

Expected: ALL FOUR tests FAIL with `ModuleNotFoundError: No module named 'shared.parsers.synergy.wa_meter_data'`.

### Part B: Implement (TDD green)

- [ ] **Step 4: Create the parser**

Create `src/shared/parsers/synergy/wa_meter_data.py` with:

```python
"""Synergy WA "meter data" archiver / sentinel handler.

External producer: Synergy's WA portal drops files into newTBP/ with names
``Meter_Data_WA (AU)_Electricity_<epoch>_<timestamp>.csv``. The current
production payload is a 56-byte sentinel CSV indicating "no data found" for
the queried period; the file is classified as ``processed_empty`` and moved
to newIrrevFiles/ without writing rows to the Hudi data lake.

Real-data files have not been observed in production. If Synergy starts
emitting them, the strict header match in this parser will fall through to
NotRelevantParser, and the file will land in newIrrevFiles/ — that
accumulation is the signal to add real-data parsing logic here.

Fail-safe (NotRelevantParser → newIrrevFiles/) is strictly preferred over
fail-loud (ParserError → newParseErr/) on format drift, because the alarm
on ParseError counts is tuned for genuine corruption, not for new
producers.
"""

from __future__ import annotations

from pathlib import Path

from aws_lambda_powertools import Logger

from shared.parsers import NotRelevantParser, ParserOutcome

logger = Logger(service="synergy-wa-meter-data-parser", child=True)

FILENAME_PREFIX = "Meter_Data_WA (AU)_Electricity_"
SENTINEL_HEADER = "Unnamed: 0,NMI,Unnamed: 2"


def synergy_wa_meter_data_parser(file_name: str) -> ParserOutcome:
    path = Path(file_name)
    if not path.name.startswith(FILENAME_PREFIX):
        raise NotRelevantParser("Not a Synergy WA meter data file")

    try:
        with path.open(encoding="utf-8-sig") as f:
            first_line = f.readline().strip()
    except (OSError, UnicodeDecodeError) as e:
        raise NotRelevantParser(
            f"Synergy WA file not readable as text: {e}"
        ) from e

    if first_line != SENTINEL_HEADER:
        raise NotRelevantParser(
            f"Synergy WA file format drifted. First line: {first_line!r}"
        )

    logger.info(
        "synergy_wa_no_data_sentinel",
        extra={"file": str(path)},
    )
    return ParserOutcome(status="processed_empty", reason="no_data_available")
```

- [ ] **Step 5: Tests pass for new parser**

```bash
uv run pytest tests/unit/parsers/synergy/ -v
```

Expected: All four tests PASS.

### Part C: Wire into dispatcher, remove from interval.py

- [ ] **Step 6: Register in dispatcher**

Edit `src/shared/parsers/dispatcher.py`. Add the import after the existing parser imports (at the end of the import block):

```python
from shared.parsers.synergy.wa_meter_data import synergy_wa_meter_data_parser
```

Then replace `PARSERS = [...]` (currently lines 25-36) with:

```python
PARSERS = [
    # Position 0: most specific filename prefix. Synergy WA files use a
    # distinctive ``Meter_Data_WA (AU)_Electricity_`` prefix that no other
    # parser claims, so a cheap NotRelevantParser fail-fast costs nothing
    # for all other inputs.
    synergy_wa_meter_data_parser,
    noosa_solar_parser,
    envizi_vertical_parser_water,
    envizi_vertical_parser_electricity,
    racv_elec_parser,
    racv_billing_parser,
    bunnings_billing_parser,
    demand_parser,
    interval_parser,
    envizi_vertical_parser_water_bulk,
    green_square_private_wire_schneider_comx_parser,
]
```

- [ ] **Step 7: Remove WA detection from `interval.py`**

In `src/shared/parsers/optima/interval.py`, replace the `_is_no_data_sentinel` function (lines 29-50) with the AU/NZ-only version:

```python
def _is_no_data_sentinel(raw_df: pd.DataFrame) -> bool:
    """Detect the BidEnergy AU/NZ 'No data is available' sentinel.

    The BidEnergy "Export Interval Usage Csv" endpoint returns a 148-byte
    sentinel CSV with a single row containing 'No data is available' in the
    BuyerShortName column and every other column blank when a site has no
    data for the requested range.

    (The Synergy WA "Meter_Data_WA (AU)_Electricity_*" sentinel is a
    different shape from a different producer and is handled by
    ``shared.parsers.synergy.wa_meter_data.synergy_wa_meter_data_parser``
    — registered ahead of this parser in the dispatcher.)
    """
    if len(raw_df) == 1 and "BuyerShortName" in raw_df.columns:
        buyer_short_name = raw_df["BuyerShortName"].iloc[0]
        if pd.notna(buyer_short_name) and str(buyer_short_name).strip() == "No data is available":
            other_values = raw_df.drop(columns=["BuyerShortName"]).iloc[0]
            non_blank_values = other_values.notna() & other_values.astype(str).str.strip().ne("")
            if not non_blank_values.any():
                return True

    return False
```

And restore the strict cheap relevance gate (currently lines 63-67) to:

```python
    # All three column markers must appear in the header row.
    if not all(token in first_line for token in ("Date", "Start Time", "Identifier")):
        raise NotRelevantParser("Not an Optima interval CSV")
```

(Removes the `is_wa_sentinel_header` variable and the disjunction with `Unnamed: 0`/`NMI`/`Unnamed: 2`.)

- [ ] **Step 8: Delete the orphan WA test from `test_interval.py`**

```bash
grep -n "test_wa_no_data_found_fixture_returns_processed_empty\|wa_no_data_found" tests/unit/parsers/optima/test_interval.py
```

Open the file at the match line. Delete the entire `test_wa_no_data_found_fixture_returns_processed_empty` test method (including its decorator if any).

If the enclosing class becomes empty (no other test methods after deletion), delete the empty class definition as well. Use `grep -A 20 "class TestXxx"` to inspect class contents before deciding.

Repo-wide check for any other reference to the old fixture path:

```bash
grep -rn "optima_interval/wa_no_data_found\|optima_interval.*wa_no_data" tests/ src/
```

Expected: empty output. If any match appears, update it to the new path `tests/unit/fixtures/synergy/wa_no_data_found.csv`.

### Part D: Lint, full regression, commit

- [ ] **Step 9: Run all parser tests**

```bash
uv run pytest tests/unit/parsers/synergy/ tests/unit/parsers/optima/test_interval.py tests/unit/test_non_nem_parsers.py -v
```

Expected: All pass. The Synergy fixture should now match via the new parser; the orphan WA test is gone.

- [ ] **Step 10: Repo-wide regression**

```bash
uv run pytest -q
```

Expected: PASS (~770 tests, possibly -1 from the deleted orphan test and +4 from the new Synergy tests, net +3).

- [ ] **Step 11: Lint**

```bash
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
```

Expected: clean.

- [ ] **Step 12: Commit (Commit 2 of 3)**

```bash
git add src/shared/parsers/synergy/ \
        tests/unit/parsers/synergy/ \
        tests/unit/fixtures/synergy/ \
        src/shared/parsers/dispatcher.py \
        src/shared/parsers/optima/interval.py \
        tests/unit/parsers/optima/test_interval.py
# git mv from Step 1 also stages the deletion at tests/unit/fixtures/optima_interval/
git status  # verify nothing unintended
git commit -m "refactor: extract Synergy WA parser; restore interval.py SRP

Synergy WA's portal drops 'Meter_Data_WA (AU)_Electricity_*.csv' sentinel
files into newTBP/. f8282f4 added WA recognition inline in the Optima
interval parser, which coupled two unrelated producers. Extract a dedicated
synergy_wa_meter_data_parser (strict filename prefix + exact header match,
fail-safe NotRelevantParser on drift), register at PARSERS[0], and remove
the WA branch from interval.py.

Fixture moved from tests/unit/fixtures/optima_interval/wa_no_data_found.csv
to tests/unit/fixtures/synergy/wa_no_data_found.csv. The single WA test in
tests/unit/parsers/optima/test_interval.py is replaced by a dedicated
tests/unit/parsers/synergy/test_wa_meter_data.py with 4 tests covering
sentinel, prefix-rejection, drift fall-through, and the real production
filename pattern."
```

---

## Task 3: Commit 3 — Cache-hit log fix (branches on Task 0 decision)

This Task produces **exactly one commit**. Commit happens only at the final step. The implementation steps differ based on the form recorded in `docs/superpowers/plans/2026-05-12-task-0-decision.md`.

**Files:**
- Read: `docs/superpowers/plans/2026-05-12-task-0-decision.md`
- Rewrite: `tests/unit/test_persistence_cache_hit_log.py`
- Modify or delete: `src/functions/file_processor/persistence.py`
- Conditionally modify: `src/functions/file_processor/pipeline.py` (Form A only)

### Step 1: Read the Task 0 decision

- [ ] **Step 1: Read the decision form**

```bash
cat docs/superpowers/plans/2026-05-12-task-0-decision.md
```

Identify the path:
- **Form A** → Step 2A (native mechanism replaces subclass)
- **Form B** → Step 2B (fix subclass using `data.get_payload()`)
- **Form C** → Step 2C (fallback: log only `idempotency_key`)

### Step 2: Rewrite the test (TDD red, shared across all forms)

- [ ] **Step 2: Replace `tests/unit/test_persistence_cache_hit_log.py` in full**

Common shared rewrite for all forms — the test uses `capsys` + JSON parsing to capture stdout the way CloudWatch sees it, and constructs a real `DataRecord` to exercise the production code path.

**For Forms A and B** (`get_payload()` works → bucket/key reachable):

```python
"""Tests for idempotent_cache_hit structured log emission.

The previous version of this test passed for the wrong reasons: it captured
LogRecord attributes via caplog (pre-formatter, so the production bug — that
the JSON formatter was being bypassed — was invisible) AND it passed a dict
where production passes a DataRecord (so the isinstance(data, dict) branch
masked the real type-confusion bug).

This rewrite asserts against stdout JSON (what CloudWatch sees) using a real
DataRecord constructed via Powertools' public API.
"""

from __future__ import annotations

import json
from unittest.mock import patch

import boto3
import pytest
from aws_lambda_powertools.utilities.idempotency.exceptions import (
    IdempotencyItemAlreadyExistsError,
)
from aws_lambda_powertools.utilities.idempotency.persistence.base import DataRecord
from aws_lambda_powertools.utilities.idempotency.persistence.dynamodb import (
    DynamoDBPersistenceLayer,
)
from moto import mock_aws


@pytest.fixture
def idempotency_table():
    with mock_aws():
        ddb = boto3.client("dynamodb", region_name="ap-southeast-2")
        ddb.create_table(
            TableName="sbm-ingester-idempotency",
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        yield ddb


def _make_data_record(idempotency_key: str, payload: dict) -> DataRecord:
    """Build a DataRecord that exercises the production save_inprogress path."""
    return DataRecord(
        idempotency_key=idempotency_key,
        status="INPROGRESS",
        payload_hash="",
        # response_data field holds the serialized payload; the production
        # code accesses it via data.get_payload() which json.loads it back.
        response_data=json.dumps(payload),
    )


def _extract_cache_hit_logs(captured_out: str) -> list[dict]:
    """Parse capsys stdout for JSON log lines whose 'message' is the cache-hit marker."""
    lines = [
        json.loads(line)
        for line in captured_out.splitlines()
        if line.strip().startswith("{")
    ]
    return [line for line in lines if line.get("message") == "idempotent_cache_hit"]


class TestIdempotentCacheHitLog:
    def test_emits_structured_json_with_source_fields(
        self, capsys, idempotency_table
    ) -> None:
        from functions.file_processor.persistence import (
            InstrumentedDynamoDBPersistenceLayer,
        )

        layer = InstrumentedDynamoDBPersistenceLayer(
            table_name="sbm-ingester-idempotency",
        )
        data = _make_data_record(
            idempotency_key="abc123",
            payload={"bucket": "sbm-file-ingester", "key": "newTBP/foo.csv"},
        )

        with patch.object(
            DynamoDBPersistenceLayer,
            "save_inprogress",
            side_effect=IdempotencyItemAlreadyExistsError(),
        ), pytest.raises(IdempotencyItemAlreadyExistsError):
            layer.save_inprogress(data=data)

        out = capsys.readouterr().out
        cache_hits = _extract_cache_hit_logs(out)
        assert len(cache_hits) == 1, f"Expected 1 cache-hit JSON line; got {len(cache_hits)} in:\n{out}"
        log = cache_hits[0]
        assert log["source_bucket"] == "sbm-file-ingester"
        assert log["source_key"] == "newTBP/foo.csv"
        assert log["idempotency_key"] == "abc123"

    def test_no_log_on_successful_save(self, capsys, idempotency_table) -> None:
        from functions.file_processor.persistence import (
            InstrumentedDynamoDBPersistenceLayer,
        )

        layer = InstrumentedDynamoDBPersistenceLayer(
            table_name="sbm-ingester-idempotency",
        )
        data = _make_data_record(idempotency_key="def456", payload={"bucket": "b", "key": "k"})

        with patch.object(DynamoDBPersistenceLayer, "save_inprogress", return_value=None):
            layer.save_inprogress(data=data)

        out = capsys.readouterr().out
        cache_hits = _extract_cache_hit_logs(out)
        assert cache_hits == []
```

**For Form C** (`get_payload()` doesn't work → only `idempotency_key`):

Use the same file structure as above but change the first test's assertion to:

```python
        log = cache_hits[0]
        assert log["idempotency_key"] == "abc123"
        # source_bucket / source_key are NOT logged in Form C — they aren't
        # reachable from a DataRecord at save_inprogress time. The operational
        # runbook describes how to resolve idempotency_key → file via DynamoDB.
        assert "source_bucket" not in log
        assert "source_key" not in log
```

Run the test:

```bash
uv run pytest tests/unit/test_persistence_cache_hit_log.py -v
```

Expected: `test_emits_structured_json_with_source_fields` FAILS against the current persistence.py (either no JSON line on stdout, or fields are `None`). This is the TDD red-state that proves the existing test was a false positive.

### Step 3: Implement based on form

#### Form A: Replace subclass with native mechanism

- [ ] **Step 3A.1: Delete the subclass file**

```bash
git rm src/functions/file_processor/persistence.py
```

- [ ] **Step 3A.2: Edit `pipeline.py` to remove subclass references**

In `src/functions/file_processor/pipeline.py`:
1. Remove the `from functions.file_processor.persistence import InstrumentedDynamoDBPersistenceLayer` import.
2. Replace the persistence layer constructor (currently `InstrumentedDynamoDBPersistenceLayer(...)`) with `DynamoDBPersistenceLayer(...)`.
3. Apply the native mechanism per the decision record (e.g. `IdempotencyConfig(log_event=True)` or whatever Task 0 identified).

The exact code lines are determined by what the decision record specifies. Verify the change with:

```bash
grep -n "InstrumentedDynamoDBPersistenceLayer\|DynamoDBPersistenceLayer\|IdempotencyConfig" src/functions/file_processor/pipeline.py
```

Expected: no references to `InstrumentedDynamoDBPersistenceLayer` remain.

- [ ] **Step 3A.3: Run tests**

```bash
uv run pytest tests/unit/test_persistence_cache_hit_log.py -v
```

Expected: both tests pass via the native mechanism.

#### Form B: Fix the subclass in place

- [ ] **Step 3B.1: Rewrite `persistence.py`**

Replace the entire content of `src/functions/file_processor/persistence.py` with:

```python
"""Persistence layer subclass that logs idempotency cache hits.

Powertools does not expose a native cache-hit hook in the currently pinned
version (aws-lambda-powertools >= 3.24.0). The cache-hit path materialises
as IdempotencyItemAlreadyExistsError raised by save_inprogress when the
record already exists. We catch it here, emit a structured log with file
identification, and re-raise so Powertools handles the cached response
normally.

The Logger MUST share its service string with the parent Logger in
``functions.file_processor.app`` and ``...pipeline`` so that ``child=True``
resolves to a process-wide Powertools logger with the JSON formatter
attached. With a mismatched service string, child=True falls back to a
stdlib logger that drops ``extra=`` kwargs from JSON output entirely.

Field source:
  - ``DataRecord.idempotency_key`` — the hash key.
  - ``DataRecord.get_payload()`` — the deserialized input payload dict
    (containing the SourceFile fields ``bucket`` and ``key``). Verified
    against the pinned Powertools version in Task 0.
"""

from __future__ import annotations

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.idempotency import DynamoDBPersistenceLayer
from aws_lambda_powertools.utilities.idempotency.exceptions import (
    IdempotencyItemAlreadyExistsError,
)
from aws_lambda_powertools.utilities.idempotency.persistence.base import DataRecord

logger = Logger(service="file-processor", child=True)


class InstrumentedDynamoDBPersistenceLayer(DynamoDBPersistenceLayer):
    """DynamoDB persistence layer that logs cache hits with source identification."""

    def save_inprogress(
        self,
        data: DataRecord,
        remaining_time_in_millis: int | None = None,
    ) -> None:
        try:
            return super().save_inprogress(data, remaining_time_in_millis)
        except IdempotencyItemAlreadyExistsError:
            payload = data.get_payload() or {}
            logger.info(
                "idempotent_cache_hit",
                extra={
                    "idempotency_key": data.idempotency_key,
                    "source_bucket": payload.get("bucket"),
                    "source_key": payload.get("key"),
                },
            )
            raise
```

#### Form C: Fallback — log only `idempotency_key`

- [ ] **Step 3C.1: Rewrite `persistence.py` to log only the hash**

Replace the entire content of `src/functions/file_processor/persistence.py` with:

```python
"""Persistence layer subclass that logs idempotency cache hits.

DESIGN NOTE: At save_inprogress time, Powertools' DataRecord does NOT carry
the original input payload (verified in Task 0). We therefore log only the
idempotency_key; the runbook describes how to reverse-lookup the source file
via DynamoDB table ``sbm-ingester-idempotency``.

Service string MUST match the parent Logger in app.py / pipeline.py for
child=True to inherit the JSON formatter.
"""

from __future__ import annotations

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.idempotency import DynamoDBPersistenceLayer
from aws_lambda_powertools.utilities.idempotency.exceptions import (
    IdempotencyItemAlreadyExistsError,
)
from aws_lambda_powertools.utilities.idempotency.persistence.base import DataRecord

logger = Logger(service="file-processor", child=True)


class InstrumentedDynamoDBPersistenceLayer(DynamoDBPersistenceLayer):
    def save_inprogress(
        self,
        data: DataRecord,
        remaining_time_in_millis: int | None = None,
    ) -> None:
        try:
            return super().save_inprogress(data, remaining_time_in_millis)
        except IdempotencyItemAlreadyExistsError:
            logger.info(
                "idempotent_cache_hit",
                extra={"idempotency_key": data.idempotency_key},
            )
            raise
```

Additionally append a short runbook to the existing `CLAUDE.md` (or `docs/ARCHITECTURE.md`):

> **idempotent_cache_hit log → source file lookup.** When this log fires, the original `bucket`/`key` is not in the log line. To resolve: query DynamoDB table `sbm-ingester-idempotency` with `id = <idempotency_key>` and read the `data` attribute (or `response_data` for a completed cache hit). If both are opaque, scan recent S3 events in the SQS DLQ around the log timestamp.

### Step 4: Verify (all forms)

- [ ] **Step 4: Run the cache-hit test + full regression + lint**

```bash
uv run pytest tests/unit/test_persistence_cache_hit_log.py -v
uv run pytest -q
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
```

Expected: all pass.

### Step 5: Commit (Commit 3 of 3)

- [ ] **Step 5: Commit**

Stage the files based on form:
- **Form A**: `git add -A` then verify with `git status`.
- **Forms B / C**: `git add src/functions/file_processor/persistence.py tests/unit/test_persistence_cache_hit_log.py docs/superpowers/plans/2026-05-12-task-0-decision.md` (plus any CLAUDE.md / ARCHITECTURE.md update from Form C).

Commit message (adapt the body to the form taken):

```bash
git commit -m "fix: idempotent_cache_hit log emits structured fields

Two compounding bugs in persistence.py:

1. Logger(service='instrumented-persistence', child=True) had no parent
   matching that service string, so Powertools fell back to a stdlib
   logger that dropped extra= kwargs from JSON output. Aligned service to
   'file-processor' so child=True resolves to the Powertools logger with
   its JSON formatter.

2. The save_inprogress override used isinstance(data, dict) — but
   Powertools passes a DataRecord, not a dict. payload always fell
   through to {} and fields were always None. Replaced with the
   DataRecord public API (data.get_payload() / data.idempotency_key —
   API verified in Task 0; see docs/superpowers/plans/2026-05-12-task-0-decision.md).

Existing test_persistence_cache_hit_log.py passed for the wrong reasons:
caplog captured LogRecord attributes pre-formatter (so the formatter bug
was invisible) AND the test passed a dict (so the type-confusion bug was
masked). Rewritten with capsys + json parsing and a real DataRecord,
which now exercises the same code path as production."
```

(For Form A, replace bug-fix language with "replaced subclass with Powertools native cache-hit hook X". For Form C, note "logs only idempotency_key; runbook added for file lookup".)

---

## Task 4: Recover 3 WA sentinel files from `newParseErr/`

Operational task — no commit. **Run only after Task 5's deploy completes.**

- [ ] **Step 1: List the active WA files**

```bash
aws s3 ls 's3://sbm-file-ingester/newParseErr/' --region ap-southeast-2 \
  | grep 'Meter_Data_WA' \
  | grep -v archived/
```

Expected: 3 files matching `Meter_Data_WA (AU)_Electricity_*` (56 bytes each).

If the count is not 3, stop and reconcile — fresh WA files may have arrived (now correctly classified as `processed_empty` post-deploy, so they'd be in `newIrrevFiles/` not `newParseErr/`; investigate any anomaly).

- [ ] **Step 2: Move each file to `newIrrevFiles/`**

For each filename from Step 1, run:

```bash
aws s3 mv \
  "s3://sbm-file-ingester/newParseErr/<exact filename>" \
  "s3://sbm-file-ingester/newIrrevFiles/<exact filename>" \
  --region ap-southeast-2
```

The filename contains parentheses and spaces — quote with double quotes as shown.

- [ ] **Step 3: Verify**

```bash
aws s3 ls 's3://sbm-file-ingester/newParseErr/' --region ap-southeast-2 \
  | grep 'Meter_Data_WA' \
  | grep -v archived/ \
  | wc -l
```

Expected: `0`.

```bash
aws s3 ls 's3://sbm-file-ingester/newIrrevFiles/' --region ap-southeast-2 \
  | grep 'Meter_Data_WA' \
  | grep -v archived/ \
  | wc -l
```

Expected: ≥ 3.

---

## Task 5: Deploy

Operational task — no commits.

- [ ] **Step 1: Pre-merge sanity**

```bash
git status                    # working tree clean
git log --oneline main..HEAD  # should show exactly 3 new commits (1 per Task 1-3) + the spec + plan commits
uv run pytest -q              # ~770 tests pass
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
```

All must pass. Stop on any failure.

- [ ] **Step 2: Terraform apply (alarm only)**

```bash
cd terraform
terraform plan -out=/tmp/post-deploy-tuning.tfplan
```

Expected: `Plan: 1 to add, 0 to change, 0 to destroy.` The single addition is `aws_cloudwatch_metric_alarm.file_processor_duplicate_event_spike`.

```bash
terraform apply /tmp/post-deploy-tuning.tfplan
cd ..
```

Expected: 1 resource added. The alarm starts in `INSUFFICIENT_DATA` state (no metric data yet — expected).

- [ ] **Step 3: Merge to `main`**

```bash
git checkout main
git pull
git merge --no-ff fix/post-deploy-tuning
```

Expected: merge commit created.

- [ ] **Step 4: Push and watch the GitHub Actions deploy**

```bash
git push origin main
gh run watch
```

Expected: workflow succeeds.

- [ ] **Step 5: Wait for Lambda update to complete (NOT the GH workflow)**

The GitHub Actions workflow returns success when `aws lambda update-function-code` returns, but Lambda then transitions through `InProgress` → `Successful` asynchronously. Wait for the active state before smoke-testing:

```bash
aws lambda wait function-updated \
  --function-name sbm-files-ingester \
  --region ap-southeast-2
```

Expected: returns silently when the function transitions to `Successful`.

- [ ] **Step 6: Smoke check — confirm new code is live**

```bash
aws logs filter-log-events \
  --log-group-name /aws/lambda/sbm-files-ingester \
  --start-time $(($(date +%s) * 1000 - 600000)) \
  --filter-pattern '"INIT_REPORT"' \
  --region ap-southeast-2 \
  --max-items 1 \
  --query 'events[*].message' --output text
```

Expected: a recent `INIT_REPORT Init Duration` line indicating a cold start on the new code in the last 10 minutes.

- [ ] **Step 7: Run Task 4 (file recovery) now that deploy is confirmed live.**

- [ ] **Step 8: 12-hour CloudWatch watch (next-day check)**

```bash
# 1) MaxRetriesExceeded should be near zero.
aws cloudwatch get-metric-statistics \
  --namespace SBM/Ingester \
  --metric-name MaxRetriesExceeded \
  --statistics Sum \
  --period 43200 \
  --start-time $(date -u -d '12 hours ago' '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || date -u -v -12H '+%Y-%m-%dT%H:%M:%SZ') \
  --end-time $(date -u '+%Y-%m-%dT%H:%M:%SZ') \
  --region ap-southeast-2

# 2) S3DuplicateEvent should be active (~30 events/day expected).
aws cloudwatch get-metric-statistics \
  --namespace SBM/Ingester \
  --metric-name S3DuplicateEvent \
  --statistics Sum \
  --period 43200 \
  --start-time $(date -u -d '12 hours ago' '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || date -u -v -12H '+%Y-%m-%dT%H:%M:%SZ') \
  --end-time $(date -u '+%Y-%m-%dT%H:%M:%SZ') \
  --region ap-southeast-2

# 3) idempotent_cache_hit log lines should now contain source_bucket / source_key
#    (Forms A and B) or at minimum idempotency_key (Form C).
aws logs filter-log-events \
  --log-group-name /aws/lambda/sbm-files-ingester \
  --start-time $(($(date +%s) * 1000 - 43200000)) \
  --filter-pattern '"idempotent_cache_hit"' \
  --region ap-southeast-2 \
  --max-items 5 \
  --query 'events[*].message' --output text
```

Expected:
1. `MaxRetriesExceeded` Sum near 0 (single digits or less over 12h)
2. `S3DuplicateEvent` Sum > 0, < 50% of `Invocations`
3. `idempotent_cache_hit` log entries are valid JSON with the expected fields

- [ ] **Step 9: Cleanup local branch (optional)**

```bash
git branch -d fix/post-deploy-tuning
```

---

## Rollback

If post-deploy monitoring reveals a regression:

- **Code rollback:**
  ```bash
  aws s3 ls s3://gega-code-deployment-bucket/sbm-files-ingester/ --region ap-southeast-2 | sort
  aws lambda update-function-code \
    --function-name sbm-files-ingester \
    --s3-bucket gega-code-deployment-bucket \
    --s3-key sbm-files-ingester/<previous-zip-key>.zip \
    --region ap-southeast-2
  ```
- **Terraform rollback** (alarm only, if it misbehaves):
  ```bash
  cd terraform
  terraform destroy -target=aws_cloudwatch_metric_alarm.file_processor_duplicate_event_spike
  ```
- **No DynamoDB cache invalidation needed** — these fixes do not change the idempotency cache key shape or TTL.

---

## Self-Review Checklist

- [x] **Spec coverage:** Task 1 → Issue 1 (404, metric, alarm, REQUEUE_DELAY revert). Task 2 → Issue 2 (Synergy parser, fixture move, interval.py cleanup, orphan test removal). Task 3 → Issue 3 (cache-hit log, with three implementation forms based on Task 0's verified API findings). Task 4 → recovery. Task 5 → deploy + monitoring + rollback.
- [x] **No placeholders:** every code block is complete; no "TBD" / "similar to" / "implement later".
- [x] **Type consistency:** `StabilityResult` used consistently in Task 1 (Parts A & B) and referenced in Task 1 Step 4 test mock. `DataRecord.idempotency_key` / `data.get_payload()` consistent in Task 3 Forms A/B; Form C is explicit about omitting `source_bucket`/`source_key`. `synergy_wa_meter_data_parser` consistent across Task 2 Steps 4-6.
- [x] **One commit per Task** (Tasks 1-3 = 3 commits total). Each Task's commit is the last step. No `git commit --amend` across Tasks. Subagent-driven mode: each subagent owns one Task and produces exactly one commit.
- [x] **Forward-only on `f8282f4`:** `REQUEUE_DELAY 90→60` revert and `interval.py` WA removal happen as normal commits inside Task 1 and Task 2 respectively. No `git reset`.
- [x] **Task 0 verifies the critical assumption:** `DataRecord.get_payload()` behaviour determines Task 3's path (Form A/B/C). Fallback (Form C) exists if the API does not work as initially inferred.
