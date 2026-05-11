# Post-Deploy Tuning Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix three production issues surfaced by post-deploy CloudWatch review of the per-file ingest refactor: (1) `MaxRetriesExceeded` alarm noise from `HeadObject` 404s on duplicate S3 events, (2) WA "No data found" sentinels misclassified as parse errors, (3) `idempotent_cache_hit` log missing structured fields.

**Architecture:** Three forward-only commits on top of `f8282f4` on branch `fix/post-deploy-tuning`. Each commit is one logical fix with TDD (failing test → minimal implementation → passing test → commit). One Terraform-only step adds a CloudWatch alarm. Stability check and requeue logic remain in place — a known production streaming-uploader scenario requires them.

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
| `tests/unit/fixtures/synergy/wa_no_data_found.csv` | Create (move) | Sentinel fixture (moved from `tests/unit/fixtures/optima_interval/`) |
| `tests/unit/fixtures/optima_interval/wa_no_data_found.csv` | Delete | Moved to `synergy/` |
| `tests/unit/parsers/synergy/__init__.py` | Create | Empty test package marker |
| `tests/unit/parsers/synergy/test_wa_meter_data.py` | Create | Tests for Synergy WA parser |
| `tests/unit/parsers/optima/test_interval.py` | Modify | Remove `test_wa_no_data_found_fixture_returns_processed_empty` |
| `src/shared/parsers/optima/interval.py` | Modify | Remove WA branch from `_is_no_data_sentinel`, restore strict header gate |
| `src/shared/parsers/dispatcher.py` | Modify | Register `synergy_wa_meter_data_parser` at position 0 of `PARSERS` |
| `src/functions/file_processor/persistence.py` | Modify or delete | Branches on Task 0 outcome — either fix the subclass or remove entirely |
| `tests/unit/test_persistence_cache_hit_log.py` | Rewrite | Use `capsys` + JSON parsing; pass a `DataRecord` instead of `dict` |
| `src/functions/file_processor/pipeline.py` | Conditionally modify | Only if Task 0 chooses path 6A (replace subclass with `log_event=True` or equivalent) |

---

## Task 0: Investigate Powertools `log_event=True` for cache-hit observability

**Goal:** Decide between two branches in Task 6:
- **Path 6A:** A native Powertools mechanism gives us `source_bucket` + `source_key` + `idempotency_key` on cache hits with no custom subclass → delete `InstrumentedDynamoDBPersistenceLayer`.
- **Path 6B:** No native mechanism is suitable → fix the subclass in place.

**Time budget:** 30 minutes. If inconclusive after 30 min, default to 6B.

**Files:**
- Read: `aws-lambda-powertools` source via local venv

- [ ] **Step 1: Inspect Powertools idempotency module locally**

```bash
uv run python -c "
import aws_lambda_powertools as p
import os
print('Powertools:', p.__version__)
print('Path:', os.path.dirname(p.__file__))
"
```

Expected output: version `3.24.0` or later, path under `.venv/`.

- [ ] **Step 2: Check `@idempotent_function` for a `log_event` or hook parameter**

```bash
uv run python -c "
from aws_lambda_powertools.utilities.idempotency import idempotent_function
import inspect
print(inspect.signature(idempotent_function))
print('---')
print(inspect.getsource(idempotent_function))
" | head -60
```

What to look for:
- Any parameter named `log_event`, `on_cache_hit`, `cache_hit_handler`, `event_logger`, or similar
- Powertools' own logging of `IdempotencyItemAlreadyExistsError` with sufficient context

- [ ] **Step 3: Check `DynamoDBPersistenceLayer` for a hook**

```bash
uv run python -c "
from aws_lambda_powertools.utilities.idempotency.persistence.dynamodb import DynamoDBPersistenceLayer
import inspect
src = inspect.getsource(DynamoDBPersistenceLayer)
print(src[:3000])
"
```

What to look for: any callback/hook for cache hit, or a built-in log statement that already emits the fields we need.

- [ ] **Step 4: Search for documented config flags**

```bash
uv run python -c "
from aws_lambda_powertools.utilities.idempotency.config import IdempotencyConfig
import inspect
print(inspect.signature(IdempotencyConfig))
print('---')
print(IdempotencyConfig.__init__.__doc__ or '(no docstring)')
"
```

What to look for: any `log_*` or `event_*` flag.

- [ ] **Step 5: Decide and record**

Write the decision into the plan inline by editing this file. Replace this checkbox section with:

```markdown
**Decision: 6A** (Powertools mechanism `<name>` provides `<fields>` — subclass to be removed)

— OR —

**Decision: 6B** (No suitable native mechanism; subclass fix in place)
```

Default if undecided after 30 min: **6B**.

No commit for Task 0 — it's investigation only.

---

## Task 1: Fix `check_file_stability` to detect HEAD 404 (TDD)

**Files:**
- Modify: `src/functions/file_processor/app.py:59-108` (`check_file_stability` and its return type)
- Modify: `tests/unit/test_file_stability.py` (update for new return type, add 404 case)

- [ ] **Step 1: Write the failing test for 404 handling**

Append this test to `tests/unit/test_file_stability.py` inside the existing `TestCheckFileStability` class:

```python
    def test_head_returns_404_marks_vanished(self, mock_s3_client: Any, mock_logger: Any) -> None:
        """HEAD returns 404 (not 'NoSuchKey') when a prior delivery already moved the file."""
        from botocore.exceptions import ClientError

        from functions.file_processor.app import check_file_stability

        # HeadObject returns Code="404", Message="Not Found" — distinct from
        # GetObject's NoSuchKey. Real S3 returns this on a duplicate event for
        # an already-moved key.
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
        """Defensive coverage for older boto3 paths that surface NoSuchKey on HEAD."""
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

Also update **every existing** `is_stable, size = check_file_stability(...)` tuple unpack in this file to use the new `StabilityResult` attribute form. Search and replace:

```bash
grep -n "is_stable, size = check_file_stability\|is_stable, _ = check_file_stability\|, size = check_file_stability" tests/unit/test_file_stability.py
```

For each match, replace tuple-unpack with attribute access:

```python
# Before:
is_stable, size = check_file_stability("test-bucket", "test-key")

# After:
result = check_file_stability("test-bucket", "test-key")
is_stable, size = result.stable, result.size
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/unit/test_file_stability.py -v
```

Expected: All three new tests FAIL with `AttributeError: 'tuple' object has no attribute 'stable'` (or similar). Updated existing tests should pass after the unpack rewrite is in place once `StabilityResult` exists.

- [ ] **Step 3: Implement `StabilityResult` and refactor `check_file_stability`**

Replace lines 1-108 of `src/functions/file_processor/app.py` (everything from the module docstring through the end of `check_file_stability`) with:

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

- [ ] **Step 4: Run tests to verify all pass**

```bash
uv run pytest tests/unit/test_file_stability.py -v
```

Expected: All tests PASS, including the three new ones.

- [ ] **Step 5: No commit yet — Task 2 finishes the lambda_handler caller side, both commit together.**

---

## Task 2: Update `lambda_handler` to skip on `vanished` and emit metric (TDD)

**Files:**
- Modify: `src/functions/file_processor/app.py:136-192` (`lambda_handler`)
- Modify: `tests/unit/test_lambda_handler.py` (new test)

- [ ] **Step 1: Write the failing test**

Find an existing test class in `tests/unit/test_lambda_handler.py` for the stability check path. Append:

```python
    def test_vanished_file_skipped_silently_with_metric(
        self, mock_lambda_context: Any
    ) -> None:
        """When stability check returns vanished=True, handler logs + emits
        S3DuplicateEvent metric and does NOT requeue or raise MaxRetriesExceeded.
        """
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
            response = lambda_handler(event, mock_lambda_context)

        assert response["statusCode"] == 200
        assert response["skipped"] == 0  # vanished is not "skipped" — it's "duplicate"
        assert response["requeued"] == 0
        mock_stab.assert_called_once()
        mock_requeue.assert_not_called()
        mock_ingest.assert_not_called()
        # Metric: S3DuplicateEvent +1, NOT MaxRetriesExceeded.
        metric_names = [c.kwargs["name"] for c in mock_metrics.add_metric.call_args_list]
        assert "S3DuplicateEvent" in metric_names
        assert "MaxRetriesExceeded" not in metric_names
```

Make sure `import json` and `from unittest.mock import patch` are imported at the top of the file.

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/unit/test_lambda_handler.py::TestLambdaHandlerWithStabilityCheck::test_vanished_file_skipped_silently_with_metric -v
```

Expected: FAIL with `AttributeError` (no `StabilityResult` import yet failing the test setup) OR with the assertion that `S3DuplicateEvent` is not in the metric list.

If the test's class name differs, adjust the path or place the test at module scope.

- [ ] **Step 3: Update `lambda_handler` to handle vanished**

Replace lines 136-192 of `src/functions/file_processor/app.py` (the entire `lambda_handler` function) with:

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

- [ ] **Step 4: Run the full test_lambda_handler suite + test_file_stability suite**

```bash
uv run pytest tests/unit/test_lambda_handler.py tests/unit/test_file_stability.py -v
```

Expected: All pass.

- [ ] **Step 5: Run the full repo test suite as a regression check**

```bash
uv run pytest -q
```

Expected: PASS (~770 tests). Watch for any tests that destructured `check_file_stability` return value as a tuple — fix them with the same attribute-access pattern from Task 1 Step 1.

- [ ] **Step 6: Commit (Commit 1 of 3)**

```bash
git add src/functions/file_processor/app.py tests/unit/test_file_stability.py tests/unit/test_lambda_handler.py
git commit -m "fix: treat HEAD 404 as duplicate S3 event + revert REQUEUE_DELAY 90->60

Root cause for the FileProcessor-MaxRetriesExceeded alarm noise was not slow
uploads but HeadObject returning Code='404' / Message='Not Found' on duplicate
S3 events for already-moved keys (NoSuchKey check in check_file_stability
never fired because that's GetObject semantics). The 90s REQUEUE_DELAY bump
in f8282f4 treated the wrong symptom.

Refactor check_file_stability to return a StabilityResult dataclass with a
vanished flag, and have lambda_handler skip vanished records silently with
a new S3DuplicateEvent metric. Reverts REQUEUE_DELAY_SECONDS 90 -> 60."
```

---

## Task 3: Add `FileProcessor-DuplicateEventSpike` alarm in Terraform

**Files:**
- Modify: `terraform/monitoring.tf` (append a new alarm resource)

- [ ] **Step 1: Append the alarm resource**

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

- [ ] **Step 2: Validate the change with `terraform plan`**

```bash
cd terraform && terraform plan -out=/tmp/duplicate-event-spike.tfplan
```

Expected:
```
Plan: 1 to add, 0 to change, 0 to destroy.
```

The single addition should be `aws_cloudwatch_metric_alarm.file_processor_duplicate_event_spike`. If anything else appears, stop and investigate.

- [ ] **Step 3: Commit (folds into Commit 1)**

```bash
cd ..
git add terraform/monitoring.tf
git commit --amend --no-edit
```

This keeps the Terraform change in the same logical commit as the Lambda code that emits the metric.

**Do not run `terraform apply` yet.** Apply happens in Task 8 (deploy) so the alarm and Lambda code go live together.

---

## Task 4: Create Synergy WA parser (TDD)

**Files:**
- Create: `src/shared/parsers/synergy/__init__.py` (empty)
- Create: `src/shared/parsers/synergy/wa_meter_data.py`
- Create: `tests/unit/parsers/synergy/__init__.py` (empty)
- Create: `tests/unit/parsers/synergy/test_wa_meter_data.py`
- Move: `tests/unit/fixtures/optima_interval/wa_no_data_found.csv` →
  `tests/unit/fixtures/synergy/wa_no_data_found.csv`

- [ ] **Step 1: Move the fixture**

```bash
mkdir -p tests/unit/fixtures/synergy
git mv tests/unit/fixtures/optima_interval/wa_no_data_found.csv tests/unit/fixtures/synergy/wa_no_data_found.csv
```

Verify the file is byte-identical (should be 56 bytes):

```bash
wc -c tests/unit/fixtures/synergy/wa_no_data_found.csv
```

Expected: `56 tests/unit/fixtures/synergy/wa_no_data_found.csv`.

- [ ] **Step 2: Create empty package markers**

```bash
touch src/shared/parsers/synergy/__init__.py
mkdir -p tests/unit/parsers/synergy
touch tests/unit/parsers/synergy/__init__.py
```

- [ ] **Step 3: Write the failing tests**

Create `tests/unit/parsers/synergy/test_wa_meter_data.py`:

```python
"""Tests for the Synergy WA meter data parser.

Synergy WA's portal drops files into newTBP/ with names like
`Meter_Data_WA (AU)_Electricity_*.csv`. The current production payload is
always a 56-byte sentinel CSV (3 columns, 3 rows) indicating "no data found"
for the queried period. Real data files have not yet been observed — if/when
they appear, the strict header check below will fall through to
NotRelevantParser, surfacing the new format in newIrrevFiles/ for follow-up.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from shared.parsers import NotRelevantParser, ParserOutcome
from shared.parsers.synergy.wa_meter_data import synergy_wa_meter_data_parser

FIXTURE_DIR = Path(__file__).parent.parent.parent / "fixtures" / "synergy"


class TestSynergyWaMeterDataParser:
    def test_sentinel_fixture_returns_processed_empty(self) -> None:
        outcome = synergy_wa_meter_data_parser(str(FIXTURE_DIR / "wa_no_data_found.csv"))

        assert isinstance(outcome, ParserOutcome)
        assert outcome.status == "processed_empty"
        assert outcome.reason == "no_data_available"

    def test_rejects_files_without_synergy_wa_prefix(self, tmp_path: Path) -> None:
        f = tmp_path / "interval_au_single_day.csv"
        f.write_text("Date,Start Time,Identifier\n")

        with pytest.raises(NotRelevantParser, match="Not a Synergy WA"):
            synergy_wa_meter_data_parser(str(f))

    def test_falls_through_on_header_drift(self, tmp_path: Path) -> None:
        """A future format with a different header must NOT be silently consumed.

        Falls through to NotRelevantParser so the dispatcher routes the file to
        newIrrevFiles/, where its accumulation prompts us to add real parsing.
        """
        f = tmp_path / "Meter_Data_WA (AU)_Electricity_1778999999_2026051300000000.csv"
        f.write_text("Date,NMI,Usage\n2026-05-13,12345,1.23\n")

        with pytest.raises(NotRelevantParser, match="drifted"):
            synergy_wa_meter_data_parser(str(f))

    def test_filename_with_dot_and_paren_chars_accepted(self) -> None:
        """The literal filename pattern includes parens and a dot — the prefix
        matcher must handle those without surprises."""
        # We use the real fixture because its filename was renamed during the
        # move; build a temp file with the production filename pattern.
        outcome = synergy_wa_meter_data_parser(str(FIXTURE_DIR / "wa_no_data_found.csv"))
        # The fixture filename does NOT match the production prefix — so this
        # path proves we are NOT prefix-matching by accident on filenames
        # without it. (We rely on the dispatcher seeing the real filename.)
        # This is the same call as the first test; we keep it to assert the
        # outcome is reproducible from path string alone, not from file metadata.
        assert outcome.status == "processed_empty"
```

Wait — the last test as written is meaningless. Replace it with a true production-name fixture call:

```python
    def test_production_filename_pattern_matches(self, tmp_path: Path) -> None:
        """Use the actual production filename pattern with the sentinel body."""
        prod_path = (
            tmp_path
            / "Meter_Data_WA (AU)_Electricity_1778517074_2026051202315309.csv"
        )
        prod_path.write_bytes(
            (FIXTURE_DIR / "wa_no_data_found.csv").read_bytes()
        )

        outcome = synergy_wa_meter_data_parser(str(prod_path))

        assert outcome.status == "processed_empty"
        assert outcome.reason == "no_data_available"
```

Delete the `test_filename_with_dot_and_paren_chars_accepted` placeholder above and replace with this version.

- [ ] **Step 4: Run tests to verify they fail**

```bash
uv run pytest tests/unit/parsers/synergy/ -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'shared.parsers.synergy.wa_meter_data'`.

- [ ] **Step 5: Implement the parser**

Create `src/shared/parsers/synergy/wa_meter_data.py`:

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

    # Strict signature: any drift falls through to NotRelevantParser (and
    # thus to newIrrevFiles/) rather than ParserError. See module docstring.
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

- [ ] **Step 6: Run tests to verify they pass**

```bash
uv run pytest tests/unit/parsers/synergy/ -v
```

Expected: All four tests PASS.

- [ ] **Step 7: No commit yet — Task 5 finishes the refactor, both commit together.**

---

## Task 5: Register Synergy parser, remove WA detection from `interval.py`

**Files:**
- Modify: `src/shared/parsers/dispatcher.py` (register at position 0)
- Modify: `src/shared/parsers/optima/interval.py` (remove WA logic)
- Modify: `tests/unit/parsers/optima/test_interval.py` (remove WA test)

- [ ] **Step 1: Register parser in dispatcher**

Edit `src/shared/parsers/dispatcher.py`. Add an import after the existing parser imports (alphabetical-ish, but synergy comes after racv, so add at the end of the import block):

```python
from shared.parsers.synergy.wa_meter_data import synergy_wa_meter_data_parser
```

Then replace the `PARSERS = [...]` list (currently lines 25-36) with:

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

- [ ] **Step 2: Remove WA detection from `interval.py`**

Edit `src/shared/parsers/optima/interval.py`. Replace `_is_no_data_sentinel` (lines 29-50) with the AU/NZ-only version:

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

And restore the strict cheap relevance gate (lines 65-67):

```python
    # All three column markers must appear in the header row.
    if not all(token in first_line for token in ("Date", "Start Time", "Identifier")):
        raise NotRelevantParser("Not an Optima interval CSV")
```

(Removes the `is_wa_sentinel_header` variable and the disjunction with `Unnamed: 0`/`NMI`/`Unnamed: 2`.)

- [ ] **Step 3: Remove the orphaned WA test from `test_interval.py`**

```bash
grep -n "test_wa_no_data_found_fixture_returns_processed_empty" tests/unit/parsers/optima/test_interval.py
```

Open the file at that line and delete the entire test method (typically 8-12 lines including the docstring and any decorators).

If the test uses the fixture path `FIXTURE_DIR / "wa_no_data_found.csv"`, that fixture no longer exists at `optima_interval/`. Verify no other test in this file references it:

```bash
grep -n "wa_no_data_found" tests/unit/parsers/optima/test_interval.py
```

Expected: no matches after deletion.

- [ ] **Step 4: Run the relevant test suites**

```bash
uv run pytest tests/unit/parsers/synergy/ tests/unit/parsers/optima/test_interval.py tests/unit/test_non_nem_parsers.py -v
```

Expected: All pass. The Synergy WA fixture should now match via the new parser, not the interval parser.

- [ ] **Step 5: Run the full repo test suite**

```bash
uv run pytest -q
```

Expected: PASS. Watch for any test that still references the old fixture path `tests/unit/fixtures/optima_interval/wa_no_data_found.csv` and update it to the new location if found.

- [ ] **Step 6: Commit (Commit 2 of 3)**

```bash
git add src/shared/parsers/synergy/ \
        tests/unit/parsers/synergy/ \
        tests/unit/fixtures/synergy/ \
        src/shared/parsers/dispatcher.py \
        src/shared/parsers/optima/interval.py \
        tests/unit/parsers/optima/test_interval.py
# git mv from Task 4 Step 1 also stages the deletion at tests/unit/fixtures/optima_interval/
git status  # verify nothing unintended is staged
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

## Task 6: Fix `idempotent_cache_hit` log (TDD; branches on Task 0 outcome)

This task has two implementation paths depending on Task 0's investigation result. Both paths share the test rewrite — write the test first, then choose the implementation.

### Step A (both paths): Rewrite the test using `capsys` + JSON parsing

**Files:**
- Rewrite: `tests/unit/test_persistence_cache_hit_log.py`

- [ ] **Step 1: Replace the existing test file in full**

Overwrite `tests/unit/test_persistence_cache_hit_log.py` with:

```python
"""Tests for idempotent_cache_hit structured log emission.

The previous version of this test passed for the wrong reasons: it captured
LogRecord attributes via caplog (pre-formatter, so the production bug — that
the JSON formatter was being bypassed — was invisible) AND it passed a dict
where production passes a DataRecord (so the isinstance(data, dict) branch
masked the real type-confusion bug).

This rewrite asserts against stdout JSON, which is what CloudWatch sees, and
constructs a real DataRecord so the test exercises the same code path as
production.
"""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest
from aws_lambda_powertools.utilities.idempotency.exceptions import (
    IdempotencyItemAlreadyExistsError,
)
from aws_lambda_powertools.utilities.idempotency.persistence.base import (
    DataRecord,
)
from aws_lambda_powertools.utilities.idempotency.persistence.dynamodb import (
    DynamoDBPersistenceLayer,
)
from moto import mock_aws

from functions.file_processor.persistence import (
    InstrumentedDynamoDBPersistenceLayer,
)


@pytest.fixture
def idempotency_table():
    import boto3

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
    """Build a DataRecord that mimics what Powertools constructs internally."""
    return DataRecord(
        idempotency_key=idempotency_key,
        status="INPROGRESS",
        payload_hash="",
        response_data=json.dumps(payload),
    )


class TestIdempotentCacheHitLog:
    def test_emits_structured_json_with_source_fields(
        self, capsys, idempotency_table
    ) -> None:
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
        lines = [
            json.loads(line)
            for line in out.splitlines()
            if line.strip().startswith("{")
        ]
        cache_hits = [
            line for line in lines if line.get("message") == "idempotent_cache_hit"
        ]
        assert len(cache_hits) == 1, f"Expected 1 cache-hit JSON line; got {len(cache_hits)} in:\n{out}"
        log = cache_hits[0]
        assert log["source_bucket"] == "sbm-file-ingester"
        assert log["source_key"] == "newTBP/foo.csv"
        assert log["idempotency_key"] == "abc123"

    def test_no_log_on_successful_save(self, capsys, idempotency_table) -> None:
        layer = InstrumentedDynamoDBPersistenceLayer(
            table_name="sbm-ingester-idempotency",
        )
        data = _make_data_record(idempotency_key="def456", payload={"bucket": "b", "key": "k"})

        with patch.object(DynamoDBPersistenceLayer, "save_inprogress", return_value=None):
            layer.save_inprogress(data=data)

        out = capsys.readouterr().out
        lines = [
            json.loads(line)
            for line in out.splitlines()
            if line.strip().startswith("{")
        ]
        cache_hits = [
            line for line in lines if line.get("message") == "idempotent_cache_hit"
        ]
        assert cache_hits == []
```

- [ ] **Step 2: Run the test to verify it fails against current code**

```bash
uv run pytest tests/unit/test_persistence_cache_hit_log.py -v
```

Expected: `test_emits_structured_json_with_source_fields` FAILS because:
- The current code uses `service="instrumented-persistence", child=True` with no parent matching that service → stdlib formatter → no JSON line on stdout, OR
- The current code's `payload = data if isinstance(data, dict) else {}` falls through to `{}` for a `DataRecord` → fields are `None` in any JSON that does emit

This is the proof that the existing test was a false positive.

### Step B: Now choose implementation path based on Task 0

#### Path 6B (default — fix the subclass)

- [ ] **Step 3 (6B): Replace `persistence.py`**

Overwrite `src/functions/file_processor/persistence.py` with:

```python
"""Persistence layer subclass that logs idempotency cache hits.

Powertools does not expose a native cache-hit hook in the currently pinned
version (aws-lambda-powertools >= 3.24.0). The cache-hit path materialises
as IdempotencyItemAlreadyExistsError raised by save_inprogress when the
record already exists. We catch it here, emit a structured log with file
identification, and re-raise so Powertools handles the cached response
normally.

The Logger MUST share its service string with the parent Logger in
``functions.file_processor.app`` so that ``child=True`` resolves to a
process-wide Powertools logger with the JSON formatter attached. With a
mismatched service string, child=True falls back to a stdlib logger that
drops ``extra=`` kwargs from JSON output entirely.

Field source:
  - ``DataRecord.idempotency_key`` — the hash key.
  - ``DataRecord.get_payload()`` — the serialized payload dict (containing
    the SourceFile fields ``bucket`` and ``key``).
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

#### Path 6A (only if Task 0 found a native mechanism)

- [ ] **Step 3 (6A): Apply the native mechanism**

Replace `persistence.py` and pipeline decorator usage per Task 0's recorded findings. Concrete changes will be:
1. Delete `src/functions/file_processor/persistence.py`.
2. Edit `src/functions/file_processor/pipeline.py` to remove the import and constructor reference to `InstrumentedDynamoDBPersistenceLayer`, and add the native configuration (e.g. `IdempotencyConfig(log_event=True)`) so cache hits log with file identification.
3. Verify the log line still carries `source_bucket`, `source_key`, `idempotency_key` (test from Step 1 enforces this).

Exact code is determined by what Task 0 finds — if Powertools provides a flag/hook with full field coverage, this path is preferred and the subclass is deleted.

### Step C (both paths): Verify

- [ ] **Step 4: Run the rewritten test**

```bash
uv run pytest tests/unit/test_persistence_cache_hit_log.py -v
```

Expected: Both tests PASS.

- [ ] **Step 5: Run the full repo test suite**

```bash
uv run pytest -q
```

Expected: PASS (~770 tests).

- [ ] **Step 6: Commit (Commit 3 of 3)**

If Path 6B:

```bash
git add src/functions/file_processor/persistence.py tests/unit/test_persistence_cache_hit_log.py
git commit -m "fix: idempotent_cache_hit log emits structured fields

Two compounding bugs in persistence.py:

1. Logger(service='instrumented-persistence', child=True) had no parent
   matching that service string, so Powertools fell back to a stdlib
   logger that dropped extra= kwargs from JSON output. Aligned service
   to 'file-processor' (the parent Logger in app.py and pipeline.py) so
   child=True resolves to the Powertools logger with its JSON formatter.

2. The save_inprogress override used isinstance(data, dict) — but
   Powertools passes a DataRecord, not a dict. payload always fell
   through to {} and fields were always None. Replaced with
   data.get_payload() and data.idempotency_key (the documented
   DataRecord public API).

Existing test_persistence_cache_hit_log.py passed for the wrong reasons:
caplog captured LogRecord attributes pre-formatter (so the formatter bug
was invisible) AND the test passed a dict (so the type-confusion bug
was masked). Rewritten to assert against capsys stdout JSON with a real
DataRecord, which now exercises the same code path as production."
```

If Path 6A:

```bash
git add -A
git commit -m "fix: replace InstrumentedDynamoDBPersistenceLayer with Powertools native cache-hit logging

[brief description of the native mechanism chosen, why it covers all
required fields, and what was deleted as a result]"
```

---

## Task 7: Recover 3 WA sentinel files from `newParseErr/`

**Files:**
- S3 bucket: `s3://sbm-file-ingester/`

These three files were misclassified as parse errors by the pre-fix `interval.py`. After Commit 2 (the dispatcher routes WA files to the new parser → `processed_empty`), retroactive recovery is a simple S3 move from `newParseErr/` to `newIrrevFiles/`.

**Run this only after Task 8's deploy completes** — otherwise the recovered files might race back into the SQS queue and re-hit the old code if the deploy is mid-flight.

- [ ] **Step 1: List the 3 active WA files**

```bash
aws s3 ls 's3://sbm-file-ingester/newParseErr/' --region ap-southeast-2 \
  | grep 'Meter_Data_WA' \
  | grep -v archived/
```

Expected: 3 files matching `Meter_Data_WA (AU)_Electricity_*` (56 bytes each), e.g.:
```
2026-05-11 10:31:58         56 Meter_Data_WA (AU)_Electricity_1778459473_2026051110313572.csv
2026-05-11 18:32:11         56 Meter_Data_WA (AU)_Electricity_1778488273_2026051118315106.csv
2026-05-12 02:32:09         56 Meter_Data_WA (AU)_Electricity_1778517074_2026051202315309.csv
```

If the list contains more or fewer files than expected, stop and reconcile before moving.

- [ ] **Step 2: Move each file to `newIrrevFiles/`**

For each file from Step 1, run:

```bash
aws s3 mv \
  "s3://sbm-file-ingester/newParseErr/Meter_Data_WA (AU)_Electricity_<epoch>_<ts>.csv" \
  "s3://sbm-file-ingester/newIrrevFiles/Meter_Data_WA (AU)_Electricity_<epoch>_<ts>.csv" \
  --region ap-southeast-2
```

Substitute `<epoch>_<ts>` with each filename from Step 1.

Expected output per move:
```
move: s3://sbm-file-ingester/newParseErr/... to s3://sbm-file-ingester/newIrrevFiles/...
```

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

Expected: at least `3` (could be more if new WA files have already been routed there by the new parser since deploy).

No commit — Task 7 is operational, not code.

---

## Task 8: Deploy

- [ ] **Step 1: Final pre-merge sanity**

```bash
git status                # working tree clean
git log --oneline main..HEAD  # should show exactly 3 new commits on top of f8282f4 + the spec commit(s)
uv run pytest -q          # ~770 tests pass
uv run ruff check .       # lint clean
uv run ruff format --check .  # format clean
```

If any of these fail, fix before continuing.

- [ ] **Step 2: Terraform apply (alarm only)**

```bash
cd terraform
terraform plan -out=/tmp/post-deploy-tuning.tfplan
```

Expected: `Plan: 1 to add, 0 to change, 0 to destroy.` (the new `FileProcessor-DuplicateEventSpike` alarm).

```bash
terraform apply /tmp/post-deploy-tuning.tfplan
cd ..
```

Expected: Apply complete. 1 resource added. The alarm starts in `INSUFFICIENT_DATA` state — that's expected (no data yet for the new metric).

- [ ] **Step 3: Merge branch to `main`**

```bash
git checkout main
git pull
git merge --no-ff fix/post-deploy-tuning
```

Expected: Fast-forward not possible (because `--no-ff`); merge commit created. Resolve any conflicts (should not be any if main hasn't moved).

- [ ] **Step 4: Push to origin — triggers GitHub Actions auto-deploy**

```bash
git push origin main
```

Expected: GitHub Actions starts a workflow that:
- Builds Lambda zips
- Uploads to `gega-code-deployment-bucket`
- Runs `aws lambda update-function-code` for `sbm-files-ingester`

Watch the run:
```bash
gh run watch
```

Expected: Workflow succeeds. Lambda code version updated.

- [ ] **Step 5: Smoke check the deployed Lambda**

```bash
# Confirm the new code is live by checking a recent Lambda log group for
# the new s3_duplicate_event log line (will only appear when an actual
# duplicate fires — but the code path is exercised on every cold start).
aws logs filter-log-events \
  --log-group-name /aws/lambda/sbm-files-ingester \
  --start-time $(($(date +%s) * 1000 - 600000)) \
  --filter-pattern '"INIT_REPORT"' \
  --region ap-southeast-2 \
  --max-items 1 \
  --query 'events[*].message' --output text
```

Expected: A recent `INIT_REPORT Init Duration` line confirming a cold start happened on the new code.

- [ ] **Step 6: Run Task 7 (recovery) now that the deploy is live.**

- [ ] **Step 7: Watch for 12 hours**

After 12 hours (next-day check), verify:

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

# 3) idempotent_cache_hit log lines should now contain source_bucket / source_key.
aws logs filter-log-events \
  --log-group-name /aws/lambda/sbm-files-ingester \
  --start-time $(($(date +%s) * 1000 - 43200000)) \
  --filter-pattern '"idempotent_cache_hit"' \
  --region ap-southeast-2 \
  --max-items 5 \
  --query 'events[*].message' --output text
```

Expected:
1. `MaxRetriesExceeded` Sum ≤ a small number (single digits over 12h, ideally 0)
2. `S3DuplicateEvent` Sum > 0, < 50% of `Invocations`
3. `idempotent_cache_hit` log entries are JSON with top-level `source_bucket` and `source_key` keys

- [ ] **Step 8: Cleanup the local branch (optional)**

```bash
git branch -d fix/post-deploy-tuning
```

---

## Rollback

If post-deploy monitoring reveals a regression:

- **Code rollback:** revert the Lambda to the previous code version:
  ```bash
  # Find the prior zip key:
  aws s3 ls s3://gega-code-deployment-bucket/sbm-files-ingester/ --region ap-southeast-2 | sort
  # Apply:
  aws lambda update-function-code \
    --function-name sbm-files-ingester \
    --s3-bucket gega-code-deployment-bucket \
    --s3-key sbm-files-ingester/<previous-zip-key>.zip \
    --region ap-southeast-2
  ```
- **Terraform rollback:** if only the alarm misbehaves:
  ```bash
  cd terraform
  terraform destroy -target=aws_cloudwatch_metric_alarm.file_processor_duplicate_event_spike
  ```
- **No DynamoDB cache invalidation needed** — these fixes do not change the idempotency cache key shape or TTL.

---

## Self-Review Checklist (run AFTER plan complete)

- [x] Spec coverage: Tasks 1+2+3 cover Issue 1 (404 fix + metric + alarm + REQUEUE_DELAY revert). Tasks 4+5 cover Issue 2 (Synergy parser + interval.py cleanup + fixture move + orphan-test removal). Task 6 covers Issue 3 (cache-hit log with both implementation paths). Task 7 covers recovery. Task 8 covers deploy + monitoring.
- [x] Placeholder scan: No "TBD", "TODO", "implement later", "similar to" — all code blocks fully written.
- [x] Type consistency: `StabilityResult` referenced consistently in Tasks 1 and 2. `DataRecord.idempotency_key` and `data.get_payload()` consistent in Task 6 spec and implementation. `synergy_wa_meter_data_parser` name consistent across Tasks 4, 5.
- [x] Commit structure matches spec: 3 commits (one per fix) on `fix/post-deploy-tuning`, plus the existing spec commits.
- [x] Forward-only on `f8282f4`: REQUEUE_DELAY 90→60 revert and WA interval.py removal happen as normal commits (not `git reset`).
