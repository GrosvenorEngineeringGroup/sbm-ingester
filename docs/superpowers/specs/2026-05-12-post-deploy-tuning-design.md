# Post-Deploy Tuning — Per-File Ingest Refactor

**Date:** 2026-05-12
**Status:** Approved (forward-only on top of `f8282f4`)
**Branch:** `fix/post-deploy-tuning`

## Background

The per-file ingest refactor (`feat/per-file-ingest-refactor`, 17 tasks, 22 commits)
shipped to production and ran cleanly for 14 hours overnight: 0 Lambda errors, 0 DLQ
messages, healthy throughput. Post-deploy CloudWatch review surfaced three
non-blocking issues:

1. **`FileProcessor-MaxRetriesExceeded` alarm fired 35× overnight.** All affected
   files eventually processed (S3 re-emitted them); no data loss. Alarm is noisy.
2. **3 WA "No data found" sentinel files/night land in `newParseErr/`.** They should
   be classified as `processed_empty`, not parse failures.
3. **`idempotent_cache_hit` log line is missing structured fields** (`source_bucket`,
   `source_key`, `idempotency_key`). The line emits as plain string instead of JSON
   with fields.

A local commit `f8282f4` attempted to address (1) by bumping `REQUEUE_DELAY_SECONDS`
60 → 90 and (2) via inline WA detection in `interval.py`. **Root-cause investigation
found (1) was misdiagnosed — the delay bump treats the wrong symptom.** This spec
captures the redesigned fixes and ships them as forward commits on top of `f8282f4`
(no rebase / no force-push).

## Investigation Findings

### Issue 1: MaxRetriesExceeded — root cause is HEAD 404, not slow uploads

24h CloudWatch Logs analysis:
- 53 unique S3 keys hit max retries (65 alarm events total)
- All affected files stabilized in **<12 seconds** on first delivery
- All ETags are single-part PUTs (no multipart in-flight)
- No single producer dominates: Optima, NEM12, RACV, manual test files all hit it

**Actual mechanism:**
- S3 `ObjectCreated` events are at-least-once — duplicates are normal
- First delivery processes the file, moves it `newTBP/` → `newP/` (or `newIrrevFiles/`,
  or `newParseErr/`)
- Second delivery arrives ~5s later, HEADs the now-deleted key, gets `404 Not Found`
- **Compounding bug** in `check_file_stability`: catches `Code == "NoSuchKey"`, but
  `HeadObject` returns `Code="404"` / `Message="Not Found"` (`NoSuchKey` is
  `GetObject` semantics only). The 404 branch never fires.
- Falls through to generic error → returns `(False, 0)` → requeue → max retries → alarm

**No retry-count or delay value fixes a 404.** The 90s bump in `f8282f4` adds no
value (and is not harmful — just a no-op).

### Issue 2: WA sentinel — files are not from our exporter

The `Meter_Data_WA (AU)_Electricity_*` files are **not produced by
`optima-interval-exporter`**. They are an external drop from Synergy WA's portal
(visible in S3 going back to 2025-W31, cadence ~3/day, 56-byte 3-column sentinel
CSVs). Folding their recognition into `interval.py` couples two unrelated producers
(BidEnergy + Synergy) into one parser.

### Issue 3: cache-hit log — two bugs

`src/functions/file_processor/persistence.py` has two compounding bugs:

1. **Wrong type assumption.** `isinstance(data, dict)` check falls through because
   Powertools passes a `DataRecord` instance, not a `dict`. `payload = {}`, so all
   fields are `None` before serialization.
2. **Mismatched child logger service.** `Logger(service="instrumented-persistence",
   child=True)` has no matching parent (parents elsewhere use `"file-processor"`).
   Powertools falls back to stdlib `logging.Logger`, which silently drops the
   `extra=` dict.

## Design

### Fix 1: HEAD 404 → vanished, skip silently

**File:** `src/functions/file_processor/app.py`

Change `check_file_stability` to return a structured result instead of a 2-tuple:

```python
@dataclass(frozen=True, slots=True)
class StabilityResult:
    stable: bool
    size: int
    vanished: bool  # HEAD returned 404 → key already processed by earlier delivery

# In except ClientError:
code = e.response["Error"]["Code"]
status = e.response["ResponseMetadata"]["HTTPStatusCode"]
if code in ("NoSuchKey", "404") or status == 404:
    return StabilityResult(stable=False, size=0, vanished=True)
raise  # other errors propagate
```

In `lambda_handler`, when `vanished=True`:
- Emit `info`-level structured log `s3_object_vanished` (with `source_bucket`,
  `source_key`, `retry_count`)
- Emit CloudWatch metric `S3DuplicateEvent` (Count, 1)
- Delete the SQS message (no requeue)
- **Do not** emit `MaxRetriesExceeded` metric — this is the expected path now

**Also revert `REQUEUE_DELAY_SECONDS` 90 → 60** (the `f8282f4` change was based on
the wrong root cause). Real slow uploads (multipart) are not part of the observed
failure population and are handled correctly by the existing retry loop.

### Fix 2: Separate Synergy WA parser

**New files:**
- `src/shared/parsers/synergy/__init__.py`
- `src/shared/parsers/synergy/wa_meter_data.py`

```python
def synergy_wa_meter_data_parser(file_name: str) -> ParserOutcome:
    path = Path(file_name)
    if not path.name.startswith("Meter_Data_WA (AU)_Electricity_"):
        raise NotRelevantParser("Not a Synergy WA meter data file")

    with path.open() as f:
        first_line = f.readline().strip()

    # Strict sentinel signature; any drift falls through to newIrrevFiles/
    if first_line != "Unnamed: 0,NMI,Unnamed: 2":
        raise NotRelevantParser(
            f"Synergy WA file format drifted. First line: {first_line!r}"
        )

    return ParserOutcome(status="processed_empty", reason="no_data_available")
```

**Modify:**
- `src/shared/parsers/dispatcher.py` — register the new parser
- `src/shared/parsers/optima/interval.py` — **remove** the WA detection logic
  added in `f8282f4` (no longer needed; cleaner SRP)

**Why fail-safe (NotRelevantParser) on drift, not fail-loud (ParserError):**
If Synergy starts emitting real data, filename or header will change. Falling
through to `NotRelevantParser` routes files to `newIrrevFiles/` where they
accumulate visibly, prompting us to add real-data parsing logic. This is
strictly safer than `ParserError` (which would re-introduce parse-error noise).

### Fix 3: Cache-hit log — align service + use DataRecord API

**File:** `src/functions/file_processor/persistence.py`

```python
logger = Logger(service="file-processor", child=True)  # match parent service

class InstrumentedDynamoDBPersistenceLayer(DynamoDBPersistenceLayer):
    def save_inprogress(self, data, remaining_time_in_millis=None):
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

**TDD requirement:** before writing the fix, write a failing test that asserts
the structured JSON log contains `source_bucket`, `source_key`, and
`idempotency_key`. This validates the Powertools `DataRecord` API (`get_payload`,
`idempotency_key`) — currently inferred from naming, not verified.

### Recovery: 3 WA files in `newParseErr/`

After deploying fixes 1–3, manually move the 3 WA sentinel files from
`s3://sbm-file-ingester/newParseErr/` to `s3://sbm-file-ingester/newIrrevFiles/`
using `aws s3 mv`. No code change required.

## Out of Scope

- **Reverting `f8282f4`.** Forward-only commits on top. The 90→60 revert and the
  removal of inline WA detection happen in this branch as normal commits.
- **Historical `newParseErr/archived/`.** Hundreds of archived WA sentinels exist
  going back to 2025-W31. They stay archived; only the 3 active files are
  recovered.
- **Reducing `MAX_REQUEUE_RETRIES = 3`.** With 404 handling in place, requeues
  should be rare (only genuine slow files). The constant stays unchanged.

## Testing

| Concern | Test |
|---------|------|
| HEAD 404 returns `vanished=True` | `tests/unit/test_file_stability.py` — mock `head_object` to raise `ClientError` with `Code="404"` |
| `lambda_handler` skips requeue on vanished | `tests/unit/test_lambda_handler.py` — assert no SQS `send_message` call, `S3DuplicateEvent` metric emitted |
| Synergy WA parser matches sentinel | `tests/unit/test_synergy_wa_parser.py` — fixture-based, assert `processed_empty` |
| Synergy WA parser falls through on drift | Same file — header mismatch → `NotRelevantParser` |
| Dispatcher routes WA files to new parser | `tests/unit/test_non_nem_parsers.py` |
| `interval.py` no longer matches WA files | Same — assert `NotRelevantParser` raised |
| Cache-hit log emits structured fields | `tests/unit/test_persistence_cache_hit_log.py` — moto + capture, assert JSON fields present |

All tests follow TDD: failing test first, then implementation.

## Rollout

1. Implement fixes in three commits on `fix/post-deploy-tuning`:
   - `fix: treat HEAD 404 as vanished + S3DuplicateEvent metric (revert REQUEUE_DELAY)`
   - `refactor: extract Synergy WA parser, remove inline detection from interval.py`
   - `fix: cache-hit log service alignment + DataRecord payload access`
2. Run full test suite — must remain green (~770 tests passing).
3. Merge `fix/post-deploy-tuning` → `main`.
4. Push to `origin/main` — GitHub Actions auto-deploys.
5. Manual S3 mv of 3 WA files in `newParseErr/`.
6. Watch CloudWatch for 12h: `MaxRetriesExceeded` should drop to ~0,
   `S3DuplicateEvent` should appear at expected rate (~30/day), `idempotent_cache_hit`
   log should contain `source_bucket`/`source_key`.

## Risks

- **HEAD 404 fix masks genuine bugs.** If our move-after-process logic breaks (file
  not moved despite outcome cached), every duplicate event becomes a silent skip.
  Mitigation: `S3DuplicateEvent` metric trend monitoring; a spike to >50% of
  invocations should trigger investigation.
- **Synergy WA parser dispatcher order.** If a future producer happens to write
  `Meter_Data_WA (AU)_Electricity_*` files with a different format, they'd match
  the prefix and fall through to `NotRelevantParser` (safe). Risk is low.
- **Powertools `DataRecord` API.** Public-but-undocumented. Mitigated by TDD —
  failing test before code change.
