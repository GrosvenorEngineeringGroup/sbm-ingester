# Task 0 Decision

**Powertools version:** 3.24.0 (path: `.venv/lib/python3.13/site-packages/aws_lambda_powertools`)

## DataRecord.get_payload() at save_inprogress time

**The method does NOT exist on `DataRecord` at all.** Verified by inspection of
`aws_lambda_powertools.utilities.idempotency.persistence.base.DataRecord`:

- `DataRecord.__init__` signature:
  `(self, idempotency_key: str, status: str = '', expiry_timestamp: int | None = None, in_progress_expiry_timestamp: int | None = None, response_data: str = '', payload_hash: str = '', sort_key: str | None = None) -> None`
- Public attrs/methods: `['get_expiration_datetime', 'is_expired', 'response_json_as_dict', 'status']`
- `hasattr(DataRecord, 'get_payload') == False`

The input payload is **not stored on `DataRecord`** — only its `payload_hash`.
At save_inprogress time, the in-progress record carries `idempotency_key`,
`status="INPROGRESS"`, `expiry_timestamp`, `in_progress_expiry_timestamp`, and
`payload_hash`. `response_data` is empty (not yet computed). At cache-hit time
(when a prior completed record is loaded), `response_data` carries the
**previous run's serialized output** (`ParserOutcome` for our case) — not the
input payload either.

## Reachable from DataRecord at save_inprogress

`idempotency_key`, `payload_hash`, `status`. **NOT** the original `bucket` /
`key`.

## Native cache-hit hook

Partially yes — `IdempotencyConfig(response_hook=...)`
(`aws_lambda_powertools.utilities.idempotency.hook.IdempotentHookFunction`)
exists and fires on cache hit at
`base.py:276-278`:

```python
if self.config.response_hook:
    logger.debug("Response hook configured, invoking function")
    return self.config.response_hook(serialized_response, data_record)
```

Signature: `(response: Any, idempotent_data: DataRecord) -> Any`.

**However, this hook is not suitable for our requirement** of logging
`source_bucket` / `source_key`:

1. `serialized_response` is the **previous run's `ParserOutcome`** (a
   `dict[str, Any]` after Powertools' JSON deserialization), which has no
   `bucket` / `key` fields (`ParserOutcome` carries status / counts /
   dataframes / reasons, not the source file reference).
2. `data_record` is a `DataRecord` — as shown above, it does not carry the
   input payload at all.
3. The hook only fires when a **completed** prior record is found and its
   `response_data` is non-empty. It does NOT fire when the prior delivery is
   still INPROGRESS (which is also a legitimate "cache hit" cause).

`@idempotent_function` itself has no `log_event`, `on_cache_hit`, or similar
parameter. Inspected signature:
`(function, *, data_keyword_argument, persistence_store, config=None,
output_serializer=None, key_prefix=None, **kwargs)`. `IdempotencyConfig` has
no `log_*` or `event_*` flag.

## Critical correction to the plan's premise

The plan's Form B/C templates type the `save_inprogress` argument as
`data: DataRecord` and call `data.get_payload()`. **Both are incorrect for the
pinned Powertools version.** The actual base-class signature is:

```python
def save_inprogress(self, data: dict[str, Any], remaining_time_in_millis: int | None = None) -> None:
```

`data` is the **input payload as a `dict`**, produced by `_prepare_data` in
`aws_lambda_powertools/utilities/idempotency/base.py:48-59`, which converts
our `SourceFile` dataclass to `{"bucket": "...", "key": "..."}` via
`dataclasses.asdict`. Thus the **already-shipped** subclass at
`src/functions/file_processor/persistence.py` is correct as-is:

```python
def save_inprogress(self, data: dict[str, Any], remaining_time_in_millis=None) -> None:
    try:
        return super().save_inprogress(data, remaining_time_in_millis)
    except IdempotencyItemAlreadyExistsError:
        payload = data if isinstance(data, dict) else {}
        logger.info(
            "idempotent_cache_hit",
            extra={
                "source_bucket": payload.get("bucket"),
                "source_key": payload.get("key"),
            },
        )
        raise
```

Both existing tests in `tests/unit/test_persistence_cache_hit_log.py` pass
(verified: `uv run pytest tests/unit/test_persistence_cache_hit_log.py -v`).

## Task 3 path: 6C (with caveat)

Strictly per the form templates, this is **Form C** because the planned
mechanism (`DataRecord.get_payload()`) does not exist. However:

- The current `InstrumentedDynamoDBPersistenceLayer` already logs
  `source_bucket` and `source_key` correctly by treating `data` as the input
  payload **dict** (which it is). No fallback to "log only `idempotency_key`"
  is required.
- **Task 3 should NOT rewrite the subclass with `data: DataRecord` /
  `data.get_payload()`** — that would introduce an `AttributeError` regression
  (dicts have no `get_payload`) and lose the working bucket/key fields.
- The plan's claim that "the existing test was a false positive" is itself
  wrong: the test patches the parent `save_inprogress` to raise
  `IdempotencyItemAlreadyExistsError`, and the subclass's dict-keyed lookup
  works on the same dict it was given. Real Powertools behaviour matches the
  test fixture for this code path.

**Recommended Task 3 action:** verify the existing subclass and its tests
against this decision record; do not "fix" the code in the direction Form B
suggested. If the plan owner still wants tighter binding to `idempotency_key`
(for log-to-DynamoDB cross-referencing), add `data.get("idempotency_key")` to
the log — but note that the dict `data` does not contain the hash either;
the hash is only computed by `_get_hashed_idempotency_key` inside the parent
`save_inprogress` before the conditional write. To log the hash, the subclass
would need to call `self._get_hashed_idempotency_key(data=data)` directly.

## Runbook addendum (per Form C template)

> **`idempotent_cache_hit` log → source file lookup.** The current log line
> already carries `source_bucket` and `source_key` derived from the input
> payload dict passed to `save_inprogress`. Querying DynamoDB
> (`sbm-ingester-idempotency`) is therefore not required for the common case.
> If `source_bucket` / `source_key` are missing (e.g. a future refactor
> changes the payload shape), recover them by reading the SQS message body
> at the corresponding timestamp from the DLQ or main queue; the idempotency
> table does NOT persist the original payload (only its hash).

## Files cited

- `.venv/lib/python3.13/site-packages/aws_lambda_powertools/utilities/idempotency/persistence/base.py:256-330` — `save_inprogress` / `save_success` signatures.
- `.venv/lib/python3.13/site-packages/aws_lambda_powertools/utilities/idempotency/base.py:48-59` — `_prepare_data` (dataclass → dict).
- `.venv/lib/python3.13/site-packages/aws_lambda_powertools/utilities/idempotency/base.py:154-179` — `_process_idempotency` calls `save_inprogress(data=self.data, ...)`.
- `.venv/lib/python3.13/site-packages/aws_lambda_powertools/utilities/idempotency/base.py:273-280` — `response_hook` invocation on cache hit.
- `.venv/lib/python3.13/site-packages/aws_lambda_powertools/utilities/idempotency/config.py:22` — `IdempotencyConfig(..., response_hook=...)`.
- `.venv/lib/python3.13/site-packages/aws_lambda_powertools/utilities/idempotency/hook.py:9-15` — `IdempotentHookFunction` Protocol.
- `src/functions/file_processor/persistence.py` — current (working) implementation.
- `src/shared/source_file.py` — `SourceFile(frozen=True, slots=True)` dataclass.
