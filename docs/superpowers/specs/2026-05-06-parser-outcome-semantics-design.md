# Parser Outcome Semantics - Design Spec

**Status:** Draft for review
**Date:** 2026-05-06
**Owner:** zeyu

## Problem

The ingestion pipeline currently treats parser return values too narrowly. A parser returns `ParserResult = list[tuple[str, DataFrame]]`, and `file_processor` decides source-file movement by checking whether any Neptune IDs were resolved while iterating those DataFrames:

- at least one mapped point -> `newP/`
- no mapped points -> `newIrrevFiles/`
- parser/processing exception -> `newParseErr/`

That works for standard interval-like parsers that return DataFrames to the main `file_processor` writer. It does not work for parsers that either:

- successfully process an empty/no-data file,
- write Hudi rows as a side effect and return `[]`,
- send the file to an external sink and return `[]`, or
- can distinguish "no rows" from "rows exist but all are unmapped".

This caused valid Optima demand files to be moved to `newIrrevFiles/` even when the parser had already written Hudi output, and caused valid no-data demand files to be grouped with genuinely unmapped files.

## Goal

Introduce one explicit parser outcome vocabulary so source-file movement is based on parser semantics, not on guessing from `[]`.

The target directory semantics are:

- `newP/`: the file was recognized and handled successfully, including valid empty/no-data files and external-sink-only files.
- `newIrrevFiles/`: the file was recognized and contains data, but none of that data maps to known monitor points.
- `newParseErr/`: the file matched a parser but could not be parsed or could not be processed successfully.

## Non-Goals

- Changing how Hudi rows are formatted.
- Replacing existing parser implementations wholesale.
- Changing S3 bucket names or archive prefixes.
- Adding retry/DLQ behavior for parser-side S3 write failures. For now those failures remain processing errors and move to `newParseErr/`.
- Reclassifying historical files that have already been moved, except through separate manual remediation if needed.

## Outcome Vocabulary

### Success statuses returned by parsers

| Status | Meaning | Source file destination |
|---|---|---|
| `processed` | The file was recognized and at least one valid business row was written or made available to be written. Partial unmapped rows are allowed. | `newP/` |
| `processed_empty` | The file was recognized and valid, but contained no valid candidate rows to write. No Hudi file should be emitted solely for this outcome. | `newP/` |
| `unmapped` | The file was recognized and contained valid candidate rows, but every candidate failed only because no monitor-point mapping existed. | `newIrrevFiles/` |
| `processed_external` | The file was recognized and successfully handled by an external sink, without writing Hudi rows. | `newP/` |

### Exceptions used for control flow and failures

| Exception | Meaning | Handling |
|---|---|---|
| `NotRelevantParser` | This parser does not apply to the file. | Dispatcher tries the next parser. |
| `ParserError` | This file appears to belong to the parser, but has invalid format, required fields, timestamps, encoding, or structure. | Move to `newParseErr/`. |
| `ProcessingError` | The file parsed, but a write/upload/post-parse operation failed. | Move to `newParseErr/`. |

Do not use a bare empty list to decide file disposition. An empty DataFrame result may still be a successful `processed_empty` outcome.

## Edge-Case Rules

| Scenario | Outcome |
|---|---|
| File has valid headers/metadata but zero data rows | `processed_empty` |
| BidEnergy demand file contains `No data found` | `processed_empty` |
| BidEnergy interval file contains `No data is available` | `processed_empty` |
| File has valid candidate rows and at least one mapped row/point | `processed` |
| File has valid candidate rows, some mapped and some unmapped | `processed`, with `unmapped_count` logged/metriced |
| File has valid candidate rows but zero mapped rows/points, and all failures are mapping misses | `unmapped` |
| File has only blank optional value cells after removing metadata/header rows | `processed_empty` |
| File has source rows but none can become valid candidate rows because required fields, timestamps, or numeric values are malformed | `ParserError` |
| File is successfully forwarded to a non-Hudi bucket/system | `processed_external` |
| File matches parser filename/content gate but is malformed | `ParserError` |
| Parser writes to S3 and `put_object` fails | `ProcessingError` |
| Parser filename/content gate does not match | `NotRelevantParser` |

## Count Semantics

The implementation should use consistent counters rather than inferring outcome from `rows_written` alone:

| Field | Meaning |
|---|---|
| `source_row_count` | Source data rows after removing metadata, header rows, sentinel lines, and fully blank rows. |
| `candidate_row_count` | Source rows or source row/column cells that have the required identifier, timestamp/date, value, and parser-specific fields needed to attempt mapping/writing. |
| `rows_written` | Hudi rows written directly by the parser or produced for the main file processor writer. |
| `unmapped_count` | Valid candidates skipped because mapping lookup missed. |

`unmapped` is valid only when `candidate_row_count > 0`, `rows_written == 0`, and every skipped candidate failed because of a mapping miss. Invalid timestamps, invalid numeric values, missing required columns, malformed matched files, and parser-side write failures must not be hidden as `unmapped`.

For DataFrame-returning parsers, a valid candidate is a parseable timestamp plus a channel/value cell that the file processor would normally attempt to map and write. Metadata-only columns, unsupported suffix columns, fully blank rows, and null value cells do not by themselves make a file `unmapped`.

Each parser should document whether its `candidate_row_count` unit is source rows or expanded row/column cells, and tests should assert that parser-specific unit consistently.

## Proposed Data Model

Add a small outcome module, for example `src/shared/parsers/outcome.py`:

```python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

import pandas as pd

ParserStatus = Literal[
    "processed",
    "processed_empty",
    "unmapped",
    "processed_external",
]

ParserResult = list[tuple[str, pd.DataFrame]]


@dataclass(frozen=True)
class ParserOutcome:
    status: ParserStatus
    dfs: ParserResult = field(default_factory=list)
    source_row_count: int = 0
    candidate_row_count: int = 0
    rows_written: int = 0
    unmapped_count: int = 0
    reason: str | None = None


class NotRelevantParser(Exception):
    """Raised when a parser does not apply to the file."""


class ParserError(Exception):
    """Raised when a matching file cannot be parsed."""


class ProcessingError(Exception):
    """Raised when parsed data cannot be written or otherwise handled."""
```

`ParserResult` can continue to exist for DataFrame-returning parsers, but dispatcher-facing parser functions should return `ParserOutcome`.

## Dispatcher Design

Replace the current "try any exception and continue" dispatcher behavior with explicit exception handling. Each parser should perform a cheap filename/header/content relevance gate before parsing the full file:

- `NotRelevantParser`: continue to the next parser.
- `ParserError` / `ProcessingError`: stop dispatch and propagate the failure.
- unexpected exceptions before the relevance gate completes: wrap as `NotRelevantParser` only when the parser can prove the file is not relevant.
- unexpected exceptions after the relevance gate completes: treat as `ParserError` after logging.

This prevents a parser from identifying a file as its own, failing due to malformed content, and then letting unrelated parsers try to interpret it.

## File Processor Design

`file_processor` should separate two concerns:

1. Convert parser output into Hudi rows, when `outcome.dfs` is present.
2. Move the source file according to `outcome.status`.

For standard NEM and DataFrame-returning non-NEM parsers:

- Build `file_neptune_ids` as today.
- If at least one mapped point is found, set/keep outcome as `processed`.
- If no mapped point is found and the source parser produced valid candidate rows, classify as `unmapped`.
- If the source parser explicitly reported `processed_empty`, keep it as `processed_empty`.
- If rows exist but required timestamp/value/channel fields are malformed or cannot form valid candidates, raise `ParserError`.

For side-effect parsers:

- `demand_parser` and `bunnings_billing_parser` return `processed`, `processed_empty`, or `unmapped` based on source rows, valid candidate rows, rows written, and mapping misses.
- `racv_billing_parser` returns `processed_external` after successfully writing to `gegoptimareports`.

Directory mapping should then be centralized:

```python
if outcome.status in {"processed", "processed_empty", "processed_external"}:
    move_s3_file(BUCKET_NAME, local_file_path, PROCESSED_DIR)
elif outcome.status == "unmapped":
    move_s3_file(BUCKET_NAME, local_file_path, IRREVFILES_DIR)
```

Errors move to `PARSE_ERR_DIR`.

## Current Parser Mapping

| Parser/source | Current behavior | Target outcome behavior |
|---|---|---|
| NEM12/NEM13 | Returns DataFrames through `nem_adapter`. | `processed` if any mapped point; `unmapped` only if valid candidate interval cells exist and none map. |
| Envizi electricity/water/bulk | Returns DataFrames. | `processed` if any mapped point; `unmapped` only if valid candidate cells exist and none map. |
| RACV electricity | Returns DataFrames; all-zero data raises. | `processed` if any mapped point; `processed_empty` for valid all-zero/no-usable-data only if the file is otherwise valid; malformed remains `ParserError`. |
| RACV Noosa solar | Returns DataFrames with direct `p:` point IDs. | `processed` if any point data; `processed_empty` if valid file has no usable point rows. |
| Green Square ComX | Returns DataFrames. | `processed` if mapped; `unmapped` only if valid candidate cells exist and none map. |
| Optima interval | Returns DataFrames; no-data sentinel currently returns `[]`. | `processed` if mapped; `processed_empty` for `No data is available`; `unmapped` only if valid candidate interval cells exist and all miss mapping. |
| Optima demand | Writes Hudi side-effect, returns `[]`. | `processed` if `rows_written > 0`; `processed_empty` for no-data/header-only; `unmapped` only if valid candidate demand cells exist and all miss mapping; malformed timestamps/values remain `ParserError`. |
| Bunnings billing | Writes Hudi side-effect, returns `[]`. | `processed` if `rows_written > 0`; `unmapped` only if valid candidate billing cells exist and all miss mapping; `processed_empty` if valid report has no rows; malformed billing dates/values remain `ParserError`. |
| RACV billing | Writes original report to `gegoptimareports`, returns `[]`. | `processed_external` after successful external write. |

## Metrics and Logging

The pipeline should log enough information to audit why a file moved to each destination:

- `processed`: mapped point count / rows written
- `processed_empty`: reason (`no_data_sentinel`, `header_only`, `all_zero_valid`, etc.) and source row count
- `unmapped`: source row count, candidate row count, and unmapped count
- `processed_external`: target bucket/key or external sink name
- `parse_error` / `processing_error`: parser name and exception message

Existing CloudWatch metrics can remain, but their meanings become cleaner:

- `ValidProcessedFiles` includes `processed`, `processed_empty`, and `processed_external`.
- `IrrelevantFiles` includes only `unmapped`.
- `ParseErrorFiles` includes parser and processing errors.

## Testing Plan

Add focused unit tests around disposition, not just parser output shape:

1. `demand_parser`
   - no-data sentinel -> `processed_empty`
   - header-only valid file -> `processed_empty`
   - rows written -> `processed`
   - valid candidate rows exist but all are unmapped -> `unmapped`
   - rows exist but all timestamps or numeric values are invalid -> `ParserError`
   - S3 `put_object` failure -> `ProcessingError`
2. `bunnings_billing_parser`
   - rows written -> `processed`
   - valid candidate rows exist but all are unmapped -> `unmapped`
   - valid empty report -> `processed_empty`
   - rows exist but all billing dates or numeric values are invalid -> `ParserError`
   - S3 `put_object` failure -> `ProcessingError`
3. `racv_billing_parser`
   - successful external upload -> `processed_external`
   - external upload failure -> `ProcessingError`
4. `interval_parser`
   - no-data sentinel -> `processed_empty`
   - rows with mappings through file processor -> `processed`
   - rows that parse but all miss mappings through file processor -> `unmapped`
5. `file_processor`
   - `processed_empty` moves to `newP/`
   - `processed_external` moves to `newP/`
   - `unmapped` moves to `newIrrevFiles/`
   - `ParserError` / `ProcessingError` move to `newParseErr/`
   - partial mapping remains `processed`
6. Dispatcher
   - `NotRelevantParser` continues
   - `ParserError` stops and propagates
   - `ProcessingError` stops and propagates
   - unexpected exception after a relevance gate is treated as `ParserError`
   - all parsers returning `NotRelevantParser` produces a no-valid-parser parse error

## Migration Strategy

This is a behavioral cleanup and should be done in one feature branch:

1. Add `ParserOutcome` and parser exception types.
2. Update dispatcher to handle explicit exceptions.
3. Update side-effect parsers first (`demand`, `bunnings_billing`, `racv_billing`) because they are currently most misleading.
4. Update no-data parsers (`interval`, demand no-data) to return `processed_empty`.
5. Update standard DataFrame parsers to return `ParserOutcome(status="processed", dfs=...)`.
6. Update `file_processor` disposition logic.
7. Update tests and docs.

Temporary compatibility with legacy list returns is acceptable during implementation, but the final state should not rely on `[]` for disposition.

## Open Decisions

- Historical files already moved to `newIrrevFiles/` are out of scope for this code change. If needed, they should be remediated by a separate one-off S3 move/backfill script after the new semantics deploy.
- `processed_external` currently covers RACV billing only. If that flow is no longer desired, remove or replace the parser separately; do not overload `unmapped`.
