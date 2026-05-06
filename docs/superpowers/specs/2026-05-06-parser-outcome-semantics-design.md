# Parser Outcome Semantics - Design Spec

**Status:** Refined post-implementation review and production-data audit
**Date:** 2026-05-06
**Owner:** zeyu

## Problem

Before the parser outcome implementation, the ingestion pipeline treated parser return values too narrowly. Dispatcher-facing parsers exposed only `ParserResult = list[tuple[str, DataFrame]]`, and `file_processor` decided source-file movement by checking whether any Neptune IDs were resolved while iterating those DataFrames.

Production audit (2026-05-06 sample of `s3://sbm-file-ingester/`) confirmed three classes of legacy misclassification:

- **~600 daily NEM12 files with only `100`/`900` records** routed to `newParseErr/` because nemreader returns `[]` and the dispatcher fallback chain never claims them ("No valid parser found"). These are legitimate "no data for this NMI on this date" reports from BidEnergy.
- **~70 daily side-effect parser successes** (RACV billing 4 MB UTF-16 reports, Bunnings demand profile, Optima demand) routed to `newIrrevFiles/` because the parsers returned `[]` after writing Hudi rows.
- **~8 daily Envizi/Meter_Data sentinel files** ("No data found" body) routed to `newParseErr/` for the same reason as NEM12 empties.

The implemented behaviour now uses explicit `ParserOutcome` statuses, fixing all three classes.

A post-implementation review and production-data audit identified additional refinements that this spec incorporates: closed `ParserReason` and `SkipReason` enums; identifier-level partial-recognition signals with parser-namespaced kinds; explicit relevance-gate ordering with BOM-aware encoding; NEM12 empty-payload special handling; sidecar audit log path; vendor inventory; explicit out-of-scope validations.

## Goal

Provide one explicit parser outcome vocabulary so source-file movement is based on parser semantics, partial recognition is observable at identifier and suffix granularity, and all "data not landed in Hudi" paths produce auditable signals.

Target directory semantics:

- `newP/`: file recognized and handled successfully — fully processed, partially processed, validly empty, or forwarded to an external sink.
- `newIrrevFiles/`: file recognized and contains valid candidate data, but none of that data maps to known monitor points.
- `newParseErr/`: file matched a parser but could not be parsed structurally, or a write/upload after parsing failed.

## Layer Context

The Hudi table `default.sensordata_default` (schema `sensorid, ts, val, unit, quality, ats`) serves as **both Bronze and Silver layers**. There is no separate downstream layer that filters bad rows; dashboards and downstream services consume the Hudi table directly.

Implication: the file_processor performs limited row-level filtering at ingest, but the filtering criterion is **anchorability and parseability**, not business validation. Specifically:

- Rows that cannot be anchored (missing or unparseable timestamp, missing identifier, broken row shape) are skipped.
- Cells whose values cannot be parsed are skipped.
- Cells that are explicitly blank are skipped (vendor said "no reading").
- Everything else lands in Hudi.

Business-level validation (negative consumption, future timestamps, sensor sanity ranges) is explicitly out of scope and belongs to a future silver layer or downstream consumer.

## Non-Goals

- Changing Hudi schema or column semantics.
- Adding new sink paths beyond `newP/`, `newIrrevFiles/`, `newParseErr/`.
- Adding a separate quarantine bucket.
- Pipeline-level row markers in Hudi columns (the `quality` column is reserved for vendor codes; the pipeline must not write its own values into it).
- Reclassifying historical files.
- Business-level validation of values, timestamps, identifiers (see "Out of Scope Validations").
- Cross-file deduplication beyond the existing DynamoDB idempotency layer.
- Adding parsers for vendor formats not currently handled (see "Vendor Inventory").

## Outcome Vocabulary

### Success statuses returned by parsers

| Status | Meaning | Source file destination |
|---|---|---|
| `processed` | The file was recognized and at least one valid business row was written or made available to be written. Partial unmapped rows allowed; partial unknown suffixes allowed; partial skipped rows allowed. | `newP/` |
| `processed_empty` | The file was recognized and valid, but contained no valid candidate rows to write. No Hudi file should be emitted solely for this outcome. | `newP/` |
| `unmapped` | The file was recognized and contained valid candidate rows, but every candidate failed only because no monitor-point mapping existed. | `newIrrevFiles/` |
| `processed_external` | The file was recognized and successfully handled by an external sink, without writing Hudi rows. | `newP/` |

### Reason enum (closed)

`ParserReason` is a closed `Literal` type. New reasons must be added to the type and to the table below before use.

| Reason | When | Allowed status |
|---|---|---|
| `no_data_sentinel` | Vendor sentinel content (BidEnergy "No data found", "No data is available", or NEM12 `100`/`900`-only payload) | `processed_empty` |
| `zero_rows` | Header/metadata valid, zero data rows after stripping headers | `processed_empty` |
| `all_blank` | Data rows exist; all value cells are blank, whitespace, or explicit null markers | `processed_empty` |
| `all_zero_valid` | RACV electricity all-zero export over a valid period | `processed_empty` |
| `all_unknown_suffix` | DataFrame returned only suffix columns the file processor does not recognize | `processed_empty` |
| `all_skipped` | Mixed skip reasons (malformed, blank, anchor failure) leave zero candidates | `processed_empty` |
| `external_gegoptimareports` | RACV billing successful upload to `gegoptimareports` | `processed_external` |
| `idempotency_skip` | DynamoDB idempotency layer detected a previously processed file (synthesized by file_processor, not a parser return) | `processed_empty` |

### Skip reason enum (closed)

`SkipReason` is a closed `Literal` type. Used in the `skip_reasons` counter on `ParserOutcome` to attribute each skipped row to a category.

| Skip reason | Meaning |
|---|---|
| `unparseable_value` | A value cell in a numeric/required column contained non-empty content that could not be coerced (e.g., `"abc"`, `"1.2.3"`). |
| `blank_value` | A value cell was explicitly empty, whitespace-only, or a known null marker (`""`, `" "`, `"N/A"`, `"NULL"`, `"-"`). |
| `unparseable_timestamp` | A single row's timestamp could not be parsed. |
| `row_anchor_failure` | Parser-specific: row could not be anchored to a sensor. Each parser defines what counts (missing NMI, missing topic ID, malformed `p:` ID, missing site name in metadata, etc.). |
| `row_shape_mismatch` | Row had structurally inconsistent shape (extra/missing trailing cells) but was tolerable to skip rather than reject the file. |

### Exceptions used for control flow and failures

| Exception | Meaning | Handling |
|---|---|---|
| `NotRelevantParser` | This parser does not apply to the file. Raised by the cheap relevance gate only. | Dispatcher tries the next parser. |
| `ParserError` | This file matches the parser but is structurally unprocessable: zero bytes, unreadable, encoding-broken, missing required schema columns, entire timestamp column unparseable, entire identifier column missing. **Not** raised on row-level data quality issues in matched files. | Move to `newParseErr/`. |
| `ProcessingError` | Parsing succeeded structurally, but a write/upload/post-parse operation failed (S3 PUT failure, external sink failure, source move failure after Hudi commit). | Move to `newParseErr/`. |

`ParserError` and `ProcessingError` both route to `newParseErr/`; the distinction is purely diagnostic.

`ParserError` is reserved for **file-level structural failures**, not row-level data quality. A matched file with one bad cell does NOT raise `ParserError` — that row is skipped and counted in `skip_reasons`.

## Layer-Level Recognition

A file passes through up to five recognition layers. Partial happens at L3, L4, L5.

| Layer | Action | Partial possible | Failure mode |
|---|---|---|---|
| L1 file gate | filename + cheap header sniff (BOM-aware) | no | `NotRelevantParser` (try next parser); if none claim → `ParserError` |
| L2 schema | required columns exist; identifier column reachable; timestamp column reachable | no | `ParserError` (file rejected) |
| L3 row | per-row timestamp/value parse | yes | row skipped + counted; file continues |
| L4 NMI / identifier | identifier extracted then mapped (or `p:` ID bypass) | yes | row counted as unmapped or anchor-failure-skipped; file continues |
| L5 channel/suffix | suffix recognized + channel mapped (DataFrame parsers only) | yes | column skipped + suffix recorded; file continues |

L3-L5 partial NEVER raises `ParserError`. The matched parser owns row-level filtering AND value normalization, but the contract caps its action at "skip and count" — it must not reject the whole file for partial issues.

## Outcome Decision Matrix

The matrix is the authoritative source for "which outcome does scenario X produce". Tests must reflect every row.

### File-level (L1) recognition

| Scenario | Outcome | Disposition |
|---|---|---|
| Filename matches no parser gate | dispatcher: "no valid parser" → `ParserError` | `newParseErr/` |
| Filename matches but cheap header sniff fails | `NotRelevantParser` from that parser; dispatcher continues | depends on next parser |
| Filename matches and header sniff passes | parser owns the file from this point | depends on later layers |

### Schema (L2) — after gate passes

| Scenario | Outcome | Disposition |
|---|---|---|
| 0-byte file | `ParserError` | `newParseErr/` |
| File unreadable / corrupted body | `ParserError` | `newParseErr/` |
| Wrong encoding in matched file (e.g. UTF-16 in a UTF-8-expected parser) | `ParserError` | `newParseErr/` |
| Required schema column missing | `ParserError` | `newParseErr/` |
| Required column renamed | `NotRelevantParser` if cheap sniff catches it; otherwise `ParserError` | depends |
| Entire timestamp column unparseable | `ParserError` | `newParseErr/` |
| Entire identifier column missing | `ParserError` | `newParseErr/` |
| Extra unknown columns present | ignored, processing continues | — |

### Row-level (L3) data validity in matched files

| Scenario | Outcome | Disposition |
|---|---|---|
| All rows valid | continue to L4/L5 | — |
| Single row has unparseable numeric (e.g., `"abc"` in a numeric column) | row skipped + `skip_reasons["unparseable_value"]` += 1 | file continues |
| Single row has unparseable timestamp | row skipped + `skip_reasons["unparseable_timestamp"]` += 1 | file continues |
| Single row has blank/whitespace/null-marker value | row skipped + `skip_reasons["blank_value"]` += 1 | file continues |
| Single row has wrong shape (extra/missing trailing cell, optional fields) | row skipped + `skip_reasons["row_shape_mismatch"]` += 1 | file continues |
| Single row missing required identifier | row skipped + `skip_reasons["row_anchor_failure"]` += 1 | file continues |
| All rows skipped (mixed reasons) | `processed_empty(reason="all_skipped")` | `newP/` |
| All rows blank cells (no malformed) | `processed_empty(reason="all_blank")` | `newP/` |
| Header valid but zero data rows | `processed_empty(reason="zero_rows")` | `newP/` |
| Vendor sentinel ("No data found"/"No data is available") | `processed_empty(reason="no_data_sentinel")` | `newP/` |
| NEM12 file with only `100`/`900` records (no `200`/`300`) | `processed_empty(reason="no_data_sentinel")` | `newP/` |
| RACV all-zero valid period | `processed_empty(reason="all_zero_valid")` | `newP/` |

### Business entity (L4) — NMI / identifier mapping

| Scenario | Outcome | Disposition |
|---|---|---|
| All identifiers map | `processed` | `newP/` |
| Some identifiers map, some don't | `processed`, `unmapped_identifiers` populated | `newP/` |
| No identifiers map, valid candidates exist | `unmapped` | `newIrrevFiles/` |
| No identifiers map, zero candidates | `processed_empty(reason="zero_rows" or "all_blank")` | `newP/` |
| Direct `p:` Neptune ID (e.g., Noosa solar) | treated as mapped, written to Hudi | `newP/` |
| `p:` ID format malformed | row skipped + `skip_reasons["row_anchor_failure"]` += 1 | file continues |

### Channel / suffix (L5) — DataFrame parsers only

| Scenario | Outcome | Disposition |
|---|---|---|
| All suffixes known, at least one mapped | `processed` | `newP/` |
| Some known suffixes mapped, others unmapped | `processed`, `unmapped_count` and `unmapped_identifiers` populated | `newP/` |
| Some known suffixes + some unknown suffixes | `processed`, `unsupported_suffixes` populated | `newP/` |
| All suffixes unknown | `processed_empty(reason="all_unknown_suffix")` + alarm | `newP/` |

### Side-effect parser write (D8)

| Scenario | Outcome | Disposition |
|---|---|---|
| All S3 PUTs succeed, ≥1 row written | `processed` | `newP/` |
| All S3 PUTs succeed, no rows to write | `processed_empty` | `newP/` |
| All identifiers unmapped, no PUTs attempted | `unmapped` | `newIrrevFiles/` |
| Any S3 PUT fails (network/auth/throttling) | `ProcessingError` | `newParseErr/` |
| External-sink upload (RACV billing) succeeds | `processed_external(reason="external_gegoptimareports")` | `newP/` |
| External-sink upload fails | `ProcessingError` | `newParseErr/` |

### Idempotency (D10)

| Scenario | Outcome | Disposition |
|---|---|---|
| DynamoDB idempotency layer indicates already processed | `processed_empty(reason="idempotency_skip")` synthesized by file_processor; parsers not invoked | `newP/` |
| First time seen | normal processing | depends |

## Row-Level Filtering Boundary

This section is the heart of the contract. It defines what gets written to Hudi vs what gets skipped vs what fails the file.

### Always written to Hudi

A row is written to Hudi if and only if:
- Its timestamp parses to a valid `pd.Timestamp`.
- Its identifier resolves (Neptune mapping hit, or direct `p:` ID, or parser-defined anchor).
- Its value coerces to a finite numeric (after parser-specific transforms).

When written, the row carries:
- `sensorid`: parser-derived (NMI + suffix, direct `p:` ID, or vendor-mapped).
- `ts`: parsed timestamp.
- `val`: numeric value.
- `unit`: parser-derived from column or vendor convention.
- `quality`: vendor-provided string if available, else NULL.
- `ats`: ingestion timestamp.

### Always skipped (file continues)

| Cell/row condition | Skip reason | Counted in |
|---|---|---|
| Value is unparseable non-empty content | `unparseable_value` | `rows_skipped`, `skip_reasons` |
| Value is `""`, whitespace, `"N/A"`, `"NULL"`, `"-"`, or other vendor null marker | `blank_value` | `rows_skipped`, `skip_reasons` |
| Single-row timestamp unparseable | `unparseable_timestamp` | `rows_skipped`, `skip_reasons` |
| Identifier missing or unrecoverable for this row | `row_anchor_failure` | `rows_skipped`, `skip_reasons` |
| Row shape inconsistent (extra/missing trailing optional cells) | `row_shape_mismatch` | `rows_skipped`, `skip_reasons` |
| Identifier resolved but no Neptune mapping | (not skipped — counted as unmapped) | `unmapped_count`, `unmapped_identifiers` |
| Channel suffix not in `NMI_DATA_STREAM_COMBINED` | (not skipped — counted as unsupported) | `unsupported_suffixes` |

### File-level rejection (`ParserError`)

Only the following conditions cause `ParserError` after the relevance gate succeeds:

- File cannot be read (encoding, corruption, 0 bytes).
- Required schema column entirely missing.
- Timestamp column entirely unparseable across all rows.
- Identifier column entirely missing across all rows.
- Header structure inconsistent with parser expectation in a way that prevents column mapping.

A file with N valid rows + 1 unparseable row produces N Hudi rows + `newP/` (`processed`) — never `ParserError`.

### Vendor-specific value normalization

A parser MAY transform vendor-specific value strings into numeric values before applying the unparseable_value rule. Example: `noosa_solar` maps `"Normal Operation"` → 4, `"Error Exists"` → 7 via a vendor status table. This is not a skip and not an error — it is a recognized parser transform.

The contract does not enumerate or constrain these transforms; they are per-parser implementation. Parsers must document their transforms in the parser-specific spec.

## Partial Recognition Semantics

Partial recognition is real and common in production. The pipeline surfaces it as observable signals while keeping disposition stable.

### Disposition rules per layer

- **L3 partial** (some rows malformed/blank/anchor-failed): file succeeds (`processed`). Skipped rows recorded in `skip_reasons` and `rows_skipped`.
- **L4 partial** (some identifiers unmapped): file succeeds (`processed`). Unmapped identifiers recorded in `unmapped_identifiers`.
- **L5 partial** (some unknown suffixes): file succeeds (`processed`). Unknown suffixes recorded in `unsupported_suffixes`. First appearance triggers alarm.
- **L5 fully unknown**: file succeeds with `processed_empty(reason="all_unknown_suffix")` + alarm.

### Why disposition does not change for L3–L5 partial

`newP/` means "we successfully handled this file as far as our contract goes". Routing partial-mapped files elsewhere fragments downstream consumption and delays operator response on the truly broken files. Identifier-level signals plus ratio metrics give equivalent observability without altering disposition.

### Signal fields

| Field | Type | Populated by | Purpose |
|---|---|---|---|
| `unmapped_count` | `int` | parser or file_processor | Numeric counter of skipped candidates due to mapping miss. |
| `unmapped_identifiers` | `tuple[tuple[str, str], ...]` | parser or file_processor | (`kind`, `value`) pairs of identifiers that missed mapping. `kind` is parser-namespaced (e.g., `"nmi"`, `"comx_topic"`, `"p_id"`). De-duplicated, capped at 100 per file. |
| `unsupported_suffixes` | `frozenset[str]` | file_processor | Distinct suffix strings observed but not in `NMI_DATA_STREAM_COMBINED`. |
| `rows_skipped` | `int` | parser or file_processor | Source rows skipped (regardless of reason). Counts source rows, NOT expanded Hudi output rows. |
| `skip_reasons` | `dict[SkipReason, int]` | parser or file_processor | Source-row count by skip reason. Keys must be valid `SkipReason` values. |

`unmapped_identifiers` MUST use the (`kind`, `value`) tuple form. Suggested kinds (extend per parser):

| `kind` | Used by | `value` example |
|---|---|---|
| `nmi` | NEM12, Optima interval, Optima demand, Bunnings billing | `"OPTIMA_VCCCRE0075"`, `"4001260599"` |
| `nem12_nmi` | NEM12 streaming | `"4310894358"` |
| `p_id` | Noosa Solar, future direct-`p:` parsers | `"p:racv:r:31425a11-ef34f44a"` |
| `envizi_column_nmi` | Envizi vertical | `"4310894358 (kWh)"` |
| `comx_topic` | Green Square ComX | `"Aquatic Centre/1621"` |

Dashboards must filter or group by `kind` before aggregating; mixing kinds is meaningless.

## Count and Identifier Semantics

| Field | Definition |
|---|---|
| `source_row_count` | Source data rows after stripping metadata, header, sentinel lines, fully-blank rows. Per-parser unit documented in parser spec. |
| `candidate_row_count` | Rows that passed L3+L4 anchor checks (parseable timestamp, resolvable identifier) and were eligible for L5 mapping/writing. Per-parser unit (rows or row × channel) documented. |
| `rows_written` | Hudi rows actually written. Always counts Hudi output rows. |
| `rows_skipped` | Source rows skipped at L3 due to value/timestamp/anchor/shape issues. Counts source rows. |
| `unmapped_count` | L4/L5 skips due to mapping miss. Counts the same unit as `candidate_row_count`. |

Parser unit conventions:

| Parser | `candidate_row_count` unit | `rows_written` unit |
|---|---|---|
| NEM12 | source rows × channels | Hudi rows |
| Optima interval | source rows × channels | Hudi rows |
| Envizi vertical_* | source rows × value-columns | Hudi rows |
| Optima demand | source rows | Hudi rows (3× source row: kw + kva + pf) |
| Bunnings billing | source rows | Hudi rows (1× source row) |
| RACV billing | N/A (external sink) | 0 |
| RACV Noosa solar | source rows × `p:` ID columns | Hudi rows |
| Green Square ComX | source rows × topic columns | Hudi rows |

`unmapped` is valid only when `candidate_row_count > 0`, `rows_written == 0`, `unmapped_count == candidate_row_count`, and every skipped candidate failed because of a mapping miss. Invalid timestamps, blank cells, malformed values, and write failures must NOT be hidden as `unmapped`.

## Data Model

```python
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Literal

import pandas as pd

ParserStatus = Literal[
    "processed",
    "processed_empty",
    "unmapped",
    "processed_external",
]

ParserReason = Literal[
    "no_data_sentinel",
    "zero_rows",
    "all_blank",
    "all_zero_valid",
    "all_unknown_suffix",
    "all_skipped",
    "external_gegoptimareports",
    "idempotency_skip",
]

SkipReason = Literal[
    "unparseable_value",
    "blank_value",
    "unparseable_timestamp",
    "row_anchor_failure",
    "row_shape_mismatch",
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
    unmapped_identifiers: tuple[tuple[str, str], ...] = ()
    unsupported_suffixes: frozenset[str] = field(default_factory=frozenset)
    rows_skipped: int = 0
    skip_reasons: Counter[SkipReason] = field(default_factory=Counter)
    reason: ParserReason | None = None


class NotRelevantParser(Exception):
    """Raised by the cheap relevance gate only."""


class ParserError(Exception):
    """Raised when a matching file is structurally unprocessable.

    Reserved for file-level failures only. Row-level data quality issues
    must be skipped and counted, not raised as ParserError.
    """


class ProcessingError(Exception):
    """Raised when validated data fails to write or be forwarded."""
```

Cross-field invariants tests must assert:

```
status="processed"          → rows_written ≥ 1
status="processed_empty"    → rows_written = 0, unmapped_count = 0
status="unmapped"           → rows_written = 0, candidate_row_count > 0,
                              unmapped_count = candidate_row_count
status="processed_external" → rows_written = 0, dfs = []
rows_skipped <= sum(skip_reasons.values())  (cell-level skip counts can exceed row count when a row has multiple value columns)

Invariants apply to the **final outcome** (post-`file_processor` recomputation for DataFrame parsers, or directly for side-effect parsers). Intermediate raw `ParserOutcome` returned by DataFrame parsers is advisory — its `status="processed"` with `rows_written=0` is normal because `rows_written` is computed downstream by `_compute_dataframe_final_status`. The test-only invariant helper must be applied to final outcomes only.
```

## Dispatcher Design

Each parser must perform its relevance gate before any I/O that can fail for content reasons. The gate must be cheap and BOM-aware:

1. Filename suffix / substring match.
2. First-line read with `open(file, encoding="utf-8-sig")` to strip UTF-8 BOM, or binary read of magic bytes for non-UTF-8 vendors.
3. For multi-section formats (e.g., ComX with 6+ rows of metadata before data), read up to N initial lines.

`pd.read_csv` and similar full-file parses must NOT happen inside the relevance gate. After the gate succeeds, the parser owns the file; any later read failure is `ParserError`, not `NotRelevantParser`.

Dispatcher exception handling:

- `NotRelevantParser` → continue to next parser.
- `ParserError` / `ProcessingError` → stop dispatch, propagate.
- Unexpected exception → `logger.exception`, then re-raise as `ParserError`. This prevents misclassifying a buggy parser as "not my file".
- After all parsers raise `NotRelevantParser` → dispatcher raises `ParserError("No valid parser for <file>")`.

## File Processor Design

`file_processor` separates two concerns:

1. Convert parser output (DataFrames or side-effect writes) into Hudi rows.
2. Move the source file according to the final outcome status.

### NEM12 path with empty-payload special case

The NEM12 path runs before non-NEM dispatcher fallback:

```python
try:
    stream = stream_as_data_frames(local_file_path, split_days=True)
    first_item = next(stream, None)
    if first_item is None:
        # Empty NEM12 (only 100/900 records). Do NOT fall through to non-NEM
        # parsers — none of them will match NEM12 format and the file would
        # incorrectly route to newParseErr/. Detect via filename prefix or
        # first-line content match for "100,NEM12,*" and emit processed_empty.
        if _looks_like_nem12(local_file_path):
            outcome = ParserOutcome(
                status="processed_empty",
                reason="no_data_sentinel",
                source_row_count=0,
            )
        else:
            raise ValueError("No data parsed from file")
    else:
        outcome = ParserOutcome(status="processed", dfs=chain([first_item], stream))
except (NemParseError, ValueError):  # narrowed from bare Exception
    # Genuine NEM12 parse failure or non-NEM12 file.
    try:
        outcome = get_non_nem_outcome(local_file_path, ...)
    except (ParserError, ProcessingError) as e:
        ...
```

`_looks_like_nem12` reads first ~50 bytes and checks for `100,NEM12,` prefix.

### NEM12 fallback narrowing

The NEM12 path's broad `except Exception` is narrowed to specific exceptions raised by `nemreader` / `nem_adapter` indicating "not a NEM12 file" or "no data". Other unexpected exceptions propagate as `ParserError`. This preserves "matched parser owns data quality" for NEM12.

### Status authority

- **DataFrame parsers** (interval, envizi/*, racv/*, comx) return `ParserOutcome(status="processed", dfs=...)` or `processed_empty`. The `status` field is **advisory**; file_processor recomputes from row counts.
- **Side-effect parsers** (demand, bunnings_billing, racv_billing) return their final status directly; file_processor honors as-is.

### File processor's final-status calculation (DataFrame path)

```python
if rows_written > 0:
    final_status = "processed"
elif candidate_row_count > 0 and unmapped_count == candidate_row_count:
    final_status = "unmapped"
elif candidate_row_count == 0 and unsupported_suffixes:
    final_status = "processed_empty"   # reason = "all_unknown_suffix"
elif rows_skipped > 0 and rows_written == 0 and candidate_row_count == 0:
    final_status = "processed_empty"   # reason = "all_skipped"
else:
    final_status = "processed_empty"   # reason inherited from outcome (e.g. "all_blank", "zero_rows")
```

### DataFrame consumer error policy

The DataFrame consumer (`_candidate_values`) MUST NOT raise on row-level data quality issues. Implementation:

- Numeric coerce uses `pd.to_numeric(errors="coerce")` for value columns.
- NaN cells (post-coerce) are skipped with `skip_reasons["unparseable_value"]` (if originally non-empty) or `skip_reasons["blank_value"]` (if originally empty/whitespace/null marker).
- Timestamps use `pd.to_datetime(errors="coerce")`. NaT cells skip with `skip_reasons["unparseable_timestamp"]`.
- Row shape mismatches in DataFrame iteration are skipped with `skip_reasons["row_shape_mismatch"]`.

`ProcessingError` is reserved for:
- S3 PUT / external sink failures.
- Hudi commit failures.
- Source file move failures after a successful Hudi commit.

### Source file movement

```python
if outcome.status in {"processed", "processed_empty", "processed_external"}:
    move_s3_file(BUCKET_NAME, local_file_path, PROCESSED_DIR)
elif outcome.status == "unmapped":
    move_s3_file(BUCKET_NAME, local_file_path, IRREVFILES_DIR)
```

Errors → `PARSE_ERR_DIR`.

Source movement only happens after all per-file Hudi upload futures complete successfully. If a source move fails after a successful Hudi commit, the writer's committed final keys are deleted as compensation, and the file is moved to `newParseErr/` as `ProcessingError`.

## Encoding Policy

Vendor encodings observed in production:

| Encoding | Vendors |
|---|---|
| UTF-8 (no BOM) | NEM12 (most), Optima interval/demand, Bunnings billing/demand, ComX |
| UTF-8 with BOM (`\xef\xbb\xbf`) | R1746/R1748 (currently unhandled), RACV Noosa Solar |
| UTF-16 LE with BOM (`\xff\xfe`) | RACV "Usage and Spend Report" billing, Bunnings "Usage and Spend" billing |

Rules:

1. **Cheap relevance gates** must use `encoding="utf-8-sig"` for text reads, which strips UTF-8 BOM transparently. Plain `open(file)` is not acceptable.
2. **Side-effect parsers** that forward bytes verbatim (e.g., `racv_billing`) read the file in binary mode (`open(file, "rb")` or `Path.read_bytes()`) and pass through. They MUST NOT decode.
3. **DataFrame parsers** that need UTF-16 (currently none in production beyond RACV billing's external-sink path) must explicitly declare encoding in `pd.read_csv(encoding=...)`. Default UTF-8.
4. A matched file that fails decode after the relevance gate succeeds → `ParserError`.

## Hudi Staging/Final Key Naming

Each `DirectCSVWriter` instance is bound to one source file and owns a unique writer token. Staging and final keys both include the writer token to prevent cross-file collision in concurrent processing.

```
staging key: sensorDataFiles/.staging/<writer_token>/<batch_index>.csv
final key:   sensorDataFiles/<batch_timestamp>-<writer_token>-<batch_index>.csv
```

Boundary rules:

1. `flush()` writes a CSV batch to the staging key only; final keys not yet visible to Glue.
2. `commit()` is called only after all source-file validations and uploads succeed; copies staging keys to final keys.
3. `abort()` deletes only this writer's staging keys and any final keys this writer has already committed. Must not delete keys owned by other writers.
4. Source file move to `newP/` happens after `commit()` succeeds. If move fails, `abort()` runs to clean up final keys; file routes to `newParseErr/` as `ProcessingError`.

These conventions are part of the contract; changes require explicit spec updates and a new concurrent-writer-safety test.

## Vendor Inventory

The 10 implemented non-NEM parsers and their dispatcher order:

| # | Parser | Filename pattern | Identifier source | Encoding |
|---|---|---|---|---|
| 1 | `noosa_solar_parser` | `RACV_Noosa_Solar_*.csv` | `p:` IDs as column headers | UTF-8 BOM |
| 2 | `envizi_vertical_parser_water` | `Meter_Data_*_Water_*.csv` | column header `<NMI> (kL)` | UTF-8 |
| 3 | `envizi_vertical_parser_electricity` | `Meter_Data_*_Electricity_*.csv` | column header `<NMI> (kWh)` | UTF-8 |
| 4 | `racv_elec_parser` | (RACV electricity specific) | per-parser | UTF-8 |
| 5 | `racv_billing_parser` | `*RACV*Usage and Spend Report*.csv` | N/A (external sink) | UTF-16 LE BOM, binary forward |
| 6 | `bunnings_billing_parser` | `*Bunnings*Usage and Spend*.csv` | row column `Identifier` | UTF-16 LE BOM (decoded then CSV-parsed) |
| 7 | `demand_parser` | `*demand profile*.csv` | row column `Identifier` | UTF-8 |
| 8 | `interval_parser` | (Optima interval files; runs after NEM12 path falls through) | row column or column header | UTF-8 |
| 9 | `envizi_vertical_parser_water_bulk` | bulk water variant | column header | UTF-8 |
| 10 | `green_square_private_wire_schneider_comx_parser` | `<Device Name>_<timestamp>.csv` (filename = site) | metadata section `Device Name` + `Topic ID*` | UTF-8 |

NEM12 path (in `file_processor` directly via `nemreader`):
- `5MINNEM12MDFF_*.csv`, `NEM12MDFF_*.csv` (date-range)
- `NEM12#*`, `nem12#*`, `nem12_*`
- `optima_bunnings_NMI#OPTIMA_*`, `optima_racv_NMI#OPTIMA_*`

### Currently unhandled formats observed in newP/

| Pattern | Format | Status |
|---|---|---|
| `R1746_*.csv`, `R1748_*.csv` | UTF-8 BOM, columns `Associate_Name,Start_Period,End_Period,Group 1..3,Location,Meter,Serial_No,Component,Period_Day,Interval_Start,Interval_End,kWh,kVAh,kVARh,PF,kW,kVA,kVAR,Contains Estimates,Max_Demand_Allowed`, 5-min intervals, quoted strings | No known parser; files appear in `newP/` (likely manual placement or legacy unmaintained path). **No parser will be added in this work.** Document for future investigation. |

If R1746/R1748 starts arriving via the live ingestion pipeline and consistently fails, it will surface as `newParseErr/` volume increase and trigger investigation. The contract does not promise to handle it.

## Quality Column Policy

The Hudi `quality` column is reserved for vendor-provided values. Rules:

1. Vendor provides an explicit quality string (e.g., NEM12 `A`, `E`, `S14`; R1746 `Yes`/`No` for Contains Estimates) → write the string verbatim.
2. Vendor does not provide quality → write NULL. Never write empty string `""`.
3. The pipeline MUST NOT write its own quality markers (e.g., `"I"` for invalid, `"M"` for missing, `"SBM_*"` prefixes). All pipeline-level quality metadata lives in `ParserOutcome` fields and sidecar audit logs, NOT in Hudi columns.
4. Downstream queries treat NULL quality as "vendor unspecified". Filtering on quality is downstream's responsibility.

## Metrics, Logging, and Sidecar Audit

### Per-file structured log fields (always emitted)

- `status`: outcome status
- `reason`: outcome reason (or null)
- `source_row_count`, `candidate_row_count`, `rows_written`, `unmapped_count`, `rows_skipped`
- `skip_reasons`: object mapping `SkipReason` → count
- `unmapped_identifiers`: list of `[kind, value]` pairs (truncated to 50 if larger)
- `unsupported_suffixes`: list

### CloudWatch metrics

Existing:
- `ValidProcessedFiles`: includes `processed`, `processed_empty`, `processed_external`.
- `IrrelevantFiles`: includes only `unmapped`.
- `ParseErrorFiles`: `ParserError` and `ProcessingError`.
- `ProcessedMonitorPoints`, `TotalMonitorPoints`.

New:
- `PartialMappedRatio` per file = `unmapped_count / max(candidate_row_count, 1)`. Dimension: file path.
- `UnsupportedSuffixesFound`: count of files with non-empty `unsupported_suffixes`. Dimension: suffix.
- `RowsSkippedRatio` per file = `rows_skipped / max(source_row_count, 1)`.
- `MalformedValueCount`: count of `unparseable_value` skips. Dimension: file path.
- `UnmappedIdentifierKind`: count by `kind` (e.g., `nmi`, `p_id`).

Alarm thresholds (suggested, tuned per environment):

- `PartialMappedRatio > 0.5` for any file → WARN log; sustained >5 files in 1h → page.
- `RowsSkippedRatio > 0.1` per file → WARN log; >0.5 → page.
- `UnsupportedSuffixesFound > 0` for previously-unseen suffix → page.

### Sidecar audit log

For each file with `rows_skipped > 0` or `unmapped_count > 0`, write a sidecar JSON:

```
s3://hudibucketsrc/audit/<batch_ts>/<source_filename>.skipped.json
```

Schema:

```json
{
  "source_file": "<key>",
  "outcome": {
    "status": "processed",
    "reason": null,
    "source_row_count": 100,
    "candidate_row_count": 95,
    "rows_written": 88,
    "rows_skipped": 7,
    "unmapped_count": 0
  },
  "skip_reasons": {"unparseable_value": 3, "blank_value": 4},
  "unmapped_identifiers": [],
  "unsupported_suffixes": [],
  "skipped_samples": [
    {"row": 47, "column": "Usage", "value": "abc", "reason": "unparseable_value"},
    ...
  ]
}
```

`skipped_samples` is capped at 100 entries per file. When exceeded, append a final entry `{"truncated": true, "total_skipped": <N>}` and emit a metric.

## Behavioral Changes from Legacy

These are intentional and may produce different results than `origin/main` on the same input. Operators must monitor `newP/` / `newIrrevFiles/` / `newParseErr/` volume for 1-2 weeks post-deployment.

| Change | Old behaviour | New behaviour | Daily volume estimate |
|---|---|---|---|
| Empty NEM12 (`100`/`900`-only) | nemreader returns `[]` → `newParseErr/` ("No valid parser") | `processed_empty(reason="no_data_sentinel")` → `newP/` | ~600 files/day shift `newParseErr/` → `newP/` |
| Envizi/Meter_Data sentinel ("No data found") | `newParseErr/` | `processed_empty(reason="no_data_sentinel")` → `newP/` | ~8 files/day shift `newParseErr/` → `newP/` |
| Side-effect parser successes (demand, bunnings_billing, racv_billing) | Returned `[]` → `newIrrevFiles/` | `processed`/`processed_external` → `newP/` | ~70 files/day shift `newIrrevFiles/` → `newP/` |
| `ParserError` mid-dispatch | Dispatcher caught broad Exception, tried next parser, eventually "no valid parser" | Dispatcher stops on first `ParserError`, file goes to `newParseErr/` with the matched parser's error message | rare |
| Read failure after relevance gate | `NotRelevantParser`, dispatcher continues | `ParserError`, dispatcher stops | rare |
| Row-level malformed value in matched file | `pd.to_numeric(errors="coerce")` silently dropped; row absent from Hudi | Same as legacy: row skipped; `rows_skipped` and `skip_reasons["unparseable_value"]` populated | no Hudi-row change |
| All-unknown-suffix DataFrame | `newIrrevFiles/` (no mapped points) | `processed_empty(reason="all_unknown_suffix")` → `newP/` + alarm | rare |
| Partial NMI mapping | `newP/` with no signal | `newP/` with `unmapped_identifiers` and `PartialMappedRatio` metric | unchanged disposition |
| Unknown suffix column | Silent `continue`, no signal | Recorded in `unsupported_suffixes`, alarm on first appearance | unchanged disposition |
| NaN/blank cells in DataFrame written as literal `"nan"` strings | Hudi rows contained `val="nan"` strings | Skipped; not written | -1 row per NaN cell in Hudi |

The combined volume shift means `newParseErr/` should drop by ~600/day and `newP/` should rise by ~700/day. This is the intended outcome of the work.

## Out of Scope Validations

The pipeline accepts any well-formed numeric value and any parseable timestamp. The following are explicitly NOT validated:

| Concern | Rationale |
|---|---|
| Negative values for typically-positive sensors | Some channels (generation, net export) are legitimately negative. |
| Future timestamps | Backfills, edge-device clock skew, timezone normalization can produce future timestamps. |
| Pre-site-start timestamps | Site metadata may be incomplete. |
| Duplicate `(NMI, channel, timestamp)` within one file | Hudi upsert deduplicates downstream. |
| Direct `p:` Neptune ID existence in Neptune | Parser is trusted to produce valid IDs. Orphans are downstream cleanup. |
| Cross-file deduplication | DynamoDB idempotency catches exact-file duplicates. Content-level dedup is downstream. |
| Quality code semantic validation | Quality column accepts any string; downstream interprets `A`/`E`/`S14`/etc. |
| R1746/R1748 format support | No known parser; not in scope of this work. |

If any becomes required, it should be implemented as a separate validator stage between `file_processor` and Glue, not folded into the parser outcome contract.

## Testing Plan

### Per-parser tests

For each parser, cover every applicable row of the Outcome Decision Matrix:
- Each `ParserReason` value the parser can emit.
- Each `SkipReason` value the parser can produce (not all parsers produce all five).
- BOM handling (UTF-8 BOM, plain UTF-8 — and UTF-16 LE for racv_billing).
- Vendor-specific value normalization where applicable (Noosa status strings).

### File processor tests

- DataFrame parser status authority: parser returns `processed_empty`, file_processor with mapped data returns `processed`.
- Final-status calculation: every branch in the calculation block.
- `unsupported_suffixes` populated when columns include unrecognized suffixes; metric emitted.
- `unmapped_identifiers` populated with correct (`kind`, `value`) pairs.
- `processed_empty(reason="all_unknown_suffix")` when DataFrame has only unknown suffixes.
- `processed_empty(reason="all_skipped")` when all rows skipped due to mixed reasons.
- All-or-nothing per-file Hudi commit (existing).
- Source move failure rolls back Hudi commit (existing).
- NEM12 empty-payload `(100/900-only)` → `processed_empty(reason="no_data_sentinel")` → `newP/`. Test must verify file does NOT fall through to non-NEM dispatcher.
- Sidecar audit log written for files with skips; capped at 100 samples.

### Dispatcher tests

- Cheap relevance gate must not trigger `pd.read_csv`. Patch `pd.read_csv` to raise; if invoked during gate phase, the parser misuses the gate.
- Read failure after gate is `ParserError`, not `NotRelevantParser`.
- NEM12 fallback narrowed: only specific exceptions trigger fallback; other exceptions propagate as `ParserError`.
- BOM-aware sniff: cheap gate accepts UTF-8 BOM and plain UTF-8.

### Metrics tests

- `PartialMappedRatio`, `RowsSkippedRatio`, `MalformedValueCount`, `UnsupportedSuffixesFound` emitted with correct values on representative inputs.
- Closed `ParserReason` and `SkipReason` enums: any unknown reason in test fixtures causes test failure.

### Cross-field invariant tests

- For each terminal status, assert the corresponding cross-field invariant holds.
- For `unmapped`, assert all skip reasons in `skip_reasons` are zero.

## Migration Strategy

The original implementation (Tasks 1–9 on branch `feat/parser-outcome-semantics`) is committed. Refinements identified during post-implementation review and production-data audit are tracked as follow-up Tasks 10+ in the implementation plan.

Each follow-up task is independently deployable and additive: it improves observability or removes silent edge cases without breaking the principle that no file currently producing Hudi rows on `origin/main` may stop producing them.

## Open Decisions

- Historical files already moved to `newIrrevFiles/` are out of scope. Manual remediation script if needed.
- `processed_external` currently covers RACV billing only. Removal/replacement is a separate concern.
- Whether to record `idempotency_skip` files with a separate metric is deferred.
- The cap of `unmapped_identifiers` at 100 entries is preliminary; revisit once dashboards exist.
- Whether to investigate R1746/R1748 ingestion path is deferred to a separate ticket.
- The list of `blank_value` markers (currently `""`, whitespace, `"N/A"`, `"NULL"`, `"-"`) may need extension as new vendors arrive. Adding a marker is a config change, not a contract change.
