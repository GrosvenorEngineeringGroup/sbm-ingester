# Parser Outcome Contract — Code Gap Analysis

**Status:** Audit 2026-05-06, code at branch `feat/parser-outcome-semantics` HEAD `cb80ceb`
**Spec:** `docs/superpowers/specs/2026-05-06-parser-outcome-semantics-design.md`
**Plan:** `docs/superpowers/plans/2026-05-06-parser-outcome-semantics.md`
**Verdict:** `CONTRACT_PARTIAL_IMPLEMENTATION`

## Summary

The committed code (Tasks 1–9) implements the original outcome contract, which has since been refined by the spec rewrite in commit `cb80ceb`. Tasks 10–19 in the plan describe the refinement work. After a meta-review of the gap analysis itself, four additional gaps (G18–G21) were identified and several severity classifications were corrected.

This document is the authoritative inventory of what the current code violates vs the refined contract, mapped to the existing or new follow-up tasks that fix each.

| Severity | Count | Examples |
|---|---|---|
| BLOCKER | 8 | Missing outcome fields, open enums, row-level `ParserError`, NEM12 fallthrough, `quality=""` writes, missing final-status calc |
| MAJOR | 4 | Missing metrics, silent suffix skip, no audit log, no namespaced identifiers |
| MINOR | 3 | BOM (no current production impact for handled vendors), broad `except` (disposition unchanged), no cross-field invariants |
| OK | 1 | Noosa STATUS_MAP preserved |

A BLOCKER means the contract is currently violated in a way that produces wrong dispositions or loses data observability. A MAJOR means a contract requirement is not met but does not produce wrong outputs today. A MINOR is structural.

---

## Gap Inventory

Each gap cites file:line at HEAD and maps to a remediation task (existing or new).

### G1: ParserOutcome missing 4 fields  · BLOCKER · Task 12

**Spec:** `ParserOutcome` must carry `unmapped_identifiers: tuple[tuple[str, str], ...]`, `unsupported_suffixes: frozenset[str]`, `rows_skipped: int`, `skip_reasons: Counter[SkipReason]`.

**Current:** [src/shared/parsers/outcome.py:21-28](src/shared/parsers/outcome.py:21) defines only 7 fields. The 4 new fields are absent.

**Impact:** L3/L4/L5 partial-recognition signals are not carried back to file_processor. Dashboards, sidecar audit, and metrics all depend on these fields.

### G2: Open `reason` field, no closed `SkipReason` enum  · BLOCKER · Task 12

**Spec:** `ParserReason` is closed `Literal[8]`; `SkipReason` is closed `Literal[5]`.

**Current:** `src/shared/parsers/outcome.py:28` declares `reason: str | None`. No `ParserReason` or `SkipReason` types exist. Parsers use ad-hoc strings (`"blank_values"`, `"all_candidates_unmapped"`, `"no_valid_point_rows"`, `"gegoptimareports"`, `"no_rows"`) — none match the spec enum.

**Impact:** Reason values are unverified. Spec-illegal values pass without test failure.

### G3: Row-level `ParserError` raises in 10 parser sites  · BLOCKER · Task 10 (DataFrame) + Task 11 (side-effect)

**Spec:** Matched parsers must NEVER raise `ParserError` on row-level data quality. Skip-and-count instead.

**Current sites** (each raises on a single bad row):

| File | Line | Trigger |
|---|---|---|
| [optima/interval.py](src/shared/parsers/optima/interval.py:42) | 42 | `_coerce_numeric_column` non-blank coerce failure |
| [optima/interval.py](src/shared/parsers/optima/interval.py:77) | 77 | single timestamp parse failure |
| [optima/demand.py](src/shared/parsers/optima/demand.py:90) | 90, 94 | `_validate_row_shape` extra/missing trailing cell |
| [optima/bunnings_billing.py](src/shared/parsers/optima/bunnings_billing.py:135) | 135, 139 | same row-shape pattern |
| [envizi/vertical_electricity.py](src/shared/parsers/envizi/vertical_electricity.py:24) | 24, 46 | single bad numeric / timestamp |
| [envizi/vertical_water.py](src/shared/parsers/envizi/vertical_water.py:24) | 24, 46 | same |
| [envizi/vertical_water_bulk.py](src/shared/parsers/envizi/vertical_water_bulk.py:24) | 24, 46 | same |
| [racv/elec.py](src/shared/parsers/racv/elec.py:27) | 27, 52 | same |
| [racv/noosa_solar.py](src/shared/parsers/racv/noosa_solar.py:81) | 81-83 | bad numeric in `p:` column |
| [green_square/comx.py](src/shared/parsers/green_square/comx.py:56) | 56, 74 | bad energy / timestamp |

**Impact:** A file with N valid rows + 1 bad row produces 0 Hudi rows + `newParseErr/`. Direct violation of "preserve partially-correct file processing" P0.

### G4: `_candidate_values` raises ProcessingError on row-level issues  · BLOCKER · Task 16

**Spec:** DataFrame consumer MUST NOT raise on row-level. Skip-and-count.

**Current:** [src/functions/file_processor/app.py:184-195](src/functions/file_processor/app.py:184) raises `ProcessingError` for malformed timestamp, missing timestamp, non-numeric value, NaN value.

**Impact:** Same as G3 but at consumer layer; entire file rejected for one bad cell.

### G5: Side-effect parser strict raises  · BLOCKER · Task 11

**Spec:** demand and bunnings_billing parsers must skip-and-count, not reject the whole file.

**Current sites:**

| File | Line | What it does |
|---|---|---|
| [optima/demand.py](src/shared/parsers/optima/demand.py:219) | 219-231 | `if build.invalid_count > 0: raise ParserError` discarding all valid built rows |
| [optima/demand.py](src/shared/parsers/optima/demand.py:262) | 262 | late `raise ParserError("No valid demand candidates")` |
| [optima/demand.py](src/shared/parsers/optima/demand.py:88) | 88-94 | `_validate_row_shape` aborts file on first bad shape |
| [optima/bunnings_billing.py](src/shared/parsers/optima/bunnings_billing.py:247) | 247-259 | `if build.invalid_count > 0: raise ParserError` |
| [optima/bunnings_billing.py](src/shared/parsers/optima/bunnings_billing.py:292) | 292 | late `raise ParserError` |
| [optima/bunnings_billing.py](src/shared/parsers/optima/bunnings_billing.py:133) | 133-139 | `_validate_row_shape` aborts |

**Impact:** A demand or bunnings_billing file with one trailing-comma row throws away all valid Hudi rows for that file.

### G6: NEM12 empty-payload falls through to non-NEM dispatcher  · BLOCKER · Task 13

**Spec:** NEM12 `100`/`900`-only files emit `processed_empty(reason="no_data_sentinel")` directly. Do NOT fall through.

**Current:** [src/functions/file_processor/app.py:564-586](src/functions/file_processor/app.py:564) on empty stream raises `ValueError`, falls through to `output_as_data_frames`, then to `get_non_nem_outcome`. None of the non-NEM parsers accept NEM12 format → `ParserError("No valid parser found")` → `newParseErr/`.

**Impact:** ~600 daily empty NEM12 files (BidEnergy "no data for this NMI on this date") go to `newParseErr/` instead of `newP/`. This is the #1 operational misclassification.

### G7: Cheap relevance gates not BOM-aware AND do full-file parse  · MINOR · Task 14

(Reclassified from MAJOR. Production audit found BOM only in R1746 (unhandled by design) and Noosa Solar (already correctly handles BOM via `encoding="utf-8-sig"`). For the 10 handled vendors, no production data is currently misrouted due to BOM. The "full-parse-before-gate" sub-finding remains a real defect but does not affect disposition.)


**Spec:** Cheap gates use `encoding="utf-8-sig"` and avoid `pd.read_csv` before relevance is confirmed.

**Current:**

| File | Line | Issue |
|---|---|---|
| [optima/demand.py](src/shared/parsers/optima/demand.py:106) | 106, 196 | `Path(...).open(encoding="utf-8")` — no BOM strip |
| [optima/interval.py](src/shared/parsers/optima/interval.py:48) | 48 | `pd.read_csv(file_name)` before relevance check at line 53 — full parse + no encoding |
| [envizi/vertical_*.py](src/shared/parsers/envizi/vertical_electricity.py:36) | 36 (each) | `pd.read_csv(file_name)` before column-set check — full parse + no encoding |
| [racv/elec.py](src/shared/parsers/racv/elec.py:36) | 36 | `pd.read_csv(file_name, skiprows=[0,1])` — full parse + no encoding |
| [green_square/comx.py](src/shared/parsers/green_square/comx.py:15) | 15 | `pd.read_csv(file_name, header=None, nrows=2)` — partial but no encoding |

**OK:** [racv/noosa_solar.py:34](src/shared/parsers/racv/noosa_solar.py:34) uses `encoding="utf-8-sig"`. [optima/racv_billing.py](src/shared/parsers/optima/racv_billing.py) reads binary for byte-forward.

**Impact:** UTF-8 BOM files (R1746-style) might silently fail content sniff. Full-file parse before gate is also a spec violation but doesn't cause wrong output.

### G8: No `rows_skipped` / `skip_reasons` tracking anywhere  · BLOCKER · Task 12

**Spec:** Parsers and file_processor must populate `rows_skipped` and `skip_reasons: Counter[SkipReason]`.

**Current:** No code populates these fields (they don't exist yet). The closest existing concept is `invalid_count` on `DemandBuildResult` ([demand.py:67](src/shared/parsers/optima/demand.py:67)) and `BillingBuildResult` ([bunnings_billing.py:73](src/shared/parsers/optima/bunnings_billing.py:73)) — but it's a single integer mixing all skip reasons, then used to raise (G5), not propagated to outcome.

**Impact:** Skipped rows are completely invisible. No way to dashboard or alarm on partial-data-loss.

### G9: No sidecar audit log writer  · MAJOR · Task 15

**Spec:** `s3://hudibucketsrc/audit/<batch_ts>/<source_filename>.skipped.json` with up to 100 sample tuples; metric on truncation.

**Current:** No code writes audit sidecars. `rg "audit/"` returned no relevant matches.

**Impact:** When dashboards see "rows skipped", operators have no per-row drill-down.

### G10: 5 new metrics not emitted  · MAJOR · Task 15

**Spec:** `PartialMappedRatio`, `RowsSkippedRatio`, `MalformedValueCount`, `UnsupportedSuffixesFound`, `UnmappedIdentifierKind`.

**Current:** [src/functions/file_processor/app.py:720-725](src/functions/file_processor/app.py:720) emits only the legacy 6 metrics. None of the new five.

**Impact:** No alarm path exists for partial-data-loss escalation.

### G11: Pipeline writes empty string into `quality` column  · BLOCKER · NEW Task 20

(Reclassified from MAJOR to BLOCKER. Athena/Presto do NOT coerce `""` to `NULL`. Every existing dashboard or query that filters `WHERE quality IS NULL` silently misclassifies these rows. This is a production-impacting query semantic bug, not just cosmetic.)


**Spec line 570:** "Vendor does not provide quality → write NULL. Never write empty string `""`."

**Current:**

| File | Line | What it writes |
|---|---|---|
| [file_processor/app.py:197](src/functions/file_processor/app.py:197) | 197 | `quality = "" if pd.isna(quality_raw) else str(quality_raw)` |
| [file_processor/app.py:453](src/functions/file_processor/app.py:453) (write_row) | 453 | uses `quality` directly in CSV — empty string lands in Hudi |
| [optima/demand.py:177](src/shared/parsers/optima/demand.py:177) | 177 | constructs Hudi row with empty trailing comma (no vendor quality) |
| [optima/bunnings_billing.py:200](src/shared/parsers/optima/bunnings_billing.py:200) | 200 | same |

**Impact:** Hudi rows get `quality=""` instead of `quality=NULL`. Downstream queries like `WHERE quality IS NULL` won't match these rows.

**This gap is NOT covered by Tasks 10–19. Add as Task 20.**

### G12: Silent skip on unknown suffix columns  · MAJOR · Task 12

**Spec:** Unknown suffixes recorded in `unsupported_suffixes` + alarm metric.

**Current:** [src/functions/file_processor/app.py:611-612](src/functions/file_processor/app.py:611) is `if suffix not in NMI_DATA_STREAM_COMBINED: continue`. Silent. Nothing recorded.

**Impact:** Schema drift (vendor renames a column suffix) goes completely undetected until dashboards complain.

### G13: No namespaced unmapped identifiers  · MAJOR · Task 12

**Spec:** `unmapped_identifiers: tuple[tuple[str, str], ...]` where first element is parser-specific `kind` (`"nmi"`, `"p_id"`, `"comx_topic"`, etc.).

**Current:** No code populates this. The `unmapped_count` integer is incremented at [file_processor/app.py:631](src/functions/file_processor/app.py:631) but the resolved-but-unmapped NMI string at [file_processor/app.py:627](src/functions/file_processor/app.py:627) (`f"{nmi}-{suffix}"`) is discarded.

**Impact:** Operators can't trace partial-mapping back to specific NMIs/p:IDs/topics. Dashboards aggregating identifiers across vendor types would mix kinds.

### G14: No cross-field invariant assertion  · MINOR · NEW Task 21

**Spec:** Multiple invariants per status (e.g., `status="processed" → rows_written ≥ 1`; `sum(skip_reasons.values()) == rows_skipped`).

**Current:** [src/shared/parsers/outcome.py](src/shared/parsers/outcome.py) has no `__post_init__`. Tests do not assert these invariants.

**Impact:** Invalid outcome combinations can be constructed silently — e.g., `ParserOutcome(status="unmapped", rows_written=5)`. Spec calls for these to be impossible.

**This gap is NOT covered by Tasks 10–19. Add as Task 21.**

### G15: NEM12 fallback uses bare `except Exception`  · MINOR · Task 17

(Reclassified from MAJOR. A `RuntimeError`/`AttributeError` from nemreader's internals would silently fall through to non-NEM dispatcher, which then reports "no valid parser" → `newParseErr/`. The disposition outcome is unchanged; only the diagnostic log message differs. Operator response is identical. Task 17 should still narrow the catch for diagnosability, but no production behaviour depends on it.)


**Spec:** Narrow to `(ValueError, NemParseError)` so genuine NEM12 parser bugs don't silently fall through.

**Current:** [src/functions/file_processor/app.py:571,578](src/functions/file_processor/app.py:571) catches bare `Exception`. No `nemreader` exceptions imported.

**Impact:** A `RuntimeError`/`AttributeError` in NEM12 path silently routes the file to non-NEM dispatcher.

### G16: ~25 obsolete tests asserting strict raise  · BLOCKER · within Tasks 10/11

**Spec implication:** Tests that assert `ParserError` on single-row malformed must flip to "rows skipped + count" assertions.

**Current sites (must update with the corresponding parser fix):**

| Test | Lines | Asserts |
|---|---|---|
| [test_interval.py](tests/unit/parsers/optima/test_interval.py) | 53, 71, 115, 124, 150, 159 | `pytest.raises(ParserError)` on bad numeric / timestamp |
| [test_vertical_water.py](tests/unit/parsers/envizi/test_vertical_water.py) | 92 | bad numeric |
| [test_vertical_electricity.py](tests/unit/parsers/envizi/test_vertical_electricity.py) | 66 | bad numeric |
| [test_vertical_water_bulk.py](tests/unit/parsers/envizi/test_vertical_water_bulk.py) | 119 | bad numeric |
| [test_noosa_solar.py](tests/unit/parsers/racv/test_noosa_solar.py) | 102 | bad p: numeric |
| [test_elec.py](tests/unit/parsers/racv/test_elec.py) | 179 | bad numeric |
| [test_comx.py](tests/unit/parsers/green_square/test_comx.py) | 193 | bad energy value |
| [test_demand.py](tests/unit/parsers/optima/test_demand.py) | 148, 344, 366, 378, 388 | malformed shape / "no valid candidates" |
| [test_bunnings_billing.py](tests/unit/parsers/optima/test_bunnings_billing.py) | 440, 490, 509 | same |

**Impact:** After Tasks 10/11 land, these tests fail. They must be updated as part of the same commit (TDD-style) so each task's tests stay green.

### G18: Hudi staging/final key naming divergent from spec  · MAJOR · Task 12 (or new Task 23)

**Spec lines 519-524:** staging key `sensorDataFiles/.staging/<writer_token>/<batch_index>.csv`, final key `sensorDataFiles/<batch_timestamp>-<writer_token>-<batch_index>.csv`. `<batch_index>` is a deterministic counter.

**Current:** [src/functions/file_processor/app.py:462-464](src/functions/file_processor/app.py:462) uses `sensorDataFilesStaging/<token>/...` (no `.staging/` subprefix) and `sensorDataFiles/batch_<ts>_<token>_<random>.csv` with `random.randint(1, 1_000_000)`.

**Impact:** `random` introduces a 1-in-1M collision chance per writer's flushes. Spec's deterministic `<batch_index>` makes collisions impossible by construction.

**Resolution:** Either align spec to current naming (accept the rare collision risk and document it), OR update `DirectCSVWriter` to use deterministic batch index. **Decision deferred** — no production incidents recorded; lowest priority.

### G19: `idempotency_skip` synthesis path absent  · BLOCKER · Task 12 (expand)

**Spec lines 79, 196:** file_processor must synthesize `processed_empty(reason="idempotency_skip")` when DynamoDB idempotency layer reports a previously-processed file.

**Current:** No code references `idempotency_skip` anywhere in `src/`. The DynamoDB idempotency layer does its own routing (move source to `newP/`) without producing a `ParserOutcome`.

**Impact:** The structured-log fields in the spec's "Metrics, Logging, and Sidecar Audit" section (`status`, `reason`, etc.) cannot be emitted for skipped duplicates because no outcome exists for them. Audit and metrics blind for this path.

**Note:** Spec was inconsistent here. Initially `idempotency_skip` was paired with `processed`, which would violate the `processed → rows_written ≥ 1` invariant. Spec corrected to `processed_empty`.

### G20: Final-status calculation block absent in file_processor (DataFrame path)  · BLOCKER · Task 12 (expand)

**Spec lines 460-471:** the file_processor's DataFrame-path final-status calc must explicitly emit `processed_empty(reason="all_skipped")` when `rows_skipped > 0 and rows_written == 0 and candidate_row_count == 0`.

**Current:** [src/functions/file_processor/app.py:564-680](src/functions/file_processor/app.py:564) has no calc block matching the spec's ladder. No `all_skipped` literal in code tree.

**Impact:** A file where all rows fail (mixed reasons) currently leads to undefined disposition — likely a `ParserError` propagation depending on which raise fires first. Spec wants `processed_empty(reason="all_skipped")` → `newP/` + alarm.

**Resolution:** Task 12's step list must include "implement the final-status calc block per spec lines 460-471" explicitly, not just add the fields.

### G21: `all_unknown_suffix` reason emission path absent  · MAJOR · Task 12 (expand)

**Spec lines 76, 179, 466:** when `unsupported_suffixes` is non-empty AND `candidate_row_count == 0`, file_processor emits `processed_empty(reason="all_unknown_suffix")` + alarm.

**Current:** [src/functions/file_processor/app.py:611-612](src/functions/file_processor/app.py:611) silently `continue`s on unrecognized suffix. No escalation to a reason. No alarm.

**Impact:** Schema drift (vendor renames a suffix, all columns become "unknown") goes silently to `processed_empty` with no reason at all, no alarm, no operator visibility. The alarm path is the only signal that schema drift has happened.

**Resolution:** Task 12's step list must include "in file_processor's final-status calc, when only unknown suffixes are present, set reason='all_unknown_suffix' and emit `UnsupportedSuffixesFound` metric".

### G17: Vendor-specific value normalization preserved · OK

**Spec:** Per-parser vendor string mapping is allowed (Noosa Solar's `FRONIUS_MODE_MAP`).

**Current:** [racv/noosa_solar.py:11-25](src/shared/parsers/racv/noosa_solar.py:11) defines `FRONIUS_MODE_MAP`, applied at line 95. Spec endorses this.

**Impact:** None. Spec-compliant.

---

## Gap → Task Mapping

| Gap | Severity | Task | Status |
|---|---|---|---|
| G1 (4 fields) | BLOCKER | 12 | Plan covers |
| G2 (closed enums) | BLOCKER | 12 | Plan covers |
| G3 (10 parser raises) | BLOCKER | 10 + 11 | Plan covers |
| G4 (_candidate_values raise) | BLOCKER | 16 | Plan covers |
| G5 (side-effect strict) | BLOCKER | 11 | Plan covers |
| G6 (NEM12 fallthrough) | BLOCKER | 13 | Plan covers — **also handle NEM13** |
| G7 (BOM + full-parse-in-gate) | MINOR | 14 | Plan covers (full-parse restructure is the real value here) |
| G8 (no skip tracking) | BLOCKER | 12 | Plan covers |
| G9 (no audit sidecar) | MAJOR | 15 | Plan covers |
| G10 (5 new metrics) | MAJOR | 15 | Plan covers |
| G11 (`quality=""` writes) | **BLOCKER** | 20 | Plan covers — **also explicit `write_row` signature change + Athena verification** |
| G12 (silent suffix skip) | MAJOR | 12 | Plan covers |
| G13 (no namespaced identifiers) | MAJOR | 12 | Plan covers |
| G14 (no invariants) | MINOR | 21 | Plan covers — **test-only assertions, not `__post_init__` raise** |
| G15 (broad `except`) | MINOR | 17 | Plan covers (diagnostics only) |
| G16 (~25 obsolete tests) | BLOCKER | 10 + 11 | Plan covers — line numbers enumerated |
| G17 (Noosa STATUS_MAP) | OK | — | — |
| **G18 (Hudi key naming)** | MAJOR | deferred | **Decision required** — accept current `random` collision risk or align to spec's deterministic index |
| **G19 (idempotency_skip synthesis)** | BLOCKER | 12 (expand) | Add file_processor synthesis on DynamoDB hit |
| **G20 (final-status calc absent)** | BLOCKER | 12 (expand) | Add the spec's calc ladder explicitly |
| **G21 (`all_unknown_suffix` emission)** | MAJOR | 12 (expand) | Add reason synthesis + alarm in calc ladder |

**Plan coverage gaps (resolved):**
- G11: Task 20 added (quality NULL policy enforcement)
- G14: Task 21 added (cross-field invariants — **test-only**, not `__post_init__` raise, to avoid pipeline-crash deployment risk)
- G7: Task 14 expanded with full-parse-before-gate restructure
- G16: line numbers enumerated in Tasks 10 + 11

**Plan coverage gaps (newly identified, route to Task 12 expansion):**
- G19: idempotency_skip synthesis path
- G20: file_processor's final-status calc ladder
- G21: `all_unknown_suffix` reason emission + alarm

**Plan coverage gaps (decision deferred):**
- G18: Hudi key naming convention. Either align spec to current `random.randint` form or update code to deterministic batch index. No production incident; not blocking deploy.

---

## Recommended Execution Order

The order is dictated by dependency: foundation fields first, then parser-side reverts, then file_processor reverts, then observability, then verification.

| Phase | Tasks | Rationale |
|---|---|---|
| 1. Foundation | 12 (fields + enums) | All later tasks reference `rows_skipped` / `skip_reasons` / `unmapped_identifiers`. Add fields with default empty values first so other tasks don't break. |
| 2. Parser reverts | 10, 11 | Restore permissive coercion. Update tests in same commits (G16). |
| 3. Consumer revert | 16 | `_candidate_values` skip-and-count instead of raise. |
| 4. NEM12 special case | 13 | Direct `processed_empty` for empty payloads. |
| 5. Encoding | 14 | BOM handling + restructure gates that do full-parse. |
| 6. Quality column | 20 (NEW) | Replace `quality=""` with NULL writes. |
| 7. Observability | 12 (continued: populate fields), 15 (audit + metrics) | After fields exist and parsers track, wire into file_processor + sidecar + metrics. |
| 8. Invariants | 21 (NEW) | `__post_init__` validation now that all fields are populated. |
| 9. Exception narrowing audit | 17 | Verify Task 7's narrowing is intact under refined contract. |
| 10. Documentation | 18 | Repo-level CLAUDE.md updates. |
| 11. Full verification | 19 | Lint + format + full test suite + behaviour-shift report. |

Each phase is independently committable. Phase 1 must come first; Phases 2-6 can interleave; Phase 7+ depends on Phases 1-6.

---

## What This Document Is For

This is the **acceptance bar** for the refined contract work. After Tasks 10–21 land, every gap above must be resolved. Re-running the audit (or the contract review prompt with this gap list as input) must produce verdict `CONTRACT_FULLY_IMPLEMENTED`.

If a future change introduces a new gap, add it here as G18+ before fixing in code.
