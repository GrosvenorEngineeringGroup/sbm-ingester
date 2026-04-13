# Optima Interval Exporter — Migration to BidEnergy NEM12 Endpoint

**Date:** 2026-04-13
**Status:** Draft — pending user review
**Scope:** `src/functions/optima_exporter/interval_exporter/**`, related tests, Terraform env var
**Non-scope:** File processor, NEM12 adapter, other parsers, billing exporter, DynamoDB schema, IAM policies, authentication code path

---

## 1. Background

### 1.1 Current flow

The `optima-interval-exporter` Lambda runs daily per project (bunnings, racv) and pulls interval electricity usage from BidEnergy for every site in DynamoDB. Today it calls:

```
GET https://app.bidenergy.com/BuyerReport/ExportActualIntervalUsageProfile
```

which returns a flat CSV (`BuyerShortName,Country,Commodity,Identifier,...,Usage,Generation,...`). The CSV is uploaded to `s3://sbm-file-ingester/newTBP/` where the main `sbm-files-ingester` picks it up, fails the NEM12 parser, falls through to `get_non_nem_df()`, matches `optima_parser`, which prepends `Optima_` to the raw BidEnergy identifier so the resulting NMI (`Optima_<id>`) matches Neptune mapping keys and gets written to the Hudi data lake.

### 1.2 Why migrate

BidEnergy exposes a newer endpoint that returns the same data in AEMO NEM12 format:

```
GET https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12
```

Advantages:

- **Uses the industry-standard NEM12 parser** (`src/shared/nem_adapter.py`) that already powers the rest of the ingestion pipeline; removes dependency on the hand-rolled `optima_parser` for this data path.
- **Retains data quality flags** (`A`/`E`/`S*`) in the NEM12 `300`/`400` records — absent from the old flat CSV. Downstream Hudi table already has a `quality` column.
- **Multi-channel in one file** (B1/E1/K1/Q1) with native unit metadata per channel, instead of fixed `Usage`/`Generation` columns.
- **Faster and smaller for long ranges**: single-site 2-year pull = 5.7 MB / 5.9 s (measured).

### 1.3 Empirical validation performed during design

All findings below were verified against production systems before drafting this spec:

| Check | Result |
|---|---|
| New endpoint returns HTTP 200 with same query params | ✅ `application/vnd.csv`, 254 KB for 31-day range |
| Response is valid NEM12 (`100`/`200`/`300`/`900`) | ✅ |
| `nem_adapter.output_as_data_frames()` parses the response | ✅ 1 NMI × 4 channels × 8 928 rows (5-min intervals) |
| 2-year single-site pull timing | ✅ 5.9 s download, 1.8 s parse, 5.7 MB |
| DynamoDB covers AU (468) + NZ (64) sites | ✅ 532 Optima sites total |
| Neptune mapping keys are prefixed `Optima_<nmi>-<ch>` | ✅ 529/532 sites (3 have no mapping at all) |
| Same cookie works for both old and new endpoints | ✅ |

---

## 2. Problem statement

Replacing the endpoint naïvely would silently drop **all** Optima data.

### 2.1 The NMI-prefix mismatch (root cause)

- BidEnergy returns the **bare NMI** in the NEM12 `200` record (e.g. `200,4001348123,...`) — verified for both old CSV and new NEM12 endpoints.
- `nem_adapter` reads the `200` record NMI verbatim; no prefix handling exists.
- `file_processor` (`src/functions/file_processor/app.py:465`) looks up `f"{nmi}-{suffix}"` directly in Neptune mappings.
- Neptune mappings for Optima sites are all keyed `Optima_<nmi>-<ch>`; **none** are keyed with bare NMI (cross-checked against all 532 sites).

The old flow worked by accident: the flat CSV never parsed as NEM12, fell through to `optima_parser`, which (at `src/shared/non_nem_parsers.py:152`) wraps identifiers with `f"Optima_{name}"`. This `Optima_` prefix is a Python-layer namespace convention, not something BidEnergy supplies. A file-format switch that lets NEM12 win on first try bypasses the convention entirely.

### 2.2 Other robustness gaps surfaced during review

| # | Item | Current state |
|---|---|---|
| 1 | Default `OPTIMA_DAYS_BACK` | `7` (7-day rolling overlap) — user wants `1` (yesterday only) |
| 2 | Partial-date bug | When only `endDate` is provided, `start_date` is computed from *today* rather than from the provided `endDate`, producing `start > end` for back-fill cases |
| 3 | No `start ≤ end` validation | Non-check lets invalid ranges hit BidEnergy and waste a round-trip |
| 4 | Download timeout | `120 s` — marginal for 2-year pulls during traffic spikes |
| 5 | Content-type brittle | `application/vnd.csv` matches none of `"text/csv"` / `"application/csv"` — only the `not is_html` fall-through saves it |
| 6 | No NEM12 structural validation after download | Empty / truncated responses silently uploaded |

---

## 3. Goals and non-goals

### 3.1 Goals

1. Download interval data via the new NEM12 endpoint for every Optima site (AU and NZ).
2. Keep the external contract of `optima-interval-exporter` Lambda unchanged: same event shape, same S3 upload location and filename convention, same DynamoDB schema, same credential env vars, same EventBridge schedules.
3. Change default export range to **yesterday only** (1 day).
4. Continue to support arbitrary historical back-fill via `startDate` / `endDate` event parameters (up to Lambda 900 s budget).
5. Ensure Optima-namespace data lands in the existing `Optima_<nmi>-<ch>` Neptune entries — no regressions for the Hudi data lake consumer.
6. Leave non-Optima NEM12 files (AEMO MDFF pushes, building sensors, etc.) completely untouched by any code in this change.
7. Fix the partial-date bug and add `start ≤ end` validation.
8. Make downloader robust against the new endpoint's `application/vnd.csv` content type and malformed NEM12 responses.

### 3.2 Non-goals

- Billing exporter (`billing_exporter/**`) — out of scope.
- Login endpoint / credential-in-URL concern — tracked as a separate hardening item.
- Retry-on-401 (session refresh mid-run) — deferred.
- Transient network retries — deferred.
- IAM least-privilege tightening (removing unused `dynamodb:Scan`) — deferred.
- Changes to `file_processor`, `nem_adapter`, `non_nem_parsers`, Neptune mapping, DynamoDB schema.
- Chunking long ranges across multiple Lambda invocations — 2-year × 532 sites fits in one 900 s run (measurements in §1.3).

---

## 4. Design

### 4.1 Principle: namespace isolation happens at the *source*, not at ingest time

`newTBP/` receives NEM12 files from many unrelated sources. Examples seen in the archived-processed directory:

- `optima_bunnings_NMI#Optima_*_*.csv` — this migration's output
- `5MINNEM12MDFF_*` — AEMO-standard MDFF pushes (must stay bare NMI)
- `Building_*`, `Centre_*` — other project sensor exports

Adding an `Optima_` prefix in `file_processor` (downstream) would pollute every non-Optima NEM12 file. Instead, the prefix is applied **only where the file is born** — inside the optima exporter's downloader. Other file sources do not import, call, or touch this code.

### 4.2 Endpoint and parameter change

```
- GET https://app.bidenergy.com/BuyerReport/ExportActualIntervalUsageProfile
+ GET https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12
```

All eight query parameters stay identical (`nmi`, `isCsv`, `start`, `end`, `filter.SiteIdStr`, `filter.commodities`, `filter.countrystr`, `filter.SiteStatus`). `filter.countrystr=AU|NZ` is the same enum. Cookie authentication flow and date format (`dd MMM yyyy`) are unchanged.

### 4.3 NEM12 content rewrite (core change)

A new helper in `downloader.py`:

```python
_NEM12_200_RE: Final = re.compile(rb"^200,([^,]+),", re.MULTILINE)


def _prefix_nmi_in_nem12(content: bytes, prefix: str) -> bytes:
    """
    Rewrite the NMI field of every `200` record in a NEM12 file by prepending `prefix`.

    BidEnergy emits the bare NMI; Optima data in this project lives under the
    `Optima_` namespace in Neptune and DynamoDB. Applying the prefix here keeps
    downstream code (nem_adapter, file_processor) oblivious to the convention —
    it just sees a NEM12 file whose 200-record NMI already matches Neptune.
    """
    if not content.startswith(b"100,"):
        raise ValueError("Input is not a NEM12 file (missing 100 header)")

    prefix_bytes = prefix.encode("ascii")

    def _replace(match: re.Match[bytes]) -> bytes:
        nmi = match.group(1)
        if nmi.startswith(prefix_bytes):  # idempotent
            return match.group(0)
        return b"200," + prefix_bytes + nmi + b","

    return _NEM12_200_RE.sub(_replace, content)
```

Properties:

- **Scoped by import**: only lives in `optima_exporter.interval_exporter.downloader`; a guard test enforces it is not imported elsewhere.
- **Called only when `download_csv` is invoked with a non-empty `nmi_prefix`** (see §4.4).
- **Idempotent**: rerunning against already-prefixed content produces identical output.
- **Byte-level**: no encoding assumptions beyond NEM12's ASCII semantics.
- **Fail-fast on non-NEM12 input** (e.g. HTML error page that slipped past content checks) — raises before any corruption happens.

### 4.4 Explicit `nmi_prefix` parameter on `download_csv`

```python
def download_csv(
    cookies: str,
    site_id_str: str,
    start_date: str,
    end_date: str,
    project: str,
    nmi: str,
    country: str = "AU",
    nmi_prefix: str = "",   # <-- new, default empty = no rewrite
) -> tuple[bytes, str] | None:
```

`processor.process_site` passes `nmi_prefix="Optima_"` explicitly via a module constant:

```python
OPTIMA_NMI_PREFIX = "Optima_"
```

This makes the business convention visible at the call site, guards against future reuse that might accidentally apply the prefix, and keeps `download_csv` reusable in principle.

### 4.5 Content-type handling

The NEM12 endpoint returns `Content-Type: application/vnd.csv`. The existing `text/csv` / `application/csv` substring checks don't match. Replace with:

```python
content_type = response.headers.get("Content-Type", "").lower()
starts_like_nem12 = response.content[:4] == b"100,"
if "csv" in content_type or starts_like_nem12:
    # accept
```

This accepts the new vendor-specific content type *and* any future variant where the body starts with a NEM12 header — while still rejecting HTML error pages (which start with `<!DOCTYPE` or `<html`).

### 4.6 NEM12 structural validation

After content-type check and before upload, a lightweight sanity check:

```python
def _is_valid_nem12_structure(content: bytes) -> bool:
    return (
        content.startswith(b"100,")
        and b"\n200," in content
        and b"\n300," in content
        and content.rstrip().endswith(b"900")
    )
```

Failure logs a structured error with site id and first 500 bytes of the response, returns `None` (same as existing failure path), and the file is not uploaded.

### 4.7 Request timeout

`timeout=120` → `timeout=300`. Two years × one site measured 5.9 s; 300 s leaves ample headroom for back-fill spikes while staying well inside Lambda's 900 s budget.

### 4.8 Processor date-range fixes

#### 4.8.1 Default export range → yesterday only

```python
# config.py — unchanged, env var default is parsed
OPTIMA_DAYS_BACK = int(os.environ.get("OPTIMA_DAYS_BACK", "7"))  # leave source default

# terraform/optima_exporter.tf
OPTIMA_DAYS_BACK = "1"   # was "7"
```

The tf change produces a 1-day run in production. The source default stays at 7 so running the Lambda locally without env overrides behaves conservatively (wider range easier to spot-check than a single day).

#### 4.8.2 Fix partial-date bug

Current (`processor.py:148-152`):

```python
if not end_date:
    end_date = (today - timedelta(days=1)).isoformat()
if not start_date:
    start_date = (today - timedelta(days=OPTIMA_DAYS_BACK)).isoformat()
```

Replace with:

```python
today = datetime.now(UTC).date()
if not end_date:
    end_date = (today - timedelta(days=1)).isoformat()  # yesterday
if not start_date:
    end_d = date.fromisoformat(end_date)
    start_date = (end_d - timedelta(days=OPTIMA_DAYS_BACK - 1)).isoformat()
```

Anchoring `start_date` to the provided `end_date` (not today) keeps back-fill semantics sensible: asking for `endDate=2024-01-15` with default `DAYS_BACK=1` now pulls only `2024-01-15`, not a now-centred window that overshoots.

#### 4.8.3 Reject invalid ranges

```python
if date.fromisoformat(start_date) > date.fromisoformat(end_date):
    return {"statusCode": 400, "body": f"startDate ({start_date}) > endDate ({end_date})"}
```

---

## 5. Architecture and data flow (after migration)

```
EventBridge cron 14:00 Sydney daily
    → Lambda: optima-interval-exporter
        → DynamoDB sbm-optima-config (query by project)
        → for each site in parallel (MAX_WORKERS=10):
            login_bidenergy() → cookie
            download_csv(..., nmi_prefix="Optima_")
                ├─ GET ExportIntervalUsageProfileNem12
                ├─ validate content-type / is_html / is_valid_nem12
                ├─ _prefix_nmi_in_nem12(content, "Optima_")   <-- new
                └─ return (content, filename)
            upload_to_s3(content, filename)
                → s3://sbm-file-ingester/newTBP/optima_<project>_NMI#<OPTIMA_NMI>_<start>_<end>_<ts>.csv

[file-processor path — unchanged]
    S3 event → SQS → sbm-files-ingester
        → output_as_data_frames() succeeds (valid NEM12)
        → NMI in returned dataframes is "Optima_<bare>" (because we rewrote 200 records)
        → lookup Optima_<bare>-<ch> in Neptune mapping → HIT
        → write to hudibucketsrc/sensorDataFiles/
        → Glue DataImportIntoLake → Hudi table
```

### 5.1 File naming compatibility

The filename pattern `optima_<project>_NMI#<NMI>_<start>_<end>_<ts>.csv` is unchanged. `<NMI>` in the filename continues to be the DynamoDB-stored `Optima_<id>` form (already prefixed), so filenames look identical before and after migration. The only content-level change is that the `200` record inside the file now also carries the prefix.

### 5.2 Idempotency at the Hudi layer

The Hudi data lake keys on `(sensorId, ts)`. Re-processing the same day (e.g. two overlapping exports, or this migration running against a day that was already ingested via the old path) produces *upserts*, not duplicates. Equivalent sensors resolved via either path resolve to the same Neptune ID and therefore the same `sensorId` column value. The switch is a drop-in.

---

## 6. File-level change manifest

| File | Change |
|---|---|
| `src/functions/optima_exporter/interval_exporter/downloader.py` | Endpoint URL; timeout 120 → 300; content-type logic; `_prefix_nmi_in_nem12()`; `_is_valid_nem12_structure()`; `download_csv` gets `nmi_prefix` param |
| `src/functions/optima_exporter/interval_exporter/processor.py` | Pass `nmi_prefix="Optima_"` to `download_csv`; fix partial-date anchor; add `start > end` validation; declare `OPTIMA_NMI_PREFIX` constant |
| `terraform/optima_exporter.tf` | `OPTIMA_DAYS_BACK = "1"` for interval exporter |
| `tests/unit/optima_exporter/interval_exporter/test_downloader.py` | Update 11 mocked URLs; add tests for prefix rewrite (idempotent / multiple `200` rows / non-NEM12 input); add content-type test for `application/vnd.csv`; add NEM12 structure-validation tests; add end-to-end test (downloader output parses cleanly via `nem_adapter` and yields `Optima_`-prefixed NMI) |
| `tests/unit/optima_exporter/interval_exporter/test_processor.py` | Update `TestGetDateRange` expectations; update `TestPartialDateParameters::test_process_export_with_only_end_date_uses_default_start` to the corrected semantics; add `test_rejects_start_after_end`; add `test_default_days_back_is_one_in_production_config` (env-var coverage) |
| New: `tests/unit/optima_exporter/interval_exporter/test_prefix_scoping.py` | Guard test — `_prefix_nmi_in_nem12` symbol only appears inside `src/functions/optima_exporter/**` |
| New fixture: `tests/unit/fixtures/optima_bidenergy_nem12_sample.csv` | Real BidEnergy NEM12 response (redacted) for end-to-end parsing tests |

No changes to: `app.py`, `uploader.py`, `optima_shared/**`, shared parsers, Lambda IAM role, DynamoDB schema, SQS config, EventBridge schedules.

---

## 7. Testing strategy

### 7.1 Unit tests (primary coverage)

- **Prefix rewrite**: single `200` record, multiple records, idempotency on already-prefixed input, non-NEM12 input raises `ValueError`, `300` records that start with numeric dates are not touched.
- **Content-type acceptance**: `application/vnd.csv`, `text/csv`, `application/csv`, and NEM12-body-with-html-content-type all accepted; HTML body rejected.
- **NEM12 structural validation**: valid file passes, truncated file (no `900`) fails, missing `200` fails, missing `300` fails, empty body fails.
- **Downloader integration**: mock endpoint returns synthetic NEM12, downloader output passed through `nem_adapter.output_as_data_frames()` yields dataframes whose NMI column is `Optima_<bare>`.
- **Processor date logic**: default (yesterday only) with `DAYS_BACK=1`; only `startDate` given; only `endDate` given (regression test for the fix); both given; invalid range rejected with 400.
- **Scoping guard**: greps the source tree for `_prefix_nmi_in_nem12` and asserts only `optima_exporter` files reference it.

### 7.2 Regression surface

- Existing `test_downloader.py` (11 tests) re-checked with new URL to ensure filename generation, timeout handling, 401/404/500 handling, large-file handling, and country parameter behaviour all carry over unchanged.
- `test_processor.py` tests for parallel processing, MAX_WORKERS, auth-failure 401, NMI-not-found 404, 207 partial failure remain green without modification beyond the partial-date fix.

### 7.3 Manual / staging verification (post-merge, pre-production schedule)

1. Invoke Lambda manually with `{"project": "bunnings", "nmi": "Optima_4001348123", "startDate": "2026-04-10", "endDate": "2026-04-12"}`.
2. Verify the S3 object in `newTBP/` is NEM12 with `200,Optima_4001348123,...` line present.
3. Wait for `sbm-files-ingester` SQS consumption; verify it lands in `newP/` (not `newIrrevFiles/`) and the CloudWatch `sbm-ingester-metrics-log` reports a non-zero monitor-point count.
4. Athena query `SELECT COUNT(*) FROM default.sensordata_default WHERE sensorid = '<neptune-id-for-4001348123>' AND ts BETWEEN '2026-04-10' AND '2026-04-12'` — expect approximately `4 channels × 3 days × 288 intervals = 3456` rows (or the 5-min equivalent given the actual channel count).
5. Repeat against one NZ site to confirm country flag still propagates.

### 7.4 Coverage threshold

Repo pre-push hook enforces ≥ 90 % coverage. Adding new `_prefix_nmi_in_nem12`, `_is_valid_nem12_structure`, and the `nmi_prefix` branch of `download_csv` with tests as above keeps the new code fully covered.

---

## 8. Rollout and rollback

### 8.1 Rollout

Single PR, CI-deployed via existing GitHub Actions workflow on merge to `main`. No Terraform apply beyond the `OPTIMA_DAYS_BACK = "1"` env-var update (part of the same PR's `terraform plan`/`apply` cycle).

Timing: deploy outside the 14:00 Sydney scheduled window. The next day's scheduled run exercises the new path end-to-end.

### 8.2 Rollback

1. Revert the PR in Git.
2. Re-deploy via the same automated workflow.
3. Optional one-time: re-invoke the Lambda with `startDate` / `endDate` to re-pull any day that might have failed during the transition. Hudi upsert makes this safe to re-run.

Because the output S3 location, filename convention, and downstream code path are unchanged, rollback carries no data-migration burden.

---

## 9. Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| BidEnergy NEM12 endpoint behaviour differs between AU and NZ | Low | Medium | Params are a generic filter; no code branch on country. Staging run against one NZ site as part of §7.3 |
| `nem_adapter` chokes on some real BidEnergy NEM12 quirk (extra columns, odd terminators) | Low | Medium | Already parsed 31-day and 2-year samples cleanly; structural validation rejects malformed files before they reach the ingester |
| A yet-unseen Optima site uses an NMI that collides with a bare-NMI Neptune entry from another source | Very low | Low | Cross-check of 532 sites: 0 bare-only matches, 1 dual-mapped (harmless). Namespace separation by design makes collisions benign |
| Session cookie expires mid-run during a 2-year × 532-site back-fill | Low | Low-medium | Max observed full-sweep time ≈ 5 min; ASP.NET session TTLs are typically ≥ 20 min. If this ever bites, add 401-retry-with-relogin (deferred — see §3.2) |
| `application/vnd.csv` content-type changes again on BidEnergy's side | Very low | Low | `"csv" in content_type` plus `starts_like_nem12` body sniff covers foreseeable variants |

---

## 10. Open questions

None — all behavioural choices confirmed with the user during brainstorming:

- Default range: **1 day (yesterday only)**.
- Partial-date bug fix: **include** in this PR.
- `start > end` validation: **include**.
- Timeout bump to 300 s: **include**.
- NEM12 structural validation: **include**.
- Authentication hardening, retry policies, IAM tightening: **defer**.

---

## 11. Acceptance criteria

1. `optima-interval-exporter` with no event overrides (EventBridge-style invocation) pulls yesterday's data for every AU and NZ site in DynamoDB, writes valid NEM12 files into `s3://sbm-file-ingester/newTBP/`, and each file's `200` record contains `Optima_<bare-nmi>`.
2. Manual invocation with arbitrary `startDate` / `endDate` back-fills the requested window for the requested project / optional NMI.
3. Downstream `sbm-files-ingester` processes the files via the standard NEM12 path (not the `non_nem` fallback), populates the `quality` column, and emits non-zero monitor-point counts in `sbm-ingester-metrics-log`.
4. All 487 existing tests still pass; new tests for prefix rewrite, structural validation, content-type, partial-date fix, and scoping guard also pass; coverage ≥ 90 %.
5. Non-Optima NEM12 files in `newTBP/` (e.g. `5MINNEM12MDFF_*`, `Building_*`) continue to be processed with their original NMIs unchanged — verified by inspecting that `_prefix_nmi_in_nem12` is not reachable from any code path outside `optima_exporter`.
