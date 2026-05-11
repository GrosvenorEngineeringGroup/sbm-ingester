"""ingest_file — process one source file end-to-end inside the idempotent boundary.

All side effects live INSIDE the @idempotent_function boundary so that
duplicate SQS deliveries hit the Powertools cache and do not replay any
state-changing operation.

Contract evolution: deterministic content failures (ParserError) are caught
and RETURNED as ParserOutcome(status="parse_failed", reason="parser_error").
Returned outcomes are cached for 12 h. Transient infrastructure failures
(S3 5xx, DynamoDB throttle, etc.) are RAISED so Powertools deletes the
in-progress record and SQS retry can re-execute.

Decorator order is load-bearing: @tracer.capture_method must be the OUTER
decorator and @idempotent_function the INNER (closest to def). Powertools
docs require this order so cache hits are still captured in X-Ray.
"""

from __future__ import annotations

import tempfile
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from itertools import chain
from pathlib import Path
from typing import TYPE_CHECKING, Any

import boto3
import pandas as pd
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities.idempotency import (
    IdempotencyConfig,
    idempotent_function,
)
from aws_lambda_powertools.utilities.idempotency.serialization.base import (
    BaseIdempotencySerializer,
)

from functions.file_processor.csv_writer import HudiSourceCsvWriter
from functions.file_processor.persistence import InstrumentedDynamoDBPersistenceLayer
from shared.audit import SAMPLE_CAP as AUDIT_SAMPLE_CAP
from shared.audit import write_audit_sidecar
from shared.common import (
    INPUT_BUCKET,
    PARSE_ERR_DIR,
    PROCESSED_DIR,
    UNMAPPED_DIR,
)
from shared.nem_adapter import _is_nem_envelope_only, stream_as_data_frames
from shared.parsers import ParserError, ParserOutcome, ProcessingError
from shared.parsers._mappings import get_nem12_mappings
from shared.parsers.dispatcher import dispatch_non_nem

if TYPE_CHECKING:
    from shared.parsers.outcome import SkipReason
    from shared.source_file import SourceFile

logger = Logger(service="file-processor")
tracer = Tracer(service="file-processor")
metrics = Metrics(namespace="SBM/Ingester")

persistence_layer = InstrumentedDynamoDBPersistenceLayer(
    table_name="sbm-ingester-idempotency",
    key_attr="file_key",
)
idempotency_config = IdempotencyConfig(
    expires_after_seconds=43200,  # 12 hours TTL
)


class _ParserOutcomeIdempotencySerializer(BaseIdempotencySerializer):
    """Serialize ParserOutcome for Powertools idempotency caching.

    ParserOutcome contains a ``frozenset`` (unsupported_suffixes) and a
    ``Counter`` (skip_reasons); Powertools' default JSON encoder cannot
    handle frozensets. ``to_dict`` produces a plain-Python representation;
    ``from_dict`` rehydrates without restoring the non-JSON-friendly types
    (downstream consumers of the cached outcome only read scalar fields).
    """

    def to_dict(self, data: ParserOutcome) -> dict:
        return {
            "status": data.status,
            "reason": data.reason,
            "source_row_count": data.source_row_count,
            "candidate_row_count": data.candidate_row_count,
            "rows_written": data.rows_written,
            "unmapped_count": data.unmapped_count,
            "rows_skipped": data.rows_skipped,
            "unmapped_identifiers": [list(pair) for pair in data.unmapped_identifiers],
            "unsupported_suffixes": sorted(data.unsupported_suffixes),
            "skip_reasons": dict(data.skip_reasons),
        }

    def from_dict(self, data: dict) -> ParserOutcome:
        return ParserOutcome(
            status=data["status"],
            reason=data.get("reason"),
            source_row_count=data.get("source_row_count", 0),
            candidate_row_count=data.get("candidate_row_count", 0),
            rows_written=data.get("rows_written", 0),
            unmapped_count=data.get("unmapped_count", 0),
            rows_skipped=data.get("rows_skipped", 0),
            unmapped_identifiers=tuple(tuple(pair) for pair in data.get("unmapped_identifiers", [])),
            unsupported_suffixes=frozenset(data.get("unsupported_suffixes", [])),
            skip_reasons=Counter(data.get("skip_reasons", {})),
        )


_parser_outcome_serializer = _ParserOutcomeIdempotencySerializer()

S3_WRITE_WORKERS = 4
CSV_FLUSH_ROW_THRESHOLD = 50000

s3_resource = boto3.resource("s3")
s3_client = boto3.client("s3")

NMI_DATA_STREAM_SUFFIX = list("ABCDEFJKLPQRSTUGHYMWVZ")
NMI_DATA_STREAM_CHANNEL = list("123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
NMI_DATA_STREAM_COMBINED = frozenset(i + j for i in NMI_DATA_STREAM_SUFFIX for j in NMI_DATA_STREAM_CHANNEL)

# Narrowed exception tuple for "this is not a NEM12 file or has no payload".
# Anything outside this set propagates so genuine parser bugs surface.
_NEM_FALLTHROUGH_ERRORS: tuple[type[BaseException], ...] = (
    ValueError,
    KeyError,
    IndexError,
    AssertionError,
    UnicodeDecodeError,
    StopIteration,
)


@dataclass(frozen=True)
class DataFrameCandidate:
    ts: Any
    val: float
    quality: str | None


def _is_blank_value(value: Any) -> bool:
    return isinstance(value, str) and value.strip() == ""


def extract_valid_readings(
    df: pd.DataFrame,
    col: str,
    t_start_col: pd.Series,
    quality_col: pd.Series | None = None,
    skip_counter: Counter[SkipReason] | None = None,
    samples_sink: list[dict[str, Any]] | None = None,
) -> list[DataFrameCandidate]:
    """Return valid candidate rows; record row-level skips in ``skip_counter``.

    Per the parser-outcome contract, row-level data quality issues never raise.
    Bad rows are skipped silently with the disqualifying reason recorded in
    ``skip_counter`` (mutated in place when supplied).
    """
    candidates: list[DataFrameCandidate] = []
    value_col = df[col]
    quality_values = quality_col if quality_col is not None else [None] * len(value_col)

    def _record_sample(row_idx: int, raw: Any, reason: str) -> None:
        if samples_sink is None:
            return
        if len(samples_sink) >= AUDIT_SAMPLE_CAP:
            return
        samples_sink.append({"row": row_idx, "column": col, "value": str(raw), "reason": reason})

    for row_idx, (ts_raw, val_raw, quality_raw) in enumerate(zip(t_start_col, value_col, quality_values, strict=False)):
        if pd.isna(val_raw) or _is_blank_value(val_raw):
            if skip_counter is not None:
                skip_counter["blank_value"] += 1
            _record_sample(row_idx, val_raw, "blank_value")
            continue

        ts = pd.to_datetime(ts_raw, errors="coerce")
        if pd.isna(ts):
            if skip_counter is not None:
                skip_counter["unparseable_timestamp"] += 1
            _record_sample(row_idx, ts_raw, "unparseable_timestamp")
            continue

        try:
            val = float(val_raw)
        except (TypeError, ValueError):
            if skip_counter is not None:
                skip_counter["unparseable_value"] += 1
            _record_sample(row_idx, val_raw, "unparseable_value")
            continue

        if pd.isna(val):
            if skip_counter is not None:
                skip_counter["unparseable_value"] += 1
            _record_sample(row_idx, val_raw, "unparseable_value")
            continue

        quality = None if pd.isna(quality_raw) else str(quality_raw)
        candidates.append(DataFrameCandidate(ts=ts, val=val, quality=quality))

    return candidates


def _processed_destination_for_status(status: str) -> str:
    if status in {"processed", "processed_empty", "processed_external"}:
        return PROCESSED_DIR
    if status == "unmapped":
        return UNMAPPED_DIR
    if status == "parse_failed":
        return PARSE_ERR_DIR
    raise ValueError(f"Unsupported parser outcome status: {status}")


def _move_source_file(source_key: str, dest_prefix: str) -> str | None:
    """Copy source under newTBP/<file> to <dest_prefix>/<file>, then delete original."""
    file_name = source_key.split("/")[-1]
    full_source_key = source_key if source_key.startswith("newTBP/") else f"newTBP/{file_name}"
    dest_key = f"{dest_prefix.rstrip('/')}/{file_name}"
    copied_dest = False

    try:
        bucket = s3_resource.Bucket(INPUT_BUCKET)
        bucket.Object(dest_key).copy({"Bucket": INPUT_BUCKET, "Key": full_source_key})
        copied_dest = True
        bucket.Object(full_source_key).delete()
        return dest_key
    except Exception as e:
        if copied_dest:
            try:
                s3_resource.Object(INPUT_BUCKET, dest_key).delete()
            except Exception as cleanup_error:
                logger.warning(
                    "Failed to clean up destination after source move failure",
                    extra={"source": full_source_key, "dest": dest_key, "error": str(cleanup_error)},
                )
        logger.error(
            "File move failed",
            exc_info=True,
            extra={"source": full_source_key, "dest": dest_key, "error": str(e)},
        )
        return None


def _download_to_tmp(source_file: SourceFile, tmp_dir: Path) -> Path:
    file_name = Path(source_file.key).name
    local_path = tmp_dir / file_name
    s3_resource.Bucket(source_file.bucket).download_file(source_file.key, str(local_path))
    return local_path


def _parse_one_file(local_path: Path) -> ParserOutcome:
    """Return a ParserOutcome from streaming parser → non-NEM dispatcher.

    Raises ParserError for both "parser found nothing" and any narrow
    fallthrough error that the non-NEM dispatcher cannot handle either.
    """
    file_path = str(local_path)
    try:
        stream = stream_as_data_frames(file_path, split_days=True)
        first_item = next(stream, None)
        if first_item is None:
            if _is_nem_envelope_only(file_path):
                return ParserOutcome(
                    status="processed_empty",
                    reason="no_data_sentinel",
                    source_row_count=0,
                )
            raise ValueError("No data parsed from file")
        return ParserOutcome(status="processed", dataframes=chain([first_item], stream))  # type: ignore[arg-type]
    except _NEM_FALLTHROUGH_ERRORS:
        return dispatch_non_nem(file_path)


def _process_dataframes(
    outcome: ParserOutcome,
    csv_writer: HudiSourceCsvWriter,
    nem12_mappings: dict,
) -> tuple[ParserOutcome, dict[str, Any]]:
    """Walk parser DataFrames; write rows; build per-file metric / audit accumulators.

    Returns the final outcome (via derive_final) and an accumulators dict
    used downstream for metrics + audit.
    """
    candidate_row_count = 0
    unmapped_count = 0
    rows_written = 0
    mapped_monitor_points_count = 0
    skip_counter: Counter[SkipReason] = Counter(outcome.skip_reasons)
    unsupported_suffixes: set[str] = set(outcome.unsupported_suffixes)
    unmapped_identifiers: set[tuple[str, str]] = set(outcome.unmapped_identifiers)
    skipped_samples: list[dict[str, Any]] = []

    for nmi, df in outcome.dataframes:
        if "t_start" not in df.columns and df.index.name == "t_start":
            df = df.reset_index()
        if "t_start" not in df.columns:
            raise ParserError(f"Missing t_start column for {nmi}")

        t_start_col = df["t_start"]

        for col in df.columns:
            suffix = col.split("_")[0]
            if suffix not in NMI_DATA_STREAM_COMBINED:
                if col not in {"t_start", "t_end", "event_code", "event_desc"} and not col.startswith("quality_"):
                    unsupported_suffixes.add(suffix)
                continue

            quality_col_name = f"quality_{suffix}"
            quality_col = df[quality_col_name] if quality_col_name in df.columns else None
            candidates = extract_valid_readings(df, col, t_start_col, quality_col, skip_counter, skipped_samples)
            if not candidates:
                continue

            candidate_row_count += len(candidates)

            if nmi.startswith("p:"):
                neptune_id = nmi
            else:
                lookup_key = f"{nmi}-{suffix}"
                neptune_id = nem12_mappings.get(lookup_key)

            if neptune_id is None:
                unmapped_count += len(candidates)
                if len(unmapped_identifiers) < 100:
                    unmapped_identifiers.add(("nem12_nmi", f"{nmi}-{suffix}"))
                continue

            mapped_monitor_points_count += 1
            unit_name = col.split("_")[1].lower() if "_" in col else "kwh"

            for candidate in candidates:
                csv_writer.write_row(neptune_id, candidate.ts, candidate.val, unit_name, candidate.quality)
                rows_written += 1
                if csv_writer.row_count >= CSV_FLUSH_ROW_THRESHOLD:
                    csv_writer.flush()

    csv_writer.flush()

    rows_skipped_total = sum(skip_counter.values())
    final_outcome = outcome.derive_final(
        rows_written=rows_written,
        candidate_row_count=candidate_row_count,
        unmapped_count=unmapped_count,
        unsupported_suffixes=frozenset(unsupported_suffixes),
        rows_skipped=rows_skipped_total,
    )
    # Strip non-JSON-serializable fields before returning so the outcome can
    # be cached by Powertools idempotency:
    #   - dataframes (iterator/list of DataFrames): consumed by this loop;
    #     downstream consumers read accumulators, not this field.
    # unsupported_suffixes (frozenset) and skip_reasons (Counter) are kept on
    # the dataclass but Powertools' Encoder cannot serialize them, so we hand
    # Powertools an idempotency-safe shadow via output_serializer (see
    # _IdempotencyOutputSerializer below).
    final_outcome = replace(final_outcome, dataframes=[])
    accumulators = {
        "candidate_row_count": candidate_row_count,
        "unmapped_count": unmapped_count,
        "rows_written": rows_written,
        "rows_skipped": rows_skipped_total,
        "skip_counter": skip_counter,
        "unsupported_suffixes": unsupported_suffixes,
        "unmapped_identifiers": unmapped_identifiers,
        "skipped_samples": skipped_samples,
        "mapped_monitor_points_count": mapped_monitor_points_count,
    }
    return final_outcome, accumulators


def _emit_per_file_metrics(outcome: ParserOutcome, accumulators: dict[str, Any]) -> None:
    if outcome.status == "processed":
        metrics.add_metric(name="ValidProcessedFiles", unit=MetricUnit.Count, value=1)
    elif outcome.status == "unmapped":
        metrics.add_metric(name="IrrelevantFiles", unit=MetricUnit.Count, value=1)
    elif outcome.status == "parse_failed":
        metrics.add_metric(name="ParseErrorFiles", unit=MetricUnit.Count, value=1)
    elif outcome.status == "processed_empty":
        metrics.add_metric(name="ProcessedEmptyFiles", unit=MetricUnit.Count, value=1)

    metrics.add_metric(
        name="ProcessedMonitorPoints",
        unit=MetricUnit.Count,
        value=accumulators.get("mapped_monitor_points_count", 0),
    )

    candidate = accumulators.get("candidate_row_count", 0)
    unmapped = accumulators.get("unmapped_count", 0)
    if candidate > 0:
        metrics.add_metric(name="PartialMappedRatio", unit=MetricUnit.Percent, value=(unmapped / candidate) * 100.0)

    rows_skipped = accumulators.get("rows_skipped", 0)
    source_rows = max(outcome.source_row_count, candidate + rows_skipped)
    if source_rows > 0:
        metrics.add_metric(name="RowsSkippedRatio", unit=MetricUnit.Percent, value=(rows_skipped / source_rows) * 100.0)

    skip_counter: Counter[SkipReason] = accumulators.get("skip_counter", Counter())
    metrics.add_metric(
        name="MalformedValueCount",
        unit=MetricUnit.Count,
        value=int(skip_counter.get("unparseable_value", 0)),
    )

    if accumulators.get("unsupported_suffixes"):
        metrics.add_metric(name="UnsupportedSuffixesFound", unit=MetricUnit.Count, value=1)

    unmapped_identifiers = accumulators.get("unmapped_identifiers", set())
    if unmapped_identifiers:
        kinds: Counter[str] = Counter(kind for kind, _ in unmapped_identifiers)
        for kind, count in kinds.items():
            metrics.add_metric(name=f"UnmappedIdentifierKind_{kind}", unit=MetricUnit.Count, value=count)


def _emit_parser_outcome_log(
    source_file: SourceFile,
    outcome: ParserOutcome,
    accumulators: dict[str, Any],
    duration_ms: float,
    dest_prefix: str,
) -> None:
    logger.info(
        "parser_outcome",
        extra={
            "bucket": source_file.bucket,
            "key": source_file.key,
            "final_status": outcome.status,
            "final_reason": outcome.reason,
            "source_row_count": outcome.source_row_count,
            "candidate_row_count": accumulators.get("candidate_row_count", 0),
            "rows_written": accumulators.get("rows_written", 0),
            "rows_skipped": accumulators.get("rows_skipped", 0),
            "unmapped_count": accumulators.get("unmapped_count", 0),
            "skip_reasons": dict(accumulators.get("skip_counter", Counter())),
            "unsupported_suffixes": sorted(accumulators.get("unsupported_suffixes", set())),
            "unmapped_identifiers_truncated": sorted(accumulators.get("unmapped_identifiers", set()))[:50],
            "destination_prefix": dest_prefix,
            "duration_ms": duration_ms,
        },
    )


def _finalize_parse_failed(
    source_file: SourceFile,
    start_ts: pd.Timestamp,
    reason: str,
) -> ParserOutcome:
    """Build the parse_failed outcome and emit metrics + duration + log.

    Centralizes the per-file finalization sequence shared by all three
    parse_failed branches in ``ingest_file``. The source-file move stays at
    each call site since it has its own failure-visibility concern.
    """
    outcome = ParserOutcome(
        status="parse_failed",
        reason=reason,
        source_row_count=0,
    )
    _emit_per_file_metrics(outcome, {})
    duration_ms = (pd.Timestamp.now() - start_ts).total_seconds() * 1000.0
    metrics.add_metric(name="FileProcessingDurationMs", unit=MetricUnit.Milliseconds, value=duration_ms)
    _emit_parser_outcome_log(source_file, outcome, {}, duration_ms, PARSE_ERR_DIR)
    return outcome


def _move_to_parse_err_or_warn(source_file: SourceFile) -> None:
    """Move source to PARSE_ERR_DIR; WARN-log if the move fails.

    The parse_failed outcome is cached by Powertools idempotency; if the
    source-file move silently fails (S3 throttle, perm error, etc.), the
    file remains in newTBP/ and the cached outcome makes the inconsistency
    invisible on retry. WARN-log so it surfaces in CloudWatch.
    """
    dest = _move_source_file(source_file.key, PARSE_ERR_DIR)
    if dest is None:
        logger.warning(
            "parse_failed outcome cached but source file NOT moved to newParseErr/",
            extra={"bucket": source_file.bucket, "key": source_file.key},
        )


@tracer.capture_method
@idempotent_function(
    data_keyword_argument="source_file",
    persistence_store=persistence_layer,
    config=idempotency_config,
    output_serializer=_parser_outcome_serializer,
)
def ingest_file(source_file: SourceFile) -> ParserOutcome:
    """Process one source file end-to-end inside the idempotent boundary."""
    start_ts = pd.Timestamp.now()

    nem12_mappings = get_nem12_mappings()

    accumulators: dict[str, Any] = {}
    final_outcome: ParserOutcome | None = None

    with tempfile.TemporaryDirectory() as tmp_dir:
        executor = ThreadPoolExecutor(max_workers=S3_WRITE_WORKERS)
        csv_writer = HudiSourceCsvWriter(batch_timestamp=str(uuid.uuid4()), executor=executor)
        try:
            local_path = _download_to_tmp(source_file, Path(tmp_dir))
            try:
                parsed = _parse_one_file(local_path)
            except (ParserError, ProcessingError):
                # Deterministic content failure → cache this outcome so retry
                # does not keep re-attempting the broken file.
                _move_to_parse_err_or_warn(source_file)
                return _finalize_parse_failed(source_file, start_ts, reason="parser_error")

            try:
                if parsed.dataframes:
                    final_outcome, accumulators = _process_dataframes(parsed, csv_writer, nem12_mappings)
                else:
                    final_outcome = parsed

                csv_writer.commit()
            except (ParserError, ProcessingError):
                csv_writer.abort()
                _move_to_parse_err_or_warn(source_file)
                return _finalize_parse_failed(source_file, start_ts, reason="parser_error")
            except Exception:
                # Transient infrastructure failure — abort writer, do NOT
                # move source, raise so Powertools deletes in-progress and
                # SQS retry can re-execute.
                csv_writer.abort()
                logger.error(
                    "Transient failure during ingest; raising for retry",
                    exc_info=True,
                    extra={"bucket": source_file.bucket, "key": source_file.key},
                )
                raise

            try:
                dest_prefix = _processed_destination_for_status(final_outcome.status)
            except ValueError:
                # Unknown status — treat as parse_failed.
                csv_writer.abort()
                _move_to_parse_err_or_warn(source_file)
                return _finalize_parse_failed(source_file, start_ts, reason="processing_error")

            move_dest = _move_source_file(source_file.key, dest_prefix)
            if move_dest is None:
                # Source-move failed AFTER Hudi commit — roll back Hudi and
                # raise so retry re-executes (transient failure assumption).
                csv_writer.abort()
                raise ProcessingError(f"Source-move to {dest_prefix} failed after Hudi commit for {source_file.key}")

            # Audit sidecar — best-effort; never fails the file's disposition.
            if (
                accumulators.get("rows_skipped", 0) > 0
                or accumulators.get("unmapped_count", 0) > 0
                or accumulators.get("unsupported_suffixes")
            ):
                try:
                    write_audit_sidecar(
                        batch_ts=csv_writer.batch_timestamp,
                        source_filename=Path(source_file.key).name,
                        outcome_summary={
                            "status": final_outcome.status,
                            "reason": final_outcome.reason,
                            "source_row_count": final_outcome.source_row_count,
                            "candidate_row_count": accumulators.get("candidate_row_count", 0),
                            "rows_written": accumulators.get("rows_written", 0),
                            "rows_skipped": accumulators.get("rows_skipped", 0),
                            "unmapped_count": accumulators.get("unmapped_count", 0),
                        },
                        skip_reasons=dict(accumulators.get("skip_counter", Counter())),
                        unmapped_identifiers=sorted(accumulators.get("unmapped_identifiers", set())),
                        unsupported_suffixes=sorted(accumulators.get("unsupported_suffixes", set())),
                        skipped_samples=accumulators.get("skipped_samples", []),
                        s3_client=s3_client,
                        total_skipped=accumulators.get("rows_skipped", 0),
                    )
                except Exception as audit_err:
                    logger.warning(
                        "audit_sidecar_write_failed",
                        extra={"key": source_file.key, "error": str(audit_err)},
                    )

            _emit_per_file_metrics(final_outcome, accumulators)
            duration_ms = (pd.Timestamp.now() - start_ts).total_seconds() * 1000.0
            metrics.add_metric(name="FileProcessingDurationMs", unit=MetricUnit.Milliseconds, value=duration_ms)
            _emit_parser_outcome_log(source_file, final_outcome, accumulators, duration_ms, dest_prefix)
            return final_outcome
        finally:
            executor.shutdown(wait=True)
