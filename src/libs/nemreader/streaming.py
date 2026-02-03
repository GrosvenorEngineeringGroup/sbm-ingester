"""
Streaming NEM12 parser for memory-efficient processing.

This module provides a streaming alternative to the standard NEMFile parser,
designed to process large NEM12 files with minimal memory footprint by
yielding data for each NMI/suffix combination as soon as it's complete.
"""

import csv
import io
import logging
import zipfile
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import TextIO

from .nem_objects import EventRecord, NmiDetails, Reading
from .split_days import split_multiday_reads

log = logging.getLogger(__name__)

MINUTES_PER_DAY = 24 * 60


def stream_nem12_file(
    file_path: str,
    split_days: bool = False,
) -> Generator[tuple[str, str, str, list[Reading]]]:
    """
    Stream NEM12 file yielding (nmi, suffix, uom, readings) for each channel.

    Memory efficient: Only holds one NMI/suffix block's data at a time.
    When a new 200 row is encountered, the previous block is yielded and cleared.

    Args:
        file_path: Path to NEM12 file (CSV or ZIP)
        split_days: Whether to split multi-day readings into daily intervals

    Yields:
        Tuples of (nmi, suffix, uom, readings) for each channel block

    Example:
        for nmi, suffix, uom, readings in stream_nem12_file("data.csv"):
            # Process each channel's readings
            for reading in readings:
                print(f"{nmi}-{suffix}: {reading.t_start} = {reading.read_value}")
    """
    with _open_nem_file(file_path) as file_handle:
        yield from _parse_nem12_streaming(file_handle, split_days)


@contextmanager
def _open_nem_file(file_path: str) -> Generator[TextIO]:
    """
    Open NEM file (CSV or ZIP) and return a text file handle.

    Supports both plain CSV files and ZIP archives containing a single CSV.
    """
    try:
        # Try to open as ZIP first
        zf = zipfile.ZipFile(file_path)
        files = zf.namelist()
        if len(files) != 1:
            raise ValueError(f"ZIP must contain exactly one file, found {len(files)}")

        with zf.open(files[0]) as binary_file:
            # Wrap binary stream in text wrapper for line-by-line reading
            text_wrapper = io.TextIOWrapper(binary_file, encoding="utf-8")
            yield text_wrapper

    except zipfile.BadZipFile:
        # Not a ZIP, open as regular CSV
        with Path(file_path).open(encoding="utf-8") as f:
            yield f


def _parse_nem12_streaming(
    file_handle: TextIO,
    split_days: bool,
) -> Generator[tuple[str, str, str, list[Reading]]]:
    """
    Core streaming parser for NEM12 format.

    Parses the file line by line, yielding complete NMI/suffix blocks
    as they are encountered.
    """
    reader = csv.reader(file_handle)

    # Current block state
    current_nmi: str | None = None
    current_suffix: str | None = None
    current_uom: str | None = None
    current_interval: int = 30
    current_meter: str = ""
    # List of lists: each 300 row produces a list of readings
    # This structure allows 400 rows to modify the last 300 row's readings
    current_readings: list[list[Reading]] = []

    for row_num, row in enumerate(reader, start=1):
        if not row:
            continue

        try:
            record_indicator = int(row[0])
        except (ValueError, IndexError):
            continue

        # Handle header (100)
        if record_indicator == 100:
            # Validate NEM12 format
            if len(row) > 1 and row[1] not in ("NEM12", "NEM13"):
                log.warning(f"Unexpected version header: {row[1]}")
            continue

        # Handle NMI details (200)
        if record_indicator == 200:
            # Yield previous block if exists
            if current_nmi and current_readings:
                yield from _yield_channel_data(
                    current_nmi,
                    current_suffix,
                    current_uom,
                    current_readings,
                    split_days,
                )

            # Parse new NMI details
            try:
                nmi_details = _parse_200_row(row)
                current_nmi = nmi_details.nmi
                current_suffix = nmi_details.nmi_suffix
                current_uom = nmi_details.uom
                current_interval = nmi_details.interval_length
                current_meter = nmi_details.meter_serial_number
                current_readings = []
            except (IndexError, ValueError) as e:
                log.error(f"Error parsing 200 row at line {row_num}: {e}")
                continue

        # Handle interval data (300)
        elif record_indicator == 300 and current_nmi:
            try:
                readings = _parse_300_row_to_readings(
                    row,
                    current_interval,
                    current_uom,
                    current_meter,
                    row_num,
                )
                if readings:
                    current_readings.append(readings)
            except (IndexError, ValueError) as e:
                log.error(f"Error parsing 300 row at line {row_num}: {e}")
                continue

        # Handle event record (400)
        elif record_indicator == 400 and current_readings:
            try:
                event = _parse_400_row(row, current_interval)
                # Apply event to the last 300 row's readings
                current_readings[-1] = _apply_event_to_readings(current_readings[-1], event)
            except (IndexError, ValueError) as e:
                log.error(f"Error parsing 400 row at line {row_num}: {e}")
                continue

        # Handle end of file (900)
        elif record_indicator == 900:
            # Yield final block
            if current_nmi and current_readings:
                yield from _yield_channel_data(
                    current_nmi,
                    current_suffix,
                    current_uom,
                    current_readings,
                    split_days,
                )
            # Don't break - some files have multiple 900 records (Powercor)
            current_nmi = None
            current_readings = []

    # Handle files without 900 record
    if current_nmi and current_readings:
        log.warning("Missing end of data (900) row")
        yield from _yield_channel_data(
            current_nmi,
            current_suffix,
            current_uom,
            current_readings,
            split_days,
        )


def _yield_channel_data(
    nmi: str,
    suffix: str,
    uom: str,
    readings_nested: list[list[Reading]],
    split_days: bool,
) -> Generator[tuple[str, str, str, list[Reading]]]:
    """
    Flatten nested readings and yield as a single channel block.
    """
    # Flatten: [[r1, r2], [r3, r4]] -> [r1, r2, r3, r4]
    readings = [r for day_reads in readings_nested for r in day_reads]

    if split_days:
        readings = list(split_multiday_reads(readings))

    yield (nmi, suffix, uom, readings)

    # Clear to help GC
    readings_nested.clear()


def _parse_200_row(row: list) -> NmiDetails:
    """
    Parse NMI details record (200).

    Format: RecordIndicator,NMI,NMIConfiguration,RegisterID,NMISuffix,
            MDMDataStreamIdentifier,MeterSerialNumber,UOM,IntervalLength,
            NextScheduledReadDate
    """
    next_read = None
    if len(row) > 9 and row[9]:
        next_read = _parse_datetime(row[9])

    return NmiDetails(
        nmi=row[1],
        nmi_configuration=row[2],
        register_id=row[3],
        nmi_suffix=row[4],
        mdm_datastream_identifier=row[5],
        meter_serial_number=row[6],
        uom=row[7],
        interval_length=int(row[8]),
        next_scheduled_read_date=next_read,
    )


def _parse_300_row_to_readings(
    row: list,
    interval: int,
    uom: str,
    meter_serial_number: str,
    row_num: int,
) -> list[Reading]:
    """
    Parse interval data record (300) directly to Reading objects.

    Format: RecordIndicator,IntervalDate,IntervalValue1...IntervalValueN,
            QualityMethod,ReasonCode,ReasonDescription,UpdateDateTime,MSATSLoadDateTime
    """
    num_intervals = MINUTES_PER_DAY // interval
    expected_min_cols = 2 + num_intervals + 1  # indicator + date + values + quality

    if len(row) < expected_min_cols:
        log.warning(f"Row {row_num}: Expected {num_intervals} intervals, got {len(row) - 3} values. Skipping.")
        return []

    interval_date = _parse_datetime(row[1])
    if interval_date is None:
        log.warning(f"Row {row_num}: Invalid date '{row[1]}'. Skipping.")
        return []

    last_interval_idx = 2 + num_intervals
    quality_method = row[last_interval_idx] if len(row) > last_interval_idx else ""

    # Optional fields
    reason_code = row[last_interval_idx + 1] if len(row) > last_interval_idx + 1 else ""
    reason_desc = row[last_interval_idx + 2] if len(row) > last_interval_idx + 2 else ""

    # Parse interval values
    interval_delta = timedelta(minutes=interval)
    readings = []

    for i, val in enumerate(row[2:last_interval_idx]):
        t_start = interval_date + (i * interval_delta)
        t_end = t_start + interval_delta
        read_value = _parse_reading_value(val)

        readings.append(
            Reading(
                t_start=t_start,
                t_end=t_end,
                read_value=read_value,
                uom=uom,
                meter_serial_number=meter_serial_number,
                quality_method=quality_method,
                event_code=reason_code,
                event_desc=reason_desc,
                val_start=None,
                val_end=None,
            )
        )

    return readings


def _parse_400_row(row: list, interval_length: int) -> EventRecord:
    """
    Parse interval event record (400).

    Format: RecordIndicator,StartInterval,EndInterval,QualityMethod,
            ReasonCode,ReasonDescription

    Note: Intervals are 1-indexed.
    """
    num_intervals = MINUTES_PER_DAY // interval_length
    start_interval = int(row[1])
    end_interval = int(row[2])

    if not (1 <= start_interval <= num_intervals):
        raise ValueError(f"Invalid start interval: {start_interval}")
    if not (1 <= end_interval <= num_intervals):
        raise ValueError(f"Invalid end interval: {end_interval}")
    if end_interval < start_interval:
        raise ValueError(f"End interval {end_interval} < start {start_interval}")

    return EventRecord(
        start_interval=start_interval,
        end_interval=end_interval,
        quality_method=row[3] if len(row) > 3 else "",
        reason_code=row[4] if len(row) > 4 else "",
        reason_description=row[5] if len(row) > 5 else "",
    )


def _apply_event_to_readings(
    readings: list[Reading],
    event: EventRecord,
) -> list[Reading]:
    """
    Apply event record to readings, updating quality/event fields.

    Event intervals are 1-indexed, so we subtract 1 for 0-indexed list access.
    """
    for i in range(event.start_interval - 1, event.end_interval):
        if i < len(readings):
            old = readings[i]
            readings[i] = Reading(
                t_start=old.t_start,
                t_end=old.t_end,
                read_value=old.read_value,
                uom=old.uom,
                meter_serial_number=old.meter_serial_number,
                quality_method=event.quality_method,
                event_code=event.reason_code,
                event_desc=event.reason_description,
                val_start=old.val_start,
                val_end=old.val_end,
            )
    return readings


def _parse_datetime(record: str | None) -> datetime | None:
    """Parse NEM datetime string (Date8, DateTime12, DateTime14)."""
    if not record:
        return None

    record = record.strip()
    format_strings = {
        8: "%Y%m%d",
        12: "%Y%m%d%H%M",
        14: "%Y%m%d%H%M%S",
    }

    try:
        return datetime.strptime(record, format_strings[len(record)])
    except (ValueError, KeyError):
        return None


def _parse_reading_value(val: str) -> float | None:
    """Convert reading value to float."""
    if not val or val == "":
        return None
    try:
        return float(val)
    except ValueError:
        return None
