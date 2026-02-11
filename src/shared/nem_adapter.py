"""
Adapter module for nemreader library.

This module provides a compatibility layer between the internal nemreader library
and the API expected by file_processor.

Key features:
- Column names include unit suffix: "E1_kWh", "B1_kWh" etc.
- Supports both batch (output_as_data_frames) and streaming (stream_as_data_frames) modes
- Streaming mode is memory-efficient for large files
"""

import logging
from collections.abc import Generator

import pandas as pd
from aws_lambda_powertools import Logger

# Use internal nemreader fork for streaming support
from libs.nemreader import NEMFile, stream_nem12_file
from libs.nemreader.split_days import split_multiday_reads

log = logging.getLogger(__name__)
logger = Logger(service="nem-adapter", child=True)


def output_as_data_frames(
    file_name: str,
    split_days: bool = True,
) -> list[tuple[str, pd.DataFrame]]:
    """
    Parse NEM12/NEM13 file and return list of (NMI, DataFrame) tuples.

    This adapter maintains compatibility with the internal nemreader output format:
    - Column names include unit suffix: "E1_kWh", "B1_kWh" etc.
    - DataFrame columns: t_start, t_end, quality_method, event_code, event_desc, <channel>_<unit>

    Args:
        file_name: Path to NEM12/NEM13 file (supports .csv and .zip)
        split_days: If True, split readings that span multiple days

    Returns:
        List of tuples: (nmi_string, pandas_dataframe)
    """
    try:
        nf = NEMFile(file_name, strict=False)
        nd = nf.nem_data()
    except Exception as e:
        log.error(f"Failed to parse NEM file {file_name}: {e}")
        raise

    data_frames = []

    for nmi in nd.readings:
        try:
            nmi_df = _build_nmi_dataframe(
                nmi,
                nd.readings[nmi],
                nd.transactions[nmi],
                split_days=split_days,
            )
            if nmi_df is not None:
                data_frames.append((nmi, nmi_df))
        except Exception as e:
            logger.error("Error processing NMI", exc_info=True, extra={"nmi": nmi, "file": file_name, "error": str(e)})
            continue

    return data_frames


def _build_nmi_dataframe(
    nmi: str,
    nmi_readings: dict,
    nmi_transactions: dict,
    split_days: bool = True,
) -> pd.DataFrame | None:
    """
    Build a DataFrame for a single NMI with columns named suffix_unit.

    Args:
        nmi: NMI identifier
        nmi_readings: Dict mapping channel suffix to list of Reading objects
        nmi_transactions: Dict mapping channel suffix to transaction list
        split_days: If True, split multi-day readings

    Returns:
        DataFrame with t_start index and columns for each channel (suffix_unit format)
    """
    channels = list(nmi_transactions.keys())

    if not channels:
        return None

    # Apply split_days transformation if needed
    if split_days:
        for ch in channels:
            nmi_readings[ch] = list(split_multiday_reads(nmi_readings[ch]))

    first_ch = channels[0]
    first_readings = nmi_readings.get(first_ch, [])

    if not first_readings:
        return None

    # Get unit from the first reading
    first_uom = first_readings[0].uom or "kWh"

    # Build base DataFrame with metadata columns
    d = {
        "t_start": [x.t_start for x in first_readings],
        "t_end": [x.t_end for x in first_readings],
        "quality_method": [x.quality_method for x in first_readings],
        "event_code": [x.event_code for x in first_readings],
        "event_desc": [x.event_desc for x in first_readings],
    }

    # Add first channel with suffix_unit column name
    col_name = f"{first_ch}_{first_uom}"
    d[col_name] = [x.read_value for x in first_readings]

    df = pd.DataFrame(data=d, index=d["t_start"])

    # Add additional channels
    for ch in channels[1:]:
        ch_readings = nmi_readings.get(ch, [])
        if not ch_readings:
            continue

        ch_uom = ch_readings[0].uom or "kWh"
        col_name = f"{ch}_{ch_uom}"

        index = [x.t_start for x in ch_readings]
        values = [x.read_value for x in ch_readings]
        ser = pd.Series(data=values, index=index, name=col_name)
        df.loc[:, col_name] = ser

    return df


def stream_as_data_frames(
    file_name: str,
    split_days: bool = True,
) -> Generator[tuple[str, pd.DataFrame]]:
    """
    Stream NEM12 file yielding (NMI, DataFrame) pairs.

    Memory-efficient alternative to output_as_data_frames().
    Only holds one NMI's data in memory at a time.

    Args:
        file_name: Path to NEM12 file (supports .csv and .zip)
        split_days: If True, split readings that span multiple days

    Yields:
        Tuples of (nmi_string, pandas_dataframe)

    Example:
        for nmi, df in stream_as_data_frames("large_file.csv"):
            # Process each NMI's data
            process(nmi, df)
            # Memory is freed after each iteration
    """
    # Accumulate channels for the same NMI
    # Key: nmi, Value: list of (suffix, uom, readings)
    nmi_channels: dict[str, list[tuple[str, str, list]]] = {}
    last_nmi: str | None = None

    for nmi, suffix, uom, readings in stream_nem12_file(file_name, split_days=split_days):
        # If we encounter a new NMI, yield the previous one
        if last_nmi is not None and nmi != last_nmi and last_nmi in nmi_channels:
            df = _build_dataframe_from_channels(nmi_channels[last_nmi])
            if df is not None:
                yield (last_nmi, df)
            del nmi_channels[last_nmi]

        # Accumulate channel data for current NMI
        if nmi not in nmi_channels:
            nmi_channels[nmi] = []
        nmi_channels[nmi].append((suffix, uom, readings))
        last_nmi = nmi

    # Yield remaining NMIs
    for nmi, channels in nmi_channels.items():
        df = _build_dataframe_from_channels(channels)
        if df is not None:
            yield (nmi, df)


def _build_dataframe_from_channels(
    channels: list[tuple[str, str, list]],
) -> pd.DataFrame | None:
    """
    Build a DataFrame from a list of channel data.

    Optimized: Single-pass iteration over readings for better performance.

    Args:
        channels: List of (suffix, uom, readings) tuples

    Returns:
        DataFrame with t_start index and columns for each channel (suffix_unit format)
    """
    if not channels:
        return None

    # Use first channel as base
    first_suffix, first_uom, first_readings = channels[0]

    if not first_readings:
        return None

    # Pre-allocate lists for single-pass iteration
    n = len(first_readings)
    t_start_list = [None] * n
    t_end_list = [None] * n
    quality_list = [None] * n
    event_code_list = [None] * n
    event_desc_list = [None] * n
    first_values = [None] * n

    # Single pass over first channel readings
    for i, reading in enumerate(first_readings):
        t_start_list[i] = reading.t_start
        t_end_list[i] = reading.t_end
        quality_list[i] = reading.quality_method
        event_code_list[i] = reading.event_code
        event_desc_list[i] = reading.event_desc
        first_values[i] = reading.read_value

    # Build DataFrame data dict
    first_col_name = f"{first_suffix}_{first_uom or 'kWh'}"
    d = {
        "t_start": t_start_list,
        "t_end": t_end_list,
        "quality_method": quality_list,
        "event_code": event_code_list,
        "event_desc": event_desc_list,
        first_col_name: first_values,
    }

    # Add additional channels with single-pass iteration
    for suffix, uom, readings in channels[1:]:
        if not readings:
            continue

        col_name = f"{suffix}_{uom or 'kWh'}"
        # Single pass: extract both index and values
        index = [None] * len(readings)
        values = [None] * len(readings)
        for i, reading in enumerate(readings):
            index[i] = reading.t_start
            values[i] = reading.read_value

        # Store for later DataFrame assignment
        d[f"_idx_{col_name}"] = index
        d[col_name] = values

    # Create base DataFrame
    df = pd.DataFrame(data={k: v for k, v in d.items() if not k.startswith("_idx_")}, index=t_start_list)

    # Assign additional channels using stored indices
    for suffix, uom, readings in channels[1:]:
        if not readings:
            continue
        col_name = f"{suffix}_{uom or 'kWh'}"
        idx_key = f"_idx_{col_name}"
        if idx_key in d:
            ser = pd.Series(data=d[col_name], index=d[idx_key], name=col_name)
            df.loc[:, col_name] = ser

    return df
