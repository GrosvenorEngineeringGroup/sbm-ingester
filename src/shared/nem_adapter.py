"""
Adapter module for nemreader library.

This module provides a compatibility layer between the external nemreader library (v0.9.2+)
and the internal API expected by gemsDataParseAndWrite.py.

Key difference: External nemreader's output_as_data_frames returns column names like "E1",
but the internal code expects "E1_kWh" (suffix_unit format) to extract unit information.
"""

import logging

import pandas as pd
from aws_lambda_powertools import Logger
from nemreader import NEMFile
from nemreader.split_days import split_multiday_reads

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
