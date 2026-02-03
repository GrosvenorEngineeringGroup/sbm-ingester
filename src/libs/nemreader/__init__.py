"""
nemreader (internal fork)
~~~~~
Parse AEMO NEM12 (interval metering data) and
NEM13 (accumulated metering data) data files

This is a modified version optimized for memory-efficient streaming processing.
"""

import logging
from logging import NullHandler

from .nem_objects import NEMData, NEMReadings, NmiDetails, Reading
from .nem_reader import NEMFile, read_nem_file
from .split_days import split_multiday_reads
from .streaming import stream_nem12_file
from .version import __version__

__all__ = [
    "NEMData",
    "NEMFile",
    "NEMReadings",
    "NmiDetails",
    "Reading",
    "__version__",
    "read_nem_file",
    "split_multiday_reads",
    "stream_nem12_file",
]

# Set default logging handler to avoid "No handler found" warnings.
logging.getLogger(__name__).addHandler(NullHandler())
