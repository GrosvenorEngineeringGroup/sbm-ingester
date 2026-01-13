"""
SBM Ingester - File ingestion pipeline for building energy data.

This package processes meter data files (NEM12 and non-NEM formats)
uploaded to S3, transforms them into a standard format, and writes
the output to a data lake.
"""

__version__ = "0.3.0"
__author__ = "Zeyu Chen"
