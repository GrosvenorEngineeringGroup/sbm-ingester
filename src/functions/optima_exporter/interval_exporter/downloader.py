"""CSV download utilities for interval CSV export.

Endpoint: POST /BuyerReport/exportdailyusagecsv
Returns: application/zip wrapping a single CSV (or the 148-byte
"No data is available" sentinel CSV when a site has no data).
"""

import io
import zipfile
from datetime import datetime

from aws_lambda_powertools import Logger

logger = Logger(service="optima-interval-exporter")


def extract_first_csv(zip_bytes: bytes) -> bytes:
    """Open the ZIP and return the bytes of the single inner CSV verbatim.

    No synthesis, no special casing. The 148-byte "No data is available"
    sentinel CSV is returned as-is for audit retention; the parser detects
    and handles the sentinel downstream.

    Raises:
        zipfile.BadZipFile: input is not a valid ZIP.
        ValueError: ZIP contains zero entries (defensive; never observed
            in production samples).
    """
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = zf.namelist()
        if not names:
            raise ValueError("ZIP contains no entries (empty archive)")
        return zf.read(names[0])


def format_date_for_url(date_str: str) -> str:
    """Convert ISO date format to BidEnergy URL format.

    Args:
        date_str: Date in ISO format (YYYY-MM-DD)

    Returns:
        Date formatted for URL (e.g., "29 Apr 2026")

    Note:
        %b is locale-dependent. AWS Lambda Python 3.13 uses C.UTF-8 where %b
        matches "Apr", "Jun", etc.; CI runners are the same. Non-English dev
        locales would produce different output and break local testing.
    """
    return datetime.strptime(date_str, "%Y-%m-%d").strftime("%d %b %Y")
