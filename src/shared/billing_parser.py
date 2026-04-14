"""Bunnings BidEnergy "Usage and Spend Report" parser.

Reads UTF-16 LE encoded monthly billing CSVs, looks up Neptune point IDs from
the shared nem12_mappings.json, and writes Hudi-format sensor rows directly
to the Hudi source bucket. Designed to slot into the existing non_nem_parsers
dispatch chain: matches by filename, side-effects the Hudi CSV, returns [].
"""

from __future__ import annotations

import csv
import io
from pathlib import Path

import pandas as pd
from aws_lambda_powertools import Logger

logger = Logger(service="bunnings-billing-parser", child=True)

ParserResult = list[tuple[str, pd.DataFrame]]


def _decode_utf16_csv(file_path: str) -> list[str]:
    """Decode a UTF-16 LE (with BOM) CSV and normalise line endings.

    Returns a list of logical lines with trailing newline stripped.
    """
    raw = Path(file_path).read_bytes()
    text = raw.decode("utf-16-le")
    if text.startswith("\ufeff"):
        text = text[1:]
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    return text.split("\n")


def _parse_billing_rows(file_path: str) -> list[dict[str, str]]:
    """Skip 7 metadata rows and return data rows as DictReader dicts."""
    lines = _decode_utf16_csv(file_path)
    # Row 1-5: metadata key:value; Row 6-7: blank; Row 8: header; Row 9+: data
    # Feed from row 8 onward (index 7) so DictReader treats row 8 as header.
    data_section = "\n".join(lines[7:])
    reader = csv.DictReader(io.StringIO(data_section))
    return [row for row in reader if row.get("Identifier")]


def _get_nem12_mappings() -> dict:
    """Stub — real implementation arrives in Task 5."""
    return {}


def _process_rows_and_write(rows: list[dict[str, str]], mappings: dict) -> int:
    """Stub — real implementation arrives in Task 4/6."""
    return 0


def bunnings_usage_and_spend_parser(file_name: str, error_file_path: str) -> ParserResult:
    """Parse Bunnings billing CSV and write Hudi sensor rows to S3.

    Args:
        file_name: Local path to the downloaded CSV.
        error_file_path: CloudWatch log group for parse errors (unused here,
            accepted for signature compatibility with other non_nem_parsers).

    Returns:
        Always []. Tells file_processor there are no interval-data NMIs to
        stream; the original CSV is then moved to newIrrevFiles/ by the
        caller. The actual billing data is written as a side effect to
        s3://hudibucketsrc/sensorDataFiles/.

    Raises:
        Exception: If file_name does not look like a Bunnings billing CSV.
            Lets the dispatcher try the next parser in the chain.
    """
    if "Bunnings-Usage and Spend Report" not in file_name:
        raise Exception("Not Bunnings Usage and Spend File")

    rows = _parse_billing_rows(file_name)
    mappings = _get_nem12_mappings()
    rows_written = _process_rows_and_write(rows, mappings)
    logger.info(
        "bunnings_billing_parsed",
        extra={"file": file_name, "source_rows": len(rows), "rows_written": rows_written},
    )
    return []
