"""Bunnings BidEnergy "Usage and Spend Report" parser.

Reads UTF-16 LE encoded monthly billing CSVs, looks up Neptune point IDs from
the shared nem12_mappings.json, and writes Hudi-format sensor rows directly
to the Hudi source bucket. Designed to slot into the existing non_nem_parsers
dispatch chain: matches by filename, side-effects the Hudi CSV, returns [].
"""

from __future__ import annotations

import csv
import io
import json
from datetime import datetime
from pathlib import Path

import boto3
import pandas as pd
from aws_lambda_powertools import Logger

logger = Logger(service="bunnings-billing-parser", child=True)

ParserResult = list[tuple[str, pd.DataFrame]]

MAPPINGS_BUCKET = "sbm-file-ingester"
MAPPINGS_KEY = "nem12_mappings.json"

_nem12_mappings_cache: dict | None = None


# (CSV column name, billing suffix used in nem12_mappings key, unit source)
# unit_source: "usage" → Usage Measurement Unit, "spend" → Spend Currency
CSV_FIELD_MAPPING: list[tuple[str, str, str]] = [
    ("Peak", "billing-peak-usage", "usage"),
    ("OffPeak", "billing-off-peak-usage", "usage"),
    ("Shoulder", "billing-shoulder-usage", "usage"),
    ("Total Usage", "billing-total-usage", "usage"),
    ("Total GreenPower", "billing-total-greenpower-usage", "usage"),
    ("Estimated Peak", "billing-estimated-peak-usage", "usage"),
    ("Estimated OffPeak", "billing-estimated-off-peak-usage", "usage"),
    ("Estimated Shoulder", "billing-estimated-shoulder-usage", "usage"),
    ("Total Estimated Usage", "billing-total-estimated-usage", "usage"),
    ("Total Estimated GreenPower", "billing-total-estimated-greenpower-usage", "usage"),
    ("Energy Charge", "billing-energy-charge", "spend"),
    ("Total Network Charge", "billing-network-charge", "spend"),
    ("Environmental Charge", "billing-environmental-charge", "spend"),
    ("Metering Charge", "billing-metering-charge", "spend"),
    ("Other Charge", "billing-other-charge", "spend"),
    ("Total Spend", "billing-total-spend", "spend"),
    ("GreenPower Spend", "billing-greenpower-spend", "spend"),
    ("Estimated Energy Charge", "billing-estimated-energy-charge", "spend"),
    ("Estimated Network Charge", "billing-estimated-network-charge", "spend"),
    ("Estimated Environmental Charge", "billing-estimated-environmental-charge", "spend"),
    ("Estimated Metering Charge", "billing-estimated-metering-charge", "spend"),
    ("Estimated Other Charge", "billing-estimated-other-charge", "spend"),
    ("Total Estimated Spend", "billing-total-estimated-spend", "spend"),
]


def _billing_date_to_ts(date_str: str) -> str | None:
    """Convert 'Mmm YYYY' (e.g. 'Mar 2026') to 'YYYY-MM-01 00:00:00'.

    Returns None if the string does not parse; callers skip such rows.
    """
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str.strip(), "%b %Y")
    except ValueError:
        return None
    return dt.strftime("%Y-%m-01 00:00:00")


def _pick_unit(billing_suffix: str, usage_unit: str, spend_currency: str) -> str:
    """Choose the Hudi unit string based on the billing-field suffix.

    Spend/charge fields use the Spend Currency column (e.g. AUD); everything
    else uses Usage Measurement Unit (e.g. kWh). Returned lowercased to match
    existing Hudi rows (e.g. 'kwh').
    """
    if "charge" in billing_suffix or "spend" in billing_suffix:
        return (spend_currency or "aud").lower()
    return (usage_unit or "kwh").lower()


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
    """Lazy-load nem12_mappings.json from S3 once per Lambda container.

    Cached at module level; lives for the container's warm lifetime. Cold
    starts pay one ~1 MB S3 GET. Mapping refresh happens hourly via the
    sbm-files-ingester-nem12-mappings-to-s3 Lambda — stale containers simply
    miss new NMIs until they recycle, which is acceptable because new NMIs
    skip silently and catch up on the next monthly run.
    """
    global _nem12_mappings_cache
    if _nem12_mappings_cache is None:
        obj = boto3.client("s3").get_object(Bucket=MAPPINGS_BUCKET, Key=MAPPINGS_KEY)
        _nem12_mappings_cache = json.loads(obj["Body"].read())
    return _nem12_mappings_cache


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
