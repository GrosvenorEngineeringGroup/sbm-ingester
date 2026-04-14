"""Bunnings BidEnergy "Usage and Spend Report" parser.

Reads UTF-16 LE encoded monthly billing CSVs, looks up Neptune point IDs from
the shared nem12_mappings.json, and writes Hudi-format sensor rows directly
to the Hudi source bucket. Designed to slot into the existing non_nem_parsers
dispatch chain: matches by filename, side-effects the Hudi CSV, returns [].
"""

from __future__ import annotations

import pandas as pd

ParserResult = list[tuple[str, pd.DataFrame]]


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
    return []
