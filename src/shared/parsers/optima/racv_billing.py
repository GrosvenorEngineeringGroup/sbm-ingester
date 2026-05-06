"""RACV "Usage and Spend Report" archiver.

Accepts the monthly RACV billing CSV emitted by BidEnergy and uploads it
unchanged to the gegoptimareports S3 bucket. Returns a processed_external
outcome because no rows are written into the Hudi data lake; RACV billing is
consumed by a downstream system that reads the archived CSV directly.
"""

from __future__ import annotations

from pathlib import Path

import boto3
from aws_lambda_powertools import Logger

from shared.parsers import NotRelevantParser, ParserOutcome, ProcessingError

logger = Logger(service="racv-billing-parser", child=True)

S3_BUCKET = "gegoptimareports"
S3_KEY = "usageAndSpendReports/racvUsageAndSpend.csv"


def racv_billing_parser(file_name: str, error_file_path: str) -> ParserOutcome:
    _ = error_file_path
    path = Path(file_name)
    if "OptimaGenerationData" in path.name:
        raise NotRelevantParser("Not Relevant Parser For File")

    if "RACV-Usage and Spend Report" not in path.name:
        raise NotRelevantParser("Not Valid Optima Usage And Spend File")

    s3 = boto3.client("s3")
    with path.open("rb") as file:
        file_data = file.read()

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY, Body=file_data)
    except Exception as e:
        raise ProcessingError(f"Failed to upload RACV billing report: {e}") from e
    logger.info("racv_billing_uploaded", extra={"bucket": S3_BUCKET, "key": S3_KEY})
    return ParserOutcome(status="processed_external", reason="external_gegoptimareports")
