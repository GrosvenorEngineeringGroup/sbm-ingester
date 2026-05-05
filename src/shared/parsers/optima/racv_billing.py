"""RACV "Usage and Spend Report" archiver.

Accepts the monthly RACV billing CSV emitted by BidEnergy and uploads it
unchanged to the gegoptimareports S3 bucket. Returns an empty ParserResult
because no rows are written into the Hudi data lake — RACV billing is
consumed by a downstream system that reads the archived CSV directly.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import boto3
from aws_lambda_powertools import Logger

if TYPE_CHECKING:
    from shared.parsers import ParserResult

logger = Logger(service="racv-billing-parser", child=True)


def racv_billing_parser(file_name: str, error_file_path: str) -> ParserResult:
    if "OptimaGenerationData" in file_name:
        raise Exception("Not Relevant Parser For File")

    if "RACV-Usage and Spend Report" not in file_name:
        raise Exception("Not Valid Optima Usage And Spend File")

    # boto3 will use IAM role or env vars — no hardcoding creds
    s3 = boto3.client("s3")
    S3_BUCKET = "gegoptimareports"
    S3_KEY = "usageAndSpendReports/racvUsageAndSpend.csv"

    with Path(file_name).open("rb") as file:
        file_data = file.read()

    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY, Body=file_data)
    return []
