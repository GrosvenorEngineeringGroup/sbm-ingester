"""Processing logic for demand profile export."""

from datetime import UTC, datetime, timedelta
from typing import Any

from aws_lambda_powertools import Logger
from optima_shared.config import OPTIMA_DAYS_BACK, S3_UPLOAD_PREFIX

from demand_exporter.downloader import download_demand_csv
from demand_exporter.uploader import upload_to_s3

logger = Logger(service="optima-demand-exporter")


def get_date_range() -> tuple[str, str]:
    """
    Calculate date range based on OPTIMA_DAYS_BACK environment variable.

    Returns:
        Tuple of (start_date, end_date) in ISO format (YYYY-MM-DD).
        End date is always yesterday (yesterday's data is the freshest complete day).
    """
    today = datetime.now(UTC).date()
    end_date = today - timedelta(days=1)
    start_date = end_date - timedelta(days=OPTIMA_DAYS_BACK - 1)
    logger.info(
        "Calculated date range",
        extra={
            "start_date": str(start_date),
            "end_date": str(end_date),
            "days_back": OPTIMA_DAYS_BACK,
        },
    )
    return start_date.isoformat(), end_date.isoformat()


def process_site(
    cookies: str,
    nmi: str,
    site_id_str: str,
    start_date: str,
    end_date: str,
    project: str,
    country: str = "AU",
) -> dict[str, Any]:
    """
    Process a single site: download the demand CSV and upload it to S3.

    Sentinel "No data found" CSVs are still uploaded for audit retention; the
    result["no_data"] flag is set so callers can count them separately.

    Returns:
        Dict with at minimum: nmi, site_id, success, error.
        On success also: filename, s3_key, no_data (bool).
    """
    result: dict[str, Any] = {
        "nmi": nmi,
        "site_id": site_id_str,
        "success": False,
        "error": None,
    }

    download_result = download_demand_csv(
        cookies,
        site_id_str,
        start_date,
        end_date,
        project,
        nmi,
        country=country,
    )
    if download_result is None:
        result["error"] = "Failed to download CSV"
        return result

    csv_content, filename = download_result

    if not upload_to_s3(csv_content, filename):
        result["error"] = "Failed to upload to S3"
        return result

    result["success"] = True
    result["filename"] = filename
    result["s3_key"] = f"{S3_UPLOAD_PREFIX}{filename}"
    result["no_data"] = b"No data found" in csv_content
    return result
