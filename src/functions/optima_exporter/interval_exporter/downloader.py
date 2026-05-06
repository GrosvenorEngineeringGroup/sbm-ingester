"""CSV download utilities for interval CSV export.

Endpoint: POST /BuyerReport/exportdailyusagecsv
Returns: application/zip wrapping a single CSV (or the 148-byte
"No data is available" sentinel CSV when a site has no data).
"""

import io
import zipfile
from datetime import datetime

import requests
from aws_lambda_powertools import Logger
from optima_shared.config import BIDENERGY_BASE_URL

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


def download_interval_zip(
    cookies: str,
    site_id_str: str,
    start_date: str,
    end_date: str,
    project: str,
    nmi: str,
) -> tuple[bytes, str] | None:
    """Download raw BidEnergy interval ZIP bytes.

    The endpoint returns a ZIP wrapping a CSV, including the valid
    "No data is available" sentinel CSV. This function deliberately does not
    extract or inspect the inner CSV; downstream parser code owns that logic.
    """
    export_url = f"{BIDENERGY_BASE_URL}/BuyerReport/exportdailyusagecsv"
    data = {
        "siteId": site_id_str,
        "start": format_date_for_url(start_date),
        "end": format_date_for_url(end_date),
    }

    logger.info(
        "Downloading interval ZIP",
        extra={
            "project": project,
            "nmi": nmi,
            "site_id": site_id_str,
            "start_date": start_date,
            "end_date": end_date,
        },
    )

    try:
        response = requests.post(
            export_url,
            data=data,
            headers={"Cookie": cookies},
            timeout=300,
        )
    except requests.Timeout:
        logger.error(
            "Interval ZIP download failed: request timeout",
            extra={"project": project, "nmi": nmi, "site_id": site_id_str, "timeout_seconds": 300},
        )
        return None
    except requests.ConnectionError as e:
        logger.error(
            "Interval ZIP download failed: connection error",
            extra={"project": project, "nmi": nmi, "site_id": site_id_str, "error": str(e)},
        )
        return None
    except requests.RequestException as e:
        logger.error(
            "Interval ZIP download failed: request error",
            exc_info=True,
            extra={"project": project, "nmi": nmi, "site_id": site_id_str, "error": str(e)},
        )
        return None

    content_type = response.headers.get("Content-Type", "").lower()

    if response.status_code == 200:
        if "html" in content_type or not response.content.startswith(b"PK"):
            logger.error(
                "Interval ZIP download failed: received HTML/non-ZIP response",
                extra={
                    "project": project,
                    "nmi": nmi,
                    "site_id": site_id_str,
                    "content_type": content_type,
                    "first_bytes": response.content[:100].hex(),
                },
            )
            return None

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        filename = f"optima_{project.lower()}_interval_NMI#{nmi.upper()}_{start_date}_{end_date}_{timestamp}.csv"

        logger.info(
            "Interval ZIP download successful",
            extra={
                "project": project,
                "nmi": nmi,
                "site_id": site_id_str,
                "csv_filename": filename,
                "size_bytes": len(response.content),
            },
        )
        return response.content, filename

    if response.status_code in (401, 403):
        logger.error(
            "Interval ZIP download failed: authentication/authorization error",
            extra={
                "project": project,
                "nmi": nmi,
                "site_id": site_id_str,
                "status_code": response.status_code,
            },
        )
    elif response.status_code == 404:
        logger.error(
            "Interval ZIP download failed: site not found",
            extra={"project": project, "nmi": nmi, "site_id": site_id_str, "status_code": 404},
        )
    else:
        logger.error(
            "Interval ZIP download failed: unexpected response",
            extra={
                "project": project,
                "nmi": nmi,
                "site_id": site_id_str,
                "status_code": response.status_code,
                "content_type": content_type,
                "response_preview": response.text[:500] if response.text else "empty",
            },
        )

    return None
