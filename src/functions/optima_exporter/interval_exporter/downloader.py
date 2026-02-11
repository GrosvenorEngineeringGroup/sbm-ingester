"""CSV download utilities for interval data export."""

from datetime import datetime

import requests
from aws_lambda_powertools import Logger
from optima_shared.config import BIDENERGY_BASE_URL

logger = Logger(service="optima-interval-exporter")


def format_date_for_url(date_str: str) -> str:
    """
    Convert ISO date format to BidEnergy URL format.

    Args:
        date_str: Date in ISO format (YYYY-MM-DD)

    Returns:
        Date formatted for URL (dd Mmm YYYY, e.g., "01 Jan 2026")
        Note: requests library will encode spaces as + in the URL
    """
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    return date_obj.strftime("%d %b %Y")


def download_csv(
    cookies: str,
    site_id_str: str,
    start_date: str,
    end_date: str,
    project: str,
    nmi: str,
) -> tuple[bytes, str] | None:
    """
    Download CSV interval usage data from BidEnergy.

    Args:
        cookies: Authentication cookie string
        site_id_str: Site identifier GUID
        start_date: Start date in ISO format (YYYY-MM-DD)
        end_date: End date in ISO format (YYYY-MM-DD)
        project: Project name for filename
        nmi: NMI identifier for filename

    Returns:
        Tuple of (CSV content bytes, suggested filename), or None if download failed
    """
    # Format dates for URL
    start_formatted = format_date_for_url(start_date)
    end_formatted = format_date_for_url(end_date)

    # Build export URL
    export_url = f"{BIDENERGY_BASE_URL}/BuyerReport/ExportActualIntervalUsageProfile"

    params = {
        "nmi": "",  # Empty to get all NMIs for the site
        "isCsv": "true",
        "start": start_formatted,
        "end": end_formatted,
        "filter.SiteIdStr": site_id_str,
        "filter.commodities": "Electricity",
        "filter.countrystr": "AU",
        "filter.SiteStatus": "Active",
    }

    logger.info(
        "Downloading CSV data",
        extra={
            "site_id": site_id_str,
            "start_date": start_date,
            "end_date": end_date,
        },
    )

    try:
        response = requests.get(
            export_url,
            params=params,
            headers={"Cookie": cookies},
            timeout=120,  # Large files may take time
        )

        if response.status_code == 200:
            # Check if response is actually CSV (not an error page)
            content_type = response.headers.get("Content-Type", "")

            # Check for HTML content (may have BOM prefix)
            content_start = response.content[:100].lower()
            is_html = b"<!doctype" in content_start or b"<html" in content_start

            if "text/csv" in content_type or "application/csv" in content_type or not is_html:
                # Generate filename with NMI and timestamp for traceability and uniqueness
                timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                filename = f"optima_{project.lower()}_NMI#{nmi.upper()}_{start_date}_{end_date}_{timestamp}.csv"
                logger.info(
                    "CSV download successful",
                    extra={
                        "project": project,
                        "nmi": nmi,
                        "csv_filename": filename,
                        "size_bytes": len(response.content),
                    },
                )
                return response.content, filename
            logger.error(
                "CSV download failed: received HTML error page instead of CSV",
                extra={
                    "project": project,
                    "nmi": nmi,
                    "site_id": site_id_str,
                    "content_type": content_type,
                    "response_preview": response.text[:500] if response.text else "empty",
                },
            )
        elif response.status_code in (401, 403):
            logger.error(
                "CSV download failed: authentication/authorization error (session may have expired)",
                extra={
                    "project": project,
                    "nmi": nmi,
                    "status_code": response.status_code,
                },
            )
        elif response.status_code == 404:
            logger.error(
                "CSV download failed: site not found (siteIdStr may be invalid)",
                extra={
                    "project": project,
                    "nmi": nmi,
                    "site_id": site_id_str,
                    "status_code": response.status_code,
                },
            )
        else:
            logger.error(
                "CSV download failed: unexpected response",
                extra={
                    "project": project,
                    "nmi": nmi,
                    "site_id": site_id_str,
                    "status_code": response.status_code,
                    "response_preview": response.text[:500] if response.text else "empty",
                },
            )

    except requests.Timeout:
        logger.error(
            "CSV download failed: request timeout",
            extra={"project": project, "nmi": nmi, "site_id": site_id_str, "timeout_seconds": 120},
        )
    except requests.ConnectionError as e:
        logger.error(
            "CSV download failed: connection error",
            extra={"project": project, "nmi": nmi, "error": str(e)},
        )
    except requests.RequestException as e:
        logger.error(
            "CSV download failed: request error",
            exc_info=True,
            extra={"project": project, "nmi": nmi, "error": str(e)},
        )

    return None
