"""CSV download utilities for demand profile export."""

from datetime import datetime

import requests
from aws_lambda_powertools import Logger
from optima_shared.config import BIDENERGY_BASE_URL

logger = Logger(service="optima-demand-exporter")

# UTF-8 BOM + ASCII whitespace tolerated before the metadata header.
_CSV_HEADER_PREFIXES = b"\xef\xbb\xbf \t\r\n"


def format_date_for_url(date_str: str) -> str:
    """
    Convert ISO date format to BidEnergy URL format.

    Args:
        date_str: Date in ISO format (YYYY-MM-DD)

    Returns:
        Date formatted for URL (e.g., "29 Apr 2026")

    Note:
        %b is locale-dependent. AWS Lambda runtime defaults to en_US.UTF-8 /
        C.UTF-8 where %b matches "Apr", "Jun", etc. Local dev environments
        with non-English locales would produce different output.
    """
    return datetime.strptime(date_str, "%Y-%m-%d").strftime("%d %b %Y")


def download_demand_csv(
    cookies: str,
    site_id_str: str,
    start_date: str,
    end_date: str,
    project: str,
    nmi: str,
    *,
    country: str = "AU",
) -> tuple[bytes, str] | None:
    """
    Download demand profile CSV from BidEnergy.

    Args:
        cookies: Authentication cookie string
        site_id_str: Site identifier GUID
        start_date: Start date in ISO format (YYYY-MM-DD)
        end_date: End date in ISO format (YYYY-MM-DD)
        project: Project name (used in filename only)
        nmi: NMI identifier (used in filename only — never sent in URL)
        country: Country code ("AU" or "NZ")

    Returns:
        Tuple of (CSV content bytes, suggested filename), or None on failure.
        For "No data found" responses, returns the sentinel CSV bytes (caller uploads
        them for audit retention).
    """
    export_url = f"{BIDENERGY_BASE_URL}/BuyerReport/DemandProfilePartial"

    params = {
        "isCsv": "true",
        "start": format_date_for_url(start_date),
        "end": format_date_for_url(end_date),
        "filter.SiteIdStr": site_id_str,
        "filter.SiteStatus": "Active",
        "filter.commodities": "Electricity",
        "filter.countrystr": country,
    }

    logger.info(
        "Downloading demand CSV",
        extra={
            "site_id": site_id_str,
            "start_date": start_date,
            "end_date": end_date,
            "country": country,
        },
    )

    try:
        response = requests.get(
            export_url,
            params=params,
            headers={"Cookie": cookies},
            timeout=300,
        )
    except requests.Timeout:
        logger.error(
            "Demand CSV download failed: request timeout",
            extra={"project": project, "nmi": nmi, "site_id": site_id_str, "timeout_seconds": 300},
        )
        return None
    except requests.ConnectionError as e:
        logger.error(
            "Demand CSV download failed: connection error",
            extra={"project": project, "nmi": nmi, "error": str(e)},
        )
        return None
    except requests.RequestException as e:
        logger.error(
            "Demand CSV download failed: request error",
            exc_info=True,
            extra={"project": project, "nmi": nmi, "error": str(e)},
        )
        return None

    if response.status_code == 200:
        content_start = response.content[:100].lower()
        is_html = b"<!doctype" in content_start or b"<html" in content_start
        body_starts_like_csv = response.content.lstrip(_CSV_HEADER_PREFIXES).startswith(b"Commodities:")
        content_type = response.headers.get("Content-Type", "").lower()

        if not is_html and (body_starts_like_csv or "csv" in content_type):
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            filename = (
                f"optima_{project.lower()}_demand_profile_NMI#{nmi.upper()}_{start_date}_{end_date}_{timestamp}.csv"
            )

            if b"No data found" in response.content:
                logger.info(
                    "Demand CSV: BidEnergy reported no data for site (uploading sentinel for audit)",
                    extra={
                        "project": project,
                        "nmi": nmi,
                        "site_id": site_id_str,
                        "csv_filename": filename,
                        "size_bytes": len(response.content),
                    },
                )
            else:
                logger.info(
                    "Demand CSV download successful",
                    extra={
                        "project": project,
                        "nmi": nmi,
                        "csv_filename": filename,
                        "size_bytes": len(response.content),
                    },
                )

            return response.content, filename

        logger.error(
            "Demand CSV download failed: received HTML/non-CSV response",
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
            "Demand CSV download failed: authentication/authorization error",
            extra={"project": project, "nmi": nmi, "status_code": response.status_code},
        )
    elif response.status_code == 404:
        logger.error(
            "Demand CSV download failed: site not found",
            extra={"project": project, "nmi": nmi, "site_id": site_id_str, "status_code": 404},
        )
    else:
        logger.error(
            "Demand CSV download failed: unexpected response",
            extra={
                "project": project,
                "nmi": nmi,
                "site_id": site_id_str,
                "status_code": response.status_code,
                "response_preview": response.text[:500] if response.text else "empty",
            },
        )

    return None
