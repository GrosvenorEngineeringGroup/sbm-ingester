"""CSV download utilities for NEM12 export."""

import re
from datetime import datetime
from typing import Final

import requests
from aws_lambda_powertools import Logger
from optima_shared.config import BIDENERGY_BASE_URL

logger = Logger(service="optima-nem12-exporter")

# UTF-8 BOM + ASCII whitespace tolerated before the NEM12 100 header.
# ASP.NET stacks may emit BOM after a server-side encoding-config change.
_NEM12_HEADER_PREFIXES: Final[bytes] = b"\xef\xbb\xbf \t\r\n"

# Anchored at line start (re.MULTILINE) so it only matches a real 200 record,
# never numeric data that happens to start with bytes "200," inside a 300 row.
_NEM12_200_RE: Final[re.Pattern[bytes]] = re.compile(rb"^200,([^,]+),", re.MULTILINE)


def _prefix_nmi_in_nem12(content: bytes, *, prefix: str) -> bytes:
    """
    Rewrite the NMI field of every `200` record in a NEM12 file by prepending `prefix`.

    Optima data uses the `Optima_<bare-nmi>` namespace in Neptune mappings, but
    BidEnergy emits the bare NMI. Applying the prefix here keeps downstream parsers
    (nem_adapter, file_processor) oblivious to the convention - they just see a
    NEM12 file whose 200-record NMI already matches Neptune.

    Idempotent: re-running on already-prefixed content produces identical bytes.
    Tolerates a leading UTF-8 BOM and ASCII whitespace before the 100 header.
    Raises ValueError if input is not a NEM12 file.
    """
    if not content.lstrip(_NEM12_HEADER_PREFIXES).startswith(b"100,"):
        raise ValueError("Input is not a NEM12 file (missing 100 header)")

    prefix_bytes = prefix.encode("ascii")

    def _replace(match: re.Match[bytes]) -> bytes:
        nmi = match.group(1)
        if nmi.startswith(prefix_bytes):  # idempotent
            return match.group(0)
        return b"200," + prefix_bytes + nmi + b","

    return _NEM12_200_RE.sub(_replace, content)


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
    *,
    country: str = "AU",
    nmi_prefix: str,
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
        country: Country code ("AU" or "NZ")
        nmi_prefix: Prefix to prepend to NMI fields in 200 records (e.g. "Optima_"); pass "" to skip rewrite

    Returns:
        Tuple of (CSV content bytes, suggested filename), or None if download failed
    """
    # Format dates for URL
    start_formatted = format_date_for_url(start_date)
    end_formatted = format_date_for_url(end_date)

    # Build export URL
    export_url = f"{BIDENERGY_BASE_URL}/BuyerReport/ExportIntervalUsageProfileNem12"

    params = {
        "nmi": "",  # Empty to get all NMIs for the site
        "isCsv": "true",
        "start": start_formatted,
        "end": end_formatted,
        "filter.SiteIdStr": site_id_str,
        "filter.commodities": "Electricity",
        "filter.countrystr": country,
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
            timeout=300,  # Large files may take time
        )

        if response.status_code == 200:
            content_type = response.headers.get("Content-Type", "").lower()

            # HTML detection (responses may have BOM prefix)
            content_start = response.content[:100].lower()
            is_html = b"<!doctype" in content_start or b"<html" in content_start

            # Accept anything whose content-type contains "csv" (text/csv, application/csv,
            # application/vnd.csv...) or whose body sniff begins with NEM12 header bytes.
            body_starts_like_nem12 = response.content.lstrip(b"\xef\xbb\xbf \t\r\n").startswith(b"100,")
            if "csv" in content_type or (not is_html and body_starts_like_nem12):
                # Apply namespace prefix to 200 records if requested
                if nmi_prefix:
                    try:
                        body = _prefix_nmi_in_nem12(response.content, prefix=nmi_prefix)
                    except ValueError as exc:
                        logger.error(
                            "NEM12 prefix rewrite failed",
                            extra={
                                "project": project,
                                "nmi": nmi,
                                "site_id": site_id_str,
                                "error": str(exc),
                                "response_preview": response.content[:500].decode("utf-8", errors="replace"),
                            },
                        )
                        return None
                else:
                    body = response.content

                timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                filename = f"optima_{project.lower()}_NMI#{nmi.upper()}_{start_date}_{end_date}_{timestamp}.csv"
                logger.info(
                    "CSV download successful",
                    extra={
                        "project": project,
                        "nmi": nmi,
                        "csv_filename": filename,
                        "size_bytes": len(body),
                        "rewrote_nmi_prefix": bool(nmi_prefix),
                    },
                )
                return body, filename
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
            extra={"project": project, "nmi": nmi, "site_id": site_id_str, "timeout_seconds": 300},
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
