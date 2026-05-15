"""Processing logic for demand profile export."""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, date, datetime, timedelta
from typing import Any

from aws_lambda_powertools import Logger
from optima_shared.auth import login_bidenergy
from optima_shared.config import MAX_WORKERS, OPTIMA_DAYS_BACK, S3_UPLOAD_PREFIX, get_project_config
from optima_shared.dates import PREVIOUS_MONTH_MODE, previous_month_range
from optima_shared.dynamodb import get_site_by_nmi, get_sites_for_project

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


def process_export(
    project: str,
    nmi: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    mode: str | None = None,
) -> dict[str, Any]:
    """
    Process demand profile export for a project.

    Args:
        project: Project name (required)
        nmi: Optional single NMI to export
        start_date: Optional start date (YYYY-MM-DD)
        end_date: Optional end date (YYYY-MM-DD)
        mode: Optional mode flag. "previous_month" overrides start/end dates
            and OPTIMA_DAYS_BACK to cover the full previous calendar month.

    Returns:
        Response dict with statusCode and body. 200 = all OK; 207 = partial
        failure; 4xx = early reject (no retry needed by EventBridge).
    """
    project = project.lower()

    if mode == PREVIOUS_MONTH_MODE:
        start_date, end_date = previous_month_range()
        logger.info(
            "Mode previous_month: overriding date range",
            extra={"project": project, "start_date": start_date, "end_date": end_date},
        )

    # Reject inverted ranges when both dates are explicitly provided
    if start_date and end_date and date.fromisoformat(start_date) > date.fromisoformat(end_date):
        logger.warning(
            "Export rejected: startDate after endDate",
            extra={"project": project, "start_date": start_date, "end_date": end_date},
        )
        return {
            "statusCode": 400,
            "body": f"Invalid range: startDate ({start_date}) > endDate ({end_date})",
        }

    logger.info(
        "Starting demand profile export",
        extra={"project": project, "nmi": nmi, "start_date": start_date, "end_date": end_date},
    )

    config = get_project_config(project)
    if not config:
        logger.error("Export rejected: no credentials for project", extra={"project": project})
        return {
            "statusCode": 400,
            "body": f"No credentials configured for project: {project}",
        }

    if nmi:
        site = get_site_by_nmi(project, nmi)
        if not site:
            logger.warning("Export rejected: NMI not found", extra={"project": project, "nmi": nmi})
            return {
                "statusCode": 404,
                "body": f"NMI {nmi} not found for project {project}",
            }
        sites = [site]
    else:
        sites = get_sites_for_project(project)
        if not sites:
            logger.warning("Export rejected: no sites found", extra={"project": project})
            return {
                "statusCode": 404,
                "body": f"No sites found for project {project}",
            }

    if not start_date and not end_date:
        start_date, end_date = get_date_range()
    else:
        today = datetime.now(UTC).date()
        if not end_date:
            end_date = (today - timedelta(days=1)).isoformat()
        if not start_date:
            end_d = date.fromisoformat(end_date)
            start_date = (end_d - timedelta(days=OPTIMA_DAYS_BACK - 1)).isoformat()

    # Defense in depth: re-check after resolution
    if date.fromisoformat(start_date) > date.fromisoformat(end_date):
        logger.warning(
            "Export rejected: resolved startDate after endDate",
            extra={"project": project, "start_date": start_date, "end_date": end_date},
        )
        return {
            "statusCode": 400,
            "body": f"Invalid range after resolution: startDate ({start_date}) > endDate ({end_date})",
        }

    logger.info(
        "Authenticating with BidEnergy",
        extra={"project": project, "site_count": len(sites)},
    )
    cookies = login_bidenergy(config["username"], config["password"], config["client_id"])
    if cookies is None:
        logger.error("Export failed: authentication failed", extra={"project": project})
        return {
            "statusCode": 401,
            "body": "Failed to authenticate with BidEnergy",
        }

    results: list[dict[str, Any]] = []
    logger.info(
        "Processing sites in parallel",
        extra={"project": project, "site_count": len(sites), "max_workers": MAX_WORKERS},
    )

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_site = {
            executor.submit(
                process_site,
                cookies=cookies,
                nmi=site["nmi"],
                site_id_str=site["siteIdStr"],
                start_date=start_date,
                end_date=end_date,
                project=project,
                country=site.get("country", "AU"),
            ): site
            for site in sites
        }

        for future in as_completed(future_to_site):
            site = future_to_site[future]
            try:
                result = future.result()
                result["project"] = project
                results.append(result)
            except Exception as e:
                logger.error(
                    "Site processing failed with exception",
                    exc_info=True,
                    extra={"project": project, "nmi": site["nmi"], "error": str(e)},
                )
                results.append(
                    {
                        "nmi": site["nmi"],
                        "site_id": site["siteIdStr"],
                        "project": project,
                        "success": False,
                        "error": f"Thread execution failed: {e}",
                    }
                )

    success_count = sum(1 for r in results if r.get("success"))
    error_count = sum(1 for r in results if not r.get("success"))
    no_data_count = sum(1 for r in results if r.get("no_data"))

    logger.info(
        "Demand export completed",
        extra={
            "project": project,
            "total_sites": len(sites),
            "success_count": success_count,
            "error_count": error_count,
            "no_data_count": no_data_count,
        },
    )

    return {
        "statusCode": 200 if error_count == 0 else 207,
        "body": {
            "message": f"Processed {len(sites)} site(s) for {project}",
            "project": project,
            "date_range": {"start": start_date, "end": end_date},
            "success_count": success_count,
            "error_count": error_count,
            "no_data_count": no_data_count,
            "results": results,
        },
    }
