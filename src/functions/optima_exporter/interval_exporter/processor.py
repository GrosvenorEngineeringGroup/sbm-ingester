"""Processing logic for interval data export."""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta
from typing import Any

from aws_lambda_powertools import Logger
from optima_shared.auth import login_bidenergy
from optima_shared.config import MAX_WORKERS, OPTIMA_DAYS_BACK, S3_UPLOAD_PREFIX, get_project_config
from optima_shared.dynamodb import get_site_by_nmi, get_sites_for_project

from interval_exporter.downloader import download_csv
from interval_exporter.uploader import upload_to_s3

logger = Logger(service="optima-interval-exporter")


def get_date_range() -> tuple[str, str]:
    """
    Calculate date range based on OPTIMA_DAYS_BACK environment variable.

    Returns:
        Tuple of (start_date, end_date) in ISO format (YYYY-MM-DD)
    """
    today = datetime.now(UTC).date()
    end_date = today - timedelta(days=1)  # Yesterday (complete data)
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
) -> dict[str, Any]:
    """
    Process a single site: download CSV and upload to S3.

    Args:
        cookies: Authentication cookie string
        nmi: NMI identifier for the site
        site_id_str: Site identifier GUID
        start_date: Start date in ISO format
        end_date: End date in ISO format
        project: Project name for logging

    Returns:
        Dict with processing result
    """
    result: dict[str, Any] = {
        "nmi": nmi,
        "site_id": site_id_str,
        "success": False,
        "error": None,
    }

    # Download CSV
    download_result = download_csv(cookies, site_id_str, start_date, end_date, project, nmi)
    if download_result is None:
        result["error"] = "Failed to download CSV"
        return result

    csv_content, filename = download_result

    # Upload to S3
    if upload_to_s3(csv_content, filename):
        result["success"] = True
        result["filename"] = filename
        result["s3_key"] = f"{S3_UPLOAD_PREFIX}{filename}"
    else:
        result["error"] = "Failed to upload to S3"

    return result


def process_export(
    project: str,
    nmi: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    """
    Process interval data export for a project.

    Args:
        project: Project name (required)
        nmi: Optional single NMI to export
        start_date: Optional start date (YYYY-MM-DD)
        end_date: Optional end date (YYYY-MM-DD)

    Returns:
        Response dict with processing results
    """
    project = project.lower()

    logger.info(
        "Starting interval export",
        extra={"project": project, "nmi": nmi, "start_date": start_date, "end_date": end_date},
    )

    # Get project configuration
    config = get_project_config(project)
    if not config:
        logger.error("Export rejected: no credentials for project", extra={"project": project})
        return {
            "statusCode": 400,
            "body": f"No credentials configured for project: {project}",
        }

    # Determine sites to process
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

    # Determine date range
    if not start_date and not end_date:
        # Neither provided, use default range
        start_date, end_date = get_date_range()
    else:
        # At least one provided, fill in the missing one
        today = datetime.now(UTC).date()
        if not end_date:
            end_date = (today - timedelta(days=1)).isoformat()  # Yesterday
        if not start_date:
            start_date = (today - timedelta(days=OPTIMA_DAYS_BACK)).isoformat()

    # Login to BidEnergy
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

    # Process sites in parallel
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

    logger.info(
        "Export completed",
        extra={
            "project": project,
            "total_sites": len(sites),
            "success_count": success_count,
            "error_count": error_count,
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
            "results": results,
        },
    }
