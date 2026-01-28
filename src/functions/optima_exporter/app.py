"""
Optima/BidEnergy NMI Data Exporter

This Lambda function exports meter data from Optima (BidEnergy) by simulating web login
and downloading CSV reports, then uploading them to S3 for ingestion.

Invocation modes:
1. Scheduled (EventBridge): Empty event triggers export for all configured projects
2. On-demand: Simplified parameters - project (required), nmi (optional), dates (optional)

On-demand event parameters:
    project: Project name ("bunnings" or "racv") - required
    nmi: NMI identifier - optional (if not provided, exports all NMIs for the project)
    startDate: Start date in ISO format (YYYY-MM-DD) - optional
    endDate: End date in ISO format (YYYY-MM-DD) - optional

Workflow:
1. Login to BidEnergy via form POST to obtain authentication cookie
2. Download CSV interval usage data for each site
3. Upload CSV to S3 (sbm-file-ingester/newTBP/) for ingestion pipeline
4. Continue to next site on errors (no retries)

Supported projects: bunnings, racv
"""

import os
from datetime import UTC, datetime, timedelta
from typing import Any

import boto3
import requests
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from boto3.dynamodb.conditions import Key

logger = Logger(service="optima-exporter")

# S3 upload configuration
S3_UPLOAD_BUCKET = os.environ.get("S3_UPLOAD_BUCKET", "sbm-file-ingester")
S3_UPLOAD_PREFIX = os.environ.get("S3_UPLOAD_PREFIX", "newTBP/")

# DynamoDB configuration
OPTIMA_CONFIG_TABLE = os.environ.get("OPTIMA_CONFIG_TABLE", "sbm-optima-config")
OPTIMA_PROJECTS = [p.strip() for p in os.environ.get("OPTIMA_PROJECTS", "").split(",") if p.strip()]
OPTIMA_DAYS_BACK = int(os.environ.get("OPTIMA_DAYS_BACK", "7"))

# DynamoDB resource (lazy initialization)
_dynamodb = None

# S3 client (lazy initialization)
_s3_client = None


def get_dynamodb() -> Any:
    """Get DynamoDB resource with lazy initialization."""
    global _dynamodb
    if _dynamodb is None:
        _dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
    return _dynamodb


def get_s3_client() -> Any:
    """Get S3 client with lazy initialization."""
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3", region_name="ap-southeast-2")
    return _s3_client


# BidEnergy base URL
BIDENERGY_BASE_URL = os.environ.get("BIDENERGY_BASE_URL", "https://app.bidenergy.com")


def get_project_config(project: str) -> dict[str, Any] | None:
    """
    Get project credentials from environment variables.

    Args:
        project: Project name (e.g., "bunnings", "racv")

    Returns:
        Dict with username, password, client_id, or None if missing
    """
    prefix = f"OPTIMA_{project.upper()}_"
    username = os.environ.get(f"{prefix}USERNAME")
    password = os.environ.get(f"{prefix}PASSWORD")
    client_id = os.environ.get(f"{prefix}CLIENT_ID")

    if not all([username, password, client_id]):
        logger.warning("Missing credentials for project", extra={"project": project})
        return None

    return {
        "username": username,
        "password": password,
        "client_id": client_id,
    }


def get_sites_for_project(project: str) -> list[dict[str, str]]:
    """
    Fetch sites for a project from DynamoDB.

    Args:
        project: Project name (e.g., "bunnings", "racv")

    Returns:
        List of site dicts with nmi and siteIdStr
    """
    table = get_dynamodb().Table(OPTIMA_CONFIG_TABLE)
    response = table.query(KeyConditionExpression=Key("project").eq(project))
    sites = response.get("Items", [])

    # Handle pagination for large datasets
    while "LastEvaluatedKey" in response:
        response = table.query(
            KeyConditionExpression=Key("project").eq(project),
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        sites.extend(response.get("Items", []))

    # Filter out items missing required fields
    valid_sites = [{"nmi": s["nmi"], "siteIdStr": s["siteIdStr"]} for s in sites if "nmi" in s and "siteIdStr" in s]

    if len(valid_sites) < len(sites):
        logger.warning(
            "Some sites missing required fields",
            extra={"project": project, "total": len(sites), "valid": len(valid_sites)},
        )

    logger.info("Fetched sites from DynamoDB", extra={"project": project, "site_count": len(valid_sites)})
    return valid_sites


def get_site_by_nmi(project: str, nmi: str) -> dict[str, str] | None:
    """
    Get a single site from DynamoDB by project and NMI.

    Args:
        project: Project name (e.g., "bunnings", "racv")
        nmi: NMI identifier

    Returns:
        Site dict with nmi and siteIdStr, or None if not found
    """
    table = get_dynamodb().Table(OPTIMA_CONFIG_TABLE)
    response = table.get_item(Key={"project": project, "nmi": nmi})
    item = response.get("Item")

    if item and "siteIdStr" in item:
        logger.info("Found site by NMI", extra={"project": project, "nmi": nmi})
        return {"nmi": item["nmi"], "siteIdStr": item["siteIdStr"]}

    logger.warning("Site not found or missing siteIdStr", extra={"project": project, "nmi": nmi})
    return None


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


def login_bidenergy(username: str, password: str, client_id: str) -> str | None:
    """
    Login to BidEnergy and obtain authentication cookie.

    Args:
        username: BidEnergy username (email)
        password: BidEnergy password
        client_id: Client identifier (e.g., "Visualisation", "BidEnergy")

    Returns:
        Cookie string for subsequent requests, or None if login failed
    """
    login_url = f"{BIDENERGY_BASE_URL}/Account/LogOn"

    params = {
        "ClientId": client_id,
        "UserName": username,
        "Password": password,
    }

    logger.info("Attempting BidEnergy login", extra={"username": username, "client_id": client_id, "url": login_url})

    try:
        # POST with empty body - credentials are in URL params
        response = requests.post(
            login_url,
            params=params,
            headers={"Content-Length": "0"},
            allow_redirects=False,
            timeout=30,
        )

        # Successful login returns 302 redirect with .ASPXAUTH cookie
        if response.status_code == 302:
            cookies = response.cookies
            if ".ASPXAUTH" in cookies:
                cookie_str = "; ".join([f"{c.name}={c.value}" for c in cookies])
                logger.info("BidEnergy login successful", extra={"username": username})
                return cookie_str
            logger.error(
                "BidEnergy login failed: missing .ASPXAUTH cookie",
                extra={
                    "username": username,
                    "cookies_received": list(cookies.keys()),
                    "redirect_location": response.headers.get("Location", "N/A"),
                },
            )
        elif response.status_code == 200:
            # 200 usually means login page returned with error (invalid credentials)
            logger.error(
                "BidEnergy login failed: invalid credentials or account locked",
                extra={
                    "username": username,
                    "status_code": response.status_code,
                    "response_preview": response.text[:500] if response.text else "empty",
                },
            )
        else:
            logger.error(
                "BidEnergy login failed: unexpected response",
                extra={
                    "username": username,
                    "status_code": response.status_code,
                    "response_preview": response.text[:500] if response.text else "empty",
                },
            )

    except requests.Timeout:
        logger.error("BidEnergy login failed: request timeout", extra={"username": username, "timeout_seconds": 30})
    except requests.ConnectionError as e:
        logger.error("BidEnergy login failed: connection error", extra={"username": username, "error": str(e)})
    except requests.RequestException as e:
        logger.error(
            "BidEnergy login failed: request error", exc_info=True, extra={"username": username, "error": str(e)}
        )

    return None


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
                # Generate filename with NMI for traceability
                # project.lower() ensures consistent lowercase even if called directly
                filename = f"optima_{project.lower()}_NMI#{nmi.upper()}_{start_date}_{end_date}.csv"
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
        elif response.status_code == 401 or response.status_code == 403:
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


def upload_to_s3(
    file_content: bytes,
    filename: str,
    bucket: str | None = None,
    prefix: str | None = None,
) -> bool:
    """
    Upload CSV file to S3 for ingestion pipeline.

    Args:
        file_content: CSV file content as bytes
        filename: Filename for S3 object
        bucket: S3 bucket name (default: S3_UPLOAD_BUCKET)
        prefix: S3 prefix/folder (default: S3_UPLOAD_PREFIX)

    Returns:
        True if upload successful, False otherwise
    """
    bucket = bucket or S3_UPLOAD_BUCKET
    prefix = prefix or S3_UPLOAD_PREFIX
    s3_key = f"{prefix}{filename}"

    logger.info(
        "Uploading CSV to S3",
        extra={
            "bucket": bucket,
            "key": s3_key,
            "size_bytes": len(file_content),
            "file_name": filename,
        },
    )

    try:
        s3 = get_s3_client()
        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=file_content,
            ContentType="text/csv",
        )
        logger.info(
            "CSV uploaded successfully to S3",
            extra={"bucket": bucket, "key": s3_key, "file_name": filename},
        )
        return True

    except Exception as e:
        logger.error(
            "S3 upload failed",
            exc_info=True,
            extra={
                "error": str(e),
                "bucket": bucket,
                "key": s3_key,
                "file_name": filename,
            },
        )
        return False


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


def process_scheduled_export() -> dict[str, Any]:
    """
    Process scheduled export for all configured projects.

    Reads project list from OPTIMA_PROJECTS env var and site mappings from DynamoDB.

    Returns:
        Response dict with processing results

    Raises:
        RuntimeError: If no projects could be processed (all failed authentication or config)
    """
    logger.info(
        "Starting scheduled export",
        extra={"configured_projects": OPTIMA_PROJECTS, "days_back": OPTIMA_DAYS_BACK},
    )

    if not OPTIMA_PROJECTS:
        error_msg = "No projects configured in OPTIMA_PROJECTS environment variable"
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    start_date, end_date = get_date_range()
    all_results: list[dict[str, Any]] = []
    projects_processed = 0
    auth_failures: list[str] = []

    for project in OPTIMA_PROJECTS:
        if not project:
            continue

        logger.info("Processing project", extra={"project": project})

        config = get_project_config(project)
        if not config:
            logger.error("Skipping project: missing credentials", extra={"project": project})
            all_results.append({"project": project, "error": "Missing credentials"})
            auth_failures.append(f"{project}: missing credentials")
            continue

        sites = get_sites_for_project(project)
        if not sites:
            logger.warning("Skipping project: no sites found in DynamoDB", extra={"project": project})
            all_results.append({"project": project, "error": "No sites configured"})
            continue

        # Login and process sites for this project
        logger.info("Authenticating with BidEnergy", extra={"project": project, "site_count": len(sites)})
        cookies = login_bidenergy(config["username"], config["password"], config["client_id"])
        if not cookies:
            logger.error("Skipping project: authentication failed", extra={"project": project})
            all_results.append({"project": project, "error": "Authentication failed"})
            auth_failures.append(f"{project}: authentication failed")
            continue

        projects_processed += 1
        logger.info("Processing sites for project", extra={"project": project, "site_count": len(sites)})

        for site in sites:
            result = process_site(
                cookies=cookies,
                nmi=site["nmi"],
                site_id_str=site["siteIdStr"],
                start_date=start_date,
                end_date=end_date,
                project=project,
            )
            result["project"] = project
            all_results.append(result)

        project_success = sum(1 for r in all_results if r.get("project") == project and r.get("success"))
        project_errors = sum(1 for r in all_results if r.get("project") == project and not r.get("success"))
        logger.info(
            "Project processing complete",
            extra={"project": project, "success_count": project_success, "error_count": project_errors},
        )

    success_count = sum(1 for r in all_results if r.get("success"))
    error_count = len(all_results) - success_count

    logger.info(
        "Scheduled export completed",
        extra={
            "projects_configured": len(OPTIMA_PROJECTS),
            "projects_processed": projects_processed,
            "total_sites": len(all_results),
            "success_count": success_count,
            "error_count": error_count,
        },
    )

    # Raise exception if ALL configured projects failed (critical error - triggers alarm)
    if projects_processed == 0 and len(OPTIMA_PROJECTS) > 0:
        error_msg = f"All projects failed to process: {'; '.join(auth_failures)}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    return {
        "statusCode": 200 if error_count == 0 else 207,
        "body": {
            "message": f"Processed {len(all_results)} site(s) across {projects_processed} project(s)",
            "date_range": {"start": start_date, "end": end_date},
            "success_count": success_count,
            "error_count": error_count,
            "results": all_results,
        },
    }


def process_ondemand_export(event: dict[str, Any]) -> dict[str, Any]:
    """
    Process on-demand export with simplified parameters.

    Args:
        event: Lambda event with:
            - project (required): Project name
            - nmi (optional): Single NMI to export (if not provided, exports all NMIs)
            - startDate (optional): Start date (if not provided, uses default date range)
            - endDate (optional): End date (if not provided, uses default date range)

    Returns:
        Response dict with processing results
    """
    project = event.get("project", "").lower()
    nmi = event.get("nmi")
    start_date = event.get("startDate")
    end_date = event.get("endDate")

    logger.info(
        "Starting on-demand export",
        extra={"project": project, "nmi": nmi, "start_date": start_date, "end_date": end_date},
    )

    # Validate required parameters
    if not project:
        logger.warning("On-demand export rejected: missing project parameter")
        return {
            "statusCode": 400,
            "body": "Missing required parameter: project",
        }

    # Get project configuration from environment variables
    config = get_project_config(project)
    if not config:
        logger.error("On-demand export rejected: no credentials for project", extra={"project": project})
        return {
            "statusCode": 400,
            "body": f"No credentials configured for project: {project}",
        }

    # Determine sites to process
    if nmi:
        # Single NMI specified
        site = get_site_by_nmi(project, nmi)
        if not site:
            logger.warning("On-demand export rejected: NMI not found", extra={"project": project, "nmi": nmi})
            return {
                "statusCode": 404,
                "body": f"NMI {nmi} not found for project {project}",
            }
        sites = [site]
    else:
        # All NMIs for the project
        sites = get_sites_for_project(project)
        if not sites:
            logger.warning("On-demand export rejected: no sites found", extra={"project": project})
            return {
                "statusCode": 404,
                "body": f"No sites found for project {project}",
            }

    # Determine date range (use defaults if not provided)
    if not start_date or not end_date:
        start_date, end_date = get_date_range()

    username = config["username"]
    password = config["password"]
    client_id = config["client_id"]

    logger.info(
        "On-demand export: authenticating with BidEnergy",
        extra={
            "project": project,
            "site_count": len(sites),
            "start_date": start_date,
            "end_date": end_date,
            "single_nmi": nmi is not None,
        },
    )

    # Login to BidEnergy
    cookies = login_bidenergy(username, password, client_id)
    if cookies is None:
        logger.error("On-demand export failed: authentication failed", extra={"project": project})
        return {
            "statusCode": 401,
            "body": "Failed to authenticate with BidEnergy",
        }

    # Process each site
    results: list[dict[str, Any]] = []
    success_count = 0
    error_count = 0

    for site in sites:
        site_nmi = site["nmi"]
        site_id_str = site["siteIdStr"]
        result = process_site(
            cookies=cookies,
            nmi=site_nmi,
            site_id_str=site_id_str,
            start_date=start_date,
            end_date=end_date,
            project=project,
        )
        results.append(result)
        if result["success"]:
            success_count += 1
        else:
            error_count += 1

    logger.info(
        "On-demand export completed",
        extra={
            "project": project,
            "total_sites": len(sites),
            "success_count": success_count,
            "error_count": error_count,
        },
    )

    return {
        "statusCode": 200 if error_count == 0 else 207,  # 207 = Multi-Status
        "body": {
            "message": f"Processed {len(sites)} site(s)",
            "date_range": {"start": start_date, "end": end_date},
            "success_count": success_count,
            "error_count": error_count,
            "results": results,
        },
    }


@logger.inject_lambda_context
def lambda_handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:
    """
    Lambda handler for Optima data export.

    Invocation modes:
    1. Scheduled (empty event): Process all projects from OPTIMA_PROJECTS env var
    2. On-demand (with parameters): Process specific project with optional NMI and dates

    On-demand event parameters:
        project: Project name ("bunnings" or "racv") - required
        nmi: NMI identifier - optional (if not provided, exports all NMIs)
        startDate: Start date in ISO format (YYYY-MM-DD) - optional
        endDate: End date in ISO format (YYYY-MM-DD) - optional

    Returns:
        Response with processing results

    Raises:
        RuntimeError: If scheduled export fails completely (all projects failed)
    """
    # Scheduled export when: empty event OR EventBridge event (no project parameter)
    is_scheduled = not event or len(event) == 0 or not event.get("project")

    if is_scheduled:
        logger.info(
            "Lambda invoked: scheduled export mode",
            extra={"configured_projects": OPTIMA_PROJECTS, "event_keys": list(event.keys()) if event else []},
        )
        result = process_scheduled_export()
        body = result.get("body", {})
        if isinstance(body, dict):
            logger.info(
                "Export completed",
                extra={"success_count": body.get("success_count", 0), "error_count": body.get("error_count", 0)},
            )
        else:
            logger.info("Export completed", extra={"response": body})
        return result

    logger.info(
        "Lambda invoked: on-demand export mode",
        extra={"project": event.get("project"), "nmi": event.get("nmi")},
    )
    result = process_ondemand_export(event)
    body = result.get("body", {})
    if isinstance(body, dict):
        logger.info(
            "Export completed",
            extra={"success_count": body.get("success_count", 0), "error_count": body.get("error_count", 0)},
        )
    else:
        logger.info("Export completed", extra={"response": body})
    return result
