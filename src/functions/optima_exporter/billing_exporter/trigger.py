"""Billing report trigger for BidEnergy Monthly Usage and Spend reports."""

from datetime import UTC, datetime
from typing import Any

import requests
from aws_lambda_powertools import Logger
from optima_shared.auth import login_bidenergy
from optima_shared.config import (
    BIDENERGY_BASE_URL,
    OPTIMA_BILLING_MONTHS,
    get_project_config,
    get_project_countries,
)

logger = Logger(service="optima-billing-exporter")


def get_default_billing_date_range() -> tuple[str, str]:
    """
    Calculate default billing date range (past N months).

    Returns:
        Tuple of (start_date, end_date) in "Mmm YYYY" format
    """
    today = datetime.now(UTC)

    # End date: current month
    end_date = today.strftime("%b %Y")

    # Start date: OPTIMA_BILLING_MONTHS - 1 months ago
    if today.month <= OPTIMA_BILLING_MONTHS - 1:
        # Need to go to previous year
        start_month = today.month + 12 - (OPTIMA_BILLING_MONTHS - 1)
        start_year = today.year - 1
    else:
        start_month = today.month - (OPTIMA_BILLING_MONTHS - 1)
        start_year = today.year

    start_date = datetime(start_year, start_month, 1).strftime("%b %Y")

    logger.info(
        "Calculated billing date range",
        extra={"start_date": start_date, "end_date": end_date, "months": OPTIMA_BILLING_MONTHS},
    )

    return start_date, end_date


def validate_billing_date_format(date_str: str) -> bool:
    """
    Validate date string is in "Mmm YYYY" format.

    Args:
        date_str: Date string to validate

    Returns:
        True if valid, False otherwise
    """
    try:
        datetime.strptime(date_str, "%b %Y")
        return True
    except ValueError:
        return False


def trigger_monthly_usage_report(
    cookies: str,
    start_date: str,
    end_date: str,
    country: str,
) -> dict[str, Any]:
    """
    Trigger the Monthly Usage and Spend CSV report generation.

    This is an async operation - the report will be generated in the background
    and sent to the registered email address when ready.

    Args:
        cookies: Authentication cookie string from login
        start_date: Start date in "Mmm YYYY" format (e.g., "Feb 2025")
        end_date: End date in "Mmm YYYY" format (e.g., "Jan 2026")
        country: Country code ("AU" or "NZ")

    Returns:
        Dict with success status and message
    """
    export_url = f"{BIDENERGY_BASE_URL}/BuyerReportRead/Usage"

    # Only essential parameters - others use website defaults
    params = {
        "isCsv": "True",
        "filter.countrystr": country,
        "start": start_date,
        "end": end_date,
    }

    logger.info(
        "Triggering Monthly Usage and Spend report",
        extra={"start_date": start_date, "end_date": end_date, "country": country},
    )

    try:
        response = requests.get(
            export_url,
            params=params,
            headers={
                "Cookie": cookies,
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "X-Requested-With": "XMLHttpRequest",
            },
            timeout=60,
        )

        if response.status_code == 200:
            logger.info(
                "Report generation triggered successfully",
                extra={"country": country, "start_date": start_date, "end_date": end_date},
            )

            try:
                data = response.json()
                return {"success": True, "message": "Report triggered", "country": country, "response": data}
            except Exception:
                return {"success": True, "message": "Report triggered", "country": country}

        elif response.status_code in (401, 403):
            logger.error(
                "Report trigger failed: authentication error",
                extra={"country": country, "status_code": response.status_code},
            )
            return {"success": False, "message": "Authentication failed", "country": country}

        else:
            logger.error(
                "Report trigger failed: unexpected response",
                extra={
                    "country": country,
                    "status_code": response.status_code,
                    "response_preview": response.text[:500] if response.text else "empty",
                },
            )
            return {"success": False, "message": f"HTTP {response.status_code}", "country": country}

    except requests.Timeout:
        logger.error("Report trigger failed: timeout", extra={"country": country})
        return {"success": False, "message": "Timeout", "country": country}
    except requests.RequestException as e:
        logger.error("Report trigger failed: request error", extra={"country": country, "error": str(e)})
        return {"success": False, "message": str(e), "country": country}


def process_billing_export(
    project: str,
    country: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    """
    Process billing data export for a project.

    Args:
        project: Project name (required)
        country: Optional country code ("AU" or "NZ")
        start_date: Optional start date ("Mmm YYYY")
        end_date: Optional end date ("Mmm YYYY")

    Returns:
        Response dict with processing results
    """
    project = project.lower()

    logger.info(
        "Starting billing export",
        extra={"project": project, "country": country, "start_date": start_date, "end_date": end_date},
    )

    # Get project configuration
    config = get_project_config(project)
    if not config:
        logger.error("Export rejected: no credentials for project", extra={"project": project})
        return {
            "statusCode": 400,
            "body": f"No credentials configured for project: {project}",
        }

    # Determine countries to export
    supported_countries = get_project_countries(project)
    if country:
        if country not in supported_countries:
            logger.warning(
                "Export rejected: country not supported",
                extra={"project": project, "country": country, "supported": supported_countries},
            )
            return {
                "statusCode": 400,
                "body": f"Country '{country}' not supported for project '{project}'. Supported: {', '.join(supported_countries)}",
            }
        countries_to_export = [country]
    else:
        countries_to_export = supported_countries

    logger.info(
        "Countries to export",
        extra={"project": project, "countries": countries_to_export},
    )

    # Determine date range
    if start_date and end_date:
        if not validate_billing_date_format(start_date):
            return {
                "statusCode": 400,
                "body": f'Invalid start date format: "{start_date}". Expected: "Mmm YYYY"',
            }
        if not validate_billing_date_format(end_date):
            return {
                "statusCode": 400,
                "body": f'Invalid end date format: "{end_date}". Expected: "Mmm YYYY"',
            }
    elif start_date or end_date:
        return {
            "statusCode": 400,
            "body": "Both startDate and endDate must be specified together, or neither",
        }
    else:
        start_date, end_date = get_default_billing_date_range()

    # Login to BidEnergy
    logger.info("Authenticating with BidEnergy", extra={"project": project})
    cookies = login_bidenergy(config["username"], config["password"], config["client_id"])
    if cookies is None:
        logger.error("Export failed: authentication failed", extra={"project": project})
        return {
            "statusCode": 401,
            "body": "Failed to authenticate with BidEnergy",
        }

    # Trigger report for each country
    results: list[dict[str, Any]] = []
    for ctry in countries_to_export:
        result = trigger_monthly_usage_report(
            cookies=cookies,
            start_date=start_date,
            end_date=end_date,
            country=ctry,
        )
        results.append(result)

    success_count = sum(1 for r in results if r.get("success"))
    error_count = sum(1 for r in results if not r.get("success"))

    logger.info(
        "Billing export completed",
        extra={
            "project": project,
            "countries": countries_to_export,
            "success_count": success_count,
            "error_count": error_count,
        },
    )

    return {
        "statusCode": 200 if error_count == 0 else 207,
        "body": {
            "message": f"Triggered {len(results)} billing report(s) for {project}",
            "project": project,
            "date_range": {"start": start_date, "end": end_date},
            "countries": countries_to_export,
            "success_count": success_count,
            "error_count": error_count,
            "results": results,
        },
    }
