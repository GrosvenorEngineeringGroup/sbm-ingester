"""
Optima Billing Data Exporter Lambda

Triggers Monthly Usage and Spend CSV report generation from BidEnergy.
Reports are generated asynchronously and sent to the registered email.

Event parameters:
    project: Project name ("bunnings" or "racv") - required
    country: Country code ("AU" or "NZ") - optional (if not provided, exports all countries)
    startDate: Start date in "Mmm YYYY" format - optional
    endDate: End date in "Mmm YYYY" format - optional
"""

from typing import Any

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

from billing_exporter.trigger import process_billing_export

logger = Logger(service="optima-billing-exporter")


@logger.inject_lambda_context
def lambda_handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:
    """
    Lambda handler for billing data export.

    Args:
        event: Lambda event with project (required), country, startDate, endDate (optional)
        context: Lambda context

    Returns:
        Response with processing results
    """
    project = event.get("project")

    if not project:
        logger.warning("Export rejected: missing project parameter")
        return {
            "statusCode": 400,
            "body": "Missing required parameter: project",
        }

    logger.info(
        "Lambda invoked",
        extra={
            "project": project,
            "country": event.get("country"),
            "start_date": event.get("startDate"),
            "end_date": event.get("endDate"),
        },
    )

    result = process_billing_export(
        project=project,
        country=event.get("country"),
        start_date=event.get("startDate"),
        end_date=event.get("endDate"),
    )

    body = result.get("body", {})
    if isinstance(body, dict):
        logger.info(
            "Export completed",
            extra={
                "success_count": body.get("success_count", 0),
                "error_count": body.get("error_count", 0),
            },
        )

    return result
