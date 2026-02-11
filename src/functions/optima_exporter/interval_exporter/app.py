"""
Optima Interval Data Exporter Lambda

Exports interval usage data from BidEnergy by downloading CSV reports
and uploading them to S3 for ingestion pipeline processing.

Event parameters:
    project: Project name ("bunnings" or "racv") - required
    nmi: NMI identifier - optional (if not provided, exports all NMIs)
    startDate: Start date in ISO format (YYYY-MM-DD) - optional
    endDate: End date in ISO format (YYYY-MM-DD) - optional
"""

from typing import Any

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

from interval_exporter.processor import process_export

logger = Logger(service="optima-interval-exporter")


@logger.inject_lambda_context
def lambda_handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:
    """
    Lambda handler for interval data export.

    Args:
        event: Lambda event with project (required), nmi, startDate, endDate (optional)
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
            "nmi": event.get("nmi"),
            "start_date": event.get("startDate"),
            "end_date": event.get("endDate"),
        },
    )

    result = process_export(
        project=project,
        nmi=event.get("nmi"),
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
