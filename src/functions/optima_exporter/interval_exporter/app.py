"""
Optima Interval Exporter Lambda.

Exports BidEnergy interval usage CSVs by downloading them from BidEnergy
and uploading them to S3 for the existing interval_parser to consume.

Event parameters:
    project: Project name ("bunnings" or "racv") - required
    nmi: NMI identifier - optional (if not provided, exports all NMIs for the project)
    startDate: Start date in ISO format (YYYY-MM-DD) - optional
    endDate: End date in ISO format (YYYY-MM-DD) - optional
    mode: "previous_month" to re-ingest the previous calendar month
        (overrides startDate/endDate and OPTIMA_DAYS_BACK) - optional
"""

from typing import Any

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

from interval_exporter.processor import process_export

logger = Logger(service="optima-interval-exporter")


@logger.inject_lambda_context
def lambda_handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:
    """
    Lambda handler for interval CSV export.

    Returns:
        Response dict from process_export.
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
            "mode": event.get("mode"),
        },
    )

    result = process_export(
        project=project,
        nmi=event.get("nmi"),
        start_date=event.get("startDate"),
        end_date=event.get("endDate"),
        mode=event.get("mode"),
    )

    body = result.get("body", {})
    if isinstance(body, dict):
        logger.info(
            "Export completed",
            extra={
                "success_count": body.get("success_count", 0),
                "error_count": body.get("error_count", 0),
                "empty_data_count": body.get("empty_data_count", 0),
            },
        )

    return result
