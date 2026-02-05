"""
CIM Report Exporter Lambda

Exports AFDD ticket reports from CIM using Playwright browser automation
and sends them via email.

Triggered daily by EventBridge Scheduler.
"""

from typing import Any

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from report_exporter.browser import download_report, get_report_filename
from report_exporter.emailer import send_report_email

logger = Logger(service="cim-report-exporter")


@logger.inject_lambda_context
def lambda_handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:
    """
    Lambda handler for CIM report export.

    Uses Playwright to automate browser-based login and report download
    from the CIM platform, then sends the report via email.

    Args:
        event: Lambda event (not used, triggered by schedule)
        context: Lambda context

    Returns:
        Response with processing result
    """
    logger.info("CIM Report Exporter Lambda invoked")

    # Step 1: Download report using Playwright browser automation
    logger.info("Starting browser automation to download report")
    csv_content = download_report(days=90)

    if not csv_content:
        logger.error("Failed to download report")
        return {
            "statusCode": 500,
            "body": "Report download failed",
        }

    logger.info("Report downloaded successfully", extra={"size_bytes": len(csv_content)})

    # Step 2: Send email
    filename = get_report_filename(days=90)
    logger.info("Sending report via email", extra={"attachment_name": filename})

    email_sent = send_report_email(csv_content, filename)

    if not email_sent:
        logger.error("Failed to send email")
        return {
            "statusCode": 500,
            "body": "Email sending failed",
        }

    logger.info("CIM report export completed successfully")
    return {
        "statusCode": 200,
        "body": {
            "message": "Report exported and emailed successfully",
            "filename": filename,
            "size_bytes": len(csv_content),
        },
    }
