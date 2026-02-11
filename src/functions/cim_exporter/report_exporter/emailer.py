"""Email sending utilities using AWS SES SMTP."""

import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from aws_lambda_powertools import Logger
from cim_shared.config import (
    EMAIL_FROM,
    EMAIL_SUBJECT,
    EMAIL_TO,
    SMTP_HOST,
    SMTP_PASSWORD,
    SMTP_PORT,
    SMTP_USERNAME,
)

logger = Logger(service="cim-report-exporter")


def send_report_email(
    csv_content: bytes,
    filename: str,
    recipients: list[str] | None = None,
    subject: str | None = None,
) -> bool:
    """
    Send report via email with CSV attachment.

    Args:
        csv_content: CSV file content as bytes
        filename: Filename for the attachment
        recipients: List of email addresses (defaults to EMAIL_TO from config)
        subject: Email subject (defaults to EMAIL_SUBJECT from config)

    Returns:
        True if email sent successfully, False otherwise
    """
    recipients = recipients or EMAIL_TO
    subject = subject or EMAIL_SUBJECT

    logger.info(
        "Sending report email",
        extra={
            "recipients": recipients,
            "attachment_name": filename,
            "attachment_size_bytes": len(csv_content),
        },
    )

    # Create message
    msg = MIMEMultipart()
    msg["From"] = EMAIL_FROM
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject

    # Email body
    body = f"""Hi,

Please find attached the CIM AFDD Report for Charter Hall.

This report contains action data for the last 90 days across all monitored sites.

Report Details:
- Filename: {filename}
- Generated: automatically by CIM Exporter

Best regards,
VerdeOS Automation
"""
    msg.attach(MIMEText(body, "plain"))

    # Attach CSV file
    attachment = MIMEBase("application", "octet-stream")
    attachment.set_payload(csv_content)
    encoders.encode_base64(attachment)
    attachment.add_header("Content-Disposition", f"attachment; filename={filename}")
    msg.attach(attachment)

    try:
        # Connect to SMTP server
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.sendmail(EMAIL_FROM, recipients, msg.as_string())

        logger.info(
            "Email sent successfully",
            extra={"recipients": recipients, "attachment_name": filename},
        )
        return True

    except smtplib.SMTPAuthenticationError as e:
        logger.error(
            "SMTP authentication failed",
            extra={"error": str(e)},
        )
    except smtplib.SMTPException as e:
        logger.error(
            "SMTP error occurred",
            exc_info=True,
            extra={"error": str(e)},
        )
    except Exception as e:
        logger.error(
            "Failed to send email",
            exc_info=True,
            extra={"error": str(e)},
        )

    return False
