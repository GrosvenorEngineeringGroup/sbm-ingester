"""Processing logic for demand profile export."""

from datetime import UTC, datetime, timedelta

from aws_lambda_powertools import Logger
from optima_shared.config import OPTIMA_DAYS_BACK

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
