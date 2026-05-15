"""Shared date-range helpers for Optima exporters."""

from datetime import UTC, date, datetime, timedelta

PREVIOUS_MONTH_MODE = "previous_month"


def previous_month_range(today: date | None = None) -> tuple[str, str]:
    """
    Return ISO-formatted (start_date, end_date) covering the previous calendar month.

    Args:
        today: Reference date. Defaults to current UTC date. Injectable for tests.

    Returns:
        Tuple of (first_day_of_previous_month, last_day_of_previous_month).
    """
    if today is None:
        today = datetime.now(UTC).date()
    first_of_this_month = today.replace(day=1)
    last_of_previous_month = first_of_this_month - timedelta(days=1)
    first_of_previous_month = last_of_previous_month.replace(day=1)
    return first_of_previous_month.isoformat(), last_of_previous_month.isoformat()
