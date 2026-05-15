"""Unit tests for optima_shared.dates."""

from datetime import date

from optima_shared.dates import PREVIOUS_MONTH_MODE, previous_month_range


class TestPreviousMonthRange:
    def test_mid_year_returns_previous_calendar_month(self) -> None:
        assert previous_month_range(today=date(2026, 5, 12)) == ("2026-04-01", "2026-04-30")

    def test_first_of_month_returns_previous_calendar_month(self) -> None:
        # Fired on the 1st at 01:00 — should still resolve to the month that just ended.
        assert previous_month_range(today=date(2026, 6, 1)) == ("2026-05-01", "2026-05-31")

    def test_january_crosses_year_boundary(self) -> None:
        assert previous_month_range(today=date(2026, 1, 15)) == ("2025-12-01", "2025-12-31")

    def test_handles_february_after_leap_year(self) -> None:
        assert previous_month_range(today=date(2024, 3, 5)) == ("2024-02-01", "2024-02-29")


class TestPreviousMonthModeConstant:
    def test_constant_matches_string(self) -> None:
        assert PREVIOUS_MONTH_MODE == "previous_month"
