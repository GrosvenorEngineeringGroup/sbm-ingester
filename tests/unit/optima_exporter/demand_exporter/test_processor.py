"""Unit tests for demand_exporter/processor.py module.

Tests date range calculation, single-site processing, and full-export orchestration.
"""

import os

from freezegun import freeze_time

from tests.unit.optima_exporter.conftest import reload_demand_processor_module


class TestGetDateRange:
    @freeze_time("2026-01-23 10:00:00")
    def test_default_returns_yesterday_only(self) -> None:
        processor_module = reload_demand_processor_module()
        start_date, end_date = processor_module.get_date_range()
        assert start_date == "2026-01-22"
        assert end_date == "2026-01-22"

    @freeze_time("2026-01-23 10:00:00")
    def test_respects_optima_days_back(self) -> None:
        os.environ["OPTIMA_DAYS_BACK"] = "7"
        processor_module = reload_demand_processor_module()
        start_date, end_date = processor_module.get_date_range()
        assert end_date == "2026-01-22"
        assert start_date == "2026-01-16"

    @freeze_time("2026-01-01 00:30:00")
    def test_at_midnight_uses_yesterday(self) -> None:
        processor_module = reload_demand_processor_module()
        _start, end = processor_module.get_date_range()
        assert end == "2025-12-31"
