"""Tests for report module."""

import csv
from pathlib import Path


class TestGenerateReport:
    """Tests for generate_report function."""

    def test_generate_csv_report(self, tmp_path: Path) -> None:
        """generate_report creates CSV with correct headers and data."""
        from src.functions.data_gap_detector.report import generate_report

        gaps = [
            {
                "nmi_channel": "NMI1-E1",
                "point_id": "p:bunnings:abc123",
                "issue_type": "missing_dates",
                "missing_dates": "2024-01-02,2024-01-04",
                "missing_count": 2,
                "data_start": "2024-01-01",
                "data_end": "2024-01-05",
                "total_expected_days": 5,
            },
            {
                "nmi_channel": "NMI2-E1",
                "point_id": "p:bunnings:def456",
                "issue_type": "no_data",
                "missing_dates": "",
                "missing_count": 0,
                "data_start": "",
                "data_end": "",
                "total_expected_days": 0,
            },
        ]

        output_path = generate_report(gaps, "bunnings", str(tmp_path))

        assert Path(output_path).exists()
        assert "bunnings" in output_path
        assert output_path.endswith(".csv")

        # Verify content
        with Path(output_path).open() as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 2
        assert rows[0]["nmi_channel"] == "NMI1-E1"
        assert rows[0]["issue_type"] == "missing_dates"
        assert rows[1]["issue_type"] == "no_data"

    def test_generate_empty_report(self, tmp_path: Path) -> None:
        """generate_report handles empty gaps list."""
        from src.functions.data_gap_detector.report import generate_report

        output_path = generate_report([], "bunnings", str(tmp_path))

        assert Path(output_path).exists()

        with Path(output_path).open() as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 0

    def test_report_filename_format(self, tmp_path: Path) -> None:
        """generate_report creates filename with project and timestamp."""
        from src.functions.data_gap_detector.report import generate_report

        output_path = generate_report([], "racv", str(tmp_path))

        filename = Path(output_path).name
        assert filename.startswith("data_gap_report_racv_")
        assert filename.endswith(".csv")


class TestGetReportHeaders:
    """Tests for get_report_headers function."""

    def test_headers_in_order(self) -> None:
        """get_report_headers returns headers in correct order."""
        from src.functions.data_gap_detector.report import get_report_headers

        headers = get_report_headers()

        assert headers[0] == "nmi_channel"
        assert headers[1] == "point_id"
        assert headers[2] == "issue_type"
        assert "missing_dates" in headers
        assert "missing_count" in headers
        assert "data_start" in headers
        assert "data_end" in headers
        assert "total_expected_days" in headers
