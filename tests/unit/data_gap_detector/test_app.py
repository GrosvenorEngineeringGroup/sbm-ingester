"""Tests for app module (Lambda handler and CLI)."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


@pytest.fixture
def mock_lambda_context() -> MagicMock:
    """Create a mock Lambda context for testing."""
    context = MagicMock()
    context.function_name = "test-function"
    context.memory_limit_in_mb = 128
    context.invoked_function_arn = "arn:aws:lambda:ap-southeast-2:123456789:function:test"
    context.aws_request_id = "test-request-id"
    return context


class TestLambdaHandler:
    """Tests for lambda_handler function."""

    @patch("src.functions.data_gap_detector.app.run_detection")
    def test_handler_with_valid_project(self, mock_run: MagicMock, mock_lambda_context: MagicMock) -> None:
        """lambda_handler calls run_detection with correct params."""
        from src.functions.data_gap_detector.app import lambda_handler

        mock_run.return_value = {
            "statusCode": 200,
            "body": {"issues_found": 0, "report_path": "/tmp/report.csv"},
        }

        event = {"project": "bunnings"}
        result = lambda_handler(event, mock_lambda_context)

        mock_run.assert_called_once()
        assert result["statusCode"] == 200

    def test_handler_missing_project(self, mock_lambda_context: MagicMock) -> None:
        """lambda_handler returns 400 when project is missing."""
        from src.functions.data_gap_detector.app import lambda_handler

        result = lambda_handler({}, mock_lambda_context)

        assert result["statusCode"] == 400
        assert "project" in result["body"].lower()

    def test_handler_invalid_project(self, mock_lambda_context: MagicMock) -> None:
        """lambda_handler returns 400 for invalid project."""
        from src.functions.data_gap_detector.app import lambda_handler

        result = lambda_handler({"project": "invalid"}, mock_lambda_context)

        assert result["statusCode"] == 400
        assert "bunnings" in result["body"].lower() or "racv" in result["body"].lower()


class TestRunDetection:
    """Tests for run_detection orchestration function."""

    @patch("src.functions.data_gap_detector.app.query_all_sensors")
    @patch("src.functions.data_gap_detector.app.load_mappings")
    def test_run_detection_full_flow(
        self,
        mock_load: MagicMock,
        mock_query: MagicMock,
        tmp_path: Path,
    ) -> None:
        """run_detection orchestrates full detection flow."""
        from datetime import date

        from src.functions.data_gap_detector.app import run_detection

        mock_load.return_value = {
            "NMI1-E1": "p:bunnings:abc123",
            "NMI2-E1": "p:racv:def456",
        }

        mock_query.return_value = (
            pd.DataFrame(
                {
                    "sensorId": ["p:bunnings:abc123"],
                    "data_date": [date(2024, 1, 1)],
                    "record_count": [48],
                }
            ),
            [],  # No failed sensors
        )

        result = run_detection(
            project="bunnings",
            start_date="2024-01-01",
            end_date="2024-01-03",
            output_dir=str(tmp_path),
            mappings_path=str(tmp_path / "mappings.json"),
        )

        assert result["statusCode"] == 200
        assert "issues_found" in result["body"]

    @patch("src.functions.data_gap_detector.app.load_mappings")
    def test_run_detection_no_sensors(self, mock_load: MagicMock, tmp_path: Path) -> None:
        """run_detection handles no sensors for project."""
        from src.functions.data_gap_detector.app import run_detection

        mock_load.return_value = {
            "NMI1-E1": "p:racv:abc123",  # No bunnings sensors
        }

        result = run_detection(
            project="bunnings",
            output_dir=str(tmp_path),
            mappings_path=str(tmp_path / "mappings.json"),
        )

        assert result["statusCode"] == 200
        assert result["body"]["issues_found"] == 0

    @patch("src.functions.data_gap_detector.app.query_all_sensors")
    @patch("src.functions.data_gap_detector.app.load_mappings")
    def test_run_detection_with_failed_sensors(
        self,
        mock_load: MagicMock,
        mock_query: MagicMock,
        tmp_path: Path,
    ) -> None:
        """run_detection includes failed sensors in report."""
        from datetime import date

        from src.functions.data_gap_detector.app import run_detection

        mock_load.return_value = {
            "NMI1-E1": "p:bunnings:abc123",
            "NMI2-E1": "p:bunnings:def456",
        }

        # One sensor succeeds, one fails
        mock_query.return_value = (
            pd.DataFrame(
                {
                    "sensorId": ["p:bunnings:abc123"],
                    "data_date": [date(2024, 1, 1)],
                    "record_count": [48],
                }
            ),
            ["p:bunnings:def456"],  # Failed sensor
        )

        result = run_detection(
            project="bunnings",
            start_date="2024-01-01",
            end_date="2024-01-03",
            output_dir=str(tmp_path),
            mappings_path=str(tmp_path / "mappings.json"),
        )

        assert result["statusCode"] == 200
        # Should include: 1 missing_dates issue (abc123 missing Jan 2-3) + 1 query_failed (def456)
        assert result["body"]["issues_found"] >= 1


class TestParseArgs:
    """Tests for parse_args CLI argument parser."""

    def test_parse_required_args(self) -> None:
        """parse_args parses required project argument."""
        from src.functions.data_gap_detector.app import parse_args

        args = parse_args(["--project", "bunnings"])

        assert args.project == "bunnings"

    def test_parse_optional_dates(self) -> None:
        """parse_args parses optional date arguments."""
        from src.functions.data_gap_detector.app import parse_args

        args = parse_args(
            [
                "--project",
                "racv",
                "--start-date",
                "2024-01-01",
                "--end-date",
                "2024-01-31",
            ]
        )

        assert args.project == "racv"
        assert args.start_date == "2024-01-01"
        assert args.end_date == "2024-01-31"

    def test_parse_default_values(self) -> None:
        """parse_args uses defaults for optional args."""
        from src.functions.data_gap_detector.app import parse_args

        args = parse_args(["--project", "bunnings"])

        assert args.start_date is None
        assert args.end_date is None
