"""Unit tests for interval_exporter/app.py module.

Tests the Lambda handler entry point for interval data export.
"""

from unittest.mock import MagicMock, patch


class TestLambdaHandler:
    """Tests for lambda_handler function."""

    def test_event_with_project_triggers_export(self, mock_lambda_context: MagicMock) -> None:
        """Test that event with project triggers export."""
        from interval_exporter.app import lambda_handler

        with patch("interval_exporter.app.process_export") as mock_export:
            mock_export.return_value = {"statusCode": 200, "body": {}}

            event = {
                "project": "bunnings",
                "nmi": "NMI001",
                "startDate": "2026-01-01",
                "endDate": "2026-01-07",
            }

            lambda_handler(event, mock_lambda_context)

            mock_export.assert_called_once()

    def test_missing_project_returns_400(self, mock_lambda_context: MagicMock) -> None:
        """Test that missing project returns 400."""
        from interval_exporter.app import lambda_handler

        result = lambda_handler({}, mock_lambda_context)

        assert result["statusCode"] == 400
        assert "project" in result["body"].lower()
