"""Unit tests for nem12_exporter/app.py module.

Tests the Lambda handler entry point for NEM12 export.
"""

from unittest.mock import MagicMock, patch


class TestLambdaHandler:
    """Tests for lambda_handler function."""

    def test_event_with_project_triggers_export(self, mock_lambda_context: MagicMock) -> None:
        """Test that event with project triggers export."""
        from nem12_exporter.app import lambda_handler

        with patch("nem12_exporter.app.process_export") as mock_export:
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
        from nem12_exporter.app import lambda_handler

        result = lambda_handler({}, mock_lambda_context)

        assert result["statusCode"] == 400
        assert "project" in result["body"].lower()
