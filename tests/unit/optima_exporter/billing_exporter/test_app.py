"""Unit tests for billing_exporter/app.py module.

Tests the Lambda handler entry point for billing report export.
"""

from unittest.mock import MagicMock, patch


class TestBillingLambdaHandler:
    """Tests for billing Lambda handler."""

    def test_missing_project_returns_400(self, mock_billing_lambda_context: MagicMock) -> None:
        """Test that missing project returns 400."""
        from billing_exporter.app import lambda_handler

        result = lambda_handler({}, mock_billing_lambda_context)

        assert result["statusCode"] == 400
        assert "project" in result["body"].lower()

    def test_event_with_project_triggers_export(self, mock_billing_lambda_context: MagicMock) -> None:
        """Test that event with project triggers billing export."""
        from billing_exporter.app import lambda_handler

        with patch("billing_exporter.app.process_billing_export") as mock_export:
            mock_export.return_value = {"statusCode": 200, "body": {}}

            event = {"project": "bunnings"}

            lambda_handler(event, mock_billing_lambda_context)

            mock_export.assert_called_once()
