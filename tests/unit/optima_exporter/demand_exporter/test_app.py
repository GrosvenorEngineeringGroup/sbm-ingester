"""Unit tests for demand_exporter/app.py — the Lambda handler entry point."""

from unittest.mock import MagicMock, patch


class TestLambdaHandler:
    def test_event_with_project_triggers_export(self, mock_demand_lambda_context: MagicMock) -> None:
        from demand_exporter.app import lambda_handler

        with patch("demand_exporter.app.process_export") as mock_export:
            mock_export.return_value = {"statusCode": 200, "body": {}}

            event = {
                "project": "racv",
                "nmi": "Optima_3117512760",
                "startDate": "2026-04-29",
                "endDate": "2026-04-29",
            }

            result = lambda_handler(event, mock_demand_lambda_context)

            mock_export.assert_called_once_with(
                project="racv",
                nmi="Optima_3117512760",
                start_date="2026-04-29",
                end_date="2026-04-29",
                mode=None,
            )
            assert result["statusCode"] == 200

    def test_missing_project_returns_400(self, mock_demand_lambda_context: MagicMock) -> None:
        from demand_exporter.app import lambda_handler

        result = lambda_handler({}, mock_demand_lambda_context)

        assert result["statusCode"] == 400
        assert "project" in result["body"].lower()

    def test_event_with_only_project_uses_processor_defaults(self, mock_demand_lambda_context: MagicMock) -> None:
        from demand_exporter.app import lambda_handler

        with patch("demand_exporter.app.process_export") as mock_export:
            mock_export.return_value = {"statusCode": 200, "body": {}}

            lambda_handler({"project": "bunnings"}, mock_demand_lambda_context)

            mock_export.assert_called_once_with(
                project="bunnings",
                nmi=None,
                start_date=None,
                end_date=None,
                mode=None,
            )
