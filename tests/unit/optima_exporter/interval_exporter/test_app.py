"""Unit tests for interval_exporter/app.py Lambda handler."""

from unittest.mock import MagicMock, patch


class TestLambdaHandler:
    def test_event_with_project_triggers_export(self) -> None:
        from interval_exporter.app import lambda_handler

        context = MagicMock()
        with patch("interval_exporter.app.process_export") as mock_export:
            expected_result = {
                "statusCode": 200,
                "body": {
                    "success_count": 1,
                    "error_count": 0,
                    "empty_data_count": 0,
                },
            }
            mock_export.return_value = expected_result

            event = {
                "project": "racv",
                "nmi": "Optima_3117512760",
                "startDate": "2026-04-29",
                "endDate": "2026-04-29",
            }

            result = lambda_handler(event, context)

            mock_export.assert_called_once_with(
                project="racv",
                nmi="Optima_3117512760",
                start_date="2026-04-29",
                end_date="2026-04-29",
            )
            assert result == expected_result

    def test_event_with_project_logs_invocation_before_export(self) -> None:
        from interval_exporter.app import lambda_handler

        call_order = []
        context = MagicMock()

        def record_log(*args: object, **kwargs: object) -> None:
            call_order.append(("log", args, kwargs))

        def record_export(**kwargs: object) -> dict[str, object]:
            call_order.append(("export", kwargs))
            return {"statusCode": 200, "body": {}}

        with (
            patch("interval_exporter.app.logger.info", side_effect=record_log),
            patch("interval_exporter.app.process_export", side_effect=record_export),
        ):
            lambda_handler(
                {
                    "project": "racv",
                    "nmi": "Optima_3117512760",
                    "startDate": "2026-04-29",
                    "endDate": "2026-04-29",
                },
                context,
            )

        assert call_order[0] == (
            "log",
            ("Lambda invoked",),
            {
                "extra": {
                    "project": "racv",
                    "nmi": "Optima_3117512760",
                    "start_date": "2026-04-29",
                    "end_date": "2026-04-29",
                }
            },
        )
        assert call_order[1][0] == "export"

    def test_missing_project_returns_400(self) -> None:
        from interval_exporter.app import lambda_handler

        with patch("interval_exporter.app.process_export") as mock_export:
            result = lambda_handler({}, MagicMock())

        assert result["statusCode"] == 400
        assert "project" in result["body"].lower()
        mock_export.assert_not_called()

    def test_event_with_only_project_forwards_optional_values_as_none(self) -> None:
        from interval_exporter.app import lambda_handler

        context = MagicMock()
        with patch("interval_exporter.app.process_export") as mock_export:
            mock_export.return_value = {"statusCode": 200, "body": {}}

            lambda_handler({"project": "bunnings"}, context)

            mock_export.assert_called_once_with(
                project="bunnings",
                nmi=None,
                start_date=None,
                end_date=None,
            )
