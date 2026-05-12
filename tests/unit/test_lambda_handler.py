"""Tests for the SQS adapter (lambda_handler)."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from functions.file_processor.app import lambda_handler
from shared.parsers.outcome import ParserOutcome


class _MockLambdaContext:
    """Minimal Lambda context shape needed by powertools logger."""

    function_name = "test-function"
    memory_limit_in_mb = 128
    invoked_function_arn = "arn:aws:lambda:ap-southeast-2:123456789:function:test"
    aws_request_id = "test-request-id"


def _sqs_event(bucket: str, key: str, retry_count: int | None = None) -> dict:
    body = {
        "Records": [
            {"s3": {"bucket": {"name": bucket}, "object": {"key": key}}},
        ],
    }
    if retry_count is not None:
        body["_retry_count"] = retry_count
    return {"Records": [{"messageId": "abc-123", "body": json.dumps(body)}]}


class TestLambdaHandler:
    def test_calls_ingest_file_with_source_file(self) -> None:
        from functions.file_processor.app import StabilityResult

        event = _sqs_event("sbm-file-ingester", "newTBP/foo.csv")

        with (
            patch(
                "functions.file_processor.app.check_file_stability",
                return_value=StabilityResult(stable=True, size=100),
            ),
            patch("functions.file_processor.app.ingest_file") as mock_ingest,
        ):
            mock_ingest.return_value = ParserOutcome(status="processed", rows_written=1)
            result = lambda_handler(event, _MockLambdaContext())

        mock_ingest.assert_called_once()
        kwargs = mock_ingest.call_args.kwargs
        assert kwargs["source_file"].bucket == "sbm-file-ingester"
        assert kwargs["source_file"].key == "newTBP/foo.csv"
        assert result["statusCode"] == 200

    def test_url_encoded_key_decoded_before_passing_to_ingest_file(self) -> None:
        """S3 event notifications URL-encode spaces as '+'; SourceFile must receive decoded key."""
        from functions.file_processor.app import StabilityResult

        event = _sqs_event("sbm-file-ingester", "newTBP/Envizi+Water.csv")

        with (
            patch(
                "functions.file_processor.app.check_file_stability",
                return_value=StabilityResult(stable=True, size=100),
            ),
            patch("functions.file_processor.app.ingest_file") as mock_ingest,
        ):
            mock_ingest.return_value = ParserOutcome(status="processed", rows_written=1)
            lambda_handler(event, _MockLambdaContext())

        kwargs = mock_ingest.call_args.kwargs
        # Critical: key must be decoded — boto3 needs the literal name "Envizi Water.csv",
        # not the URL-encoded "Envizi+Water.csv".
        assert kwargs["source_file"].key == "newTBP/Envizi Water.csv"

    def test_unstable_file_requeues(self) -> None:
        from functions.file_processor.app import StabilityResult

        event = _sqs_event("sbm-file-ingester", "newTBP/in_flight.csv", retry_count=0)

        with (
            patch(
                "functions.file_processor.app.check_file_stability",
                return_value=StabilityResult(stable=False, size=0),
            ),
            patch("functions.file_processor.app.requeue_message", return_value=True) as mock_requeue,
            patch("functions.file_processor.app.ingest_file") as mock_ingest,
        ):
            result = lambda_handler(event, _MockLambdaContext())

        mock_requeue.assert_called_once()
        mock_ingest.assert_not_called()
        assert result["statusCode"] == 200

    def test_unstable_after_max_retries_skips(self) -> None:
        from functions.file_processor.app import StabilityResult

        event = _sqs_event("sbm-file-ingester", "newTBP/never_stabilises.csv", retry_count=3)

        with (
            patch(
                "functions.file_processor.app.check_file_stability",
                return_value=StabilityResult(stable=False, size=0),
            ),
            patch("functions.file_processor.app.requeue_message") as mock_requeue,
            patch("functions.file_processor.app.ingest_file") as mock_ingest,
            patch("functions.file_processor.app.MAX_REQUEUE_RETRIES", 3),
        ):
            result = lambda_handler(event, _MockLambdaContext())

        mock_requeue.assert_not_called()
        mock_ingest.assert_not_called()
        assert result["statusCode"] == 200


class TestLambdaHandlerDuplicateEvent:
    """Verify that a vanished S3 object is treated as a duplicate event."""

    def test_vanished_file_skipped_silently_with_metric(self) -> None:
        """When stability check returns vanished=True, handler logs + emits
        S3DuplicateEvent metric and does NOT requeue or raise MaxRetriesExceeded.
        """
        import json
        from unittest.mock import patch

        from functions.file_processor.app import StabilityResult, lambda_handler

        sqs_record_body = {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": "sbm-file-ingester"},
                        "object": {"key": "newTBP/foo.csv"},
                    }
                }
            ]
        }
        event = {
            "Records": [
                {"body": json.dumps(sqs_record_body), "messageId": "msg-1"},
            ]
        }

        class _Ctx:
            function_name = "test"
            memory_limit_in_mb = 128
            invoked_function_arn = "arn:aws:lambda:ap-southeast-2:000:function:test"
            aws_request_id = "req-1"

        with (
            patch(
                "functions.file_processor.app.check_file_stability",
                return_value=StabilityResult(stable=False, size=0, vanished=True),
            ) as mock_stab,
            patch("functions.file_processor.app.requeue_message") as mock_requeue,
            patch("functions.file_processor.app.ingest_file") as mock_ingest,
            patch("functions.file_processor.app.metrics") as mock_metrics,
        ):
            response = lambda_handler(event, _Ctx())

        assert response["statusCode"] == 200
        assert response.get("duplicate") == 1
        assert response["requeued"] == 0
        assert response["skipped"] == 0
        mock_stab.assert_called_once()
        mock_requeue.assert_not_called()
        mock_ingest.assert_not_called()
        metric_names = [call.kwargs.get("name") for call in mock_metrics.add_metric.call_args_list]
        assert "S3DuplicateEvent" in metric_names
        assert "MaxRetriesExceeded" not in metric_names


class TestRetryBudget:
    def test_max_requeue_retries_aligned_with_sqs_max_receive_count(self) -> None:
        """MAX_REQUEUE_RETRIES must match SQS maxReceiveCount in terraform/ingester.tf (= 3)."""
        from functions.file_processor.app import MAX_REQUEUE_RETRIES

        assert MAX_REQUEUE_RETRIES == 3


class TestSqsQueueUrlRequired:
    def test_module_reload_without_env_var_raises(self, monkeypatch) -> None:
        """Without SQS_QUEUE_URL in env, re-importing app.py must raise KeyError."""
        monkeypatch.delenv("SQS_QUEUE_URL", raising=False)
        import importlib

        import functions.file_processor.app as app_module

        with pytest.raises(KeyError):
            importlib.reload(app_module)
