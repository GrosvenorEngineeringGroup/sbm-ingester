"""Tests for file stability check and SQS requeue functionality."""

import json
from collections.abc import Generator
from typing import Any
from unittest.mock import patch

import pytest


class MockLambdaContext:
    """Mock Lambda context for testing."""

    def __init__(self) -> None:
        self.function_name = "test-function"
        self.memory_limit_in_mb = 128
        self.invoked_function_arn = "arn:aws:lambda:ap-southeast-2:123456789:function:test"
        self.aws_request_id = "test-request-id"


class TestCheckFileStability:
    """Tests for check_file_stability function."""

    @pytest.fixture
    def mock_s3_client(self) -> Generator[Any]:
        """Create a mock S3 client."""
        with patch("functions.file_processor.app.s3_client") as mock:
            yield mock

    @pytest.fixture
    def mock_logger(self) -> Generator[Any]:
        """Create a mock logger."""
        with patch("functions.file_processor.app.logger") as mock:
            yield mock

    def test_file_stable_immediately(self, mock_s3_client: Any, mock_logger: Any) -> None:
        """Test file that is immediately stable (size > 0 and doesn't change)."""
        from functions.file_processor.app import check_file_stability

        # File has consistent size of 1000 bytes
        mock_s3_client.head_object.return_value = {"ContentLength": 1000}

        with patch("functions.file_processor.app.FILE_STABILITY_CHECK_INTERVAL", 0.01):
            with patch("functions.file_processor.app.FILE_STABILITY_REQUIRED_CHECKS", 2):
                result = check_file_stability("test-bucket", "test-key")
                is_stable, size = result.stable, result.size

        assert is_stable is True
        assert size == 1000
        assert result.vanished is False

    def test_file_starts_empty_then_stabilizes(self, mock_s3_client: Any, mock_logger: Any) -> None:
        """Test file that starts empty but eventually stabilizes."""
        from functions.file_processor.app import check_file_stability

        # First call: empty, second call: has content, third+ calls: stable
        mock_s3_client.head_object.side_effect = [
            {"ContentLength": 0},
            {"ContentLength": 500},
            {"ContentLength": 1000},
            {"ContentLength": 1000},
            {"ContentLength": 1000},
        ]

        with patch("functions.file_processor.app.FILE_STABILITY_CHECK_INTERVAL", 0.01):
            with patch("functions.file_processor.app.FILE_STABILITY_MAX_WAIT", 1):
                with patch("functions.file_processor.app.FILE_STABILITY_REQUIRED_CHECKS", 2):
                    result = check_file_stability("test-bucket", "test-key")
                    is_stable, size = result.stable, result.size

        assert is_stable is True
        assert size == 1000
        assert result.vanished is False

    def test_file_remains_empty_timeout(self, mock_s3_client: Any, mock_logger: Any) -> None:
        """Test file that remains empty and times out."""
        from functions.file_processor.app import check_file_stability

        mock_s3_client.head_object.return_value = {"ContentLength": 0}

        with patch("functions.file_processor.app.FILE_STABILITY_CHECK_INTERVAL", 0.01):
            with patch("functions.file_processor.app.FILE_STABILITY_MAX_WAIT", 0.05):
                result = check_file_stability("test-bucket", "test-key")
                is_stable, size = result.stable, result.size

        assert is_stable is False
        assert size == 0
        assert result.vanished is False

    def test_file_keeps_changing_timeout(self, mock_s3_client: Any, mock_logger: Any) -> None:
        """Test file that keeps changing size and times out."""
        from functions.file_processor.app import check_file_stability

        # File size keeps changing
        call_count = [0]

        def increasing_size(*args: Any, **kwargs: Any) -> dict[str, int]:
            call_count[0] += 1
            return {"ContentLength": call_count[0] * 100}

        mock_s3_client.head_object.side_effect = increasing_size

        with patch("functions.file_processor.app.FILE_STABILITY_CHECK_INTERVAL", 0.01):
            with patch("functions.file_processor.app.FILE_STABILITY_MAX_WAIT", 0.05):
                result = check_file_stability("test-bucket", "test-key")
                is_stable, size = result.stable, result.size

        assert is_stable is False
        assert size == 0
        assert result.vanished is False

    def test_file_not_found(self, mock_s3_client: Any, mock_logger: Any) -> None:
        """HEAD raises a real ClientError with NoSuchKey → vanished=True path."""
        from botocore.exceptions import ClientError

        from functions.file_processor.app import check_file_stability

        mock_s3_client.head_object.side_effect = ClientError(
            error_response={
                "Error": {"Code": "NoSuchKey", "Message": "The specified key does not exist."},
                "ResponseMetadata": {"HTTPStatusCode": 404},
            },
            operation_name="HeadObject",
        )

        with patch("functions.file_processor.app.FILE_STABILITY_CHECK_INTERVAL", 0.01):
            result = check_file_stability("test-bucket", "test-key")
            is_stable, size = result.stable, result.size

        assert is_stable is False
        assert size == 0
        assert result.vanished is True

    def test_generic_exception_returns_false(self, mock_s3_client: Any, mock_logger: Any) -> None:
        """Non-ClientError exceptions hit the generic fallback (stable=False, vanished=False)."""
        from functions.file_processor.app import check_file_stability

        # Generic exception (e.g. unexpected library bug) — not a ClientError.
        mock_s3_client.head_object.side_effect = RuntimeError("unexpected boom")

        result = check_file_stability("test-bucket", "test-key")
        is_stable, size = result.stable, result.size

        assert is_stable is False
        assert size == 0
        assert result.vanished is False

    def test_head_returns_404_marks_vanished(self, mock_s3_client: Any, mock_logger: Any) -> None:
        """HEAD returns 404 (not 'NoSuchKey') when a prior delivery already moved the file."""
        from botocore.exceptions import ClientError

        from functions.file_processor.app import check_file_stability

        mock_s3_client.head_object.side_effect = ClientError(
            error_response={
                "Error": {"Code": "404", "Message": "Not Found"},
                "ResponseMetadata": {"HTTPStatusCode": 404},
            },
            operation_name="HeadObject",
        )

        result = check_file_stability("test-bucket", "test-key")

        assert result.stable is False
        assert result.size == 0
        assert result.vanished is True

    def test_head_returns_nosuchkey_marks_vanished(self, mock_s3_client: Any, mock_logger: Any) -> None:
        """Defensive coverage for code paths that may surface NoSuchKey on HEAD."""
        from botocore.exceptions import ClientError

        from functions.file_processor.app import check_file_stability

        mock_s3_client.head_object.side_effect = ClientError(
            error_response={
                "Error": {"Code": "NoSuchKey", "Message": "The specified key does not exist."},
                "ResponseMetadata": {"HTTPStatusCode": 404},
            },
            operation_name="HeadObject",
        )

        result = check_file_stability("test-bucket", "test-key")

        assert result.stable is False
        assert result.size == 0
        assert result.vanished is True

    def test_head_returns_other_client_error_propagates(self, mock_s3_client: Any, mock_logger: Any) -> None:
        """Non-404 ClientError (e.g. AccessDenied) propagates per spec, so SQS retry / DLQ handles it."""
        from botocore.exceptions import ClientError

        from functions.file_processor.app import check_file_stability

        mock_s3_client.head_object.side_effect = ClientError(
            error_response={
                "Error": {"Code": "AccessDenied", "Message": "Denied"},
                "ResponseMetadata": {"HTTPStatusCode": 403},
            },
            operation_name="HeadObject",
        )

        with pytest.raises(ClientError) as exc_info:
            check_file_stability("test-bucket", "test-key")

        assert exc_info.value.response["Error"]["Code"] == "AccessDenied"


class TestRequeueMessage:
    """Tests for requeue_message function."""

    @pytest.fixture
    def mock_sqs_client(self) -> Generator[Any]:
        """Create a mock SQS client."""
        with patch("functions.file_processor.app.sqs_client") as mock:
            yield mock

    @pytest.fixture
    def mock_logger(self) -> Generator[Any]:
        """Create a mock logger."""
        with patch("functions.file_processor.app.logger") as mock:
            yield mock

    def test_requeue_success(self, mock_sqs_client: Any, mock_logger: Any) -> None:
        """Test successful message requeue."""
        from functions.file_processor.app import requeue_message

        original_body = {"Records": [{"s3": {"bucket": {"name": "test"}}}]}

        result = requeue_message(original_body, retry_count=2)

        assert result is True
        mock_sqs_client.send_message.assert_called_once()

        # Verify retry count was incremented
        call_args = mock_sqs_client.send_message.call_args
        sent_body = json.loads(call_args.kwargs["MessageBody"])
        assert sent_body["_retry_count"] == 3

    def test_requeue_failure(self, mock_sqs_client: Any, mock_logger: Any) -> None:
        """Test failed message requeue."""
        from functions.file_processor.app import requeue_message

        mock_sqs_client.send_message.side_effect = Exception("SQS error")
        original_body = {"Records": [{"s3": {"bucket": {"name": "test"}}}]}

        result = requeue_message(original_body, retry_count=0)

        assert result is False


class TestLambdaHandlerWithStabilityCheck:
    """Integration tests: lambda_handler + check_file_stability (no check mocking).

    Unlike tests in test_lambda_handler.py (which mock `check_file_stability`
    directly), these exercise the real stability-check path by mocking
    `s3_client.head_object` so we cover the end-to-end handler behaviour.
    """

    @pytest.fixture
    def mock_s3_client(self) -> Generator[Any]:
        """Create a mock S3 client."""
        with patch("functions.file_processor.app.s3_client") as mock:
            yield mock

    @pytest.fixture
    def mock_sqs_client(self) -> Generator[Any]:
        """Create a mock SQS client."""
        with patch("functions.file_processor.app.sqs_client") as mock:
            yield mock

    @pytest.fixture
    def mock_ingest_file(self) -> Generator[Any]:
        """Mock ingest_file as imported in app.py."""
        with patch("functions.file_processor.app.ingest_file") as mock:
            yield mock

    @pytest.fixture
    def mock_context(self) -> MockLambdaContext:
        """Create a mock Lambda context."""
        return MockLambdaContext()

    @pytest.fixture
    def sample_sqs_event(self) -> dict[str, list[dict[str, str]]]:
        """Create a sample SQS event."""
        return {
            "Records": [
                {
                    "body": json.dumps(
                        {
                            "Records": [
                                {
                                    "s3": {
                                        "bucket": {"name": "test-bucket"},
                                        "object": {"key": "newTBP/test-file.csv"},
                                    }
                                }
                            ]
                        }
                    )
                }
            ]
        }

    def test_stable_file_processed(
        self,
        mock_s3_client: Any,
        mock_sqs_client: Any,
        mock_ingest_file: Any,
        mock_context: MockLambdaContext,
        sample_sqs_event: dict[str, Any],
    ) -> None:
        """Stable file → check_file_stability returns True → ingest_file is invoked."""
        from functions.file_processor.app import lambda_handler

        mock_s3_client.head_object.return_value = {"ContentLength": 1000}

        with (
            patch("functions.file_processor.app.FILE_STABILITY_CHECK_INTERVAL", 0.01),
            patch("functions.file_processor.app.FILE_STABILITY_REQUIRED_CHECKS", 2),
        ):
            result = lambda_handler(sample_sqs_event, mock_context)

        assert result["statusCode"] == 200
        assert result["processed"] == 1
        assert result["requeued"] == 0
        mock_ingest_file.assert_called_once()
        # SourceFile carries bucket/key from the SQS event
        kwargs = mock_ingest_file.call_args.kwargs
        assert kwargs["source_file"].bucket == "test-bucket"
        assert kwargs["source_file"].key == "newTBP/test-file.csv"

    def test_unstable_file_requeued(
        self,
        mock_s3_client: Any,
        mock_sqs_client: Any,
        mock_ingest_file: Any,
        mock_context: MockLambdaContext,
        sample_sqs_event: dict[str, Any],
    ) -> None:
        """Empty/unstable file → requeued via SQS; ingest_file NOT called."""
        from functions.file_processor.app import lambda_handler

        mock_s3_client.head_object.return_value = {"ContentLength": 0}

        with (
            patch("functions.file_processor.app.FILE_STABILITY_CHECK_INTERVAL", 0.01),
            patch("functions.file_processor.app.FILE_STABILITY_MAX_WAIT", 0.02),
        ):
            result = lambda_handler(sample_sqs_event, mock_context)

        assert result["statusCode"] == 200
        assert result["processed"] == 0
        assert result["requeued"] == 1
        mock_sqs_client.send_message.assert_called_once()
        mock_ingest_file.assert_not_called()

    def test_max_retries_exceeded_skipped(
        self,
        mock_s3_client: Any,
        mock_sqs_client: Any,
        mock_ingest_file: Any,
        mock_context: MockLambdaContext,
    ) -> None:
        """Unstable + retry_count at MAX → skipped (no requeue, no ingest)."""
        from functions.file_processor.app import MAX_REQUEUE_RETRIES, lambda_handler

        event = {
            "Records": [
                {
                    "body": json.dumps(
                        {
                            "_retry_count": MAX_REQUEUE_RETRIES,  # Already at max
                            "Records": [
                                {
                                    "s3": {
                                        "bucket": {"name": "test-bucket"},
                                        "object": {"key": "newTBP/test-file.csv"},
                                    }
                                }
                            ],
                        }
                    )
                }
            ]
        }

        mock_s3_client.head_object.return_value = {"ContentLength": 0}

        with (
            patch("functions.file_processor.app.FILE_STABILITY_CHECK_INTERVAL", 0.01),
            patch("functions.file_processor.app.FILE_STABILITY_MAX_WAIT", 0.02),
        ):
            result = lambda_handler(event, mock_context)

        assert result["statusCode"] == 200
        assert result["processed"] == 0
        assert result["requeued"] == 0
        assert result["skipped"] == 1
        mock_sqs_client.send_message.assert_not_called()
        mock_ingest_file.assert_not_called()

    def test_vanished_emits_single_duplicate_event_log(
        self,
        mock_s3_client: Any,
        mock_sqs_client: Any,
        mock_ingest_file: Any,
        mock_context: MockLambdaContext,
        sample_sqs_event: dict[str, Any],
    ) -> None:
        """Invariant: 1 `s3_duplicate_event` log == 1 `S3DuplicateEvent` metric.

        Stability check raises 404 → vanished=True path. Only the caller
        (lambda_handler) is allowed to emit the structured duplicate-event
        log; the inner `check_file_stability` must NOT log it (else Logs
        Insights `grep s3_duplicate_event` over-counts vs the metric).
        """
        from botocore.exceptions import ClientError

        from functions.file_processor.app import lambda_handler

        mock_s3_client.head_object.side_effect = ClientError(
            error_response={
                "Error": {"Code": "404", "Message": "Not Found"},
                "ResponseMetadata": {"HTTPStatusCode": 404},
            },
            operation_name="HeadObject",
        )

        with (
            patch("functions.file_processor.app.logger") as mock_logger,
            patch("functions.file_processor.app.metrics") as mock_metrics,
        ):
            response = lambda_handler(sample_sqs_event, mock_context)

        assert response["statusCode"] == 200
        assert response.get("duplicate") == 1
        assert response["requeued"] == 0

        # Exactly ONE info-level `s3_duplicate_event` log call.
        duplicate_log_calls = [
            c for c in mock_logger.info.call_args_list if c.args and c.args[0] == "s3_duplicate_event"
        ]
        assert len(duplicate_log_calls) == 1, (
            f"Expected exactly 1 s3_duplicate_event log, got {len(duplicate_log_calls)}: {duplicate_log_calls!r}"
        )

        # The log is the caller's log — carries `source_bucket` / `source_key` /
        # `retry_count` (richer than the removed inner log's `bucket`/`key`/`reason`).
        log_extra = duplicate_log_calls[0].kwargs.get("extra", {})
        assert log_extra.get("source_bucket") == "test-bucket"
        assert log_extra.get("source_key") == "newTBP/test-file.csv"
        assert "retry_count" in log_extra

        # Exactly ONE S3DuplicateEvent metric — invariant 1 log = 1 metric.
        metric_names = [c.kwargs.get("name") for c in mock_metrics.add_metric.call_args_list]
        assert metric_names.count("S3DuplicateEvent") == 1

        # No requeue / ingest.
        mock_sqs_client.send_message.assert_not_called()
        mock_ingest_file.assert_not_called()

        # Sanity — the inner log was previously fired with extras={"bucket": ..., "reason": "head_404"}.
        # Make sure that exact shape is gone.
        for info_call in mock_logger.info.call_args_list:
            extras = info_call.kwargs.get("extra") or {}
            assert extras.get("reason") != "head_404", (
                "Inner check_file_stability log re-introduced — violates 1 log = 1 metric invariant"
            )

    def test_access_denied_propagates_to_record_error_path(
        self,
        mock_s3_client: Any,
        mock_sqs_client: Any,
        mock_ingest_file: Any,
        mock_context: MockLambdaContext,
        sample_sqs_event: dict[str, Any],
    ) -> None:
        """Non-404 ClientError (AccessDenied) propagates out of check_file_stability.

        The handler's per-record `except Exception` catches it, logs an
        error, and continues. The SQS message is NOT explicitly requeued
        (the in-flight message will be redelivered via SQS visibility
        timeout / maxReceiveCount → DLQ on persistent failure), and
        ingest_file is never called.
        """
        from botocore.exceptions import ClientError

        from functions.file_processor.app import lambda_handler

        mock_s3_client.head_object.side_effect = ClientError(
            error_response={
                "Error": {"Code": "AccessDenied", "Message": "Denied"},
                "ResponseMetadata": {"HTTPStatusCode": 403},
            },
            operation_name="HeadObject",
        )

        response = lambda_handler(sample_sqs_event, mock_context)

        # Handler still returns 200 (per-record exceptions don't kill the batch),
        # but nothing was processed/requeued/skipped/duplicated.
        assert response["statusCode"] == 200
        assert response["processed"] == 0
        assert response["requeued"] == 0
        assert response["skipped"] == 0
        assert response.get("duplicate", 0) == 0
        mock_sqs_client.send_message.assert_not_called()
        mock_ingest_file.assert_not_called()

    def test_retry_count_incremented_on_requeue(
        self,
        mock_s3_client: Any,
        mock_sqs_client: Any,
        mock_ingest_file: Any,
        mock_context: MockLambdaContext,
    ) -> None:
        """When SQS requeues the unstable file, _retry_count is bumped by 1."""
        from functions.file_processor.app import lambda_handler

        event = {
            "Records": [
                {
                    "body": json.dumps(
                        {
                            "_retry_count": 2,
                            "Records": [
                                {
                                    "s3": {
                                        "bucket": {"name": "test-bucket"},
                                        "object": {"key": "newTBP/test-file.csv"},
                                    }
                                }
                            ],
                        }
                    )
                }
            ]
        }

        mock_s3_client.head_object.return_value = {"ContentLength": 0}

        with (
            patch("functions.file_processor.app.FILE_STABILITY_CHECK_INTERVAL", 0.01),
            patch("functions.file_processor.app.FILE_STABILITY_MAX_WAIT", 0.02),
        ):
            lambda_handler(event, mock_context)

        # Verify retry count was incremented to 3
        call_args = mock_sqs_client.send_message.call_args
        sent_body = json.loads(call_args.kwargs["MessageBody"])
        assert sent_body["_retry_count"] == 3


class TestConstants:
    """Tests for stability check constants."""

    def test_stability_constants_exist(self) -> None:
        """Test that stability check constants are defined."""
        from functions.file_processor.app import (
            FILE_STABILITY_CHECK_INTERVAL,
            FILE_STABILITY_MAX_WAIT,
            FILE_STABILITY_REQUIRED_CHECKS,
            MAX_REQUEUE_RETRIES,
            REQUEUE_DELAY_SECONDS,
        )

        assert FILE_STABILITY_CHECK_INTERVAL > 0
        assert FILE_STABILITY_MAX_WAIT > FILE_STABILITY_CHECK_INTERVAL
        assert FILE_STABILITY_REQUIRED_CHECKS >= 1
        assert MAX_REQUEUE_RETRIES >= 1
        assert REQUEUE_DELAY_SECONDS >= 0

    def test_sqs_queue_url_defined(self) -> None:
        """Test that SQS queue URL is defined."""
        from functions.file_processor.app import SQS_QUEUE_URL

        assert SQS_QUEUE_URL is not None
        assert "sqs" in SQS_QUEUE_URL.lower()
