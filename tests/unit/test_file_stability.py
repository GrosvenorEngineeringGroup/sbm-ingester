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
                is_stable, size = check_file_stability("test-bucket", "test-key")

        assert is_stable is True
        assert size == 1000

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
                    is_stable, size = check_file_stability("test-bucket", "test-key")

        assert is_stable is True
        assert size == 1000

    def test_file_remains_empty_timeout(self, mock_s3_client: Any, mock_logger: Any) -> None:
        """Test file that remains empty and times out."""
        from functions.file_processor.app import check_file_stability

        mock_s3_client.head_object.return_value = {"ContentLength": 0}

        with patch("functions.file_processor.app.FILE_STABILITY_CHECK_INTERVAL", 0.01):
            with patch("functions.file_processor.app.FILE_STABILITY_MAX_WAIT", 0.05):
                is_stable, size = check_file_stability("test-bucket", "test-key")

        assert is_stable is False
        assert size == 0

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
                is_stable, size = check_file_stability("test-bucket", "test-key")

        assert is_stable is False
        assert size == 0

    def test_file_not_found(self, mock_s3_client: Any, mock_logger: Any) -> None:
        """Test handling when file doesn't exist."""
        from functions.file_processor.app import check_file_stability

        # Create a mock NoSuchKey error response
        error = Exception("NoSuchKey")
        error.response = {"Error": {"Code": "NoSuchKey", "Message": "Not found"}}  # type: ignore[attr-defined]
        mock_s3_client.head_object.side_effect = error

        with patch("functions.file_processor.app.FILE_STABILITY_CHECK_INTERVAL", 0.01):
            is_stable, size = check_file_stability("test-bucket", "test-key")

        assert is_stable is False
        assert size == 0

    def test_s3_error_returns_false(self, mock_s3_client: Any, mock_logger: Any) -> None:
        """Test handling of S3 errors (non-NoSuchKey)."""
        from functions.file_processor.app import check_file_stability

        # Generic exception without NoSuchKey code
        mock_s3_client.head_object.side_effect = Exception("S3 error")

        is_stable, size = check_file_stability("test-bucket", "test-key")

        assert is_stable is False
        assert size == 0


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
    """Tests for lambda_handler with file stability check integration."""

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
    def mock_parse_and_write(self) -> Generator[Any]:
        """Mock parse_and_write_data function."""
        with patch("functions.file_processor.app.parse_and_write_data") as mock:
            mock.return_value = 1
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
        mock_parse_and_write: Any,
        mock_context: MockLambdaContext,
        sample_sqs_event: dict[str, Any],
    ) -> None:
        """Test that stable files are processed normally."""
        from functions.file_processor.app import lambda_handler

        mock_s3_client.head_object.return_value = {"ContentLength": 1000}

        with patch("functions.file_processor.app.FILE_STABILITY_CHECK_INTERVAL", 0.01):
            with patch("functions.file_processor.app.FILE_STABILITY_REQUIRED_CHECKS", 2):
                result = lambda_handler(sample_sqs_event, mock_context)

        assert result["statusCode"] == 200
        assert result["processed"] == 1
        assert result["requeued"] == 0
        mock_parse_and_write.assert_called_once()

    def test_unstable_file_requeued(
        self,
        mock_s3_client: Any,
        mock_sqs_client: Any,
        mock_parse_and_write: Any,
        mock_context: MockLambdaContext,
        sample_sqs_event: dict[str, Any],
    ) -> None:
        """Test that unstable files are requeued."""
        from functions.file_processor.app import lambda_handler

        mock_s3_client.head_object.return_value = {"ContentLength": 0}

        with patch("functions.file_processor.app.FILE_STABILITY_CHECK_INTERVAL", 0.01):
            with patch("functions.file_processor.app.FILE_STABILITY_MAX_WAIT", 0.02):
                result = lambda_handler(sample_sqs_event, mock_context)

        assert result["statusCode"] == 200
        assert result["processed"] == 0
        assert result["requeued"] == 1
        mock_sqs_client.send_message.assert_called_once()
        mock_parse_and_write.assert_not_called()

    def test_max_retries_exceeded_skipped(
        self,
        mock_s3_client: Any,
        mock_sqs_client: Any,
        mock_parse_and_write: Any,
        mock_context: MockLambdaContext,
    ) -> None:
        """Test that files exceeding max retries are skipped."""
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

        with patch("functions.file_processor.app.FILE_STABILITY_CHECK_INTERVAL", 0.01):
            with patch("functions.file_processor.app.FILE_STABILITY_MAX_WAIT", 0.02):
                result = lambda_handler(event, mock_context)

        assert result["statusCode"] == 200
        assert result["processed"] == 0
        assert result["requeued"] == 0
        assert result["skipped"] == 1
        mock_sqs_client.send_message.assert_not_called()
        mock_parse_and_write.assert_not_called()

    def test_retry_count_incremented_on_requeue(
        self,
        mock_s3_client: Any,
        mock_sqs_client: Any,
        mock_parse_and_write: Any,
        mock_context: MockLambdaContext,
    ) -> None:
        """Test that retry count is incremented when requeuing."""
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

        with patch("functions.file_processor.app.FILE_STABILITY_CHECK_INTERVAL", 0.01):
            with patch("functions.file_processor.app.FILE_STABILITY_MAX_WAIT", 0.02):
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
