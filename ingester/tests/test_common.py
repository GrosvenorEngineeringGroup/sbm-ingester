"""Unit tests for common.py module."""

from unittest.mock import MagicMock, patch

from freezegun import freeze_time
from mypy_boto3_logs import CloudWatchLogsClient


class TestCloudWatchLoggerInit:
    """Tests for CloudWatchLogger initialization."""

    def test_initializes_with_log_group(self) -> None:
        """Test that logger initializes with specified log group."""
        mock_client = MagicMock()
        with patch("modules.common.boto3.client", return_value=mock_client):
            from modules.common import CloudWatchLogger

            logger = CloudWatchLogger("test-log-group")

            assert logger.log_group == "test-log-group"

    def test_creates_client_with_region(self, mock_cloudwatch_logs: CloudWatchLogsClient) -> None:
        """Test that client is created with correct region."""
        mock_client = MagicMock()
        with patch("modules.common.boto3.client", return_value=mock_client) as mock_boto:
            from modules.common import CloudWatchLogger

            CloudWatchLogger("test-log-group", region_name="us-west-2")

            mock_boto.assert_called_with("logs", region_name="us-west-2")

    def test_default_region_is_sydney(self, mock_cloudwatch_logs: CloudWatchLogsClient) -> None:
        """Test that default region is ap-southeast-2 (Sydney)."""
        mock_client = MagicMock()
        with patch("modules.common.boto3.client", return_value=mock_client) as mock_boto:
            from modules.common import CloudWatchLogger

            CloudWatchLogger("test-log-group")

            mock_boto.assert_called_with("logs", region_name="ap-southeast-2")


class TestDailyStreamName:
    """Tests for daily stream name generation."""

    @freeze_time("2024-01-15 10:30:00")
    def test_stream_name_format(self, mock_cloudwatch_logs: CloudWatchLogsClient) -> None:
        """Test that stream name follows day-YYYY-MM-DD format."""
        mock_client = MagicMock()
        with patch("modules.common.boto3.client", return_value=mock_client):
            from modules.common import CloudWatchLogger

            logger = CloudWatchLogger("test-log-group")
            stream_name = logger._get_daily_stream_name()

            assert stream_name == "day-2024-01-15"

    @freeze_time("2024-12-31 23:59:59")
    def test_stream_name_at_year_end(self, mock_cloudwatch_logs: CloudWatchLogsClient) -> None:
        """Test stream name at year end."""
        mock_client = MagicMock()
        with patch("modules.common.boto3.client", return_value=mock_client):
            from modules.common import CloudWatchLogger

            logger = CloudWatchLogger("test-log-group")
            stream_name = logger._get_daily_stream_name()

            assert stream_name == "day-2024-12-31"

    @freeze_time("2024-01-01 00:00:00")
    def test_stream_name_at_midnight(self, mock_cloudwatch_logs: CloudWatchLogsClient) -> None:
        """Test stream name at midnight (UTC)."""
        mock_client = MagicMock()
        with patch("modules.common.boto3.client", return_value=mock_client):
            from modules.common import CloudWatchLogger

            logger = CloudWatchLogger("test-log-group")
            stream_name = logger._get_daily_stream_name()

            assert stream_name == "day-2024-01-01"


class TestStreamRotation:
    """Tests for log stream rotation."""

    def test_stream_changes_at_new_day(self, mock_cloudwatch_logs: CloudWatchLogsClient) -> None:
        """Test that stream changes when day changes."""
        mock_client = MagicMock()
        with patch("modules.common.boto3.client", return_value=mock_client):
            from modules.common import CloudWatchLogger

            with freeze_time("2024-01-15 23:59:00"):
                logger = CloudWatchLogger("test-log-group")
                old_stream = logger.current_stream
                assert old_stream == "day-2024-01-15"

            # Simulate day change
            with freeze_time("2024-01-16 00:01:00"):
                logger._update_stream()
                new_stream = logger.current_stream
                assert new_stream == "day-2024-01-16"

    def test_sequence_token_resets_on_new_day(self, mock_cloudwatch_logs: CloudWatchLogsClient) -> None:
        """Test that sequence token resets when day changes."""
        mock_client = MagicMock()
        with patch("modules.common.boto3.client", return_value=mock_client):
            from modules.common import CloudWatchLogger

            with freeze_time("2024-01-15 12:00:00"):
                logger = CloudWatchLogger("test-log-group")
                logger.sequence_token = "some-token"

            with freeze_time("2024-01-16 00:01:00"):
                logger._update_stream()
                assert logger.sequence_token is None


class TestEnsureStream:
    """Tests for log stream creation."""

    def test_creates_stream_if_not_exists(self, mock_cloudwatch_logs: CloudWatchLogsClient) -> None:
        """Test that stream is created if it doesn't exist."""
        mock_client = MagicMock()
        with patch("modules.common.boto3.client", return_value=mock_client):
            from modules.common import CloudWatchLogger

            logger = CloudWatchLogger("test-log-group")
            logger._ensure_stream("day-2024-01-15")

            mock_client.create_log_stream.assert_called_with(
                logGroupName="test-log-group", logStreamName="day-2024-01-15"
            )

    def test_handles_existing_stream(self, mock_cloudwatch_logs: CloudWatchLogsClient) -> None:
        """Test that existing stream doesn't cause error."""
        mock_client = MagicMock()

        # Simulate ResourceAlreadyExistsException
        class MockException(Exception):
            pass

        mock_client.exceptions.ResourceAlreadyExistsException = MockException
        mock_client.create_log_stream.side_effect = MockException("Stream exists")

        with patch("modules.common.boto3.client", return_value=mock_client):
            from modules.common import CloudWatchLogger

            logger = CloudWatchLogger("test-log-group")

            # Should not raise
            logger._ensure_stream("day-2024-01-15")


class TestLogMethod:
    """Tests for log method."""

    @freeze_time("2024-01-15 12:30:45.123")
    def test_log_message_with_timestamp(self, mock_cloudwatch_logs: CloudWatchLogsClient) -> None:
        """Test that log message includes millisecond timestamp."""
        mock_client = MagicMock()
        mock_client.put_log_events.return_value = {"nextSequenceToken": "token123"}

        with patch("modules.common.boto3.client", return_value=mock_client):
            from modules.common import CloudWatchLogger

            logger = CloudWatchLogger("test-log-group")
            logger.log("Test message")

            call_args = mock_client.put_log_events.call_args
            log_events = call_args.kwargs.get("logEvents") or call_args[1].get("logEvents")

            assert len(log_events) == 1
            assert log_events[0]["message"] == "Test message"
            # Timestamp should be in milliseconds
            assert isinstance(log_events[0]["timestamp"], int)

    def test_sequence_token_passed_when_available(self, mock_cloudwatch_logs: CloudWatchLogsClient) -> None:
        """Test that sequence token is passed when available."""
        mock_client = MagicMock()
        mock_client.put_log_events.return_value = {"nextSequenceToken": "token456"}

        with patch("modules.common.boto3.client", return_value=mock_client):
            from modules.common import CloudWatchLogger

            logger = CloudWatchLogger("test-log-group")
            logger.sequence_token = "existing-token"
            logger.log("Test message")

            call_args = mock_client.put_log_events.call_args
            assert call_args.kwargs.get("sequenceToken") == "existing-token"

    def test_sequence_token_updated_after_log(self, mock_cloudwatch_logs: CloudWatchLogsClient) -> None:
        """Test that sequence token is updated after logging."""
        mock_client = MagicMock()
        mock_client.put_log_events.return_value = {"nextSequenceToken": "new-token"}

        with patch("modules.common.boto3.client", return_value=mock_client):
            from modules.common import CloudWatchLogger

            logger = CloudWatchLogger("test-log-group")
            assert logger.sequence_token is None

            logger.log("Test message")

            assert logger.sequence_token == "new-token"

    def test_log_calls_update_stream(self, mock_cloudwatch_logs: CloudWatchLogsClient) -> None:
        """Test that log calls _update_stream to ensure correct day."""
        mock_client = MagicMock()
        mock_client.put_log_events.return_value = {"nextSequenceToken": "token"}

        with patch("modules.common.boto3.client", return_value=mock_client):
            from modules.common import CloudWatchLogger

            logger = CloudWatchLogger("test-log-group")

            with patch.object(logger, "_update_stream") as mock_update:
                logger.log("Test message")
                mock_update.assert_called_once()


class TestConstants:
    """Tests for module constants."""

    def test_log_group_constants_exist(self) -> None:
        """Test that all log group constants are defined."""
        from modules.common import (
            ERROR_LOG_GROUP,
            EXECUTION_LOG_GROUP,
            METRICS_LOG_GROUP,
            PARSE_ERROR_LOG_GROUP,
            RUNTIME_ERROR_LOG_GROUP,
        )

        assert PARSE_ERROR_LOG_GROUP == "sbm-ingester-parse-error-log"
        assert RUNTIME_ERROR_LOG_GROUP == "sbm-ingester-runtime-error-log"
        assert ERROR_LOG_GROUP == "sbm-ingester-error-log"
        assert EXECUTION_LOG_GROUP == "sbm-ingester-execution-log"
        assert METRICS_LOG_GROUP == "sbm-ingester-metrics-log"

    def test_bucket_constants_exist(self) -> None:
        """Test that bucket and directory constants are defined."""
        from modules.common import (
            BUCKET_NAME,
            IRREVFILES_DIR,
            PARSE_ERR_DIR,
            PROCESSED_DIR,
        )

        assert BUCKET_NAME == "sbm-file-ingester"
        assert PARSE_ERR_DIR == "newParseErr/"
        assert IRREVFILES_DIR == "newIrrevFiles/"
        assert PROCESSED_DIR == "newP/"
