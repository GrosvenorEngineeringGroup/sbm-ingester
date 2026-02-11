"""Unit tests for common.py module (constants only)."""


class TestConstants:
    """Tests for module constants."""

    def test_log_group_constants_exist(self) -> None:
        """Test that all log group constants are defined."""
        from shared.common import (
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
        from shared.common import (
            BUCKET_NAME,
            IRREVFILES_DIR,
            PARSE_ERR_DIR,
            PROCESSED_DIR,
        )

        assert BUCKET_NAME == "sbm-file-ingester"
        assert PARSE_ERR_DIR == "newParseErr/"
        assert IRREVFILES_DIR == "newIrrevFiles/"
        assert PROCESSED_DIR == "newP/"
