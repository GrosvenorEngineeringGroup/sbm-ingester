"""Shared pytest fixtures for optima_exporter tests.

This conftest provides fixtures specific to the optima_exporter Lambda functions.
It inherits fixtures from the parent conftest.py (AWS mocks, sample data, etc.)
and adds optima-specific fixtures.
"""

import importlib
import os
from collections.abc import Generator
from typing import Any
from unittest.mock import MagicMock

import pytest

# ==================== Module Reload Functions ====================
# These are regular functions (not fixtures) that must be imported and called directly.
# They reset module-level singletons and reload modules with fresh environment variables.


def reload_config_module() -> Any:
    """Reload the config module with fresh environment."""
    import optima_shared.config as config_module

    importlib.reload(config_module)
    return config_module


def reload_dynamodb_module() -> Any:
    """Reload the dynamodb module with fresh environment."""
    import optima_shared.dynamodb as dynamodb_module

    dynamodb_module._dynamodb = None
    importlib.reload(dynamodb_module)
    return dynamodb_module


def reload_uploader_module() -> Any:
    """Reload the uploader module with fresh environment."""
    import interval_exporter.uploader as uploader_module

    uploader_module._s3_client = None
    importlib.reload(uploader_module)
    return uploader_module


def reload_processor_module() -> Any:
    """Reload the processor module with fresh environment."""
    # First reload config to pick up new env vars
    import optima_shared.config as config_module

    importlib.reload(config_module)

    import interval_exporter.processor as processor_module

    importlib.reload(processor_module)
    return processor_module


# ==================== Environment Fixtures ====================


@pytest.fixture(autouse=True)
def reset_env() -> Generator[None]:
    """Reset environment variables before each test.

    This autouse fixture ensures a clean environment for every test.
    It sets up standard AWS and Optima configuration values.
    """
    # Save original env
    original_env = os.environ.copy()

    # Set up test environment
    os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["POWERTOOLS_TRACE_DISABLED"] = "true"
    os.environ["POWERTOOLS_METRICS_NAMESPACE"] = "test"

    # S3 upload config
    os.environ["S3_UPLOAD_BUCKET"] = "sbm-file-ingester"
    os.environ["S3_UPLOAD_PREFIX"] = "newTBP/"

    # Optima config
    os.environ["OPTIMA_PROJECTS"] = "bunnings,racv"
    os.environ["OPTIMA_DAYS_BACK"] = "7"
    os.environ["OPTIMA_CONFIG_TABLE"] = "sbm-optima-config"
    os.environ["BIDENERGY_BASE_URL"] = "https://app.bidenergy.com"

    # Project credentials - bunnings
    os.environ["OPTIMA_BUNNINGS_USERNAME"] = "bunnings@test.com"
    os.environ["OPTIMA_BUNNINGS_PASSWORD"] = "bunnings_pass"
    os.environ["OPTIMA_BUNNINGS_CLIENT_ID"] = "bunnings_client"
    os.environ["OPTIMA_BUNNINGS_COUNTRIES"] = "AU,NZ"

    # Project credentials - racv
    os.environ["OPTIMA_RACV_USERNAME"] = "racv@test.com"
    os.environ["OPTIMA_RACV_PASSWORD"] = "racv_pass"
    os.environ["OPTIMA_RACV_CLIENT_ID"] = "racv_client"
    os.environ["OPTIMA_RACV_COUNTRIES"] = "AU"

    yield

    # Restore original env
    os.environ.clear()
    os.environ.update(original_env)


# ==================== Lambda Context Fixtures ====================


@pytest.fixture
def mock_lambda_context() -> MagicMock:
    """Create mock Lambda context for interval exporter."""
    context = MagicMock()
    context.function_name = "optima-interval-exporter"
    context.memory_limit_in_mb = 256
    context.invoked_function_arn = "arn:aws:lambda:ap-southeast-2:123456789012:function:optima-interval-exporter"
    context.aws_request_id = "test-request-id"
    return context


@pytest.fixture
def mock_billing_lambda_context() -> MagicMock:
    """Create mock Lambda context for billing exporter."""
    context = MagicMock()
    context.function_name = "optima-billing-exporter"
    context.memory_limit_in_mb = 128
    context.invoked_function_arn = "arn:aws:lambda:ap-southeast-2:123456789012:function:optima-billing-exporter"
    context.aws_request_id = "test-request-id"
    return context
