"""Tests for shared.parsers._mappings.get_nem12_mappings."""

import json

import boto3
import pytest
from moto import mock_aws

from shared.parsers import _mappings as mappings_mod


@pytest.fixture
def _reset_cache():
    mappings_mod._cache = None
    yield
    mappings_mod._cache = None


@mock_aws
def test_loads_and_caches(_reset_cache, monkeypatch) -> None:
    """First call loads from S3; subsequent calls reuse the cache."""
    s3 = boto3.client("s3", region_name="ap-southeast-2")
    s3.create_bucket(
        Bucket="sbm-file-ingester",
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
    )
    mappings_payload = {
        "VCCCLG0019-billing-peak-usage": "p:bunnings:19c88bf11c8-76959f",
        "VCCCLG0019-billing-off-peak-usage": "p:bunnings:19c88bf11ca-38fd75",
    }
    s3.put_object(
        Bucket="sbm-file-ingester",
        Key="nem12_mappings.json",
        Body=json.dumps(mappings_payload).encode(),
    )

    from unittest.mock import patch

    with patch(
        "shared.parsers._mappings.boto3.client",
        wraps=boto3.client,
    ) as spy:
        first = mappings_mod.get_nem12_mappings()
        second = mappings_mod.get_nem12_mappings()

    assert first == mappings_payload
    assert second is first
    s3_calls = [c for c in spy.call_args_list if c.args and c.args[0] == "s3"]
    assert len(s3_calls) == 1, f"expected 1 s3 client call, got {len(s3_calls)}"
