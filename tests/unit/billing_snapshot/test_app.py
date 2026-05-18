"""Tests for billing_snapshot.app — Lambda orchestration."""

from pathlib import Path

import boto3
import pytest
from moto import mock_aws

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def mappings_in_s3(aws_env, monkeypatch):
    monkeypatch.setenv("MAPPINGS_BUCKET", "test-ingester")
    monkeypatch.setenv("MAPPINGS_KEY", "nem12_mappings.json")
    with mock_aws():
        client = boto3.client("s3", region_name="ap-southeast-2")
        client.create_bucket(
            Bucket="test-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )
        client.put_object(
            Bucket="test-ingester",
            Key="nem12_mappings.json",
            Body=(FIXTURES_DIR / "mappings_truncated.json").read_bytes(),
        )
        yield client


def test_load_mappings_returns_dict_and_age_hours(mappings_in_s3):
    from app import load_mappings

    mappings, age_hours = load_mappings(mappings_in_s3, bucket="test-ingester", key="nem12_mappings.json")
    assert isinstance(mappings, dict)
    assert "2002105104-billing-peak-usage" in mappings
    # Object was just uploaded; age in hours should be near 0
    assert 0 <= age_hours < 1
