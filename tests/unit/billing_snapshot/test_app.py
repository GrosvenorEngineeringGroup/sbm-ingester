"""Tests for billing_snapshot.app — Lambda orchestration."""

import csv as csv_mod
from pathlib import Path
from unittest.mock import MagicMock, patch

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


def test_lambda_handler_writes_expected_csv_to_s3(aws_env, monkeypatch):
    monkeypatch.setenv("MAPPINGS_BUCKET", "test-ingester")
    monkeypatch.setenv("MAPPINGS_KEY", "nem12_mappings.json")
    monkeypatch.setenv("OUTPUT_BUCKET", "test-reports")
    monkeypatch.setenv("OUTPUT_KEY", "bunnings-billing/billing-latest.csv")
    monkeypatch.setenv("ATHENA_WORKGROUP", "wg")
    monkeypatch.setenv("CHUNK_COUNT", "2")
    monkeypatch.setenv("MAX_WORKERS", "2")
    monkeypatch.setenv("POLL_INTERVAL_SECONDS", "0")
    monkeypatch.setenv("POLL_TIMEOUT_SECONDS", "10")

    from importlib import reload

    import app as app_mod
    import config as config_mod

    reload(config_mod)
    reload(app_mod)

    with mock_aws():
        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="test-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )
        s3.create_bucket(
            Bucket="test-reports",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )
        s3.put_object(
            Bucket="test-ingester",
            Key="nem12_mappings.json",
            Body=(FIXTURES_DIR / "mappings_truncated.json").read_bytes(),
        )

        # Stub Athena chunk runner to return canned rows for 7 billing sensors.
        canned_rows = [
            ("p:bunnings:s1-peak", "2025-01-01 00:00:00.000", "100.5", "kwh"),
            ("p:bunnings:s1-offpeak", "2025-01-01 00:00:00.000", "50.0", "kwh"),
            ("p:bunnings:s1-echarge", "2025-01-01 00:00:00.000", "15.75", "aud"),
            ("p:bunnings:s1-tspend", "2025-01-01 00:00:00.000", "60.50", "aud"),
            ("p:bunnings:s2-peak", "2025-01-01 00:00:00.000", "9.99", "nzd"),
            ("p:bunnings:s2-tspend", "2025-01-01 00:00:00.000", "3.00", "nzd"),
            ("p:bunnings:s2-echarge", "2025-01-01 00:00:00.000", "2.50", "nzd"),
        ]
        ctx = MagicMock()
        ctx.function_name = "sbm-bunnings-billing-snapshot"
        ctx.memory_limit_in_mb = 1024
        ctx.invoked_function_arn = "arn:aws:lambda:ap-southeast-2:123456789012:function:sbm-bunnings-billing-snapshot"
        ctx.aws_request_id = "test-request-id"
        with patch.object(app_mod, "athena", new=app_mod.athena):
            with patch.object(app_mod.athena, "run_chunks_parallel", return_value=canned_rows):
                app_mod.lambda_handler({}, ctx)

        result = s3.get_object(Bucket="test-reports", Key="bunnings-billing/billing-latest.csv")
        body = result["Body"].read().decode("utf-8")

    lines = body.splitlines()
    # 1 header + 2 rows (2 distinct NMI/month combos)
    assert len(lines) == 3
    reader = csv_mod.DictReader(lines)
    rows_by_nmi = {r["nmi"]: r for r in reader}
    assert rows_by_nmi["2002105104"]["currency"] == "AUD"
    assert rows_by_nmi["2002105104"]["peak_usage"] == "100.50"
    assert rows_by_nmi["2002105104"]["total_spend"] == "60.50"
    assert rows_by_nmi["0000005438UN02B"]["currency"] == "NZD"
    assert rows_by_nmi["0000005438UN02B"]["peak_usage"] == "9.99"
    # Missing cells are empty (e.g., shoulder_usage not in source)
    assert rows_by_nmi["2002105104"]["shoulder_usage"] == ""


def test_lambda_handler_raises_on_empty_pivot(aws_env, monkeypatch):
    monkeypatch.setenv("MAPPINGS_BUCKET", "test-ingester")
    monkeypatch.setenv("MAPPINGS_KEY", "nem12_mappings.json")
    monkeypatch.setenv("OUTPUT_BUCKET", "test-reports")
    monkeypatch.setenv("OUTPUT_KEY", "bunnings-billing/billing-latest.csv")
    monkeypatch.setenv("POLL_INTERVAL_SECONDS", "0")
    monkeypatch.setenv("POLL_TIMEOUT_SECONDS", "10")

    from importlib import reload

    import app as app_mod
    import config as config_mod
    from pivot import EmptyPivotError

    reload(config_mod)
    reload(app_mod)

    with mock_aws():
        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="test-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )
        s3.create_bucket(
            Bucket="test-reports",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )
        s3.put_object(
            Bucket="test-ingester",
            Key="nem12_mappings.json",
            Body=(FIXTURES_DIR / "mappings_truncated.json").read_bytes(),
        )
        ctx = MagicMock()
        ctx.function_name = "sbm-bunnings-billing-snapshot"
        ctx.memory_limit_in_mb = 1024
        ctx.invoked_function_arn = "arn:aws:lambda:ap-southeast-2:123456789012:function:sbm-bunnings-billing-snapshot"
        ctx.aws_request_id = "test-request-id"
        with patch.object(app_mod.athena, "run_chunks_parallel", return_value=[]):
            with pytest.raises(EmptyPivotError):
                app_mod.lambda_handler({}, ctx)

        # Output CSV not created
        listing = s3.list_objects_v2(Bucket="test-reports").get("Contents", [])
        assert not listing
