"""End-to-end integration test spanning optima-interval-exporter → sbm-files-ingester.

Chain under test:
  Mock BidEnergy HTTP responses
  → optima_exporter.interval_exporter.app.lambda_handler
  → file lands in s3://sbm-file-ingester/newTBP/
  → file_processor.app.parse_and_write_data
  → file moved to newP/
  → sensor data written to s3://hudibucketsrc/sensorDataFiles/
  → output CSV references Neptune-mapped sensor ID
"""

import importlib
import json
import os
import re
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import boto3
import responses
from moto import mock_aws

# Ensure src packages are importable (conftest.py already does this at collection
# time, but explicit is safe for test-file-level clarity).
sys.path.insert(0, str(Path(__file__).parents[3] / "src"))
sys.path.insert(0, str(Path(__file__).parents[3] / "src" / "functions" / "optima_exporter"))


FIXTURE_PATH = Path(__file__).parents[1] / "fixtures" / "optima_bidenergy_nem12_sample.csv"

NMI_RAW = "4001348123"
NMI_PREFIXED = f"Optima_{NMI_RAW}"

NEPTUNE_MAP = {
    f"{NMI_PREFIXED}-B1": "p:test:optima-b1-neptune-id",
    f"{NMI_PREFIXED}-E1": "p:test:optima-e1-neptune-id",
    f"{NMI_PREFIXED}-K1": "p:test:optima-k1-neptune-id",
    f"{NMI_PREFIXED}-Q1": "p:test:optima-q1-neptune-id",
}


def _reload_optima_modules() -> None:
    """Force-reload all lazy-singleton optima modules so moto intercepts fresh clients."""
    import interval_exporter.processor as processor_module
    import interval_exporter.uploader as uploader_module
    import optima_shared.config as config_module
    import optima_shared.dynamodb as dynamodb_module

    dynamodb_module._dynamodb = None
    uploader_module._s3_client = None

    importlib.reload(config_module)
    importlib.reload(dynamodb_module)
    importlib.reload(uploader_module)
    importlib.reload(processor_module)


class TestOptimaToFileProcessorFullChain:
    """E2E test: BidEnergy mock → optima Lambda → S3 → file_processor → Hudi source bucket."""

    @mock_aws
    @responses.activate
    def test_full_chain_from_bidenergy_to_hudi_source(self) -> None:
        """
        Verifies the complete chain:
        1. optima_exporter downloads & uploads NEM12 to newTBP/
        2. file_processor parses it, maps NMIs, writes to hudibucketsrc/sensorDataFiles/
        3. Source file moves to newP/
        4. Output CSV references the Neptune-mapped sensor ID for E1 channel
        """
        # ---- AWS env ----
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"
        os.environ["POWERTOOLS_TRACE_DISABLED"] = "true"
        os.environ["POWERTOOLS_METRICS_NAMESPACE"] = "test"

        # ---- Optima exporter env ----
        os.environ["S3_UPLOAD_BUCKET"] = "sbm-file-ingester"
        os.environ["S3_UPLOAD_PREFIX"] = "newTBP/"
        os.environ["OPTIMA_PROJECTS"] = "bunnings"
        os.environ["OPTIMA_CONFIG_TABLE"] = "sbm-optima-config"
        os.environ["BIDENERGY_BASE_URL"] = "https://app.bidenergy.com"
        os.environ["OPTIMA_BUNNINGS_USERNAME"] = "bunnings@test.com"
        os.environ["OPTIMA_BUNNINGS_PASSWORD"] = "bunnings_pass"
        os.environ["OPTIMA_BUNNINGS_CLIENT_ID"] = "bunnings_client"
        os.environ["OPTIMA_BUNNINGS_COUNTRIES"] = "AU"
        os.environ["OPTIMA_DAYS_BACK"] = "1"
        os.environ["OPTIMA_MAX_WORKERS"] = "1"

        # ---- Create S3 buckets ----
        s3 = boto3.client("s3", region_name="ap-southeast-2")
        for bucket in ("sbm-file-ingester", "hudibucketsrc"):
            s3.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
            )

        # ---- Create CloudWatch log groups (file_processor writes to them) ----
        logs = boto3.client("logs", region_name="ap-southeast-2")
        for lg in (
            "sbm-ingester-error-log",
            "sbm-ingester-execution-log",
            "sbm-ingester-metrics-log",
            "sbm-ingester-parse-error-log",
            "sbm-ingester-runtime-error-log",
        ):
            logs.create_log_group(logGroupName=lg)

        # ---- Seed DynamoDB optima-config table ----
        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        table = dynamodb.create_table(
            TableName="sbm-optima-config",
            KeySchema=[
                {"AttributeName": "project", "KeyType": "HASH"},
                {"AttributeName": "nmi", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "project", "AttributeType": "S"},
                {"AttributeName": "nmi", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.put_item(
            Item={
                "project": "bunnings",
                "nmi": NMI_PREFIXED,
                "siteIdStr": "d6887406-ef6d-44b3-b8de-b3bb00678f0c",
                "country": "AU",
            }
        )

        # ---- Seed NEM12 mappings in S3 (read by file_processor) ----
        s3.put_object(
            Bucket="sbm-file-ingester",
            Key="nem12_mappings.json",
            Body=json.dumps(NEPTUNE_MAP).encode(),
        )

        # ---- Reload optima modules so they pick up fresh moto clients ----
        _reload_optima_modules()

        # ---- HTTP mocks for BidEnergy ----
        # 1. Login → 302 with .ASPXAUTH cookie
        responses.add(
            responses.POST,
            "https://app.bidenergy.com/Account/LogOn",
            status=302,
            headers={
                "Location": "https://app.bidenergy.com/",
                "Set-Cookie": ".ASPXAUTH=test-cookie-value; path=/; HttpOnly",
            },
        )

        # 2. NEM12 export endpoint → fixture bytes
        nem12_bytes = FIXTURE_PATH.read_bytes()
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/ExportIntervalUsageProfileNem12",
            status=200,
            body=nem12_bytes,
            content_type="application/vnd.csv",
        )

        # ============================================================
        # STEP 1: invoke optima interval exporter lambda_handler
        # ============================================================
        from interval_exporter.app import lambda_handler as optima_lambda_handler

        mock_context = MagicMock()
        mock_context.function_name = "optima-interval-exporter"
        mock_context.memory_limit_in_mb = 256
        mock_context.invoked_function_arn = (
            "arn:aws:lambda:ap-southeast-2:123456789012:function:optima-interval-exporter"
        )
        mock_context.aws_request_id = "e2e-test-request-id"

        event = {
            "project": "bunnings",
            "nmi": NMI_PREFIXED,
            "startDate": "2026-04-10",
            "endDate": "2026-04-10",
        }

        optima_result = optima_lambda_handler(event, mock_context)

        # ---- Assert optima Lambda succeeded ----
        assert optima_result["statusCode"] == 200, (
            f"Expected 200 from optima lambda, got {optima_result['statusCode']}: {optima_result}"
        )

        # ---- Assert exactly one file landed in newTBP/ ----
        tbp_objects = s3.list_objects_v2(Bucket="sbm-file-ingester", Prefix="newTBP/")
        tbp_keys = [o["Key"] for o in tbp_objects.get("Contents", [])]
        assert len(tbp_keys) == 1, f"Expected 1 object in newTBP/, got {len(tbp_keys)}: {tbp_keys}"

        uploaded_key = tbp_keys[0]
        filename = uploaded_key.split("/")[-1]

        # Filename pattern: optima_bunnings_NMI#OPTIMA_4001348123_...<timestamp>.csv
        assert re.match(
            rf"optima_bunnings_NMI#OPTIMA_{NMI_RAW}_2026-04-10_2026-04-10_\d{{14}}\.csv$",
            filename,
        ), f"Unexpected filename: {filename}"

        # ---- Assert file content has Optima-prefixed 200 records ----
        obj_body = s3.get_object(Bucket="sbm-file-ingester", Key=uploaded_key)["Body"].read()
        assert obj_body.startswith(b"100,NEM12,"), "File should start with NEM12 header record"
        assert obj_body.count(b"200,Optima_4001348123,") == 4, "Expected 4 prefixed 200 records (one per channel)"
        assert b"200,4001348123," not in obj_body, "Bare (unprefixed) NMI must not appear in 200 records"

        # ============================================================
        # STEP 2: invoke file_processor.parse_and_write_data
        # ============================================================
        import boto3 as boto3_mod

        # Build a fresh s3_resource pointing at moto so parse_and_write_data can
        # copy/delete objects and write to hudibucketsrc.
        fresh_s3_resource = boto3_mod.resource("s3", region_name="ap-southeast-2")

        with patch("functions.file_processor.app.s3_resource", fresh_s3_resource):
            from functions.file_processor.app import parse_and_write_data

            tbp_files = [{"bucket": "sbm-file-ingester", "file_name": uploaded_key}]
            fp_result = parse_and_write_data(tbp_files=tbp_files)

        # parse_and_write_data returns 1 on success, None on failure
        assert fp_result == 1, f"parse_and_write_data returned {fp_result!r}, expected 1"

        # ---- Assert source file moved from newTBP/ to newP/ ----
        tbp_after = s3.list_objects_v2(Bucket="sbm-file-ingester", Prefix="newTBP/")
        tbp_keys_after = [o["Key"] for o in tbp_after.get("Contents", [])]
        assert len(tbp_keys_after) == 0, (
            f"File should have been removed from newTBP/ after processing, found: {tbp_keys_after}"
        )

        newp_objects = s3.list_objects_v2(Bucket="sbm-file-ingester", Prefix="newP/")
        newp_keys = [o["Key"] for o in newp_objects.get("Contents", [])]
        assert len(newp_keys) == 1, f"Expected file in newP/, got: {newp_keys}"
        assert newp_keys[0] == f"newP/{filename}"

        # ---- Assert sensor data written to hudibucketsrc/sensorDataFiles/ ----
        hudi_objects = s3.list_objects_v2(Bucket="hudibucketsrc", Prefix="sensorDataFiles/")
        hudi_keys = [o["Key"] for o in hudi_objects.get("Contents", [])]
        assert len(hudi_keys) >= 1, "Expected at least one output CSV in hudibucketsrc/sensorDataFiles/"

        # ---- Assert output CSV references Neptune-mapped E1 sensor ID ----
        e1_neptune_id = NEPTUNE_MAP[f"{NMI_PREFIXED}-E1"]
        found_e1 = False
        for key in hudi_keys:
            body = s3.get_object(Bucket="hudibucketsrc", Key=key)["Body"].read().decode("utf-8")
            if e1_neptune_id in body:
                found_e1 = True
                # Verify it appears in a data row (not just anywhere by accident)
                lines = [ln for ln in body.strip().split("\n") if ln]
                data_rows_with_e1 = [ln for ln in lines[1:] if ln.startswith(e1_neptune_id + ",")]
                assert len(data_rows_with_e1) > 0, (
                    f"E1 Neptune ID found in CSV but not as sensorId column value.\nLines: {lines[:5]}"
                )
                break

        assert found_e1, (
            f"Neptune-mapped E1 sensor ID '{e1_neptune_id}' not found in any output CSV.\nHudi bucket keys: {hudi_keys}"
        )
