"""Unit tests for demand_exporter/processor.py module.

Tests date range calculation, single-site processing, and full-export orchestration.
"""

import os
from typing import Any
from unittest.mock import MagicMock, patch  # noqa: F401  (MagicMock reserved for future tests)

import boto3
import pytest  # noqa: F401  (reserved for future tests)
import responses
from freezegun import freeze_time
from moto import mock_aws

from tests.unit.optima_exporter.conftest import reload_demand_processor_module


class TestGetDateRange:
    @freeze_time("2026-01-23 10:00:00")
    def test_default_returns_yesterday_only(self) -> None:
        processor_module = reload_demand_processor_module()
        start_date, end_date = processor_module.get_date_range()
        assert start_date == "2026-01-22"
        assert end_date == "2026-01-22"

    @freeze_time("2026-01-23 10:00:00")
    def test_respects_optima_days_back(self) -> None:
        os.environ["OPTIMA_DAYS_BACK"] = "7"
        processor_module = reload_demand_processor_module()
        start_date, end_date = processor_module.get_date_range()
        assert end_date == "2026-01-22"
        assert start_date == "2026-01-16"

    @freeze_time("2026-01-01 00:30:00")
    def test_at_midnight_uses_yesterday(self) -> None:
        processor_module = reload_demand_processor_module()
        _start, end = processor_module.get_date_range()
        assert end == "2025-12-31"


SAMPLE_CSV_BODY = (
    b'Commodities:,"Electricity"\r\n'
    b'Sites (NMIs):,"3117512760"\r\n'
    b'Status:,"Active"\r\n'
    b"Country:, Australia\r\n"
    b"Start:,01-Apr-2026\r\n"
    b"End:,30-Apr-2026\r\n"
    b"\r\n"
    b"\r\n"
    b"Business Unit,Identifier,Identifier Type,ReadingDateTime,E,kW,kVa,Power Factor,Site Name\r\n"
    b",3117512760,NMI,01-Apr-2026 00:00:00,59.1000,118.2000,120.3100,0.9825,RACV NOOSA RESORT\r\n"
)


class TestProcessSite:
    @mock_aws
    @responses.activate
    def test_successful_process_returns_success(self) -> None:
        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        processor_module = reload_demand_processor_module()
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=200,
            body=SAMPLE_CSV_BODY,
            content_type="application/vnd.csv",
        )

        result = processor_module.process_site(
            cookies=".ASPXAUTH=tok",
            nmi="Optima_3117512760",
            site_id_str="4f5855e0-0563-4bdc-b2d9-aa8d0041a2ca",
            start_date="2026-04-29",
            end_date="2026-04-29",
            project="racv",
            country="AU",
        )

        assert result["success"] is True
        assert result["nmi"] == "Optima_3117512760"
        assert result["error"] is None
        assert "filename" in result
        assert result["s3_key"].startswith("newTBP/optima_racv_demand_profile_NMI#OPTIMA_3117512760_")

    @mock_aws
    @responses.activate
    def test_no_data_sentinel_treated_as_success_and_uploaded(self) -> None:
        """No-data sentinel CSV is uploaded to S3 and reported as success with no_data flag."""
        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        processor_module = reload_demand_processor_module()
        no_data = (
            b'Commodities:,"Electricity"\r\nSites (NMIs):,"X"\r\n'
            b'Status:,"Active"\r\nCountry:, Australia\r\n'
            b"Start:,01-May-2026\r\nEnd:,03-May-2026\r\n\r\n\r\nNo data found"
        )
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=200,
            body=no_data,
            content_type="application/vnd.csv",
        )

        result = processor_module.process_site(
            cookies=".ASPXAUTH=tok",
            nmi="Optima_NODATA",
            site_id_str="site-empty",
            start_date="2026-05-01",
            end_date="2026-05-03",
            project="bunnings",
            country="AU",
        )

        assert result["success"] is True
        assert result["no_data"] is True
        assert result["s3_key"].startswith("newTBP/optima_bunnings_demand_profile_NMI#OPTIMA_NODATA_")

        # Verify the sentinel CSV is actually in S3 (audit retention)
        listing = s3.list_objects_v2(Bucket="sbm-file-ingester", Prefix="newTBP/")
        assert listing["KeyCount"] == 1
        body = s3.get_object(Bucket="sbm-file-ingester", Key=listing["Contents"][0]["Key"])["Body"].read()
        assert b"No data found" in body

    @responses.activate
    def test_download_failure_returns_error_result(self) -> None:
        processor_module = reload_demand_processor_module()
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=500,
            body=b"err",
        )

        result = processor_module.process_site(
            cookies=".ASPXAUTH=tok",
            nmi="Optima_X",
            site_id_str="site",
            start_date="2026-04-29",
            end_date="2026-04-29",
            project="racv",
            country="AU",
        )

        assert result["success"] is False
        assert result["error"] == "Failed to download CSV"

    @mock_aws
    @responses.activate
    def test_s3_upload_failure_returns_error_result(self) -> None:
        # Don't create the bucket → upload_to_s3 returns False
        processor_module = reload_demand_processor_module()
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=200,
            body=SAMPLE_CSV_BODY,
            content_type="application/vnd.csv",
        )

        result = processor_module.process_site(
            cookies=".ASPXAUTH=tok",
            nmi="Optima_X",
            site_id_str="site",
            start_date="2026-04-29",
            end_date="2026-04-29",
            project="racv",
            country="AU",
        )

        assert result["success"] is False
        assert result["error"] == "Failed to upload to S3"

    @mock_aws
    @responses.activate
    def test_country_propagates_to_url(self) -> None:
        from urllib.parse import parse_qs, urlparse

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        processor_module = reload_demand_processor_module()
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=200,
            body=SAMPLE_CSV_BODY,
            content_type="application/vnd.csv",
        )

        processor_module.process_site(
            cookies=".ASPXAUTH=tok",
            nmi="Optima_X",
            site_id_str="site",
            start_date="2026-04-29",
            end_date="2026-04-29",
            project="bunnings",
            country="NZ",
        )

        params = parse_qs(urlparse(responses.calls[0].request.url).query)
        assert params["filter.countrystr"] == ["NZ"]


class TestProcessExport:
    @freeze_time("2026-04-30 10:00:00")
    @mock_aws
    @responses.activate
    def test_happy_path_processes_all_sites(self) -> None:
        # Set up DynamoDB
        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        dynamodb.create_table(
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
        table = dynamodb.Table("sbm-optima-config")
        table.put_item(Item={"project": "racv", "nmi": "Optima_1", "siteIdStr": "site-1", "country": "AU"})
        table.put_item(Item={"project": "racv", "nmi": "Optima_2", "siteIdStr": "site-2", "country": "NZ"})

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=200,
            body=SAMPLE_CSV_BODY,
            content_type="application/vnd.csv",
        )

        processor_module = reload_demand_processor_module()
        with patch("demand_exporter.processor.login_bidenergy", return_value=".ASPXAUTH=cookies"):
            result = processor_module.process_export(project="racv")

        assert result["statusCode"] == 200
        assert result["body"]["success_count"] == 2
        assert result["body"]["error_count"] == 0
        listing = s3.list_objects_v2(Bucket="sbm-file-ingester", Prefix="newTBP/")
        assert listing["KeyCount"] == 2

    @mock_aws
    def test_missing_project_credentials_returns_400(self) -> None:
        for var in ("OPTIMA_RACV_USERNAME", "OPTIMA_RACV_PASSWORD", "OPTIMA_RACV_CLIENT_ID"):
            os.environ.pop(var, None)

        processor_module = reload_demand_processor_module()
        result = processor_module.process_export(project="racv")

        assert result["statusCode"] == 400
        assert "credentials" in result["body"].lower()

    @mock_aws
    def test_no_sites_returns_404(self) -> None:
        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        dynamodb.create_table(
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

        processor_module = reload_demand_processor_module()
        result = processor_module.process_export(project="racv")

        assert result["statusCode"] == 404
        assert "no sites" in result["body"].lower()

    @mock_aws
    def test_login_failure_returns_401(self) -> None:
        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        dynamodb.create_table(
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
        dynamodb.Table("sbm-optima-config").put_item(
            Item={"project": "racv", "nmi": "Optima_1", "siteIdStr": "site-1", "country": "AU"}
        )

        processor_module = reload_demand_processor_module()
        with patch("demand_exporter.processor.login_bidenergy", return_value=None):
            result = processor_module.process_export(project="racv")

        assert result["statusCode"] == 401
        assert "authenticate" in result["body"].lower()

    def test_inverted_date_range_returns_400(self) -> None:
        processor_module = reload_demand_processor_module()
        result = processor_module.process_export(
            project="racv",
            start_date="2026-04-30",
            end_date="2026-04-29",
        )

        assert result["statusCode"] == 400
        assert "invalid range" in result["body"].lower()

    @freeze_time("2026-04-30 10:00:00")
    @mock_aws
    @responses.activate
    def test_single_nmi_mode_processes_only_that_site(self) -> None:
        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        dynamodb.create_table(
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
        table = dynamodb.Table("sbm-optima-config")
        table.put_item(Item={"project": "racv", "nmi": "Optima_1", "siteIdStr": "site-1", "country": "AU"})
        table.put_item(Item={"project": "racv", "nmi": "Optima_2", "siteIdStr": "site-2", "country": "AU"})

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=200,
            body=SAMPLE_CSV_BODY,
            content_type="application/vnd.csv",
        )

        processor_module = reload_demand_processor_module()
        with patch("demand_exporter.processor.login_bidenergy", return_value=".ASPXAUTH=cookies"):
            result = processor_module.process_export(project="racv", nmi="Optima_1")

        assert result["statusCode"] == 200
        assert result["body"]["success_count"] == 1
        listing = s3.list_objects_v2(Bucket="sbm-file-ingester", Prefix="newTBP/")
        assert listing["KeyCount"] == 1
        assert "OPTIMA_1" in listing["Contents"][0]["Key"]

    @mock_aws
    def test_single_nmi_not_found_returns_404(self) -> None:
        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        dynamodb.create_table(
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

        processor_module = reload_demand_processor_module()
        result = processor_module.process_export(project="racv", nmi="Optima_DOES_NOT_EXIST")

        assert result["statusCode"] == 404
        assert "not found" in result["body"].lower()

    @freeze_time("2026-04-30 10:00:00")
    @mock_aws
    @responses.activate
    def test_partial_failure_returns_207(self) -> None:
        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        dynamodb.create_table(
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
        table = dynamodb.Table("sbm-optima-config")
        table.put_item(Item={"project": "racv", "nmi": "Optima_OK", "siteIdStr": "site-ok", "country": "AU"})
        table.put_item(Item={"project": "racv", "nmi": "Optima_BAD", "siteIdStr": "site-bad", "country": "AU"})

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        def callback(request: Any) -> tuple[int, dict, bytes]:
            if "site-ok" in request.url:
                return 200, {"Content-Type": "application/vnd.csv"}, SAMPLE_CSV_BODY
            return 500, {}, b"err"

        responses.add_callback(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            callback=callback,
        )

        processor_module = reload_demand_processor_module()
        with patch("demand_exporter.processor.login_bidenergy", return_value=".ASPXAUTH=cookies"):
            result = processor_module.process_export(project="racv")

        assert result["statusCode"] == 207
        assert result["body"]["success_count"] == 1
        assert result["body"]["error_count"] == 1
