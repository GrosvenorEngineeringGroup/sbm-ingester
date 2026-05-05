"""Unit tests for demand_exporter/processor.py module.

Tests date range calculation, single-site processing, and full-export orchestration.
"""

import os
from typing import Any  # noqa: F401  (used by Task 8 tests)
from unittest.mock import MagicMock, patch  # noqa: F401  (used by Task 8 tests)

import boto3
import pytest  # noqa: F401  (used by Task 8 tests)
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
