"""Unit tests for optima_shared/dynamodb.py module.

Tests DynamoDB resource initialization and site configuration queries.
"""

import boto3
from moto import mock_aws

from tests.unit.optima_exporter.conftest import reload_dynamodb_module


class TestGetDynamodb:
    """Tests for get_dynamodb function."""

    @mock_aws
    def test_lazy_initialization(self) -> None:
        """Test that DynamoDB resource is lazily initialized."""
        dynamodb_module = reload_dynamodb_module()

        # First call should create the resource
        result1 = dynamodb_module.get_dynamodb()
        assert result1 is not None

        # Second call should return the same resource
        result2 = dynamodb_module.get_dynamodb()
        assert result1 is result2

    @mock_aws
    def test_singleton_pattern(self) -> None:
        """Test that get_dynamodb returns the same instance."""
        dynamodb_module = reload_dynamodb_module()

        result1 = dynamodb_module.get_dynamodb()
        result2 = dynamodb_module.get_dynamodb()
        result3 = dynamodb_module.get_dynamodb()

        assert result1 is result2 is result3


class TestGetSitesForProject:
    """Tests for get_sites_for_project function."""

    @mock_aws
    def test_returns_sites_from_dynamodb(self) -> None:
        """Test that sites are fetched from DynamoDB."""
        # Create DynamoDB table and add data
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
        table.wait_until_exists()
        table.put_item(Item={"project": "bunnings", "nmi": "NMI001", "siteIdStr": "site-guid-001"})
        table.put_item(Item={"project": "bunnings", "nmi": "NMI002", "siteIdStr": "site-guid-002"})

        dynamodb_module = reload_dynamodb_module()
        sites = dynamodb_module.get_sites_for_project("bunnings")

        assert len(sites) == 2
        assert {"nmi": "NMI001", "siteIdStr": "site-guid-001"} in sites
        assert {"nmi": "NMI002", "siteIdStr": "site-guid-002"} in sites

    @mock_aws
    def test_handles_pagination(self) -> None:
        """Test that pagination is handled for large datasets."""
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
        table.wait_until_exists()

        # Add many items
        for i in range(50):
            table.put_item(Item={"project": "bunnings", "nmi": f"NMI{i:03d}", "siteIdStr": f"site-guid-{i:03d}"})

        dynamodb_module = reload_dynamodb_module()
        sites = dynamodb_module.get_sites_for_project("bunnings")
        assert len(sites) == 50

    @mock_aws
    def test_filters_invalid_sites(self) -> None:
        """Test that sites missing required fields are filtered out."""
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
        table.wait_until_exists()
        table.put_item(Item={"project": "bunnings", "nmi": "NMI001", "siteIdStr": "site-guid-001"})
        table.put_item(Item={"project": "bunnings", "nmi": "NMI002"})  # Missing siteIdStr

        dynamodb_module = reload_dynamodb_module()
        sites = dynamodb_module.get_sites_for_project("bunnings")

        assert len(sites) == 1
        assert sites[0]["nmi"] == "NMI001"

    @mock_aws
    def test_returns_empty_list_when_no_sites(self) -> None:
        """Test that empty list is returned when no sites exist."""
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
        table.wait_until_exists()

        dynamodb_module = reload_dynamodb_module()
        sites = dynamodb_module.get_sites_for_project("nonexistent")
        assert sites == []

    @mock_aws
    def test_handles_extra_fields(self) -> None:
        """Test handling of items with extra fields."""
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
        table.wait_until_exists()
        table.put_item(
            Item={
                "project": "bunnings",
                "nmi": "NMI001",
                "siteIdStr": "site-guid-001",
                "country": "AU",
                "extra_field": "ignored",
            }
        )

        dynamodb_module = reload_dynamodb_module()
        sites = dynamodb_module.get_sites_for_project("bunnings")

        assert len(sites) == 1
        assert sites[0] == {"nmi": "NMI001", "siteIdStr": "site-guid-001"}

    @mock_aws
    def test_with_extra_fields_complex(self) -> None:
        """Test that sites with extra fields are processed correctly."""
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
        table.wait_until_exists()
        table.put_item(
            Item={
                "project": "bunnings",
                "nmi": "NMI001",
                "siteIdStr": "site-guid-001",
                "country": "NZ",
                "extra1": "value1",
                "extra2": 123,
                "extra3": {"nested": "data"},
            }
        )

        dynamodb_module = reload_dynamodb_module()
        sites = dynamodb_module.get_sites_for_project("bunnings")

        assert len(sites) == 1
        assert sites[0] == {"nmi": "NMI001", "siteIdStr": "site-guid-001"}
        assert "extra1" not in sites[0]


class TestGetSiteByNmi:
    """Tests for get_site_by_nmi function."""

    @mock_aws
    def test_returns_site_when_found(self) -> None:
        """Test that site is returned when found."""
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
        table.wait_until_exists()
        table.put_item(Item={"project": "bunnings", "nmi": "NMI001", "siteIdStr": "site-guid-001"})

        dynamodb_module = reload_dynamodb_module()
        site = dynamodb_module.get_site_by_nmi("bunnings", "NMI001")

        assert site is not None
        assert site["nmi"] == "NMI001"
        assert site["siteIdStr"] == "site-guid-001"

    @mock_aws
    def test_returns_none_when_not_found(self) -> None:
        """Test that None is returned when site not found."""
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
        table.wait_until_exists()
        # No data inserted

        dynamodb_module = reload_dynamodb_module()
        site = dynamodb_module.get_site_by_nmi("bunnings", "NONEXISTENT")

        assert site is None

    @mock_aws
    def test_returns_none_when_missing_siteIdStr(self) -> None:
        """Test that None is returned when site exists but missing siteIdStr."""
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
        table.wait_until_exists()
        # Insert item without siteIdStr
        table.put_item(Item={"project": "bunnings", "nmi": "NMI001", "otherField": "value"})

        dynamodb_module = reload_dynamodb_module()
        site = dynamodb_module.get_site_by_nmi("bunnings", "NMI001")

        assert site is None

    @mock_aws
    def test_excludes_extra_fields(self) -> None:
        """Test that only nmi and siteIdStr are returned."""
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
        table.wait_until_exists()
        table.put_item(
            Item={
                "project": "bunnings",
                "nmi": "NMI001",
                "siteIdStr": "site-guid-001",
                "country": "NZ",
                "extraField": "ignored",
                "anotherField": 123,
            }
        )

        dynamodb_module = reload_dynamodb_module()
        site = dynamodb_module.get_site_by_nmi("bunnings", "NMI001")

        assert site is not None
        assert site == {"nmi": "NMI001", "siteIdStr": "site-guid-001"}
        assert "extraField" not in site

    @mock_aws
    def test_case_sensitive_lookup(self) -> None:
        """Test that lookup is case-sensitive."""
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
        table.wait_until_exists()
        table.put_item(Item={"project": "bunnings", "nmi": "NMI001", "siteIdStr": "site-guid-001"})

        dynamodb_module = reload_dynamodb_module()

        # Lowercase nmi should not match
        site = dynamodb_module.get_site_by_nmi("bunnings", "nmi001")
        assert site is None

        # Uppercase project should not match
        site = dynamodb_module.get_site_by_nmi("BUNNINGS", "NMI001")
        assert site is None
