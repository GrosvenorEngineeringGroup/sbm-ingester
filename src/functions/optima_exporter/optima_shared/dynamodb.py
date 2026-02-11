"""DynamoDB utilities for Optima exporters."""

from typing import Any

import boto3
from aws_lambda_powertools import Logger
from boto3.dynamodb.conditions import Key

from optima_shared.config import OPTIMA_CONFIG_TABLE

logger = Logger(service="optima-exporter")

# DynamoDB resource (lazy initialization)
_dynamodb = None


def get_dynamodb() -> Any:
    """Get DynamoDB resource with lazy initialization."""
    global _dynamodb
    if _dynamodb is None:
        _dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
    return _dynamodb


def get_sites_for_project(project: str) -> list[dict[str, str]]:
    """
    Fetch sites for a project from DynamoDB.

    Args:
        project: Project name (e.g., "bunnings", "racv")

    Returns:
        List of site dicts with nmi and siteIdStr
    """
    table = get_dynamodb().Table(OPTIMA_CONFIG_TABLE)
    response = table.query(KeyConditionExpression=Key("project").eq(project))
    sites = response.get("Items", [])

    # Handle pagination for large datasets
    while "LastEvaluatedKey" in response:
        response = table.query(
            KeyConditionExpression=Key("project").eq(project),
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        sites.extend(response.get("Items", []))

    # Filter out items missing required fields
    valid_sites = [{"nmi": s["nmi"], "siteIdStr": s["siteIdStr"]} for s in sites if "nmi" in s and "siteIdStr" in s]

    if len(valid_sites) < len(sites):
        logger.warning(
            "Some sites missing required fields",
            extra={"project": project, "total": len(sites), "valid": len(valid_sites)},
        )

    logger.info("Fetched sites from DynamoDB", extra={"project": project, "site_count": len(valid_sites)})
    return valid_sites


def get_site_by_nmi(project: str, nmi: str) -> dict[str, str] | None:
    """
    Get a single site from DynamoDB by project and NMI.

    Args:
        project: Project name (e.g., "bunnings", "racv")
        nmi: NMI identifier

    Returns:
        Site dict with nmi and siteIdStr, or None if not found
    """
    table = get_dynamodb().Table(OPTIMA_CONFIG_TABLE)
    response = table.get_item(Key={"project": project, "nmi": nmi})
    item = response.get("Item")

    if item and "siteIdStr" in item:
        logger.info("Found site by NMI", extra={"project": project, "nmi": nmi})
        return {"nmi": item["nmi"], "siteIdStr": item["siteIdStr"]}

    logger.warning("Site not found or missing siteIdStr", extra={"project": project, "nmi": nmi})
    return None
