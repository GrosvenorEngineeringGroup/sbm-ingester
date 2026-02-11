"""Configuration management for Optima exporters."""

import os
from typing import Any

from aws_lambda_powertools import Logger

logger = Logger(service="optima-exporter")

# BidEnergy base URL
BIDENERGY_BASE_URL = os.environ.get("BIDENERGY_BASE_URL", "https://app.bidenergy.com")

# DynamoDB configuration
OPTIMA_CONFIG_TABLE = os.environ.get("OPTIMA_CONFIG_TABLE", "sbm-optima-config")

# S3 upload configuration
S3_UPLOAD_BUCKET = os.environ.get("S3_UPLOAD_BUCKET", "sbm-file-ingester")
S3_UPLOAD_PREFIX = os.environ.get("S3_UPLOAD_PREFIX", "newTBP/")

# Interval exporter configuration
OPTIMA_DAYS_BACK = int(os.environ.get("OPTIMA_DAYS_BACK", "7"))
MAX_WORKERS = int(os.environ.get("OPTIMA_MAX_WORKERS", "10"))

# Billing exporter configuration
OPTIMA_BILLING_MONTHS = int(os.environ.get("OPTIMA_BILLING_MONTHS", "12"))


def get_project_config(project: str) -> dict[str, Any] | None:
    """
    Get project credentials from environment variables.

    Args:
        project: Project name (e.g., "bunnings", "racv")

    Returns:
        Dict with username, password, client_id, or None if missing
    """
    prefix = f"OPTIMA_{project.upper()}_"
    username = os.environ.get(f"{prefix}USERNAME")
    password = os.environ.get(f"{prefix}PASSWORD")
    client_id = os.environ.get(f"{prefix}CLIENT_ID")

    if not all([username, password, client_id]):
        logger.warning("Missing credentials for project", extra={"project": project})
        return None

    return {
        "username": username,
        "password": password,
        "client_id": client_id,
    }


def get_project_countries(project: str) -> list[str]:
    """
    Get supported countries for a project from environment variables.

    Args:
        project: Project name (e.g., "bunnings", "racv")

    Returns:
        List of country codes (e.g., ["AU", "NZ"])
    """
    prefix = f"OPTIMA_{project.upper()}_"
    countries_str = os.environ.get(f"{prefix}COUNTRIES", "AU")
    return [c.strip() for c in countries_str.split(",") if c.strip()]
