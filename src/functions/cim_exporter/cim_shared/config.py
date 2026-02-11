"""CIM Exporter configuration.

All configuration values are loaded from environment variables.
No default values are provided for security reasons.
"""

import os

# =============================================================================
# CIM Authentication
# =============================================================================
CIM_LOGIN_URL = os.environ["CIM_LOGIN_URL"]
CIM_USERNAME = os.environ["CIM_USERNAME"]
CIM_PASSWORD = os.environ["CIM_PASSWORD"]

# =============================================================================
# CIM URLs
# =============================================================================
CIM_BASE_URL = os.environ["CIM_BASE_URL"]

# =============================================================================
# Site Configuration
# =============================================================================
# Site IDs as comma-separated string, e.g., "232,239,242"
CIM_SITE_IDS = [int(x.strip()) for x in os.environ["CIM_SITE_IDS"].split(",")]

# =============================================================================
# AWS SES SMTP Configuration
# =============================================================================
SMTP_HOST = os.environ["SMTP_HOST"]
SMTP_PORT = int(os.environ["SMTP_PORT"])
SMTP_USERNAME = os.environ["SMTP_USERNAME"]
SMTP_PASSWORD = os.environ["SMTP_PASSWORD"]

# =============================================================================
# Email Settings
# =============================================================================
EMAIL_FROM = os.environ["EMAIL_FROM"]
EMAIL_TO = os.environ["EMAIL_TO"].split(",")
EMAIL_SUBJECT = os.environ["EMAIL_SUBJECT"]
