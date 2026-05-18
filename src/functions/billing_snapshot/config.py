"""Environment variable reads for the billing snapshot Lambda."""

from __future__ import annotations

import os


def _str(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _int(name: str, default: int) -> int:
    return int(os.environ.get(name, str(default)))


ATHENA_WORKGROUP = _str("ATHENA_WORKGROUP", "sbm-billing-snapshot")
ATHENA_DATABASE = _str("ATHENA_DATABASE", "default")
ATHENA_TABLE = _str("ATHENA_TABLE", "sensordata_default")

MAPPINGS_BUCKET = _str("MAPPINGS_BUCKET", "sbm-file-ingester")
MAPPINGS_KEY = _str("MAPPINGS_KEY", "nem12_mappings.json")

OUTPUT_BUCKET = _str("OUTPUT_BUCKET", "gegoptimareports")
OUTPUT_KEY = _str("OUTPUT_KEY", "bunnings-billing/billing-latest.csv")

HISTORY_START_DATE = _str("HISTORY_START_DATE", "2025-01-01")

CHUNK_COUNT = _int("CHUNK_COUNT", 8)
MAX_WORKERS = _int("MAX_WORKERS", 3)
POLL_INTERVAL_SECONDS = _int("POLL_INTERVAL_SECONDS", 2)
POLL_TIMEOUT_SECONDS = _int("POLL_TIMEOUT_SECONDS", 240)
