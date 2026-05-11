"""Pytest configuration for sbm-ingester tests."""

import os
import sys
from pathlib import Path

# Add function directories to sys.path for Lambda-style imports.
optima_exporter_path = Path(__file__).parent.parent / "src" / "functions" / "optima_exporter"
if str(optima_exporter_path) not in sys.path:
    sys.path.insert(0, str(optima_exporter_path))

# Required env vars for module-import-time reads. Without this, importing
# functions.file_processor.app raises KeyError because production code reads
# os.environ["SQS_QUEUE_URL"] at import time (no fallback).
os.environ.setdefault("SQS_QUEUE_URL", "https://sqs.test.local/queue")
