"""Pytest configuration for sbm-ingester tests."""

import sys
from pathlib import Path

# Add function directories to sys.path for Lambda-style imports
# This allows tests to work with the same import style as Lambda runtime

# Add optima_exporter directory for shared/interval_exporter/billing_exporter imports
optima_exporter_path = Path(__file__).parent.parent / "src" / "functions" / "optima_exporter"
if str(optima_exporter_path) not in sys.path:
    sys.path.insert(0, str(optima_exporter_path))
