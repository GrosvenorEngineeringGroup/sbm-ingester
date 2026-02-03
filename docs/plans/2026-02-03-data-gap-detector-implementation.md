# Data Gap Detector Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a Lambda function to detect missing sensor data in the Hudi data lake for bunnings/racv projects.

**Architecture:** Read sensor mappings from nem12_mappings.json, filter by project, batch query Hudi via AWS SDK for pandas (Athena), analyze date gaps, output CSV report.

**Tech Stack:** Python 3.13, AWS SDK for pandas (awswrangler), pandas, ThreadPoolExecutor, pytest, moto

---

## Task 1: Add Dependencies

**Files:**
- Modify: `pyproject.toml` (via uv add)

**Step 1: Add awswrangler dependency**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv add awswrangler
```

Expected: `pyproject.toml` updated with awswrangler dependency

**Step 2: Add tqdm dev dependency**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv add --optional dev tqdm
```

Expected: `pyproject.toml` updated with tqdm in dev dependencies

**Step 3: Sync dependencies**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv sync --all-extras
```

Expected: Dependencies installed successfully

**Step 4: Commit**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && git add pyproject.toml uv.lock && git commit -m "feat(data-gap): add awswrangler and tqdm dependencies"
```

---

## Task 2: Create Module Structure

**Files:**
- Create: `src/functions/data_gap_detector/__init__.py`
- Create: `src/functions/data_gap_detector/mappings.py`
- Create: `tests/unit/data_gap_detector/__init__.py`
- Create: `tests/unit/data_gap_detector/test_mappings.py`

**Step 1: Create directory structure**

Run:
```bash
mkdir -p /Users/zeyu/Desktop/GEG/sbm/sbm-ingester/src/functions/data_gap_detector
mkdir -p /Users/zeyu/Desktop/GEG/sbm/sbm-ingester/tests/unit/data_gap_detector
mkdir -p /Users/zeyu/Desktop/GEG/sbm/sbm-ingester/output
```

**Step 2: Create __init__.py files**

Create `src/functions/data_gap_detector/__init__.py`:
```python
"""Data gap detector for Hudi data lake."""
```

Create `tests/unit/data_gap_detector/__init__.py`:
```python
"""Tests for data gap detector."""
```

**Step 3: Add output to .gitignore**

Append to `.gitignore`:
```
# Local test output
output/
```

**Step 4: Commit**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && git add src/functions/data_gap_detector tests/unit/data_gap_detector .gitignore && git commit -m "feat(data-gap): create module structure"
```

---

## Task 3: Implement Mappings Module (TDD)

**Files:**
- Create: `src/functions/data_gap_detector/mappings.py`
- Create: `tests/unit/data_gap_detector/test_mappings.py`

**Step 1: Write failing tests for mappings**

Create `tests/unit/data_gap_detector/test_mappings.py`:
```python
"""Tests for mappings module."""

import json
import tempfile
from pathlib import Path

import pytest


class TestLoadMappings:
    """Tests for load_mappings function."""

    def test_load_mappings_returns_dict(self, tmp_path: Path) -> None:
        """load_mappings returns a dictionary from JSON file."""
        from src.functions.data_gap_detector.mappings import load_mappings

        mappings_file = tmp_path / "mappings.json"
        mappings_file.write_text('{"NMI-E1": "p:bunnings:abc123"}')

        result = load_mappings(str(mappings_file))

        assert isinstance(result, dict)
        assert result == {"NMI-E1": "p:bunnings:abc123"}

    def test_load_mappings_file_not_found(self) -> None:
        """load_mappings raises FileNotFoundError for missing file."""
        from src.functions.data_gap_detector.mappings import load_mappings

        with pytest.raises(FileNotFoundError):
            load_mappings("/nonexistent/path.json")


class TestFilterByProject:
    """Tests for filter_by_project function."""

    def test_filter_bunnings_project(self) -> None:
        """filter_by_project returns only bunnings sensors."""
        from src.functions.data_gap_detector.mappings import filter_by_project

        mappings = {
            "NMI1-E1": "p:bunnings:abc123",
            "NMI2-E1": "p:racv:def456",
            "NMI3-E1": "p:bunnings:ghi789",
            "NMI4-E1": "p:amp_sites:r:xyz",
        }

        result = filter_by_project(mappings, "bunnings")

        assert len(result) == 2
        assert "NMI1-E1" in result
        assert "NMI3-E1" in result
        assert result["NMI1-E1"] == "p:bunnings:abc123"

    def test_filter_racv_project(self) -> None:
        """filter_by_project returns only racv sensors."""
        from src.functions.data_gap_detector.mappings import filter_by_project

        mappings = {
            "NMI1-E1": "p:bunnings:abc123",
            "NMI2-E1": "p:racv:def456",
            "NMI3-E1": "p:racv:ghi789",
        }

        result = filter_by_project(mappings, "racv")

        assert len(result) == 2
        assert "NMI2-E1" in result
        assert "NMI3-E1" in result

    def test_filter_no_matching_sensors(self) -> None:
        """filter_by_project returns empty dict when no match."""
        from src.functions.data_gap_detector.mappings import filter_by_project

        mappings = {
            "NMI1-E1": "p:bunnings:abc123",
        }

        result = filter_by_project(mappings, "racv")

        assert result == {}

    def test_filter_case_insensitive(self) -> None:
        """filter_by_project is case insensitive for project name."""
        from src.functions.data_gap_detector.mappings import filter_by_project

        mappings = {
            "NMI1-E1": "p:bunnings:abc123",
            "NMI2-E1": "p:BUNNINGS:def456",
        }

        result = filter_by_project(mappings, "Bunnings")

        assert len(result) == 2


class TestExtractProject:
    """Tests for extract_project function."""

    def test_extract_bunnings(self) -> None:
        """extract_project returns project name from point_id."""
        from src.functions.data_gap_detector.mappings import extract_project

        assert extract_project("p:bunnings:abc123") == "bunnings"

    def test_extract_racv(self) -> None:
        """extract_project returns project name from point_id."""
        from src.functions.data_gap_detector.mappings import extract_project

        assert extract_project("p:racv:def456-789") == "racv"

    def test_extract_amp_sites(self) -> None:
        """extract_project handles amp_sites format."""
        from src.functions.data_gap_detector.mappings import extract_project

        assert extract_project("p:amp_sites:r:269ff25a-543a0702") == "amp_sites"

    def test_extract_invalid_format(self) -> None:
        """extract_project returns None for invalid format."""
        from src.functions.data_gap_detector.mappings import extract_project

        assert extract_project("invalid") is None
        assert extract_project("") is None
```

**Step 2: Run tests to verify they fail**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv run pytest tests/unit/data_gap_detector/test_mappings.py -v
```

Expected: FAIL with "No module named 'src.functions.data_gap_detector.mappings'"

**Step 3: Implement mappings module**

Create `src/functions/data_gap_detector/mappings.py`:
```python
"""Load and filter NEM12 mappings by project."""

import json
from pathlib import Path


def load_mappings(file_path: str) -> dict[str, str]:
    """
    Load NEM12 mappings from JSON file.

    Args:
        file_path: Path to nem12_mappings.json

    Returns:
        Dictionary mapping nmi_channel to point_id

    Raises:
        FileNotFoundError: If file does not exist
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Mappings file not found: {file_path}")

    with path.open() as f:
        return json.load(f)


def extract_project(point_id: str) -> str | None:
    """
    Extract project name from point_id.

    Point ID format: p:{project}:{id}
    Examples:
        - p:bunnings:19bbb227caf-be52d94d -> bunnings
        - p:racv:18be0cf5ac8-d0f3fda2 -> racv
        - p:amp_sites:r:269ff25a-543a0702 -> amp_sites

    Args:
        point_id: Neptune point ID

    Returns:
        Project name or None if format is invalid
    """
    if not point_id or not point_id.startswith("p:"):
        return None

    parts = point_id.split(":")
    if len(parts) < 3:
        return None

    return parts[1]


def filter_by_project(mappings: dict[str, str], project: str) -> dict[str, str]:
    """
    Filter mappings to only include sensors for a specific project.

    Args:
        mappings: Full mappings dictionary
        project: Project name (bunnings, racv)

    Returns:
        Filtered mappings dictionary
    """
    project_lower = project.lower()
    return {
        nmi_channel: point_id
        for nmi_channel, point_id in mappings.items()
        if extract_project(point_id) and extract_project(point_id).lower() == project_lower
    }
```

**Step 4: Run tests to verify they pass**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv run pytest tests/unit/data_gap_detector/test_mappings.py -v
```

Expected: All tests PASS

**Step 5: Run linter**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv run ruff check src/functions/data_gap_detector tests/unit/data_gap_detector && uv run ruff format src/functions/data_gap_detector tests/unit/data_gap_detector
```

Expected: No errors

**Step 6: Commit**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && git add src/functions/data_gap_detector/mappings.py tests/unit/data_gap_detector/test_mappings.py && git commit -m "feat(data-gap): implement mappings module with project filtering"
```

---

## Task 4: Implement Detector Module (TDD)

**Files:**
- Create: `src/functions/data_gap_detector/detector.py`
- Create: `tests/unit/data_gap_detector/test_detector.py`

**Step 1: Write failing tests for detector**

Create `tests/unit/data_gap_detector/test_detector.py`:
```python
"""Tests for detector module."""

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


class TestAnalyzeSensorGaps:
    """Tests for analyze_sensor_gaps function."""

    def test_no_data_detected(self) -> None:
        """analyze_sensor_gaps detects no_data when sensor has no records."""
        from src.functions.data_gap_detector.detector import analyze_sensor_gaps

        # Empty DataFrame - no data for this sensor
        df = pd.DataFrame(columns=["sensorId", "data_date", "record_count"])

        result = analyze_sensor_gaps(
            sensor_id="p:bunnings:abc123",
            nmi_channel="NMI-E1",
            df=df,
            start_date=None,
            end_date=None,
        )

        assert result["issue_type"] == "no_data"
        assert result["missing_count"] == 0
        assert result["data_start"] == ""
        assert result["data_end"] == ""

    def test_missing_dates_detected(self) -> None:
        """analyze_sensor_gaps detects missing dates in range."""
        from src.functions.data_gap_detector.detector import analyze_sensor_gaps

        # Data for Jan 1, 3, 5 (missing Jan 2, 4)
        df = pd.DataFrame({
            "sensorId": ["p:bunnings:abc123"] * 3,
            "data_date": [date(2024, 1, 1), date(2024, 1, 3), date(2024, 1, 5)],
            "record_count": [48, 48, 48],
        })

        result = analyze_sensor_gaps(
            sensor_id="p:bunnings:abc123",
            nmi_channel="NMI-E1",
            df=df,
            start_date=None,
            end_date=None,
        )

        assert result["issue_type"] == "missing_dates"
        assert result["missing_count"] == 2
        assert "2024-01-02" in result["missing_dates"]
        assert "2024-01-04" in result["missing_dates"]
        assert result["data_start"] == "2024-01-01"
        assert result["data_end"] == "2024-01-05"

    def test_complete_data_returns_none(self) -> None:
        """analyze_sensor_gaps returns None when data is complete."""
        from src.functions.data_gap_detector.detector import analyze_sensor_gaps

        # Complete data for Jan 1-3
        df = pd.DataFrame({
            "sensorId": ["p:bunnings:abc123"] * 3,
            "data_date": [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
            "record_count": [48, 48, 48],
        })

        result = analyze_sensor_gaps(
            sensor_id="p:bunnings:abc123",
            nmi_channel="NMI-E1",
            df=df,
            start_date=None,
            end_date=None,
        )

        assert result is None

    def test_user_specified_date_range(self) -> None:
        """analyze_sensor_gaps uses user-specified date range."""
        from src.functions.data_gap_detector.detector import analyze_sensor_gaps

        # Data only for Jan 2
        df = pd.DataFrame({
            "sensorId": ["p:bunnings:abc123"],
            "data_date": [date(2024, 1, 2)],
            "record_count": [48],
        })

        result = analyze_sensor_gaps(
            sensor_id="p:bunnings:abc123",
            nmi_channel="NMI-E1",
            df=df,
            start_date="2024-01-01",
            end_date="2024-01-03",
        )

        assert result["issue_type"] == "missing_dates"
        assert result["missing_count"] == 2
        assert "2024-01-01" in result["missing_dates"]
        assert "2024-01-03" in result["missing_dates"]
        assert result["total_expected_days"] == 3


class TestChunkList:
    """Tests for chunk_list utility function."""

    def test_chunk_list_even_split(self) -> None:
        """chunk_list splits list evenly."""
        from src.functions.data_gap_detector.detector import chunk_list

        items = [1, 2, 3, 4, 5, 6]
        result = chunk_list(items, 2)

        assert result == [[1, 2], [3, 4], [5, 6]]

    def test_chunk_list_uneven_split(self) -> None:
        """chunk_list handles uneven splits."""
        from src.functions.data_gap_detector.detector import chunk_list

        items = [1, 2, 3, 4, 5]
        result = chunk_list(items, 2)

        assert result == [[1, 2], [3, 4], [5]]

    def test_chunk_list_single_chunk(self) -> None:
        """chunk_list returns single chunk when size >= len."""
        from src.functions.data_gap_detector.detector import chunk_list

        items = [1, 2, 3]
        result = chunk_list(items, 10)

        assert result == [[1, 2, 3]]

    def test_chunk_list_empty(self) -> None:
        """chunk_list handles empty list."""
        from src.functions.data_gap_detector.detector import chunk_list

        result = chunk_list([], 5)

        assert result == []


class TestBuildQuery:
    """Tests for build_query function."""

    def test_build_query_basic(self) -> None:
        """build_query generates correct SQL."""
        from src.functions.data_gap_detector.detector import build_query

        sensor_ids = ["p:bunnings:abc", "p:bunnings:def"]
        query = build_query(sensor_ids, "2024-01-01", "2024-01-31")

        assert "SELECT" in query
        assert "sensorId" in query
        assert "DATE(ts)" in query
        assert "GROUP BY" in query
        assert "'p:bunnings:abc'" in query
        assert "'p:bunnings:def'" in query
        assert "2024-01-01" in query
        assert "2024-01-31" in query
```

**Step 2: Run tests to verify they fail**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv run pytest tests/unit/data_gap_detector/test_detector.py -v
```

Expected: FAIL with "No module named 'src.functions.data_gap_detector.detector'"

**Step 3: Implement detector module**

Create `src/functions/data_gap_detector/detector.py`:
```python
"""Core detection logic for data gaps in Hudi data lake."""

from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd

# Configuration
BATCH_SIZE = 50
MAX_WORKERS = 5
DATABASE = "default"
TABLE = "sensordata_default"
ATHENA_OUTPUT = "s3://hudibucketsrc/queryresult/"


def chunk_list(items: list, chunk_size: int) -> list[list]:
    """Split a list into chunks of specified size."""
    if not items:
        return []
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def build_query(sensor_ids: list[str], start_date: str, end_date: str) -> str:
    """
    Build Athena SQL query for batch of sensors.

    Args:
        sensor_ids: List of sensorId values to query
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        SQL query string
    """
    ids_str = ",".join(f"'{sid}'" for sid in sensor_ids)

    return f"""
        SELECT
            sensorId,
            DATE(ts) as data_date,
            COUNT(*) as record_count
        FROM {DATABASE}.{TABLE}
        WHERE sensorId IN ({ids_str})
          AND ts >= TIMESTAMP '{start_date} 00:00:00'
          AND ts <= TIMESTAMP '{end_date} 23:59:59'
        GROUP BY sensorId, DATE(ts)
        ORDER BY sensorId, data_date
    """


def analyze_sensor_gaps(
    sensor_id: str,
    nmi_channel: str,
    df: pd.DataFrame,
    start_date: str | None,
    end_date: str | None,
) -> dict[str, Any] | None:
    """
    Analyze a single sensor's data for gaps.

    Args:
        sensor_id: Neptune point ID
        nmi_channel: NMI-channel identifier
        df: DataFrame with data_date column for this sensor
        start_date: Optional user-specified start date
        end_date: Optional user-specified end date

    Returns:
        Dict with gap info or None if no gaps
    """
    # Filter to this sensor
    sensor_df = df[df["sensorId"] == sensor_id] if not df.empty else df

    # No data case
    if sensor_df.empty:
        return {
            "nmi_channel": nmi_channel,
            "point_id": sensor_id,
            "issue_type": "no_data",
            "missing_dates": "",
            "missing_count": 0,
            "data_start": "",
            "data_end": "",
            "total_expected_days": 0,
        }

    # Get actual dates
    actual_dates = set(sensor_df["data_date"].tolist())

    # Determine date range
    if start_date and end_date:
        range_start = datetime.strptime(start_date, "%Y-%m-%d").date()
        range_end = datetime.strptime(end_date, "%Y-%m-%d").date()
    else:
        range_start = min(actual_dates)
        range_end = max(actual_dates)

    # Generate expected dates
    expected_dates: set[date] = set()
    current = range_start
    while current <= range_end:
        expected_dates.add(current)
        current += timedelta(days=1)

    # Find missing dates
    missing_dates = sorted(expected_dates - actual_dates)
    total_expected = len(expected_dates)

    # No gaps - data is complete
    if not missing_dates:
        return None

    return {
        "nmi_channel": nmi_channel,
        "point_id": sensor_id,
        "issue_type": "missing_dates",
        "missing_dates": ",".join(d.strftime("%Y-%m-%d") for d in missing_dates),
        "missing_count": len(missing_dates),
        "data_start": range_start.strftime("%Y-%m-%d"),
        "data_end": range_end.strftime("%Y-%m-%d"),
        "total_expected_days": total_expected,
    }
```

**Step 4: Run tests to verify they pass**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv run pytest tests/unit/data_gap_detector/test_detector.py -v
```

Expected: All tests PASS

**Step 5: Run linter**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv run ruff check src/functions/data_gap_detector/detector.py tests/unit/data_gap_detector/test_detector.py && uv run ruff format src/functions/data_gap_detector/detector.py tests/unit/data_gap_detector/test_detector.py
```

Expected: No errors

**Step 6: Commit**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && git add src/functions/data_gap_detector/detector.py tests/unit/data_gap_detector/test_detector.py && git commit -m "feat(data-gap): implement detector module with gap analysis"
```

---

## Task 5: Implement Report Module (TDD)

**Files:**
- Create: `src/functions/data_gap_detector/report.py`
- Create: `tests/unit/data_gap_detector/test_report.py`

**Step 1: Write failing tests for report**

Create `tests/unit/data_gap_detector/test_report.py`:
```python
"""Tests for report module."""

import csv
from pathlib import Path

import pytest


class TestGenerateReport:
    """Tests for generate_report function."""

    def test_generate_csv_report(self, tmp_path: Path) -> None:
        """generate_report creates CSV with correct headers and data."""
        from src.functions.data_gap_detector.report import generate_report

        gaps = [
            {
                "nmi_channel": "NMI1-E1",
                "point_id": "p:bunnings:abc123",
                "issue_type": "missing_dates",
                "missing_dates": "2024-01-02,2024-01-04",
                "missing_count": 2,
                "data_start": "2024-01-01",
                "data_end": "2024-01-05",
                "total_expected_days": 5,
            },
            {
                "nmi_channel": "NMI2-E1",
                "point_id": "p:bunnings:def456",
                "issue_type": "no_data",
                "missing_dates": "",
                "missing_count": 0,
                "data_start": "",
                "data_end": "",
                "total_expected_days": 0,
            },
        ]

        output_path = generate_report(gaps, "bunnings", str(tmp_path))

        assert Path(output_path).exists()
        assert "bunnings" in output_path
        assert output_path.endswith(".csv")

        # Verify content
        with open(output_path) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 2
        assert rows[0]["nmi_channel"] == "NMI1-E1"
        assert rows[0]["issue_type"] == "missing_dates"
        assert rows[1]["issue_type"] == "no_data"

    def test_generate_empty_report(self, tmp_path: Path) -> None:
        """generate_report handles empty gaps list."""
        from src.functions.data_gap_detector.report import generate_report

        output_path = generate_report([], "bunnings", str(tmp_path))

        assert Path(output_path).exists()

        with open(output_path) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 0

    def test_report_filename_format(self, tmp_path: Path) -> None:
        """generate_report creates filename with project and timestamp."""
        from src.functions.data_gap_detector.report import generate_report

        output_path = generate_report([], "racv", str(tmp_path))

        filename = Path(output_path).name
        assert filename.startswith("data_gap_report_racv_")
        assert filename.endswith(".csv")


class TestGetReportHeaders:
    """Tests for get_report_headers function."""

    def test_headers_in_order(self) -> None:
        """get_report_headers returns headers in correct order."""
        from src.functions.data_gap_detector.report import get_report_headers

        headers = get_report_headers()

        assert headers[0] == "nmi_channel"
        assert headers[1] == "point_id"
        assert headers[2] == "issue_type"
        assert "missing_dates" in headers
        assert "missing_count" in headers
        assert "data_start" in headers
        assert "data_end" in headers
        assert "total_expected_days" in headers
```

**Step 2: Run tests to verify they fail**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv run pytest tests/unit/data_gap_detector/test_report.py -v
```

Expected: FAIL with "No module named 'src.functions.data_gap_detector.report'"

**Step 3: Implement report module**

Create `src/functions/data_gap_detector/report.py`:
```python
"""CSV report generation for data gap detection."""

import csv
from datetime import datetime
from pathlib import Path
from typing import Any


def get_report_headers() -> list[str]:
    """Return CSV header fields in order."""
    return [
        "nmi_channel",
        "point_id",
        "issue_type",
        "missing_dates",
        "missing_count",
        "data_start",
        "data_end",
        "total_expected_days",
    ]


def generate_report(gaps: list[dict[str, Any]], project: str, output_dir: str) -> str:
    """
    Generate CSV report from gap analysis results.

    Args:
        gaps: List of gap dictionaries
        project: Project name (for filename)
        output_dir: Directory to write report

    Returns:
        Path to generated CSV file
    """
    # Ensure output directory exists
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"data_gap_report_{project}_{timestamp}.csv"
    filepath = output_path / filename

    # Write CSV
    headers = get_report_headers()
    with filepath.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for gap in gaps:
            writer.writerow({h: gap.get(h, "") for h in headers})

    return str(filepath)
```

**Step 4: Run tests to verify they pass**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv run pytest tests/unit/data_gap_detector/test_report.py -v
```

Expected: All tests PASS

**Step 5: Run linter**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv run ruff check src/functions/data_gap_detector/report.py tests/unit/data_gap_detector/test_report.py && uv run ruff format src/functions/data_gap_detector/report.py tests/unit/data_gap_detector/test_report.py
```

Expected: No errors

**Step 6: Commit**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && git add src/functions/data_gap_detector/report.py tests/unit/data_gap_detector/test_report.py && git commit -m "feat(data-gap): implement report module for CSV generation"
```

---

## Task 6: Implement Query Executor (TDD)

**Files:**
- Create: `src/functions/data_gap_detector/query.py`
- Create: `tests/unit/data_gap_detector/test_query.py`

**Step 1: Write failing tests for query**

Create `tests/unit/data_gap_detector/test_query.py`:
```python
"""Tests for query module."""

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


class TestQueryBatch:
    """Tests for query_batch function."""

    @patch("src.functions.data_gap_detector.query.wr")
    def test_query_batch_calls_athena(self, mock_wr: MagicMock) -> None:
        """query_batch calls awswrangler with correct parameters."""
        from src.functions.data_gap_detector.query import query_batch

        mock_df = pd.DataFrame({
            "sensorId": ["p:bunnings:abc"],
            "data_date": [date(2024, 1, 1)],
            "record_count": [48],
        })
        mock_wr.athena.read_sql_query.return_value = mock_df

        result = query_batch(
            sensor_ids=["p:bunnings:abc", "p:bunnings:def"],
            start_date="2024-01-01",
            end_date="2024-01-31",
        )

        mock_wr.athena.read_sql_query.assert_called_once()
        call_args = mock_wr.athena.read_sql_query.call_args
        query = call_args[0][0]

        assert "p:bunnings:abc" in query
        assert "p:bunnings:def" in query
        assert "2024-01-01" in query
        assert "2024-01-31" in query
        assert isinstance(result, pd.DataFrame)

    @patch("src.functions.data_gap_detector.query.wr")
    def test_query_batch_handles_empty_result(self, mock_wr: MagicMock) -> None:
        """query_batch returns empty DataFrame when no data."""
        from src.functions.data_gap_detector.query import query_batch

        mock_wr.athena.read_sql_query.return_value = pd.DataFrame()

        result = query_batch(
            sensor_ids=["p:bunnings:abc"],
            start_date="2024-01-01",
            end_date="2024-01-31",
        )

        assert result.empty


class TestQueryAllSensors:
    """Tests for query_all_sensors function."""

    @patch("src.functions.data_gap_detector.query.query_batch")
    def test_query_all_sensors_batches_correctly(self, mock_query_batch: MagicMock) -> None:
        """query_all_sensors splits sensors into batches."""
        from src.functions.data_gap_detector.query import BATCH_SIZE, query_all_sensors

        # Create more sensors than BATCH_SIZE
        sensor_ids = [f"p:bunnings:sensor{i}" for i in range(BATCH_SIZE + 10)]

        mock_query_batch.return_value = pd.DataFrame({
            "sensorId": ["p:bunnings:sensor0"],
            "data_date": [date(2024, 1, 1)],
            "record_count": [48],
        })

        query_all_sensors(
            sensor_ids=sensor_ids,
            start_date="2024-01-01",
            end_date="2024-01-31",
        )

        # Should be called twice (one full batch + one partial)
        assert mock_query_batch.call_count == 2

    @patch("src.functions.data_gap_detector.query.query_batch")
    def test_query_all_sensors_concatenates_results(self, mock_query_batch: MagicMock) -> None:
        """query_all_sensors concatenates batch results."""
        from src.functions.data_gap_detector.query import query_all_sensors

        mock_query_batch.side_effect = [
            pd.DataFrame({
                "sensorId": ["p:bunnings:a"],
                "data_date": [date(2024, 1, 1)],
                "record_count": [48],
            }),
            pd.DataFrame({
                "sensorId": ["p:bunnings:b"],
                "data_date": [date(2024, 1, 2)],
                "record_count": [48],
            }),
        ]

        result = query_all_sensors(
            sensor_ids=["p:bunnings:a", "p:bunnings:b"],
            start_date="2024-01-01",
            end_date="2024-01-31",
        )

        assert len(result) == 2
        assert "p:bunnings:a" in result["sensorId"].values
        assert "p:bunnings:b" in result["sensorId"].values
```

**Step 2: Run tests to verify they fail**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv run pytest tests/unit/data_gap_detector/test_query.py -v
```

Expected: FAIL with "No module named 'src.functions.data_gap_detector.query'"

**Step 3: Implement query module**

Create `src/functions/data_gap_detector/query.py`:
```python
"""Athena query execution for data gap detection."""

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING

import awswrangler as wr
import pandas as pd
from aws_lambda_powertools import Logger

from src.functions.data_gap_detector.detector import ATHENA_OUTPUT, DATABASE, TABLE, build_query, chunk_list

if TYPE_CHECKING:
    pass

logger = Logger(service="data-gap-detector")

# Configuration
BATCH_SIZE = 50
MAX_WORKERS = 5


def query_batch(
    sensor_ids: list[str],
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    Query Athena for a batch of sensors.

    Args:
        sensor_ids: List of sensorId values to query
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        DataFrame with sensorId, data_date, record_count columns
    """
    query = build_query(sensor_ids, start_date, end_date)

    return wr.athena.read_sql_query(
        query,
        database=DATABASE,
        s3_output=ATHENA_OUTPUT,
    )


def query_all_sensors(
    sensor_ids: list[str],
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    Query all sensors in batches with concurrent execution.

    Args:
        sensor_ids: List of all sensorId values to query
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        Combined DataFrame with all results
    """
    if not sensor_ids:
        return pd.DataFrame(columns=["sensorId", "data_date", "record_count"])

    batches = chunk_list(sensor_ids, BATCH_SIZE)
    results: list[pd.DataFrame] = []

    logger.info(
        "Querying sensors",
        extra={
            "total_sensors": len(sensor_ids),
            "batch_count": len(batches),
            "batch_size": BATCH_SIZE,
            "max_workers": MAX_WORKERS,
        },
    )

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(query_batch, batch, start_date, end_date): i
            for i, batch in enumerate(batches)
        }

        for future in as_completed(futures):
            batch_num = futures[future]
            try:
                df = future.result()
                results.append(df)
                logger.info(f"Batch {batch_num + 1}/{len(batches)} complete: {len(df)} rows")
            except Exception as e:
                logger.error(f"Batch {batch_num + 1} failed: {e}")
                # Continue with other batches

    if not results:
        return pd.DataFrame(columns=["sensorId", "data_date", "record_count"])

    return pd.concat(results, ignore_index=True)
```

**Step 4: Run tests to verify they pass**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv run pytest tests/unit/data_gap_detector/test_query.py -v
```

Expected: All tests PASS

**Step 5: Run linter**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv run ruff check src/functions/data_gap_detector/query.py tests/unit/data_gap_detector/test_query.py && uv run ruff format src/functions/data_gap_detector/query.py tests/unit/data_gap_detector/test_query.py
```

Expected: No errors

**Step 6: Commit**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && git add src/functions/data_gap_detector/query.py tests/unit/data_gap_detector/test_query.py && git commit -m "feat(data-gap): implement query module with batch concurrent execution"
```

---

## Task 7: Implement App Module (Lambda Handler + CLI)

**Files:**
- Create: `src/functions/data_gap_detector/app.py`
- Create: `tests/unit/data_gap_detector/test_app.py`

**Step 1: Write failing tests for app**

Create `tests/unit/data_gap_detector/test_app.py`:
```python
"""Tests for app module (Lambda handler and CLI)."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


class TestLambdaHandler:
    """Tests for lambda_handler function."""

    @patch("src.functions.data_gap_detector.app.run_detection")
    def test_handler_with_valid_project(self, mock_run: MagicMock) -> None:
        """lambda_handler calls run_detection with correct params."""
        from src.functions.data_gap_detector.app import lambda_handler

        mock_run.return_value = {
            "statusCode": 200,
            "body": {"issues_found": 0, "report_path": "/tmp/report.csv"},
        }

        event = {"project": "bunnings"}
        result = lambda_handler(event, None)

        mock_run.assert_called_once()
        assert result["statusCode"] == 200

    def test_handler_missing_project(self) -> None:
        """lambda_handler returns 400 when project is missing."""
        from src.functions.data_gap_detector.app import lambda_handler

        result = lambda_handler({}, None)

        assert result["statusCode"] == 400
        assert "project" in result["body"].lower()

    def test_handler_invalid_project(self) -> None:
        """lambda_handler returns 400 for invalid project."""
        from src.functions.data_gap_detector.app import lambda_handler

        result = lambda_handler({"project": "invalid"}, None)

        assert result["statusCode"] == 400
        assert "bunnings" in result["body"].lower() or "racv" in result["body"].lower()


class TestRunDetection:
    """Tests for run_detection orchestration function."""

    @patch("src.functions.data_gap_detector.app.query_all_sensors")
    @patch("src.functions.data_gap_detector.app.load_mappings")
    def test_run_detection_full_flow(
        self,
        mock_load: MagicMock,
        mock_query: MagicMock,
        tmp_path: Path,
    ) -> None:
        """run_detection orchestrates full detection flow."""
        from datetime import date

        from src.functions.data_gap_detector.app import run_detection

        mock_load.return_value = {
            "NMI1-E1": "p:bunnings:abc123",
            "NMI2-E1": "p:racv:def456",
        }

        mock_query.return_value = pd.DataFrame({
            "sensorId": ["p:bunnings:abc123"],
            "data_date": [date(2024, 1, 1)],
            "record_count": [48],
        })

        result = run_detection(
            project="bunnings",
            start_date="2024-01-01",
            end_date="2024-01-03",
            output_dir=str(tmp_path),
            mappings_path=str(tmp_path / "mappings.json"),
        )

        assert result["statusCode"] == 200
        assert "issues_found" in result["body"]

    @patch("src.functions.data_gap_detector.app.load_mappings")
    def test_run_detection_no_sensors(self, mock_load: MagicMock, tmp_path: Path) -> None:
        """run_detection handles no sensors for project."""
        from src.functions.data_gap_detector.app import run_detection

        mock_load.return_value = {
            "NMI1-E1": "p:racv:abc123",  # No bunnings sensors
        }

        result = run_detection(
            project="bunnings",
            output_dir=str(tmp_path),
            mappings_path=str(tmp_path / "mappings.json"),
        )

        assert result["statusCode"] == 200
        assert result["body"]["issues_found"] == 0


class TestParseArgs:
    """Tests for parse_args CLI argument parser."""

    def test_parse_required_args(self) -> None:
        """parse_args parses required project argument."""
        from src.functions.data_gap_detector.app import parse_args

        args = parse_args(["--project", "bunnings"])

        assert args.project == "bunnings"

    def test_parse_optional_dates(self) -> None:
        """parse_args parses optional date arguments."""
        from src.functions.data_gap_detector.app import parse_args

        args = parse_args([
            "--project", "racv",
            "--start-date", "2024-01-01",
            "--end-date", "2024-01-31",
        ])

        assert args.project == "racv"
        assert args.start_date == "2024-01-01"
        assert args.end_date == "2024-01-31"

    def test_parse_default_values(self) -> None:
        """parse_args uses defaults for optional args."""
        from src.functions.data_gap_detector.app import parse_args

        args = parse_args(["--project", "bunnings"])

        assert args.start_date is None
        assert args.end_date is None
```

**Step 2: Run tests to verify they fail**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv run pytest tests/unit/data_gap_detector/test_app.py -v
```

Expected: FAIL with "No module named 'src.functions.data_gap_detector.app'"

**Step 3: Implement app module**

Create `src/functions/data_gap_detector/app.py`:
```python
"""
Data Gap Detector Lambda

Detects missing sensor data in the Hudi data lake for bunnings/racv projects.

Event parameters:
    project: Project name ("bunnings" or "racv") - required
    startDate: Start date (YYYY-MM-DD) - optional
    endDate: End date (YYYY-MM-DD) - optional

Local usage:
    uv run python -m src.functions.data_gap_detector.app --project bunnings
"""

import argparse
from pathlib import Path
from typing import Any

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

from src.functions.data_gap_detector.detector import analyze_sensor_gaps
from src.functions.data_gap_detector.mappings import filter_by_project, load_mappings
from src.functions.data_gap_detector.query import query_all_sensors
from src.functions.data_gap_detector.report import generate_report

logger = Logger(service="data-gap-detector")

# Valid projects
VALID_PROJECTS = {"bunnings", "racv"}

# Default paths
DEFAULT_MAPPINGS_PATH = str(Path(__file__).parent.parent.parent.parent / "docs" / "nem12_mappings.json")
DEFAULT_OUTPUT_DIR = str(Path(__file__).parent.parent.parent.parent / "output")


def run_detection(
    project: str,
    start_date: str | None = None,
    end_date: str | None = None,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    mappings_path: str = DEFAULT_MAPPINGS_PATH,
) -> dict[str, Any]:
    """
    Run data gap detection for a project.

    Args:
        project: Project name (bunnings or racv)
        start_date: Optional start date (YYYY-MM-DD)
        end_date: Optional end date (YYYY-MM-DD)
        output_dir: Directory for output CSV
        mappings_path: Path to nem12_mappings.json

    Returns:
        Response dict with results
    """
    logger.info(
        "Starting detection",
        extra={
            "project": project,
            "start_date": start_date,
            "end_date": end_date,
        },
    )

    # Load and filter mappings
    all_mappings = load_mappings(mappings_path)
    project_mappings = filter_by_project(all_mappings, project)

    if not project_mappings:
        logger.warning("No sensors found for project", extra={"project": project})
        report_path = generate_report([], project, output_dir)
        return {
            "statusCode": 200,
            "body": {
                "message": f"No sensors found for project {project}",
                "issues_found": 0,
                "report_path": report_path,
            },
        }

    logger.info(f"Found {len(project_mappings)} sensors for {project}")

    # Get sensor IDs
    sensor_ids = list(project_mappings.values())

    # Query data lake
    # If no dates specified, query without date filter first to find data range
    if not start_date or not end_date:
        # Query with wide date range to find actual data bounds
        df = query_all_sensors(sensor_ids, "2020-01-01", "2030-12-31")
    else:
        df = query_all_sensors(sensor_ids, start_date, end_date)

    # Analyze each sensor
    gaps: list[dict[str, Any]] = []
    for nmi_channel, point_id in project_mappings.items():
        result = analyze_sensor_gaps(
            sensor_id=point_id,
            nmi_channel=nmi_channel,
            df=df,
            start_date=start_date,
            end_date=end_date,
        )
        if result:
            gaps.append(result)

    # Generate report
    report_path = generate_report(gaps, project, output_dir)

    logger.info(
        "Detection complete",
        extra={
            "project": project,
            "total_sensors": len(project_mappings),
            "issues_found": len(gaps),
            "report_path": report_path,
        },
    )

    return {
        "statusCode": 200,
        "body": {
            "message": f"Detection complete for {project}",
            "total_sensors": len(project_mappings),
            "issues_found": len(gaps),
            "report_path": report_path,
        },
    }


@logger.inject_lambda_context
def lambda_handler(event: dict[str, Any], context: LambdaContext | None) -> dict[str, Any]:
    """
    Lambda handler for data gap detection.

    Args:
        event: Lambda event with project (required), startDate, endDate (optional)
        context: Lambda context

    Returns:
        Response with detection results
    """
    project = event.get("project", "").lower()

    if not project:
        return {
            "statusCode": 400,
            "body": "Missing required parameter: project",
        }

    if project not in VALID_PROJECTS:
        return {
            "statusCode": 400,
            "body": f"Invalid project: {project}. Must be one of: {', '.join(VALID_PROJECTS)}",
        }

    return run_detection(
        project=project,
        start_date=event.get("startDate"),
        end_date=event.get("endDate"),
    )


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Detect data gaps in Hudi data lake")
    parser.add_argument(
        "--project",
        required=True,
        choices=list(VALID_PROJECTS),
        help="Project name (bunnings or racv)",
    )
    parser.add_argument(
        "--start-date",
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Output directory for CSV report",
    )
    parser.add_argument(
        "--mappings-path",
        default=DEFAULT_MAPPINGS_PATH,
        help="Path to nem12_mappings.json",
    )

    return parser.parse_args(args)


if __name__ == "__main__":
    args = parse_args()

    result = run_detection(
        project=args.project,
        start_date=args.start_date,
        end_date=args.end_date,
        output_dir=args.output_dir,
        mappings_path=args.mappings_path,
    )

    print(f"\nResult: {result}")
```

**Step 4: Run tests to verify they pass**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv run pytest tests/unit/data_gap_detector/test_app.py -v
```

Expected: All tests PASS

**Step 5: Run linter**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv run ruff check src/functions/data_gap_detector/app.py tests/unit/data_gap_detector/test_app.py && uv run ruff format src/functions/data_gap_detector/app.py tests/unit/data_gap_detector/test_app.py
```

Expected: No errors

**Step 6: Commit**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && git add src/functions/data_gap_detector/app.py tests/unit/data_gap_detector/test_app.py && git commit -m "feat(data-gap): implement app module with Lambda handler and CLI"
```

---

## Task 8: Run Full Test Suite

**Step 1: Run all data_gap_detector tests**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv run pytest tests/unit/data_gap_detector/ -v
```

Expected: All tests PASS

**Step 2: Run full test suite**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv run pytest -v
```

Expected: All tests PASS

**Step 3: Run linter on entire module**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv run ruff check src/functions/data_gap_detector tests/unit/data_gap_detector && uv run ruff format --check src/functions/data_gap_detector tests/unit/data_gap_detector
```

Expected: No errors

---

## Task 9: Manual Integration Test (Local)

**Step 1: Test CLI with real data**

Run:
```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && uv run python -m src.functions.data_gap_detector.app --project bunnings --start-date 2024-01-01 --end-date 2024-01-31
```

Expected: CSV report generated in output/ directory

**Step 2: Verify CSV output**

Run:
```bash
ls -la /Users/zeyu/Desktop/GEG/sbm/sbm-ingester/output/
head -20 /Users/zeyu/Desktop/GEG/sbm/sbm-ingester/output/data_gap_report_bunnings_*.csv
```

Expected: CSV file with headers and data rows

**Step 3: Final commit**

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester && git add -A && git commit -m "feat(data-gap): complete data gap detector implementation"
```

---

## Summary

| Task | Description | Files |
|------|-------------|-------|
| 1 | Add dependencies | pyproject.toml |
| 2 | Create module structure | __init__.py files, output/ |
| 3 | Implement mappings module | mappings.py, test_mappings.py |
| 4 | Implement detector module | detector.py, test_detector.py |
| 5 | Implement report module | report.py, test_report.py |
| 6 | Implement query module | query.py, test_query.py |
| 7 | Implement app module | app.py, test_app.py |
| 8 | Run full test suite | - |
| 9 | Manual integration test | - |
