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
