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
        df, failed_sensors = query_all_sensors(sensor_ids, "2020-01-01", "2030-12-31")
    else:
        df, failed_sensors = query_all_sensors(sensor_ids, start_date, end_date)

    # Log failed sensors
    if failed_sensors:
        logger.warning(
            f"Failed to query {len(failed_sensors)} sensors",
            extra={"failed_count": len(failed_sensors)},
        )

    # Create reverse lookup for nmi_channel from point_id
    point_to_nmi = {v: k for k, v in project_mappings.items()}

    # Analyze each sensor
    gaps: list[dict[str, Any]] = []

    # Add failed sensors to report
    for point_id in failed_sensors:
        nmi_channel = point_to_nmi.get(point_id, "unknown")
        gaps.append(
            {
                "nmi_channel": nmi_channel,
                "point_id": point_id,
                "issue_type": "query_failed",
                "missing_dates": "",
                "missing_count": 0,
                "data_start": "",
                "data_end": "",
                "total_expected_days": 0,
            }
        )

    # Analyze successfully queried sensors
    successful_sensor_ids = set(sensor_ids) - set(failed_sensors)
    for nmi_channel, point_id in project_mappings.items():
        if point_id not in successful_sensor_ids:
            continue
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
