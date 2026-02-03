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
