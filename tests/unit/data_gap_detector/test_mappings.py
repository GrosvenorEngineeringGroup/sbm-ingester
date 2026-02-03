"""Tests for mappings module."""

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
