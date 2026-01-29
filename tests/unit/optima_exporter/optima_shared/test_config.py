"""Unit tests for optima_shared/config.py module.

Tests environment variable configuration, project credentials, and country settings.
"""

import os

from tests.unit.optima_exporter.conftest import reload_config_module


class TestGetProjectConfig:
    """Tests for get_project_config function."""

    def test_returns_config_when_all_env_vars_present(self) -> None:
        """Test that config is returned when all credentials are present."""
        config_module = reload_config_module()

        config = config_module.get_project_config("bunnings")

        assert config is not None
        assert config["username"] == "bunnings@test.com"
        assert config["password"] == "bunnings_pass"
        assert config["client_id"] == "bunnings_client"

    def test_returns_none_when_username_missing(self) -> None:
        """Test that None is returned when username is missing."""
        os.environ.pop("OPTIMA_BUNNINGS_USERNAME", None)
        config_module = reload_config_module()

        config = config_module.get_project_config("bunnings")
        assert config is None

    def test_returns_none_when_password_missing(self) -> None:
        """Test that None is returned when password is missing."""
        os.environ.pop("OPTIMA_BUNNINGS_PASSWORD", None)
        config_module = reload_config_module()

        config = config_module.get_project_config("bunnings")
        assert config is None

    def test_returns_none_when_client_id_missing(self) -> None:
        """Test that None is returned when client_id is missing."""
        os.environ.pop("OPTIMA_BUNNINGS_CLIENT_ID", None)
        config_module = reload_config_module()

        config = config_module.get_project_config("bunnings")
        assert config is None


class TestGetProjectCountries:
    """Tests for get_project_countries function."""

    def test_returns_countries_for_bunnings(self) -> None:
        """Test that countries are returned for bunnings."""
        config_module = reload_config_module()

        countries = config_module.get_project_countries("bunnings")

        assert countries == ["AU", "NZ"]

    def test_returns_countries_for_racv(self) -> None:
        """Test that countries are returned for racv."""
        config_module = reload_config_module()

        countries = config_module.get_project_countries("racv")

        assert countries == ["AU"]

    def test_returns_default_when_not_configured(self) -> None:
        """Test that default AU is returned when not configured."""
        os.environ.pop("OPTIMA_BUNNINGS_COUNTRIES", None)
        config_module = reload_config_module()

        countries = config_module.get_project_countries("bunnings")

        assert countries == ["AU"]


class TestConfiguration:
    """Tests for configuration and environment variable handling."""

    def test_default_s3_upload_bucket(self) -> None:
        """Test default S3_UPLOAD_BUCKET value."""
        os.environ.pop("S3_UPLOAD_BUCKET", None)
        config_module = reload_config_module()

        assert config_module.S3_UPLOAD_BUCKET == "sbm-file-ingester"

    def test_default_s3_upload_prefix(self) -> None:
        """Test default S3_UPLOAD_PREFIX value."""
        os.environ.pop("S3_UPLOAD_PREFIX", None)
        config_module = reload_config_module()

        assert config_module.S3_UPLOAD_PREFIX == "newTBP/"

    def test_default_days_back(self) -> None:
        """Test default OPTIMA_DAYS_BACK value."""
        os.environ.pop("OPTIMA_DAYS_BACK", None)
        config_module = reload_config_module()

        assert config_module.OPTIMA_DAYS_BACK == 7

    def test_default_config_table(self) -> None:
        """Test default OPTIMA_CONFIG_TABLE value."""
        os.environ.pop("OPTIMA_CONFIG_TABLE", None)
        config_module = reload_config_module()

        assert config_module.OPTIMA_CONFIG_TABLE == "sbm-optima-config"

    def test_default_max_workers(self) -> None:
        """Test default OPTIMA_MAX_WORKERS value."""
        os.environ.pop("OPTIMA_MAX_WORKERS", None)
        config_module = reload_config_module()

        assert config_module.MAX_WORKERS == 10

    def test_custom_max_workers(self) -> None:
        """Test custom OPTIMA_MAX_WORKERS value."""
        os.environ["OPTIMA_MAX_WORKERS"] = "5"
        config_module = reload_config_module()

        assert config_module.MAX_WORKERS == 5
