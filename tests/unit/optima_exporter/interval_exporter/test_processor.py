"""Unit tests for interval_exporter/processor.py module."""

import importlib
import os
from datetime import date
from typing import Any
from unittest.mock import patch

from freezegun import freeze_time

SENTINEL_CSV_BYTES = (
    b"BuyerShortName,Country,Commodity,Identifier,IdentifierType,DistributorId,"
    b"Date,Start Time,Usage,Generation,DemandKva,Reactive\r\n"
    b"No data is available\r\n"
)


def reload_interval_processor_module() -> Any:
    """Reload interval processor and its config dependency with fresh environment."""
    import optima_shared.config as config_module

    importlib.reload(config_module)

    import interval_exporter.processor as processor_module

    importlib.reload(processor_module)
    return processor_module


class TestGetDateRange:
    @freeze_time("2026-01-23 10:00:00")
    def test_days_back_one_returns_same_start_and_end(self) -> None:
        processor_module = reload_interval_processor_module()
        start_date, end_date = processor_module.get_date_range()

        assert start_date == "2026-01-22"
        assert end_date == "2026-01-22"

    @freeze_time("2026-01-23 10:00:00")
    def test_days_back_seven_returns_inclusive_seven_day_window(self) -> None:
        os.environ["OPTIMA_DAYS_BACK"] = "7"
        processor_module = reload_interval_processor_module()
        start_date, end_date = processor_module.get_date_range()

        assert end_date == "2026-01-22"
        assert (date.fromisoformat(end_date) - date.fromisoformat(start_date)).days == 6


class TestProcessSite:
    def test_happy_path_uploads_extracted_csv_and_returns_success(self) -> None:
        processor_module = reload_interval_processor_module()
        csv_bytes = b"header\r\nvalue\r\n"

        with (
            patch("interval_exporter.processor.download_interval_zip", return_value=(b"<zipbytes>", "out.csv")),
            patch("interval_exporter.processor.extract_first_csv", return_value=csv_bytes),
            patch("interval_exporter.processor.upload_to_s3", return_value=True) as upload_mock,
        ):
            result = processor_module.process_site(
                cookies=".ASPXAUTH=tok",
                nmi="Optima_X",
                site_id_str="site-1",
                start_date="2026-04-29",
                end_date="2026-04-29",
                project="bunnings",
            )

        assert result == {
            "nmi": "Optima_X",
            "site_id": "site-1",
            "success": True,
            "error": None,
            "filename": "out.csv",
            "s3_key": "newTBP/out.csv",
            "empty_data": False,
        }
        upload_mock.assert_called_once_with(csv_bytes, "out.csv")

    def test_sentinel_path_sets_empty_data_true(self) -> None:
        processor_module = reload_interval_processor_module()

        with (
            patch("interval_exporter.processor.download_interval_zip", return_value=(b"<zipbytes>", "out.csv")),
            patch("interval_exporter.processor.extract_first_csv", return_value=SENTINEL_CSV_BYTES),
            patch("interval_exporter.processor.upload_to_s3", return_value=True),
        ):
            result = processor_module.process_site(
                cookies=".ASPXAUTH=tok",
                nmi="Optima_X",
                site_id_str="site-1",
                start_date="2026-04-29",
                end_date="2026-04-29",
                project="bunnings",
            )

        assert result["success"] is True
        assert result["empty_data"] is True

    def test_download_failure_returns_error_result(self) -> None:
        processor_module = reload_interval_processor_module()

        with patch("interval_exporter.processor.download_interval_zip", return_value=None):
            result = processor_module.process_site(
                cookies=".ASPXAUTH=tok",
                nmi="Optima_X",
                site_id_str="site-1",
                start_date="2026-04-29",
                end_date="2026-04-29",
                project="bunnings",
            )

        assert result["success"] is False
        assert result["error"] == "Failed to download ZIP"

    def test_extract_failure_returns_error_result(self) -> None:
        processor_module = reload_interval_processor_module()

        with (
            patch("interval_exporter.processor.download_interval_zip", return_value=(b"<zipbytes>", "out.csv")),
            patch("interval_exporter.processor.extract_first_csv", side_effect=ValueError("bad zip")),
            patch("interval_exporter.processor.upload_to_s3") as upload_mock,
        ):
            result = processor_module.process_site(
                cookies=".ASPXAUTH=tok",
                nmi="Optima_X",
                site_id_str="site-1",
                start_date="2026-04-29",
                end_date="2026-04-29",
                project="bunnings",
            )

        assert result["success"] is False
        assert result["error"] == "Failed to extract CSV from ZIP"
        upload_mock.assert_not_called()

    def test_s3_upload_failure_returns_error_result(self) -> None:
        processor_module = reload_interval_processor_module()

        with (
            patch("interval_exporter.processor.download_interval_zip", return_value=(b"<zipbytes>", "out.csv")),
            patch("interval_exporter.processor.extract_first_csv", return_value=b"header\r\nvalue\r\n"),
            patch("interval_exporter.processor.upload_to_s3", return_value=False),
        ):
            result = processor_module.process_site(
                cookies=".ASPXAUTH=tok",
                nmi="Optima_X",
                site_id_str="site-1",
                start_date="2026-04-29",
                end_date="2026-04-29",
                project="bunnings",
            )

        assert result["success"] is False
        assert result["error"] == "Failed to upload to S3"


class TestProcessExport:
    @freeze_time("2026-04-30 10:00:00")
    def test_happy_path_processes_all_sites(self) -> None:
        processor_module = reload_interval_processor_module()
        sites = [
            {"nmi": "Optima_1", "siteIdStr": "site-1"},
            {"nmi": "Optima_2", "siteIdStr": "site-2"},
        ]

        with (
            patch("interval_exporter.processor.get_project_config", return_value=_config()),
            patch("interval_exporter.processor.get_sites_for_project", return_value=sites),
            patch("interval_exporter.processor.login_bidenergy", return_value=".ASPXAUTH=cookies"),
            patch(
                "interval_exporter.processor.process_site",
                side_effect=[
                    _site_result("Optima_1", "site-1", success=True),
                    _site_result("Optima_2", "site-2", success=True),
                ],
            ),
        ):
            result = processor_module.process_export(project="BUNNINGS")

        assert result["statusCode"] == 200
        assert result["body"]["project"] == "bunnings"
        assert result["body"]["date_range"] == {"start": "2026-04-29", "end": "2026-04-29"}
        assert result["body"]["success_count"] == 2
        assert result["body"]["error_count"] == 0
        assert result["body"]["empty_data_count"] == 0

    def test_partial_failure_returns_207(self) -> None:
        processor_module = reload_interval_processor_module()
        sites = [
            {"nmi": "Optima_OK", "siteIdStr": "site-ok"},
            {"nmi": "Optima_BAD", "siteIdStr": "site-bad"},
        ]

        with (
            patch("interval_exporter.processor.get_project_config", return_value=_config()),
            patch("interval_exporter.processor.get_sites_for_project", return_value=sites),
            patch("interval_exporter.processor.login_bidenergy", return_value=".ASPXAUTH=cookies"),
            patch(
                "interval_exporter.processor.process_site",
                side_effect=[
                    _site_result("Optima_OK", "site-ok", success=True),
                    _site_result("Optima_BAD", "site-bad", success=False, error="Failed to download ZIP"),
                ],
            ),
        ):
            result = processor_module.process_export(
                project="bunnings",
                start_date="2026-04-29",
                end_date="2026-04-29",
            )

        assert result["statusCode"] == 207
        assert result["body"]["success_count"] == 1
        assert result["body"]["error_count"] == 1

    def test_missing_project_credentials_returns_400(self) -> None:
        processor_module = reload_interval_processor_module()

        with patch("interval_exporter.processor.get_project_config", return_value=None):
            result = processor_module.process_export(project="bunnings")

        assert result["statusCode"] == 400
        assert "credentials" in result["body"].lower()

    def test_no_sites_returns_404(self) -> None:
        processor_module = reload_interval_processor_module()

        with (
            patch("interval_exporter.processor.get_project_config", return_value=_config()),
            patch("interval_exporter.processor.get_sites_for_project", return_value=[]),
        ):
            result = processor_module.process_export(project="bunnings")

        assert result["statusCode"] == 404
        assert "no sites" in result["body"].lower()

    def test_login_failure_returns_401(self) -> None:
        processor_module = reload_interval_processor_module()

        with (
            patch("interval_exporter.processor.get_project_config", return_value=_config()),
            patch(
                "interval_exporter.processor.get_sites_for_project",
                return_value=[{"nmi": "Optima_1", "siteIdStr": "site-1"}],
            ),
            patch("interval_exporter.processor.login_bidenergy", return_value=None),
        ):
            result = processor_module.process_export(project="bunnings")

        assert result["statusCode"] == 401
        assert "authenticate" in result["body"].lower()

    def test_inverted_dates_returns_400(self) -> None:
        processor_module = reload_interval_processor_module()

        result = processor_module.process_export(
            project="bunnings",
            start_date="2026-04-30",
            end_date="2026-04-29",
        )

        assert result["statusCode"] == 400
        assert "invalid range" in result["body"].lower()

    def test_single_nmi_mode_uses_get_site_by_nmi(self) -> None:
        processor_module = reload_interval_processor_module()
        site = {"nmi": "Optima_X", "siteIdStr": "site-x"}

        with (
            patch("interval_exporter.processor.get_project_config", return_value=_config()),
            patch("interval_exporter.processor.get_site_by_nmi", return_value=site) as get_site_by_nmi_mock,
            patch("interval_exporter.processor.get_sites_for_project") as get_sites_for_project_mock,
            patch("interval_exporter.processor.login_bidenergy", return_value=".ASPXAUTH=cookies"),
            patch(
                "interval_exporter.processor.process_site",
                return_value=_site_result("Optima_X", "site-x", success=True),
            ),
        ):
            result = processor_module.process_export(
                project="BUNNINGS",
                nmi="Optima_X",
                start_date="2026-04-29",
                end_date="2026-04-29",
            )

        assert result["statusCode"] == 200
        assert result["body"]["success_count"] == 1
        get_site_by_nmi_mock.assert_called_once_with("bunnings", "Optima_X")
        get_sites_for_project_mock.assert_not_called()

    def test_single_nmi_not_found_returns_404(self) -> None:
        processor_module = reload_interval_processor_module()

        with (
            patch("interval_exporter.processor.get_project_config", return_value=_config()),
            patch("interval_exporter.processor.get_site_by_nmi", return_value=None),
        ):
            result = processor_module.process_export(project="bunnings", nmi="Optima_DOES_NOT_EXIST")

        assert result["statusCode"] == 404
        assert "not found" in result["body"].lower()

    def test_thread_exception_returns_207_failure_result(self) -> None:
        processor_module = reload_interval_processor_module()
        sites = [{"nmi": "Optima_X", "siteIdStr": "site-x"}]

        with (
            patch("interval_exporter.processor.get_project_config", return_value=_config()),
            patch("interval_exporter.processor.get_sites_for_project", return_value=sites),
            patch("interval_exporter.processor.login_bidenergy", return_value=".ASPXAUTH=cookies"),
            patch("interval_exporter.processor.process_site", side_effect=RuntimeError("boom")),
        ):
            result = processor_module.process_export(
                project="bunnings",
                start_date="2026-04-29",
                end_date="2026-04-29",
            )

        assert result["statusCode"] == 207
        assert result["body"]["error_count"] == 1
        assert result["body"]["results"][0]["error"] == "Thread execution failed: boom"


def _config() -> dict[str, str]:
    return {
        "username": "user@example.com",
        "password": "secret",
        "client_id": "client",
    }


def _site_result(
    nmi: str,
    site_id: str,
    *,
    success: bool,
    error: str | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "nmi": nmi,
        "site_id": site_id,
        "success": success,
        "error": error,
    }
    if success:
        result.update(
            {
                "filename": f"{nmi}.csv",
                "s3_key": f"newTBP/{nmi}.csv",
                "empty_data": False,
            }
        )
    return result
