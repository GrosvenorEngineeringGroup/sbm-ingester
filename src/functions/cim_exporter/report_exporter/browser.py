"""Playwright browser automation for CIM report download."""

import asyncio
from datetime import datetime, timedelta
from pathlib import Path

from aws_lambda_powertools import Logger
from cim_shared.config import (
    CIM_BASE_URL,
    CIM_LOGIN_URL,
    CIM_PASSWORD,
    CIM_SITE_IDS,
    CIM_USERNAME,
)
from playwright.async_api import Page, async_playwright

logger = Logger(service="cim-report-exporter")


def _build_report_url(site_ids: list[int]) -> str:
    """
    Build the CIM report page URL with filters.

    Args:
        site_ids: List of site IDs to include

    Returns:
        Full report URL with query parameters
    """
    # Build site_ids parameters (each site_id is a separate query param)
    site_params = "&".join(f"site_ids={sid}" for sid in site_ids)

    # CIM report URL format (verified via browser exploration)
    return f"{CIM_BASE_URL}/reports/tickets?{site_params}&relative_date=last_90_days&include_today=true&grouping=site"


async def _login(page: Page) -> bool:
    """
    Perform Keycloak login.

    Args:
        page: Playwright page object

    Returns:
        True if login succeeded, False otherwise
    """
    logger.info("Navigating to login page")

    try:
        await page.goto(CIM_LOGIN_URL, wait_until="networkidle")

        # Wait for login form (using placeholder text as verified)
        await page.get_by_placeholder("E-mail or username").wait_for(timeout=10000)

        logger.info("Filling login credentials")
        await page.get_by_placeholder("E-mail or username").fill(CIM_USERNAME)
        await page.get_by_placeholder("Password").fill(CIM_PASSWORD)

        # Submit login form
        await page.get_by_role("button", name="Sign In").click()

        # Wait for redirect to complete (should redirect to CIM dashboard)
        await page.wait_for_url(f"{CIM_BASE_URL}/**", timeout=30000)

        logger.info("Login successful")
        return True

    except Exception as e:
        logger.error("Login failed", extra={"error": str(e)})
        return False


async def _navigate_and_download(page: Page, report_url: str) -> bytes | None:
    """
    Navigate to report page and download export.

    Args:
        page: Playwright page object (already logged in)
        report_url: URL of the report page

    Returns:
        CSV content as bytes, or None if failed
    """
    logger.info("Navigating to report page", extra={"url": report_url})

    try:
        await page.goto(report_url, wait_until="networkidle")

        # Wait for page to fully load (table should be visible)
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(3000)  # Additional wait for dynamic content

        logger.info("Report page loaded, initiating export")

        # Step 1: Click Export button to open export dialog
        export_button = page.get_by_role("button", name="Export")
        await export_button.click()

        # Wait for export dialog to appear
        await page.get_by_role("heading", name="Export").wait_for(timeout=5000)
        logger.info("Export dialog opened")

        # Step 2: Click Generate button to start generating the CSV
        generate_button = page.get_by_role("button", name="Generate")
        await generate_button.click()
        logger.info("Generate clicked, waiting for progress...")

        # Step 3: Wait for Download button to appear (progress completes)
        # The Generate button becomes disabled and Download button appears
        download_button = page.get_by_role("button", name="Download")
        await download_button.wait_for(state="visible", timeout=120000)
        logger.info("Download button appeared")

        # Step 4: Click Download and capture the file
        async with page.expect_download(timeout=60000) as download_info:
            await download_button.click()

        download = await download_info.value
        logger.info("Download started", extra={"suggested_filename": download.suggested_filename})

        # Save to temporary location and read content
        temp_path = Path("/tmp") / download.suggested_filename
        await download.save_as(temp_path)

        content = temp_path.read_bytes()
        logger.info("Download completed", extra={"size_bytes": len(content)})

        # Clean up temp file
        temp_path.unlink(missing_ok=True)

        return content

    except Exception as e:
        logger.error("Download failed", exc_info=True, extra={"error": str(e)})
        return None


async def download_report_async(days: int = 90) -> bytes | None:
    """
    Download CIM report using Playwright browser automation.

    This function:
    1. Launches a headless Chromium browser
    2. Logs in via Keycloak
    3. Navigates to the report page with site filters
    4. Triggers export and downloads the CSV

    Args:
        days: Number of days of data to include (default 90, used for filename only)

    Returns:
        CSV content as bytes, or None if failed
    """
    logger.info("Starting Playwright browser automation")

    async with async_playwright() as p:
        # Launch browser in headless mode
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-gpu",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--single-process",
            ],
        )

        try:
            # Create new page with reasonable viewport
            page = await browser.new_page(
                viewport={"width": 1280, "height": 720},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
            )

            # Set default timeout
            page.set_default_timeout(60000)

            # Login
            if not await _login(page):
                return None

            # Build report URL and download
            report_url = _build_report_url(CIM_SITE_IDS)
            return await _navigate_and_download(page, report_url)

        finally:
            await browser.close()
            logger.info("Browser closed")


def download_report(days: int = 90) -> bytes | None:
    """
    Synchronous wrapper for download_report_async.

    Args:
        days: Number of days of data to include (default 90)

    Returns:
        CSV content as bytes, or None if failed
    """
    return asyncio.get_event_loop().run_until_complete(download_report_async(days))


def get_report_filename(days: int = 90) -> str:
    """
    Generate a filename for the report.

    Args:
        days: Number of days (used to calculate date range)

    Returns:
        Filename with date range
    """
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    return f"actions-export-{start_date}-to-{end_date}.csv"
