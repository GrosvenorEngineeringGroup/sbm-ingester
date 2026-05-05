# Optima Demand Exporter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a new `optima-demand-exporter` Lambda that downloads BidEnergy "Demand Profile" CSVs daily and drops them in `s3://sbm-file-ingester/newTBP/` for the existing `demand_parser` to consume.

**Architecture:** Mirrors existing `optima-nem12-exporter` structure (4 Python modules + tests + Terraform + workflow). Reuses `optima_shared/` (auth, config, dynamodb) unchanged. Endpoint differs: `/BuyerReport/DemandProfilePartial?isCsv=true` instead of `/BuyerReport/ExportIntervalUsageProfileNem12`. Per-site `country` flows from DynamoDB (no env-var country filter). "No data found" sentinel CSVs are uploaded for audit retention.

**Tech Stack:** Python 3.13 + uv + pytest + responses (HTTP mocking) + moto (AWS mocking) + freezegun (time mocking) + boto3 + aws_lambda_powertools + Terraform + GitHub Actions.

**Spec reference:** [`docs/superpowers/specs/2026-05-05-optima-demand-exporter-design.md`](../specs/2026-05-05-optima-demand-exporter-design.md) (commit `b7fc665`).

**Working directory for ALL commands:** `/Users/zeyu/Desktop/GEG/sbm/sbm-ingester` — every `pytest`, `git`, and shell command in this plan assumes you are in this directory.

---

## File Structure

### Create

| Path | Responsibility |
|---|---|
| `src/functions/optima_exporter/demand_exporter/__init__.py` | Python package marker |
| `src/functions/optima_exporter/demand_exporter/uploader.py` | `upload_to_s3(bytes, filename) -> bool`. Verbatim copy of nem12 uploader, only logger service name renamed. |
| `src/functions/optima_exporter/demand_exporter/downloader.py` | `download_demand_csv(...) -> tuple[bytes, str] \| None` — hits `/BuyerReport/DemandProfilePartial?isCsv=true`. Includes `format_date_for_url(iso_date) -> "DD Mmm YYYY"`. Treats `"No data found"` body as success (returns bytes for audit upload). |
| `src/functions/optima_exporter/demand_exporter/processor.py` | `process_export(project, nmi=None, start_date=None, end_date=None) -> dict` — orchestrates per-project export with `ThreadPoolExecutor`. Per-site `country` from DynamoDB. |
| `src/functions/optima_exporter/demand_exporter/app.py` | `lambda_handler(event, context) -> dict` — Lambda entry point; pass-through to `process_export`. |
| `tests/unit/optima_exporter/demand_exporter/__init__.py` | Test package marker |
| `tests/unit/optima_exporter/demand_exporter/test_uploader.py` | S3 upload tests (mirrors nem12). |
| `tests/unit/optima_exporter/demand_exporter/test_downloader.py` | URL construction, happy path, no-data sentinel, error responses. |
| `tests/unit/optima_exporter/demand_exporter/test_processor.py` | Date range, single-site, full orchestration, error handling. |
| `tests/unit/optima_exporter/demand_exporter/test_app.py` | Handler routes event → process_export, missing project → 400. |

### Modify

| Path | Change |
|---|---|
| `tests/unit/optima_exporter/conftest.py` | Add `reload_demand_uploader_module()`, `reload_demand_processor_module()`, and `mock_demand_lambda_context` fixture. |
| `terraform/optima_exporter.tf` | Add log group, Lambda, 2 schedulers, CloudWatch alarm; update `optima_scheduler_invoke_lambda` Resource list. |
| `.github/workflows/main.yml` (around lines 177-178 and 252-257) | Add `cp` line in build step + `update-function-code` block in deploy step. |
| `CLAUDE.md` | Update CI/CD policy whitelist documentation to include `optima-demand-exporter`. |

### Out-of-band manual step

| Item | Procedure |
|---|---|
| `sbm-ingester-cicd-policy` IAM policy v9 | Add `arn:aws:lambda:ap-southeast-2:318396632821:function:optima-demand-exporter` to `LambdaUpdateFunctions` Resource list. Procedure: `CLAUDE.md` "Manual Sync: CI/CD IAM Policy" section. |

---

## Task 1: Package skeleton & test fixtures

**Files:**
- Create: `src/functions/optima_exporter/demand_exporter/__init__.py`
- Create: `tests/unit/optima_exporter/demand_exporter/__init__.py`
- Modify: `tests/unit/optima_exporter/conftest.py`

- [ ] **Step 1.1: Create the source package marker**

```bash
mkdir -p src/functions/optima_exporter/demand_exporter
```

Write `src/functions/optima_exporter/demand_exporter/__init__.py`:

```python
"""Demand profile exporter for Optima/BidEnergy."""
```

- [ ] **Step 1.2: Create the test package marker**

```bash
mkdir -p tests/unit/optima_exporter/demand_exporter
```

Write `tests/unit/optima_exporter/demand_exporter/__init__.py`:

```python
```

(One empty line. Same as `tests/unit/optima_exporter/nem12_exporter/__init__.py`.)

- [ ] **Step 1.3: Add reload helpers + Lambda context fixture to conftest**

Edit `tests/unit/optima_exporter/conftest.py`. After the existing `reload_processor_module` definition (currently at the end of the "Module Reload Functions" block, around line 57), add:

```python
def reload_demand_uploader_module() -> Any:
    """Reload the demand_exporter uploader module with fresh environment."""
    import demand_exporter.uploader as uploader_module

    uploader_module._s3_client = None
    importlib.reload(uploader_module)
    return uploader_module


def reload_demand_processor_module() -> Any:
    """Reload the demand_exporter processor module with fresh environment.

    Resets the DynamoDB and S3 module-level singletons before reload so
    moto's mocked clients are picked up cleanly between tests. The existing
    nem12 helper does NOT reset these — but tests that interleave real
    boto and mocked boto require this discipline.
    """
    import optima_shared.config as config_module

    importlib.reload(config_module)

    import optima_shared.dynamodb as dynamodb_module

    dynamodb_module._dynamodb = None
    importlib.reload(dynamodb_module)

    import demand_exporter.uploader as uploader_module

    uploader_module._s3_client = None
    importlib.reload(uploader_module)

    import demand_exporter.processor as processor_module

    importlib.reload(processor_module)
    return processor_module
```

Then at the end of `tests/unit/optima_exporter/conftest.py`, after `mock_billing_lambda_context`, add:

```python
@pytest.fixture
def mock_demand_lambda_context() -> MagicMock:
    """Create mock Lambda context for demand exporter."""
    context = MagicMock()
    context.function_name = "optima-demand-exporter"
    context.memory_limit_in_mb = 256
    context.invoked_function_arn = "arn:aws:lambda:ap-southeast-2:123456789012:function:optima-demand-exporter"
    context.aws_request_id = "test-request-id"
    return context
```

- [ ] **Step 1.4: Verify the skeleton imports**

Run: `uv run python -c "import demand_exporter; print(demand_exporter.__doc__)"` from the `src/functions/optima_exporter/` directory; or simpler, check the test collection:

```bash
uv run pytest tests/unit/optima_exporter/demand_exporter/ --collect-only -q
```

Expected: collects 0 tests, no import errors.

- [ ] **Step 1.5: Commit**

```bash
git add src/functions/optima_exporter/demand_exporter/__init__.py \
        tests/unit/optima_exporter/demand_exporter/__init__.py \
        tests/unit/optima_exporter/conftest.py
git commit -m "feat: add demand_exporter package skeleton and test fixtures"
```

---

## Task 2: `uploader.py` (verbatim copy + logger rename)

**Files:**
- Create: `src/functions/optima_exporter/demand_exporter/uploader.py`
- Test: `tests/unit/optima_exporter/demand_exporter/test_uploader.py`

- [ ] **Step 2.1: Write the failing tests**

Write `tests/unit/optima_exporter/demand_exporter/test_uploader.py`:

```python
"""Unit tests for demand_exporter/uploader.py module.

Tests S3 client initialization and file upload functionality.
"""

from unittest.mock import patch

import boto3
from moto import mock_aws

from tests.unit.optima_exporter.conftest import reload_demand_uploader_module


class TestGetS3Client:
    @mock_aws
    def test_lazy_initialization(self) -> None:
        uploader_module = reload_demand_uploader_module()
        result1 = uploader_module.get_s3_client()
        assert result1 is not None
        result2 = uploader_module.get_s3_client()
        assert result1 is result2


class TestUploadToS3:
    @mock_aws
    def test_successful_upload(self) -> None:
        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        uploader_module = reload_demand_uploader_module()
        csv_content = b"Commodities:,Electricity\nIdentifier,kW\n3117512760,5.5"
        filename = "optima_racv_demand_profile_NMI#OPTIMA_3117512760_2026-04-29_2026-04-29_20260505000000.csv"

        result = uploader_module.upload_to_s3(csv_content, filename)

        assert result is True
        response = s3.get_object(Bucket="sbm-file-ingester", Key=f"newTBP/{filename}")
        assert response["Body"].read() == csv_content
        assert response["ContentType"] == "text/csv"

    @mock_aws
    def test_upload_failure_returns_false(self) -> None:
        # Don't create bucket → put_object raises NoSuchBucket
        uploader_module = reload_demand_uploader_module()
        result = uploader_module.upload_to_s3(b"data", "test.csv")
        assert result is False

    @mock_aws
    def test_upload_logs_error_on_failure(self) -> None:
        uploader_module = reload_demand_uploader_module()
        with patch.object(uploader_module.logger, "error") as mock_error:
            result = uploader_module.upload_to_s3(b"data", "test.csv")
            assert result is False
            mock_error.assert_called_once()

    @mock_aws
    def test_logger_service_name_is_demand_exporter(self) -> None:
        uploader_module = reload_demand_uploader_module()
        # aws_lambda_powertools.Logger stores the service in .service
        assert uploader_module.logger.service == "optima-demand-exporter"
```

- [ ] **Step 2.2: Run tests to verify they fail**

```bash
uv run pytest tests/unit/optima_exporter/demand_exporter/test_uploader.py -v
```

Expected: ERRORS (`ModuleNotFoundError: No module named 'demand_exporter.uploader'`).

- [ ] **Step 2.3: Create `uploader.py` (verbatim copy + logger rename)**

Write `src/functions/optima_exporter/demand_exporter/uploader.py`:

```python
"""S3 upload utilities for demand profile export."""

from typing import Any

import boto3
from aws_lambda_powertools import Logger
from optima_shared.config import S3_UPLOAD_BUCKET, S3_UPLOAD_PREFIX

logger = Logger(service="optima-demand-exporter")

# S3 client (lazy initialization)
_s3_client = None


def get_s3_client() -> Any:
    """Get S3 client with lazy initialization."""
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3", region_name="ap-southeast-2")
    return _s3_client


def upload_to_s3(
    file_content: bytes,
    filename: str,
    bucket: str | None = None,
    prefix: str | None = None,
) -> bool:
    """
    Upload CSV file to S3 for ingestion pipeline.

    Args:
        file_content: CSV file content as bytes
        filename: Filename for S3 object
        bucket: S3 bucket name (default: S3_UPLOAD_BUCKET)
        prefix: S3 prefix/folder (default: S3_UPLOAD_PREFIX)

    Returns:
        True if upload successful, False otherwise
    """
    bucket = bucket or S3_UPLOAD_BUCKET
    prefix = prefix or S3_UPLOAD_PREFIX
    s3_key = f"{prefix}{filename}"

    logger.info(
        "Uploading CSV to S3",
        extra={
            "bucket": bucket,
            "key": s3_key,
            "size_bytes": len(file_content),
            "file_name": filename,
        },
    )

    try:
        s3 = get_s3_client()
        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=file_content,
            ContentType="text/csv",
        )
        logger.info(
            "CSV uploaded successfully to S3",
            extra={"bucket": bucket, "key": s3_key, "file_name": filename},
        )
        return True

    except Exception as e:
        logger.error(
            "S3 upload failed",
            exc_info=True,
            extra={
                "error": str(e),
                "bucket": bucket,
                "key": s3_key,
                "file_name": filename,
            },
        )
        return False
```

- [ ] **Step 2.4: Run tests to verify they pass**

```bash
uv run pytest tests/unit/optima_exporter/demand_exporter/test_uploader.py -v
```

Expected: 4 tests PASSED.

- [ ] **Step 2.5: Commit**

```bash
git add src/functions/optima_exporter/demand_exporter/uploader.py \
        tests/unit/optima_exporter/demand_exporter/test_uploader.py
git commit -m "feat: add demand_exporter uploader (verbatim copy of nem12 uploader)"
```

---

## Task 3: `downloader.py` — happy path CSV download

**Files:**
- Create: `src/functions/optima_exporter/demand_exporter/downloader.py`
- Test: `tests/unit/optima_exporter/demand_exporter/test_downloader.py`

- [ ] **Step 3.1: Write the failing tests**

Write `tests/unit/optima_exporter/demand_exporter/test_downloader.py`:

```python
"""Unit tests for demand_exporter/downloader.py module.

Tests date formatting and CSV download from BidEnergy DemandProfilePartial endpoint.
"""

import re
from urllib.parse import parse_qs, urlparse

import responses


class TestFormatDateForUrl:
    def test_formats_date_correctly(self) -> None:
        from demand_exporter.downloader import format_date_for_url

        assert format_date_for_url("2026-04-29") == "29 Apr 2026"

    def test_handles_different_months(self) -> None:
        from demand_exporter.downloader import format_date_for_url

        assert format_date_for_url("2026-12-01") == "01 Dec 2026"
        assert format_date_for_url("2026-09-30") == "30 Sep 2026"

    def test_handles_leap_year(self) -> None:
        from demand_exporter.downloader import format_date_for_url

        assert format_date_for_url("2024-02-29") == "29 Feb 2024"


SAMPLE_CSV_BODY = (
    b"Commodities:,\"Electricity\"\r\n"
    b"Sites (NMIs):,\"3117512760\"\r\n"
    b"Status:,\"Active\"\r\n"
    b"Country:, Australia\r\n"
    b"Start:,01-Apr-2026\r\n"
    b"End:,30-Apr-2026\r\n"
    b"\r\n"
    b"\r\n"
    b"Business Unit,Identifier,Identifier Type,ReadingDateTime,E,kW,kVa,Power Factor,Site Name\r\n"
    b",3117512760,NMI,01-Apr-2026 00:00:00,59.1000,118.2000,120.3100,0.9825,RACV NOOSA RESORT\r\n"
)


class TestDownloadDemandCsvHappyPath:
    @responses.activate
    def test_successful_download_returns_content_and_filename(self) -> None:
        from demand_exporter.downloader import download_demand_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=200,
            body=SAMPLE_CSV_BODY,
            content_type="application/vnd.csv",
        )

        result = download_demand_csv(
            cookies=".ASPXAUTH=token123",
            site_id_str="4f5855e0-0563-4bdc-b2d9-aa8d0041a2ca",
            start_date="2026-04-29",
            end_date="2026-04-29",
            project="racv",
            nmi="Optima_3117512760",
            country="AU",
        )

        assert result is not None
        content, filename = result
        assert content == SAMPLE_CSV_BODY
        # Filename: optima_<project_lower>_demand_profile_NMI#<NMI_UPPER>_<start>_<end>_<14digit_timestamp>.csv
        assert re.match(
            r"^optima_racv_demand_profile_NMI#OPTIMA_3117512760_2026-04-29_2026-04-29_\d{14}\.csv$",
            filename,
        )

    @responses.activate
    def test_request_uses_correct_url_and_params(self) -> None:
        from demand_exporter.downloader import download_demand_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=200,
            body=SAMPLE_CSV_BODY,
            content_type="application/vnd.csv",
        )

        download_demand_csv(
            cookies=".ASPXAUTH=tok",
            site_id_str="site-abc",
            start_date="2026-04-29",
            end_date="2026-04-30",
            project="racv",
            nmi="Optima_X",
            country="NZ",
        )

        assert len(responses.calls) == 1
        url = responses.calls[0].request.url
        parsed = urlparse(url)
        assert parsed.path == "/BuyerReport/DemandProfilePartial"
        params = parse_qs(parsed.query)
        assert params["isCsv"] == ["true"]
        assert params["start"] == ["29 Apr 2026"]
        assert params["end"] == ["30 Apr 2026"]
        assert params["filter.SiteIdStr"] == ["site-abc"]
        assert params["filter.SiteStatus"] == ["Active"]
        assert params["filter.commodities"] == ["Electricity"]
        assert params["filter.countrystr"] == ["NZ"]
        # Confirm there is NO `nmi` URL parameter (kept as Python arg only)
        assert "nmi" not in params

    @responses.activate
    def test_country_defaults_to_au(self) -> None:
        from demand_exporter.downloader import download_demand_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=200,
            body=SAMPLE_CSV_BODY,
            content_type="application/vnd.csv",
        )

        download_demand_csv(
            cookies=".ASPXAUTH=tok",
            site_id_str="site-abc",
            start_date="2026-04-29",
            end_date="2026-04-29",
            project="racv",
            nmi="Optima_X",
        )

        params = parse_qs(urlparse(responses.calls[0].request.url).query)
        assert params["filter.countrystr"] == ["AU"]

    @responses.activate
    def test_request_sends_cookie_header(self) -> None:
        from demand_exporter.downloader import download_demand_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=200,
            body=SAMPLE_CSV_BODY,
            content_type="application/vnd.csv",
        )

        download_demand_csv(
            cookies=".ASPXAUTH=token123",
            site_id_str="site-abc",
            start_date="2026-04-29",
            end_date="2026-04-29",
            project="racv",
            nmi="Optima_X",
        )

        assert responses.calls[0].request.headers["Cookie"] == ".ASPXAUTH=token123"

    @responses.activate
    def test_accepts_body_without_csv_content_type_when_starts_with_commodities(self) -> None:
        """Sniff trumps content-type — if body starts with `Commodities:` it is treated as CSV."""
        from demand_exporter.downloader import download_demand_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=200,
            body=SAMPLE_CSV_BODY,
            content_type="text/plain",  # wrong content-type, but body sniffs as CSV
        )

        result = download_demand_csv(
            cookies=".ASPXAUTH=tok",
            site_id_str="site-abc",
            start_date="2026-04-29",
            end_date="2026-04-29",
            project="racv",
            nmi="Optima_X",
        )

        assert result is not None
```

- [ ] **Step 3.2: Run tests to verify they fail**

```bash
uv run pytest tests/unit/optima_exporter/demand_exporter/test_downloader.py -v
```

Expected: ERRORS (`ModuleNotFoundError: No module named 'demand_exporter.downloader'`).

- [ ] **Step 3.3: Create `downloader.py` with happy-path code**

Write `src/functions/optima_exporter/demand_exporter/downloader.py`:

```python
"""CSV download utilities for demand profile export."""

from datetime import datetime

import requests
from aws_lambda_powertools import Logger
from optima_shared.config import BIDENERGY_BASE_URL

logger = Logger(service="optima-demand-exporter")

# UTF-8 BOM + ASCII whitespace tolerated before the metadata header.
_CSV_HEADER_PREFIXES = b"\xef\xbb\xbf \t\r\n"


def format_date_for_url(date_str: str) -> str:
    """
    Convert ISO date format to BidEnergy URL format.

    Args:
        date_str: Date in ISO format (YYYY-MM-DD)

    Returns:
        Date formatted for URL (e.g., "29 Apr 2026")

    Note:
        %b is locale-dependent. AWS Lambda runtime defaults to en_US.UTF-8 /
        C.UTF-8 where %b matches "Apr", "Jun", etc. Local dev environments
        with non-English locales would produce different output.
    """
    return datetime.strptime(date_str, "%Y-%m-%d").strftime("%d %b %Y")


def download_demand_csv(
    cookies: str,
    site_id_str: str,
    start_date: str,
    end_date: str,
    project: str,
    nmi: str,
    *,
    country: str = "AU",
) -> tuple[bytes, str] | None:
    """
    Download demand profile CSV from BidEnergy.

    Args:
        cookies: Authentication cookie string
        site_id_str: Site identifier GUID
        start_date: Start date in ISO format (YYYY-MM-DD)
        end_date: End date in ISO format (YYYY-MM-DD)
        project: Project name (used in filename only)
        nmi: NMI identifier (used in filename only — never sent in URL)
        country: Country code ("AU" or "NZ")

    Returns:
        Tuple of (CSV content bytes, suggested filename), or None on failure.
        For "No data found" responses, returns the sentinel CSV bytes (caller uploads
        them for audit retention).
    """
    export_url = f"{BIDENERGY_BASE_URL}/BuyerReport/DemandProfilePartial"

    params = {
        "isCsv": "true",
        "start": format_date_for_url(start_date),
        "end": format_date_for_url(end_date),
        "filter.SiteIdStr": site_id_str,
        "filter.SiteStatus": "Active",
        "filter.commodities": "Electricity",
        "filter.countrystr": country,
    }

    logger.info(
        "Downloading demand CSV",
        extra={
            "site_id": site_id_str,
            "start_date": start_date,
            "end_date": end_date,
            "country": country,
        },
    )

    try:
        response = requests.get(
            export_url,
            params=params,
            headers={"Cookie": cookies},
            timeout=300,
        )
    except requests.Timeout:
        logger.error(
            "Demand CSV download failed: request timeout",
            extra={"project": project, "nmi": nmi, "site_id": site_id_str, "timeout_seconds": 300},
        )
        return None
    except requests.ConnectionError as e:
        logger.error(
            "Demand CSV download failed: connection error",
            extra={"project": project, "nmi": nmi, "error": str(e)},
        )
        return None
    except requests.RequestException as e:
        logger.error(
            "Demand CSV download failed: request error",
            exc_info=True,
            extra={"project": project, "nmi": nmi, "error": str(e)},
        )
        return None

    if response.status_code == 200:
        content_start = response.content[:100].lower()
        is_html = b"<!doctype" in content_start or b"<html" in content_start
        body_starts_like_csv = response.content.lstrip(_CSV_HEADER_PREFIXES).startswith(b"Commodities:")
        content_type = response.headers.get("Content-Type", "").lower()

        if not is_html and (body_starts_like_csv or "csv" in content_type):
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            filename = (
                f"optima_{project.lower()}_demand_profile_NMI#{nmi.upper()}_"
                f"{start_date}_{end_date}_{timestamp}.csv"
            )

            if b"No data found" in response.content:
                logger.info(
                    "Demand CSV: BidEnergy reported no data for site (uploading sentinel for audit)",
                    extra={
                        "project": project,
                        "nmi": nmi,
                        "site_id": site_id_str,
                        "csv_filename": filename,
                        "size_bytes": len(response.content),
                    },
                )
            else:
                logger.info(
                    "Demand CSV download successful",
                    extra={
                        "project": project,
                        "nmi": nmi,
                        "csv_filename": filename,
                        "size_bytes": len(response.content),
                    },
                )

            return response.content, filename

        logger.error(
            "Demand CSV download failed: received HTML/non-CSV response",
            extra={
                "project": project,
                "nmi": nmi,
                "site_id": site_id_str,
                "content_type": content_type,
                "response_preview": response.text[:500] if response.text else "empty",
            },
        )
    elif response.status_code in (401, 403):
        logger.error(
            "Demand CSV download failed: authentication/authorization error",
            extra={"project": project, "nmi": nmi, "status_code": response.status_code},
        )
    elif response.status_code == 404:
        logger.error(
            "Demand CSV download failed: site not found",
            extra={"project": project, "nmi": nmi, "site_id": site_id_str, "status_code": 404},
        )
    else:
        logger.error(
            "Demand CSV download failed: unexpected response",
            extra={
                "project": project,
                "nmi": nmi,
                "site_id": site_id_str,
                "status_code": response.status_code,
                "response_preview": response.text[:500] if response.text else "empty",
            },
        )

    return None
```

- [ ] **Step 3.4: Run tests to verify happy path passes**

```bash
uv run pytest tests/unit/optima_exporter/demand_exporter/test_downloader.py -v
```

Expected: 8 tests PASSED.

- [ ] **Step 3.5: Commit**

```bash
git add src/functions/optima_exporter/demand_exporter/downloader.py \
        tests/unit/optima_exporter/demand_exporter/test_downloader.py
git commit -m "feat: add demand_exporter downloader with happy-path CSV download"
```

---

## Task 4: `downloader.py` — no-data sentinel handling tests

**Files:**
- Test: `tests/unit/optima_exporter/demand_exporter/test_downloader.py` (extend)

> **TDD note (regression-lock, not Red-Green):** The sentinel-handling branch was already implemented in Task 3 (`if b"No data found" in response.content` in `download_demand_csv`) for architectural completeness — the audit-upload requirement is a high-stakes contract that should be expressed as one cohesive code block, not split across two tasks. Task 4's tests therefore PASS on first run, locking that behaviour in as a regression test rather than driving new code. This is an explicit, deliberate exception to strict Red-Green-Commit. Do NOT attempt to revert Task 3's sentinel branch in order to make Task 4's tests fail first.

- [ ] **Step 4.1: Add regression tests for the no-data sentinel**

Append to `tests/unit/optima_exporter/demand_exporter/test_downloader.py`:

```python
NO_DATA_BODY = (
    b"Commodities:,\"Electricity\"\r\n"
    b"Sites (NMIs):,\"0000005438UN02B\"\r\n"
    b"Status:,\"Active\"\r\n"
    b"Country:, New Zealand\r\n"
    b"Start:,01-May-2026\r\n"
    b"End:,03-May-2026\r\n"
    b"\r\n"
    b"\r\n"
    b"No data found"
)


class TestDownloadDemandCsvNoDataSentinel:
    @responses.activate
    def test_no_data_response_returns_bytes_for_audit(self) -> None:
        """BidEnergy returns a CSV containing 'No data found' for sites with no demand
        meter. The downloader must STILL return the bytes so the caller uploads the
        sentinel CSV to S3 for audit retention."""
        from demand_exporter.downloader import download_demand_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=200,
            body=NO_DATA_BODY,
            content_type="application/vnd.csv",
        )

        result = download_demand_csv(
            cookies=".ASPXAUTH=tok",
            site_id_str="site-empty",
            start_date="2026-05-01",
            end_date="2026-05-03",
            project="bunnings",
            nmi="Optima_NODATA",
        )

        assert result is not None
        content, filename = result
        assert content == NO_DATA_BODY
        assert b"No data found" in content
        # Filename pattern still applies to sentinel CSVs
        assert filename.startswith("optima_bunnings_demand_profile_NMI#OPTIMA_NODATA_2026-05-01_2026-05-03_")

    @responses.activate
    def test_no_data_response_logs_at_info_level(self) -> None:
        """Sentinel responses log at INFO with a distinct message — not as an error."""
        from unittest.mock import patch

        from demand_exporter import downloader as dl_module

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=200,
            body=NO_DATA_BODY,
            content_type="application/vnd.csv",
        )

        with patch.object(dl_module.logger, "info") as mock_info, patch.object(dl_module.logger, "error") as mock_error:
            dl_module.download_demand_csv(
                cookies=".ASPXAUTH=tok",
                site_id_str="site-empty",
                start_date="2026-05-01",
                end_date="2026-05-03",
                project="bunnings",
                nmi="Optima_NODATA",
            )

        # No errors logged
        mock_error.assert_not_called()
        # An info log mentioning the no-data outcome was emitted
        info_messages = [call.args[0] for call in mock_info.call_args_list]
        assert any("no data" in m.lower() for m in info_messages)
```

- [ ] **Step 4.2: Run tests to verify they pass**

```bash
uv run pytest tests/unit/optima_exporter/demand_exporter/test_downloader.py::TestDownloadDemandCsvNoDataSentinel -v
```

Expected: 2 tests PASSED.

- [ ] **Step 4.3: Commit**

```bash
git add tests/unit/optima_exporter/demand_exporter/test_downloader.py
git commit -m "test: lock in no-data sentinel returns bytes for audit upload"
```

---

## Task 5: `downloader.py` — error response tests

**Files:**
- Test: `tests/unit/optima_exporter/demand_exporter/test_downloader.py` (extend)

- [ ] **Step 5.1: Add error-response regression tests**

Append to `tests/unit/optima_exporter/demand_exporter/test_downloader.py`:

```python
class TestDownloadDemandCsvErrors:
    @responses.activate
    def test_html_error_page_returns_none(self) -> None:
        from demand_exporter.downloader import download_demand_csv

        html = b"<!DOCTYPE html><html><body>Server error</body></html>"
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=200,
            body=html,
            content_type="text/html",
        )

        result = download_demand_csv(
            cookies=".ASPXAUTH=tok",
            site_id_str="site-abc",
            start_date="2026-04-29",
            end_date="2026-04-29",
            project="racv",
            nmi="Optima_X",
        )
        assert result is None

    @responses.activate
    def test_401_returns_none(self) -> None:
        from demand_exporter.downloader import download_demand_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=401,
            body=b"Unauthorized",
        )

        assert download_demand_csv(
            cookies=".ASPXAUTH=expired",
            site_id_str="site",
            start_date="2026-04-29",
            end_date="2026-04-29",
            project="racv",
            nmi="Optima_X",
        ) is None

    @responses.activate
    def test_403_returns_none(self) -> None:
        from demand_exporter.downloader import download_demand_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=403,
            body=b"Forbidden",
        )

        assert download_demand_csv(
            cookies=".ASPXAUTH=tok",
            site_id_str="site",
            start_date="2026-04-29",
            end_date="2026-04-29",
            project="racv",
            nmi="Optima_X",
        ) is None

    @responses.activate
    def test_404_returns_none(self) -> None:
        from demand_exporter.downloader import download_demand_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=404,
            body=b"Not found",
        )

        assert download_demand_csv(
            cookies=".ASPXAUTH=tok",
            site_id_str="bad-site",
            start_date="2026-04-29",
            end_date="2026-04-29",
            project="racv",
            nmi="Optima_X",
        ) is None

    @responses.activate
    def test_500_returns_none(self) -> None:
        from demand_exporter.downloader import download_demand_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=500,
            body=b"Server error",
        )

        assert download_demand_csv(
            cookies=".ASPXAUTH=tok",
            site_id_str="site",
            start_date="2026-04-29",
            end_date="2026-04-29",
            project="racv",
            nmi="Optima_X",
        ) is None

    @responses.activate
    def test_timeout_returns_none(self) -> None:
        import requests as req_lib
        from demand_exporter.downloader import download_demand_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            body=req_lib.Timeout("request timed out"),
        )

        assert download_demand_csv(
            cookies=".ASPXAUTH=tok",
            site_id_str="site",
            start_date="2026-04-29",
            end_date="2026-04-29",
            project="racv",
            nmi="Optima_X",
        ) is None

    @responses.activate
    def test_connection_error_returns_none(self) -> None:
        import requests as req_lib
        from demand_exporter.downloader import download_demand_csv

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            body=req_lib.ConnectionError("connection refused"),
        )

        assert download_demand_csv(
            cookies=".ASPXAUTH=tok",
            site_id_str="site",
            start_date="2026-04-29",
            end_date="2026-04-29",
            project="racv",
            nmi="Optima_X",
        ) is None
```

- [ ] **Step 5.2: Run tests to verify they pass**

```bash
uv run pytest tests/unit/optima_exporter/demand_exporter/test_downloader.py::TestDownloadDemandCsvErrors -v
```

Expected: 7 tests PASSED.

- [ ] **Step 5.3: Run the full downloader test file to confirm all branches**

```bash
uv run pytest tests/unit/optima_exporter/demand_exporter/test_downloader.py -v
```

Expected: 17 tests PASSED (3 date-format + 5 happy-path + 2 no-data + 7 errors).

- [ ] **Step 5.4: Commit**

```bash
git add tests/unit/optima_exporter/demand_exporter/test_downloader.py
git commit -m "test: cover downloader error branches (401/403/404/500/timeout/conn-error/HTML)"
```

---

## Task 6: `processor.py` — date range helper

**Files:**
- Create: `src/functions/optima_exporter/demand_exporter/processor.py`
- Test: `tests/unit/optima_exporter/demand_exporter/test_processor.py`

- [ ] **Step 6.1: Write the failing tests for `get_date_range`**

Write `tests/unit/optima_exporter/demand_exporter/test_processor.py`:

```python
"""Unit tests for demand_exporter/processor.py module.

Tests date range calculation, single-site processing, and full-export orchestration.
"""

import os
from typing import Any
from unittest.mock import MagicMock, patch

import boto3
import pytest
import responses
from freezegun import freeze_time
from moto import mock_aws

from tests.unit.optima_exporter.conftest import reload_demand_processor_module


class TestGetDateRange:
    @freeze_time("2026-01-23 10:00:00")
    def test_default_returns_yesterday_only(self) -> None:
        processor_module = reload_demand_processor_module()
        start_date, end_date = processor_module.get_date_range()
        assert start_date == "2026-01-22"
        assert end_date == "2026-01-22"

    @freeze_time("2026-01-23 10:00:00")
    def test_respects_optima_days_back(self) -> None:
        os.environ["OPTIMA_DAYS_BACK"] = "7"
        processor_module = reload_demand_processor_module()
        start_date, end_date = processor_module.get_date_range()
        assert end_date == "2026-01-22"
        assert start_date == "2026-01-16"

    @freeze_time("2026-01-01 00:30:00")
    def test_at_midnight_uses_yesterday(self) -> None:
        processor_module = reload_demand_processor_module()
        _start, end = processor_module.get_date_range()
        assert end == "2025-12-31"
```

- [ ] **Step 6.2: Run tests to verify they fail**

```bash
uv run pytest tests/unit/optima_exporter/demand_exporter/test_processor.py::TestGetDateRange -v
```

Expected: ERRORS (module not found).

- [ ] **Step 6.3: Create `processor.py` skeleton with `get_date_range`**

Write `src/functions/optima_exporter/demand_exporter/processor.py`:

```python
"""Processing logic for demand profile export."""

from datetime import UTC, datetime, timedelta

from aws_lambda_powertools import Logger
from optima_shared.config import OPTIMA_DAYS_BACK

logger = Logger(service="optima-demand-exporter")


def get_date_range() -> tuple[str, str]:
    """
    Calculate date range based on OPTIMA_DAYS_BACK environment variable.

    Returns:
        Tuple of (start_date, end_date) in ISO format (YYYY-MM-DD).
        End date is always yesterday (yesterday's data is the freshest complete day).
    """
    today = datetime.now(UTC).date()
    end_date = today - timedelta(days=1)
    start_date = end_date - timedelta(days=OPTIMA_DAYS_BACK - 1)
    logger.info(
        "Calculated date range",
        extra={
            "start_date": str(start_date),
            "end_date": str(end_date),
            "days_back": OPTIMA_DAYS_BACK,
        },
    )
    return start_date.isoformat(), end_date.isoformat()
```

- [ ] **Step 6.4: Run tests to verify they pass**

```bash
uv run pytest tests/unit/optima_exporter/demand_exporter/test_processor.py::TestGetDateRange -v
```

Expected: 3 tests PASSED.

- [ ] **Step 6.5: Commit**

```bash
git add src/functions/optima_exporter/demand_exporter/processor.py \
        tests/unit/optima_exporter/demand_exporter/test_processor.py
git commit -m "feat: add demand_exporter processor with get_date_range"
```

---

## Task 7: `processor.py` — `process_site` (single-NMI orchestration)

**Files:**
- Modify: `src/functions/optima_exporter/demand_exporter/processor.py`
- Modify: `tests/unit/optima_exporter/demand_exporter/test_processor.py`

- [ ] **Step 7.1: Write the failing tests for `process_site`**

Append to `tests/unit/optima_exporter/demand_exporter/test_processor.py`:

```python
SAMPLE_CSV_BODY = (
    b"Commodities:,\"Electricity\"\r\n"
    b"Sites (NMIs):,\"3117512760\"\r\n"
    b"Status:,\"Active\"\r\n"
    b"Country:, Australia\r\n"
    b"Start:,01-Apr-2026\r\n"
    b"End:,30-Apr-2026\r\n"
    b"\r\n"
    b"\r\n"
    b"Business Unit,Identifier,Identifier Type,ReadingDateTime,E,kW,kVa,Power Factor,Site Name\r\n"
    b",3117512760,NMI,01-Apr-2026 00:00:00,59.1000,118.2000,120.3100,0.9825,RACV NOOSA RESORT\r\n"
)


class TestProcessSite:
    @mock_aws
    @responses.activate
    def test_successful_process_returns_success(self) -> None:
        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        processor_module = reload_demand_processor_module()
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=200,
            body=SAMPLE_CSV_BODY,
            content_type="application/vnd.csv",
        )

        result = processor_module.process_site(
            cookies=".ASPXAUTH=tok",
            nmi="Optima_3117512760",
            site_id_str="4f5855e0-0563-4bdc-b2d9-aa8d0041a2ca",
            start_date="2026-04-29",
            end_date="2026-04-29",
            project="racv",
            country="AU",
        )

        assert result["success"] is True
        assert result["nmi"] == "Optima_3117512760"
        assert result["error"] is None
        assert "filename" in result
        assert result["s3_key"].startswith("newTBP/optima_racv_demand_profile_NMI#OPTIMA_3117512760_")

    @mock_aws
    @responses.activate
    def test_no_data_sentinel_treated_as_success_and_uploaded(self) -> None:
        """No-data sentinel CSV is uploaded to S3 and reported as success with no_data flag."""
        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        processor_module = reload_demand_processor_module()
        no_data = (
            b"Commodities:,\"Electricity\"\r\nSites (NMIs):,\"X\"\r\n"
            b"Status:,\"Active\"\r\nCountry:, Australia\r\n"
            b"Start:,01-May-2026\r\nEnd:,03-May-2026\r\n\r\n\r\nNo data found"
        )
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=200,
            body=no_data,
            content_type="application/vnd.csv",
        )

        result = processor_module.process_site(
            cookies=".ASPXAUTH=tok",
            nmi="Optima_NODATA",
            site_id_str="site-empty",
            start_date="2026-05-01",
            end_date="2026-05-03",
            project="bunnings",
            country="AU",
        )

        assert result["success"] is True
        assert result["no_data"] is True
        assert result["s3_key"].startswith("newTBP/optima_bunnings_demand_profile_NMI#OPTIMA_NODATA_")

        # Verify the sentinel CSV is actually in S3 (audit retention)
        listing = s3.list_objects_v2(Bucket="sbm-file-ingester", Prefix="newTBP/")
        assert listing["KeyCount"] == 1
        body = s3.get_object(Bucket="sbm-file-ingester", Key=listing["Contents"][0]["Key"])["Body"].read()
        assert b"No data found" in body

    @responses.activate
    def test_download_failure_returns_error_result(self) -> None:
        processor_module = reload_demand_processor_module()
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=500,
            body=b"err",
        )

        result = processor_module.process_site(
            cookies=".ASPXAUTH=tok",
            nmi="Optima_X",
            site_id_str="site",
            start_date="2026-04-29",
            end_date="2026-04-29",
            project="racv",
            country="AU",
        )

        assert result["success"] is False
        assert result["error"] == "Failed to download CSV"

    @mock_aws
    @responses.activate
    def test_s3_upload_failure_returns_error_result(self) -> None:
        # Don't create the bucket → upload_to_s3 returns False
        processor_module = reload_demand_processor_module()
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=200,
            body=SAMPLE_CSV_BODY,
            content_type="application/vnd.csv",
        )

        result = processor_module.process_site(
            cookies=".ASPXAUTH=tok",
            nmi="Optima_X",
            site_id_str="site",
            start_date="2026-04-29",
            end_date="2026-04-29",
            project="racv",
            country="AU",
        )

        assert result["success"] is False
        assert result["error"] == "Failed to upload to S3"

    @mock_aws
    @responses.activate
    def test_country_propagates_to_url(self) -> None:
        from urllib.parse import parse_qs, urlparse

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        processor_module = reload_demand_processor_module()
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=200,
            body=SAMPLE_CSV_BODY,
            content_type="application/vnd.csv",
        )

        processor_module.process_site(
            cookies=".ASPXAUTH=tok",
            nmi="Optima_X",
            site_id_str="site",
            start_date="2026-04-29",
            end_date="2026-04-29",
            project="bunnings",
            country="NZ",
        )

        params = parse_qs(urlparse(responses.calls[0].request.url).query)
        assert params["filter.countrystr"] == ["NZ"]
```

- [ ] **Step 7.2: Run tests to verify they fail**

```bash
uv run pytest tests/unit/optima_exporter/demand_exporter/test_processor.py::TestProcessSite -v
```

Expected: ERRORS (`AttributeError: module 'demand_exporter.processor' has no attribute 'process_site'`).

- [ ] **Step 7.3: Add `process_site` to `processor.py`**

First, **REPLACE the entire top-of-file imports block** (everything from `"""Processing logic for demand profile export."""` down to the line `logger = Logger(service="optima-demand-exporter")` inclusive) with the consolidated form below. This keeps imports correct and ruff-isort happy across both Task 7 and Task 8 additions:

```python
"""Processing logic for demand profile export."""

from datetime import UTC, datetime, timedelta
from typing import Any

from aws_lambda_powertools import Logger
from optima_shared.config import OPTIMA_DAYS_BACK, S3_UPLOAD_PREFIX

from demand_exporter.downloader import download_demand_csv
from demand_exporter.uploader import upload_to_s3

logger = Logger(service="optima-demand-exporter")
```

Then **append** (do NOT replace existing `get_date_range` from Task 6) the new function:

```python
def process_site(
    cookies: str,
    nmi: str,
    site_id_str: str,
    start_date: str,
    end_date: str,
    project: str,
    country: str = "AU",
) -> dict[str, Any]:
    """
    Process a single site: download the demand CSV and upload it to S3.

    Sentinel "No data found" CSVs are still uploaded for audit retention; the
    result["no_data"] flag is set so callers can count them separately.

    Returns:
        Dict with at minimum: nmi, site_id, success, error.
        On success also: filename, s3_key, no_data (bool).
    """
    result: dict[str, Any] = {
        "nmi": nmi,
        "site_id": site_id_str,
        "success": False,
        "error": None,
    }

    download_result = download_demand_csv(
        cookies,
        site_id_str,
        start_date,
        end_date,
        project,
        nmi,
        country=country,
    )
    if download_result is None:
        result["error"] = "Failed to download CSV"
        return result

    csv_content, filename = download_result

    if not upload_to_s3(csv_content, filename):
        result["error"] = "Failed to upload to S3"
        return result

    result["success"] = True
    result["filename"] = filename
    result["s3_key"] = f"{S3_UPLOAD_PREFIX}{filename}"
    result["no_data"] = b"No data found" in csv_content
    return result
```

After this step, the top of `processor.py` should match the consolidated imports block shown earlier in Step 7.3, and the file should contain `get_date_range` followed by `process_site`. Run `uv run ruff check src/functions/optima_exporter/demand_exporter/processor.py` to confirm no duplicate-import or unused-import errors before moving to the test step.

- [ ] **Step 7.4: Run tests to verify they pass**

```bash
uv run pytest tests/unit/optima_exporter/demand_exporter/test_processor.py::TestProcessSite -v
```

Expected: 5 tests PASSED.

- [ ] **Step 7.5: Commit**

```bash
git add src/functions/optima_exporter/demand_exporter/processor.py \
        tests/unit/optima_exporter/demand_exporter/test_processor.py
git commit -m "feat: add demand_exporter process_site (sentinel uploaded for audit)"
```

---

## Task 8: `processor.py` — `process_export` (full orchestration)

**Files:**
- Modify: `src/functions/optima_exporter/demand_exporter/processor.py`
- Modify: `tests/unit/optima_exporter/demand_exporter/test_processor.py`

- [ ] **Step 8.1: Write the failing tests for `process_export`**

Append to `tests/unit/optima_exporter/demand_exporter/test_processor.py`:

```python
class TestProcessExport:
    @freeze_time("2026-04-30 10:00:00")
    @mock_aws
    @responses.activate
    def test_happy_path_processes_all_sites(self) -> None:
        # Set up DynamoDB
        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        dynamodb.create_table(
            TableName="sbm-optima-config",
            KeySchema=[
                {"AttributeName": "project", "KeyType": "HASH"},
                {"AttributeName": "nmi", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "project", "AttributeType": "S"},
                {"AttributeName": "nmi", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table = dynamodb.Table("sbm-optima-config")
        table.put_item(Item={"project": "racv", "nmi": "Optima_1", "siteIdStr": "site-1", "country": "AU"})
        table.put_item(Item={"project": "racv", "nmi": "Optima_2", "siteIdStr": "site-2", "country": "NZ"})

        # Set up S3
        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        # Mock responses
        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=200,
            body=SAMPLE_CSV_BODY,
            content_type="application/vnd.csv",
        )

        processor_module = reload_demand_processor_module()
        with patch("demand_exporter.processor.login_bidenergy", return_value=".ASPXAUTH=cookies"):
            result = processor_module.process_export(project="racv")

        assert result["statusCode"] == 200
        assert result["body"]["success_count"] == 2
        assert result["body"]["error_count"] == 0
        # Both sites uploaded
        listing = s3.list_objects_v2(Bucket="sbm-file-ingester", Prefix="newTBP/")
        assert listing["KeyCount"] == 2

    @mock_aws
    def test_missing_project_credentials_returns_400(self) -> None:
        # Wipe credentials
        for var in ("OPTIMA_RACV_USERNAME", "OPTIMA_RACV_PASSWORD", "OPTIMA_RACV_CLIENT_ID"):
            os.environ.pop(var, None)

        processor_module = reload_demand_processor_module()
        result = processor_module.process_export(project="racv")

        assert result["statusCode"] == 400
        assert "credentials" in result["body"].lower()

    @mock_aws
    def test_no_sites_returns_404(self) -> None:
        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        dynamodb.create_table(
            TableName="sbm-optima-config",
            KeySchema=[
                {"AttributeName": "project", "KeyType": "HASH"},
                {"AttributeName": "nmi", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "project", "AttributeType": "S"},
                {"AttributeName": "nmi", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        processor_module = reload_demand_processor_module()
        result = processor_module.process_export(project="racv")

        assert result["statusCode"] == 404
        assert "no sites" in result["body"].lower()

    @mock_aws
    def test_login_failure_returns_401(self) -> None:
        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        dynamodb.create_table(
            TableName="sbm-optima-config",
            KeySchema=[
                {"AttributeName": "project", "KeyType": "HASH"},
                {"AttributeName": "nmi", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "project", "AttributeType": "S"},
                {"AttributeName": "nmi", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        dynamodb.Table("sbm-optima-config").put_item(
            Item={"project": "racv", "nmi": "Optima_1", "siteIdStr": "site-1", "country": "AU"}
        )

        processor_module = reload_demand_processor_module()
        with patch("demand_exporter.processor.login_bidenergy", return_value=None):
            result = processor_module.process_export(project="racv")

        assert result["statusCode"] == 401
        assert "authenticate" in result["body"].lower()

    def test_inverted_date_range_returns_400(self) -> None:
        processor_module = reload_demand_processor_module()
        result = processor_module.process_export(
            project="racv",
            start_date="2026-04-30",
            end_date="2026-04-29",
        )

        assert result["statusCode"] == 400
        assert "invalid range" in result["body"].lower()

    @freeze_time("2026-04-30 10:00:00")
    @mock_aws
    @responses.activate
    def test_single_nmi_mode_processes_only_that_site(self) -> None:
        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        dynamodb.create_table(
            TableName="sbm-optima-config",
            KeySchema=[
                {"AttributeName": "project", "KeyType": "HASH"},
                {"AttributeName": "nmi", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "project", "AttributeType": "S"},
                {"AttributeName": "nmi", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table = dynamodb.Table("sbm-optima-config")
        table.put_item(Item={"project": "racv", "nmi": "Optima_1", "siteIdStr": "site-1", "country": "AU"})
        table.put_item(Item={"project": "racv", "nmi": "Optima_2", "siteIdStr": "site-2", "country": "AU"})

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        responses.add(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            status=200,
            body=SAMPLE_CSV_BODY,
            content_type="application/vnd.csv",
        )

        processor_module = reload_demand_processor_module()
        with patch("demand_exporter.processor.login_bidenergy", return_value=".ASPXAUTH=cookies"):
            result = processor_module.process_export(project="racv", nmi="Optima_1")

        assert result["statusCode"] == 200
        assert result["body"]["success_count"] == 1
        listing = s3.list_objects_v2(Bucket="sbm-file-ingester", Prefix="newTBP/")
        assert listing["KeyCount"] == 1
        # Confirm it was Optima_1 not Optima_2
        assert "OPTIMA_1" in listing["Contents"][0]["Key"]

    @mock_aws
    def test_single_nmi_not_found_returns_404(self) -> None:
        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        dynamodb.create_table(
            TableName="sbm-optima-config",
            KeySchema=[
                {"AttributeName": "project", "KeyType": "HASH"},
                {"AttributeName": "nmi", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "project", "AttributeType": "S"},
                {"AttributeName": "nmi", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        processor_module = reload_demand_processor_module()
        result = processor_module.process_export(project="racv", nmi="Optima_DOES_NOT_EXIST")

        assert result["statusCode"] == 404
        assert "not found" in result["body"].lower()

    @freeze_time("2026-04-30 10:00:00")
    @mock_aws
    @responses.activate
    def test_partial_failure_returns_207(self) -> None:
        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")
        dynamodb.create_table(
            TableName="sbm-optima-config",
            KeySchema=[
                {"AttributeName": "project", "KeyType": "HASH"},
                {"AttributeName": "nmi", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "project", "AttributeType": "S"},
                {"AttributeName": "nmi", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table = dynamodb.Table("sbm-optima-config")
        table.put_item(Item={"project": "racv", "nmi": "Optima_OK", "siteIdStr": "site-ok", "country": "AU"})
        table.put_item(Item={"project": "racv", "nmi": "Optima_BAD", "siteIdStr": "site-bad", "country": "AU"})

        s3 = boto3.client("s3", region_name="ap-southeast-2")
        s3.create_bucket(
            Bucket="sbm-file-ingester",
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        # First call succeeds, second 500s
        def callback(request: Any) -> tuple[int, dict, bytes]:
            if "site-ok" in request.url:
                return 200, {"Content-Type": "application/vnd.csv"}, SAMPLE_CSV_BODY
            return 500, {}, b"err"

        responses.add_callback(
            responses.GET,
            "https://app.bidenergy.com/BuyerReport/DemandProfilePartial",
            callback=callback,
        )

        processor_module = reload_demand_processor_module()
        with patch("demand_exporter.processor.login_bidenergy", return_value=".ASPXAUTH=cookies"):
            result = processor_module.process_export(project="racv")

        assert result["statusCode"] == 207
        assert result["body"]["success_count"] == 1
        assert result["body"]["error_count"] == 1
```

- [ ] **Step 8.2: Run tests to verify they fail**

```bash
uv run pytest tests/unit/optima_exporter/demand_exporter/test_processor.py::TestProcessExport -v
```

Expected: ERRORS (`AttributeError: module 'demand_exporter.processor' has no attribute 'process_export'`).

- [ ] **Step 8.3: Add `process_export` to `processor.py`**

First, **REPLACE the entire top-of-file imports block** (everything from `"""Processing logic for demand profile export."""` down to `logger = Logger(service="optima-demand-exporter")` inclusive — i.e. the block established in Step 7.3) with the new consolidated form below. This merges the new `concurrent.futures`, `date`, `login_bidenergy`, `MAX_WORKERS`/`get_project_config`, and DynamoDB imports without creating duplicate `from datetime import ...` lines:

```python
"""Processing logic for demand profile export."""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, date, datetime, timedelta
from typing import Any

from aws_lambda_powertools import Logger
from optima_shared.auth import login_bidenergy
from optima_shared.config import MAX_WORKERS, OPTIMA_DAYS_BACK, S3_UPLOAD_PREFIX, get_project_config
from optima_shared.dynamodb import get_site_by_nmi, get_sites_for_project

from demand_exporter.downloader import download_demand_csv
from demand_exporter.uploader import upload_to_s3

logger = Logger(service="optima-demand-exporter")
```

Then **append** (do NOT replace existing `get_date_range` and `process_site`) the new function:

```python
def process_export(
    project: str,
    nmi: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    """
    Process demand profile export for a project.

    Args:
        project: Project name (required)
        nmi: Optional single NMI to export
        start_date: Optional start date (YYYY-MM-DD)
        end_date: Optional end date (YYYY-MM-DD)

    Returns:
        Response dict with statusCode and body. 200 = all OK; 207 = partial
        failure; 4xx = early reject (no retry needed by EventBridge).
    """
    project = project.lower()

    # Reject inverted ranges when both dates are explicitly provided
    if start_date and end_date and date.fromisoformat(start_date) > date.fromisoformat(end_date):
        logger.warning(
            "Export rejected: startDate after endDate",
            extra={"project": project, "start_date": start_date, "end_date": end_date},
        )
        return {
            "statusCode": 400,
            "body": f"Invalid range: startDate ({start_date}) > endDate ({end_date})",
        }

    logger.info(
        "Starting demand profile export",
        extra={"project": project, "nmi": nmi, "start_date": start_date, "end_date": end_date},
    )

    config = get_project_config(project)
    if not config:
        logger.error("Export rejected: no credentials for project", extra={"project": project})
        return {
            "statusCode": 400,
            "body": f"No credentials configured for project: {project}",
        }

    if nmi:
        site = get_site_by_nmi(project, nmi)
        if not site:
            logger.warning("Export rejected: NMI not found", extra={"project": project, "nmi": nmi})
            return {
                "statusCode": 404,
                "body": f"NMI {nmi} not found for project {project}",
            }
        sites = [site]
    else:
        sites = get_sites_for_project(project)
        if not sites:
            logger.warning("Export rejected: no sites found", extra={"project": project})
            return {
                "statusCode": 404,
                "body": f"No sites found for project {project}",
            }

    if not start_date and not end_date:
        start_date, end_date = get_date_range()
    else:
        today = datetime.now(UTC).date()
        if not end_date:
            end_date = (today - timedelta(days=1)).isoformat()
        if not start_date:
            end_d = date.fromisoformat(end_date)
            start_date = (end_d - timedelta(days=OPTIMA_DAYS_BACK - 1)).isoformat()

    # Defense in depth: re-check after resolution
    if date.fromisoformat(start_date) > date.fromisoformat(end_date):
        logger.warning(
            "Export rejected: resolved startDate after endDate",
            extra={"project": project, "start_date": start_date, "end_date": end_date},
        )
        return {
            "statusCode": 400,
            "body": f"Invalid range after resolution: startDate ({start_date}) > endDate ({end_date})",
        }

    logger.info(
        "Authenticating with BidEnergy",
        extra={"project": project, "site_count": len(sites)},
    )
    cookies = login_bidenergy(config["username"], config["password"], config["client_id"])
    if cookies is None:
        logger.error("Export failed: authentication failed", extra={"project": project})
        return {
            "statusCode": 401,
            "body": "Failed to authenticate with BidEnergy",
        }

    results: list[dict[str, Any]] = []
    logger.info(
        "Processing sites in parallel",
        extra={"project": project, "site_count": len(sites), "max_workers": MAX_WORKERS},
    )

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_site = {
            executor.submit(
                process_site,
                cookies=cookies,
                nmi=site["nmi"],
                site_id_str=site["siteIdStr"],
                start_date=start_date,
                end_date=end_date,
                project=project,
                country=site.get("country", "AU"),
            ): site
            for site in sites
        }

        for future in as_completed(future_to_site):
            site = future_to_site[future]
            try:
                result = future.result()
                result["project"] = project
                results.append(result)
            except Exception as e:
                logger.error(
                    "Site processing failed with exception",
                    exc_info=True,
                    extra={"project": project, "nmi": site["nmi"], "error": str(e)},
                )
                results.append(
                    {
                        "nmi": site["nmi"],
                        "site_id": site["siteIdStr"],
                        "project": project,
                        "success": False,
                        "error": f"Thread execution failed: {e}",
                    }
                )

    success_count = sum(1 for r in results if r.get("success"))
    error_count = sum(1 for r in results if not r.get("success"))
    no_data_count = sum(1 for r in results if r.get("no_data"))

    logger.info(
        "Demand export completed",
        extra={
            "project": project,
            "total_sites": len(sites),
            "success_count": success_count,
            "error_count": error_count,
            "no_data_count": no_data_count,
        },
    )

    return {
        "statusCode": 200 if error_count == 0 else 207,
        "body": {
            "message": f"Processed {len(sites)} site(s) for {project}",
            "project": project,
            "date_range": {"start": start_date, "end": end_date},
            "success_count": success_count,
            "error_count": error_count,
            "no_data_count": no_data_count,
            "results": results,
        },
    }
```

After this step, run `uv run ruff check src/functions/optima_exporter/demand_exporter/processor.py` to confirm no duplicate-import or unused-import errors. The top of the file must match the consolidated imports block already shown at the start of Step 8.3.

- [ ] **Step 8.4: Run tests to verify they pass**

```bash
uv run pytest tests/unit/optima_exporter/demand_exporter/test_processor.py::TestProcessExport -v
```

Expected: 8 tests PASSED.

- [ ] **Step 8.5: Run full processor test file**

```bash
uv run pytest tests/unit/optima_exporter/demand_exporter/test_processor.py -v
```

Expected: 16 tests PASSED (3 date-range + 5 process_site + 8 process_export).

- [ ] **Step 8.6: Commit**

```bash
git add src/functions/optima_exporter/demand_exporter/processor.py \
        tests/unit/optima_exporter/demand_exporter/test_processor.py
git commit -m "feat: add demand_exporter process_export with parallel orchestration"
```

---

## Task 9: `app.py` (Lambda handler)

**Files:**
- Create: `src/functions/optima_exporter/demand_exporter/app.py`
- Create: `tests/unit/optima_exporter/demand_exporter/test_app.py`

- [ ] **Step 9.1: Write the failing tests**

Write `tests/unit/optima_exporter/demand_exporter/test_app.py`:

```python
"""Unit tests for demand_exporter/app.py — the Lambda handler entry point."""

from unittest.mock import MagicMock, patch


class TestLambdaHandler:
    def test_event_with_project_triggers_export(self, mock_demand_lambda_context: MagicMock) -> None:
        from demand_exporter.app import lambda_handler

        with patch("demand_exporter.app.process_export") as mock_export:
            mock_export.return_value = {"statusCode": 200, "body": {}}

            event = {
                "project": "racv",
                "nmi": "Optima_3117512760",
                "startDate": "2026-04-29",
                "endDate": "2026-04-29",
            }

            result = lambda_handler(event, mock_demand_lambda_context)

            mock_export.assert_called_once_with(
                project="racv",
                nmi="Optima_3117512760",
                start_date="2026-04-29",
                end_date="2026-04-29",
            )
            assert result["statusCode"] == 200

    def test_missing_project_returns_400(self, mock_demand_lambda_context: MagicMock) -> None:
        from demand_exporter.app import lambda_handler

        result = lambda_handler({}, mock_demand_lambda_context)

        assert result["statusCode"] == 400
        assert "project" in result["body"].lower()

    def test_event_with_only_project_uses_processor_defaults(
        self, mock_demand_lambda_context: MagicMock
    ) -> None:
        from demand_exporter.app import lambda_handler

        with patch("demand_exporter.app.process_export") as mock_export:
            mock_export.return_value = {"statusCode": 200, "body": {}}

            lambda_handler({"project": "bunnings"}, mock_demand_lambda_context)

            mock_export.assert_called_once_with(
                project="bunnings",
                nmi=None,
                start_date=None,
                end_date=None,
            )
```

- [ ] **Step 9.2: Run tests to verify they fail**

```bash
uv run pytest tests/unit/optima_exporter/demand_exporter/test_app.py -v
```

Expected: ERRORS (module not found).

- [ ] **Step 9.3: Create `app.py`**

Write `src/functions/optima_exporter/demand_exporter/app.py`:

```python
"""
Optima Demand Profile Exporter Lambda

Exports BidEnergy "Demand Profile" CSVs by downloading them from BidEnergy
and uploading them to S3 for the existing demand_parser to consume.

Event parameters:
    project: Project name ("bunnings" or "racv") - required
    nmi: NMI identifier - optional (if not provided, exports all NMIs for the project)
    startDate: Start date in ISO format (YYYY-MM-DD) - optional
    endDate: End date in ISO format (YYYY-MM-DD) - optional
"""

from typing import Any

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

from demand_exporter.processor import process_export

logger = Logger(service="optima-demand-exporter")


@logger.inject_lambda_context
def lambda_handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:
    """
    Lambda handler for demand profile export.

    Returns:
        Response dict from process_export (statusCode 200/207/400/401/404).
    """
    project = event.get("project")

    if not project:
        logger.warning("Export rejected: missing project parameter")
        return {
            "statusCode": 400,
            "body": "Missing required parameter: project",
        }

    logger.info(
        "Lambda invoked",
        extra={
            "project": project,
            "nmi": event.get("nmi"),
            "start_date": event.get("startDate"),
            "end_date": event.get("endDate"),
        },
    )

    result = process_export(
        project=project,
        nmi=event.get("nmi"),
        start_date=event.get("startDate"),
        end_date=event.get("endDate"),
    )

    body = result.get("body", {})
    if isinstance(body, dict):
        logger.info(
            "Export completed",
            extra={
                "success_count": body.get("success_count", 0),
                "error_count": body.get("error_count", 0),
                "no_data_count": body.get("no_data_count", 0),
            },
        )

    return result
```

- [ ] **Step 9.4: Run tests to verify they pass**

```bash
uv run pytest tests/unit/optima_exporter/demand_exporter/test_app.py -v
```

Expected: 3 tests PASSED.

- [ ] **Step 9.5: Run full demand_exporter test directory + coverage check**

```bash
uv run pytest tests/unit/optima_exporter/demand_exporter/ --cov=src/functions/optima_exporter/demand_exporter -v
```

Expected: 40 tests PASSED (4 uploader + 17 downloader + 16 processor + 3 app). Coverage on `src/functions/optima_exporter/demand_exporter/` ≥ 90%.

- [ ] **Step 9.6: Run the full project test suite to confirm no regressions**

```bash
uv run pytest --cov=src
```

Expected: all 525+ existing tests PASSED, plus the new ones; overall coverage ≥ 90% (lefthook pre-push gate).

- [ ] **Step 9.7: Lint**

```bash
uv run ruff check src/functions/optima_exporter/demand_exporter/ tests/unit/optima_exporter/demand_exporter/
uv run ruff format src/functions/optima_exporter/demand_exporter/ tests/unit/optima_exporter/demand_exporter/
```

Expected: no errors.

- [ ] **Step 9.8: Commit**

```bash
git add src/functions/optima_exporter/demand_exporter/app.py \
        tests/unit/optima_exporter/demand_exporter/test_app.py
git commit -m "feat: add demand_exporter Lambda handler"
```

---

## Task 10: Terraform infrastructure

**Files:**
- Modify: `terraform/optima_exporter.tf`

- [ ] **Step 10.1: Add the new Lambda + log group + schedulers + alarm**

Open `terraform/optima_exporter.tf`. After the existing `aws_cloudwatch_metric_alarm.optima_billing_errors` block (currently around line 351, just before the `data "aws_sns_topic" "sbm_alerts"` block), add the following content. **Place these immediately before the `data "aws_sns_topic" "sbm_alerts"` block** so the alarm references work and the file stays organized:

```hcl
# ================================
# Lambda 3: Demand Exporter
# ================================

resource "aws_cloudwatch_log_group" "optima_demand_exporter" {
  name              = "/aws/lambda/optima-demand-exporter"
  retention_in_days = var.log_retention_days

  tags = local.common_tags
}

resource "aws_lambda_function" "optima_demand_exporter" {
  function_name = "optima-demand-exporter"
  description   = "Exports Optima Demand Profile CSVs to S3 for ingestion pipeline"
  role          = data.aws_iam_role.ingester_role.arn
  handler       = "demand_exporter.app.lambda_handler"
  runtime       = "python3.13"
  timeout       = 900
  memory_size   = 256
  s3_bucket     = var.deployment_bucket
  s3_key        = "${local.lambda_s3_prefix}/optima_exporter.zip"

  environment {
    variables = merge(local.optima_common_env, {
      POWERTOOLS_SERVICE_NAME = "optima-demand-exporter"

      # S3 upload configuration
      S3_UPLOAD_BUCKET = "sbm-file-ingester"
      S3_UPLOAD_PREFIX = "newTBP/"

      # Demand export configuration
      OPTIMA_DAYS_BACK   = "1"
      OPTIMA_MAX_WORKERS = "20"
    })
  }

  tracing_config {
    mode = "PassThrough"
  }

  depends_on = [aws_cloudwatch_log_group.optima_demand_exporter]

  tags = local.common_tags
}

# ================================
# EventBridge Scheduler: Demand (Daily, 14:30 Sydney — staggered 30min after nem12)
# ================================

resource "aws_scheduler_schedule" "optima_bunnings_demand" {
  name       = "optima-bunnings-demand-daily"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "cron(30 14 * * ? *)"
  schedule_expression_timezone = "Australia/Sydney"

  target {
    arn      = aws_lambda_function.optima_demand_exporter.arn
    role_arn = aws_iam_role.optima_scheduler_role.arn
    input    = jsonencode({ project = "bunnings" })
  }
}

resource "aws_scheduler_schedule" "optima_racv_demand" {
  name       = "optima-racv-demand-daily"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "cron(30 14 * * ? *)"
  schedule_expression_timezone = "Australia/Sydney"

  target {
    arn      = aws_lambda_function.optima_demand_exporter.arn
    role_arn = aws_iam_role.optima_scheduler_role.arn
    input    = jsonencode({ project = "racv" })
  }
}

# CloudWatch alarm — mirror existing optima_nem12_errors alarm
resource "aws_cloudwatch_metric_alarm" "optima_demand_errors" {
  alarm_name          = "optima-demand-exporter-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 3600 # 1 hour — matches optima_nem12_errors
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Optima demand exporter Lambda errors"

  dimensions = {
    FunctionName = aws_lambda_function.optima_demand_exporter.function_name
  }

  alarm_actions = [data.aws_sns_topic.sbm_alerts.arn]
  ok_actions    = [data.aws_sns_topic.sbm_alerts.arn]

  tags = local.common_tags
}
```

- [ ] **Step 10.2: Update `optima_scheduler_invoke_lambda` policy to include the new Lambda**

In `terraform/optima_exporter.tf`, search for the resource `aws_iam_role_policy.optima_scheduler_invoke_lambda` by name (do NOT use line numbers — they shift after Step 10.1's insertion). Replace its `Resource` list:

```hcl
# Find this block:
resource "aws_iam_role_policy" "optima_scheduler_invoke_lambda" {
  name = "sbm-optima-scheduler-invoke-lambda"
  role = aws_iam_role.optima_scheduler_role.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = "lambda:InvokeFunction"
      Resource = [
        aws_lambda_function.optima_nem12_exporter.arn,
        aws_lambda_function.optima_billing_exporter.arn,
      ]
    }]
  })
}
```

Replace the `Resource` list with:

```hcl
      Resource = [
        aws_lambda_function.optima_nem12_exporter.arn,
        aws_lambda_function.optima_billing_exporter.arn,
        aws_lambda_function.optima_demand_exporter.arn,
      ]
```

- [ ] **Step 10.3: Run `terraform fmt` and `terraform validate`**

```bash
cd terraform
terraform fmt
terraform validate
cd ..
```

Expected: `Success! The configuration is valid.` from `validate`. `fmt` may reformat lines — that's fine.

- [ ] **Step 10.4: Initialize Terraform (if needed) and run `terraform plan`**

```bash
cd terraform
# Required if .terraform/ does not exist or backend changed since last apply.
# Safe to re-run idempotently — exits fast if already initialized.
terraform init
terraform plan -no-color > /tmp/tfplan.txt 2>&1
grep -E "^(Plan:|Error:)" /tmp/tfplan.txt
cd ..
```

Expected from grep: `Plan: 5 to add, 1 to change, 0 to destroy.` (no `Error:` lines).

Then inspect the additions list:

```bash
grep -E "^  # aws_" /tmp/tfplan.txt
```

Expected (plan should show ONLY additions/changes for the new resources, no destroys):
- `+ aws_cloudwatch_log_group.optima_demand_exporter`
- `+ aws_lambda_function.optima_demand_exporter`
- `+ aws_scheduler_schedule.optima_bunnings_demand`
- `+ aws_scheduler_schedule.optima_racv_demand`
- `+ aws_cloudwatch_metric_alarm.optima_demand_errors`
- `~ aws_iam_role_policy.optima_scheduler_invoke_lambda` (in-place change to add the third ARN)

If you see any `-` (destroy) lines for resources you didn't touch, STOP and investigate before proceeding.

- [ ] **Step 10.5: Apply the Terraform changes**

```bash
cd terraform
terraform apply
cd ..
```

Type `yes` when prompted. Expected: 5 added, 1 changed, 0 destroyed.

After apply, verify the Lambda exists (it will be empty until first deploy):

```bash
aws lambda get-function --function-name optima-demand-exporter \
  --region ap-southeast-2 --query 'Configuration.[FunctionName,Runtime,Handler,Timeout]'
```

Expected output: `["optima-demand-exporter", "python3.13", "demand_exporter.app.lambda_handler", 900]`.

- [ ] **Step 10.6: Commit the Terraform changes**

```bash
git add terraform/optima_exporter.tf
git commit -m "feat: add optima-demand-exporter Lambda + 14:30 daily schedules + alarm"
```

---

## Task 11: GitHub Actions workflow update

**Files:**
- Modify: `.github/workflows/main.yml`

- [ ] **Step 11.1: Add the build step `cp -r` line**

In `.github/workflows/main.yml`, find the optima_exporter build block by searching for the step name `Build Optima Exporter Lambda`. The current state is:

```yaml
- name: Build Optima Exporter Lambda
  if: steps.changes.outputs.optima_exporter == 'true'
  run: |
    mkdir -p build/optima_exporter
    cp -r build/deps/* build/optima_exporter/
    cp -r src/functions/optima_exporter/optima_shared build/optima_exporter/
    cp -r src/functions/optima_exporter/nem12_exporter build/optima_exporter/
    cp -r src/functions/optima_exporter/billing_exporter build/optima_exporter/
    cd build/optima_exporter && zip -r ../../optima_exporter.zip . && cd ../..
```

Add a new line just before the `cd build/optima_exporter` zip line (so all `cp -r` lines are grouped):

```yaml
    cp -r src/functions/optima_exporter/billing_exporter build/optima_exporter/
    cp -r src/functions/optima_exporter/demand_exporter build/optima_exporter/
    cd build/optima_exporter && zip -r ../../optima_exporter.zip . && cd ../..
```

- [ ] **Step 11.2: Add the deploy step `update-function-code` block**

In `.github/workflows/main.yml`, find the block by searching for the step name `Upload Optima Exporter & Refresh`. The current state is:

```yaml
- name: Upload Optima Exporter & Refresh
  if: steps.changes.outputs.optima_exporter == 'true'
  run: |
    aws s3 cp optima_exporter.zip s3://gega-code-deployment-bucket/sbm-files-ingester/optima_exporter.zip
    aws lambda update-function-code \
      --function-name optima-nem12-exporter \
      --s3-bucket gega-code-deployment-bucket \
      --s3-key sbm-files-ingester/optima_exporter.zip \
      --publish
    aws lambda update-function-code \
      --function-name optima-billing-exporter \
      --s3-bucket gega-code-deployment-bucket \
      --s3-key sbm-files-ingester/optima_exporter.zip \
      --publish
```

Add a third `update-function-code` block at the end:

```yaml
    aws lambda update-function-code \
      --function-name optima-billing-exporter \
      --s3-bucket gega-code-deployment-bucket \
      --s3-key sbm-files-ingester/optima_exporter.zip \
      --publish
    aws lambda update-function-code \
      --function-name optima-demand-exporter \
      --s3-bucket gega-code-deployment-bucket \
      --s3-key sbm-files-ingester/optima_exporter.zip \
      --publish
```

- [ ] **Step 11.3: Verify the YAML is valid**

```bash
uv run python -c "import yaml; yaml.safe_load(open('.github/workflows/main.yml'))"
```

Expected: no output (valid YAML).

- [ ] **Step 11.4: Commit the workflow change**

```bash
git add .github/workflows/main.yml
git commit -m "ci: include optima-demand-exporter in build + deploy steps"
```

---

## Task 12: Update CLAUDE.md (CI/CD whitelist documentation)

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 12.1: Add `optima-demand-exporter` to the whitelist documentation**

In `CLAUDE.md`, find the "Manual Sync: CI/CD IAM Policy" section. The current whitelist (around line 268) lists 8 functions:

```
Current whitelisted Lambdas (as of last verified sync, 2026-04-14):
- `sbm-files-ingester`
- `sbm-files-ingester-redrive`
- `sbm-files-ingester-nem12-mappings-to-s3`
- `sbm-weekly-archiver`
- `sbm-glue-trigger`
- `optima-nem12-exporter`
- `optima-billing-exporter`
- `cim-report-exporter`
```

Replace it with:

```
Current whitelisted Lambdas (as of last verified sync, 2026-05-05):
- `sbm-files-ingester`
- `sbm-files-ingester-redrive`
- `sbm-files-ingester-nem12-mappings-to-s3`
- `sbm-weekly-archiver`
- `sbm-glue-trigger`
- `optima-nem12-exporter`
- `optima-billing-exporter`
- `optima-demand-exporter`
- `cim-report-exporter`
```

Also update the Lambda Functions table earlier in `CLAUDE.md` (around line 95). The current table lists 8 Lambdas. Add a row for the new one — find the row for `optima-billing-exporter` and insert after it:

```markdown
| `optima-demand-exporter` | Python 3.13 | 256 MB | 900s | Daily export — downloads BidEnergy Demand Profile CSVs (kW/kVa/PF), uploads to S3 (X-Ray disabled) |
```

- [ ] **Step 12.2: Commit the docs update**

```bash
git add CLAUDE.md
git commit -m "docs: add optima-demand-exporter to CI/CD whitelist + Lambda table"
```

---

## Task 13: Manual CI/CD policy update (out-of-band)

> ⚠️ **This step is NOT in CI**. It must be done manually before the next GitHub Actions deploy, or that deploy will fail with `AccessDeniedException: lambda:UpdateFunctionCode`.

- [ ] **Step 13.1: Fetch the current CI/CD policy**

```bash
aws iam get-policy-version \
    --policy-arn arn:aws:iam::318396632821:policy/sbm-ingester-cicd-policy \
    --version-id $(aws iam get-policy --policy-arn arn:aws:iam::318396632821:policy/sbm-ingester-cicd-policy --query 'Policy.DefaultVersionId' --output text) \
    --query 'PolicyVersion.Document' > /tmp/policy.json
```

- [ ] **Step 13.2: Edit `/tmp/policy.json` to add the new Lambda ARN**

Open `/tmp/policy.json` in an editor. Find the Statement with `"Sid": "LambdaUpdateFunctions"`. In its `Resource` array, add:

```json
"arn:aws:lambda:ap-southeast-2:318396632821:function:optima-demand-exporter"
```

(Append it as a new array element, with a comma separating from the previous entry.)

- [ ] **Step 13.3: Check current version count**

```bash
aws iam list-policy-versions \
  --policy-arn arn:aws:iam::318396632821:policy/sbm-ingester-cicd-policy \
  --query 'Versions[*].[VersionId,IsDefaultVersion]' --output table
```

If 5 versions exist, delete the oldest non-default version first:

```bash
# Replace <vN> with the actual oldest non-default VersionId from the table above
aws iam delete-policy-version \
  --policy-arn arn:aws:iam::318396632821:policy/sbm-ingester-cicd-policy \
  --version-id <vN>
```

- [ ] **Step 13.4: Create the new policy version and set it as default**

```bash
aws iam create-policy-version \
  --policy-arn arn:aws:iam::318396632821:policy/sbm-ingester-cicd-policy \
  --policy-document file:///tmp/policy.json --set-as-default
```

Expected: returns the new VersionId with `IsDefaultVersion: true`.

- [ ] **Step 13.5: Confirm the new ARN is in the active policy**

```bash
aws iam get-policy-version \
    --policy-arn arn:aws:iam::318396632821:policy/sbm-ingester-cicd-policy \
    --version-id $(aws iam get-policy --policy-arn arn:aws:iam::318396632821:policy/sbm-ingester-cicd-policy --query 'Policy.DefaultVersionId' --output text) \
    --query 'PolicyVersion.Document.Statement[?Sid==`LambdaUpdateFunctions`].Resource' --output json | grep optima-demand-exporter
```

Expected: prints the line containing `optima-demand-exporter`.

- [ ] **Step 13.6: Clean up**

```bash
rm /tmp/policy.json
```

(No git commit for this task — it is out-of-band AWS state.)

---

## Task 14: Push, deploy, and smoke test

> Run this task only after Task 13 is complete and verified.

- [ ] **Step 14.1: Push to main**

```bash
git push origin main
```

- [ ] **Step 14.2: Watch the GitHub Actions deploy**

```bash
gh run watch
```

Or list the most recent runs:

```bash
gh run list --limit 3
```

Expected: the most recent run on `main` succeeds. The deploy step "Upload Optima Exporter & Refresh" should show three `update-function-code` calls, all returning new function ARNs.

If the run fails with `AccessDeniedException: lambda:UpdateFunctionCode`, return to Task 13 — the IAM policy was not updated correctly.

- [ ] **Step 14.3: Verify the deployed Lambda has code**

```bash
aws lambda get-function --function-name optima-demand-exporter \
  --region ap-southeast-2 --query 'Configuration.[LastModified,CodeSize]'
```

Expected: `LastModified` matches the time of the deploy run; `CodeSize` is non-zero (typically several MB).

- [ ] **Step 14.4: Smoke test — invoke against one known RACV NMI**

Use the NMI verified during the spec phase (RACV Noosa Resort, 143k+ rows of demand data).

```bash
aws lambda invoke \
  --function-name optima-demand-exporter \
  --payload '{"project":"racv","nmi":"Optima_3117512760"}' \
  --cli-binary-format raw-in-base64-out \
  --region ap-southeast-2 \
  /tmp/demand-smoke.json && cat /tmp/demand-smoke.json
```

Expected: `{"statusCode": 200, "body": {"success_count": 1, "error_count": 0, ...}}`.

- [ ] **Step 14.5: Verify the CSV landed in S3**

```bash
aws s3 ls s3://sbm-file-ingester/newTBP/ --region ap-southeast-2 \
  | grep "optima_racv_demand_profile_NMI#OPTIMA_3117512760"
```

Expected: one file like `optima_racv_demand_profile_NMI#OPTIMA_3117512760_2026-05-04_2026-05-04_<timestamp>.csv`.

- [ ] **Step 14.6: Verify CloudWatch logs show successful processing**

```bash
aws logs tail /aws/lambda/optima-demand-exporter --since 5m --region ap-southeast-2 \
  | grep -E "Demand CSV download successful|CSV uploaded successfully"
```

Expected: both log lines present.

- [ ] **Step 14.7: Verify the file_processor consumed the CSV**

Wait ~30 seconds, then:

```bash
aws logs tail /aws/lambda/sbm-files-ingester --since 2m --region ap-southeast-2 \
  | grep -E "demand_written|demand_no_rows_written|demand_no_rows_to_process"
```

Expected: ONE of these three log keys appears, mentioning the new S3 key. Interpret the outcome:

| Log key | Meaning | Action |
|---|---|---|
| `demand_written` | Real demand data → Hudi rows uploaded successfully | ✅ All good. Proceed to Step 14.8. |
| `demand_no_rows_written` | CSV had data rows but **none** mapped to a Neptune sensor ID (mappings missing for this NMI's `Optima_<NMI>-demand-{kw,kva,pf}` keys) | ⚠️ Run `scripts/import_demand_points.py` for RACV NMIs (it currently only imports Bunnings) before re-running the smoke test, or pick a Bunnings NMI from `data/demand_points.csv` for the smoke test instead. |
| `demand_no_rows_to_process` | CSV body was the BidEnergy "No data found" sentinel | ⚠️ This NMI has no demand meter — pick a different NMI from DynamoDB and retry. |

- [ ] **Step 14.8: Verify the source CSV moved to `newP/` or `newIrrevFiles/`**

```bash
aws s3 ls s3://sbm-file-ingester/newP/ --region ap-southeast-2 \
  | grep "optima_racv_demand_profile_NMI#OPTIMA_3117512760" || \
aws s3 ls s3://sbm-file-ingester/newIrrevFiles/ --region ap-southeast-2 \
  | grep "optima_racv_demand_profile_NMI#OPTIMA_3117512760"
```

Expected: file listed in `newIrrevFiles/` (because `demand_parser` returns `[]` after writing Hudi directly — `file_neptune_ids` is empty so the file routes to IRREVFILES).

- [ ] **Step 14.9: Trigger Glue + verify Hudi data**

> **Note on RACV vs Bunnings:** The committed `data/demand_points.csv` contains **Bunnings NMIs only** — the import script (`scripts/import_demand_points.py`) was previously run only for Bunnings. So `grep "Optima_3117512760" data/demand_points.csv` returns nothing. For end-to-end verification, two options:
>
> - **(a) Easier:** Pick a Bunnings NMI from `data/demand_points.csv` for the smoke test instead of `Optima_3117512760`. Re-run Steps 14.4–14.8 with that NMI. Then look up its three sensor IDs from the CSV directly.
> - **(b) Complete:** Run `scripts/import_demand_points.py` adapted for RACV (see `scripts/generate_demand_points.py` for the Neptune walking pattern), commit the resulting RACV rows to `data/demand_points.csv`, and proceed.
>
> Pick (a) for the smoke test in this PR; defer (b) to a follow-up so RACV demand exports actually flow into Hudi after deploy. Without (b), RACV CSVs upload to S3, parse fine, but produce 0 Hudi rows (`demand_no_rows_written` log).

After waiting (or invoking Glue manually), query Athena for the three demand sensor IDs of the NMI you used for the smoke test. Look up the IDs:

```bash
grep "<smoke-test-NMI>" data/demand_points.csv
```

Then run an Athena query (substitute the three sensor IDs from the grep output):

```bash
aws athena start-query-execution \
    --query-string "SELECT sensorid, COUNT(*) as cnt, MIN(ts) as min_ts, MAX(ts) as max_ts FROM sensordata_default WHERE sensorid IN ('p:racv:<kw-id>','p:racv:<kva-id>','p:racv:<pf-id>') GROUP BY sensorid" \
    --query-execution-context '{"Database":"default"}' \
    --result-configuration '{"OutputLocation":"s3://sbm-file-ingester/athena-results/"}' \
    --region ap-southeast-2
```

Expected: 3 rows, each with `cnt` ≈ 48 (1 day × 48 half-hour intervals).

- [ ] **Step 14.10: Document smoke-test outcome**

If all of Step 14.4–14.9 passed, the implementation is live. No further action.

If any step failed, do NOT roll back the Lambda — investigate via CloudWatch logs first. The schedules will not fire until the next 14:30 Sydney; that gives time to debug ad-hoc.

---

## Self-Review

(Run by the plan author before handing off.)

**1. Spec coverage check**

| Spec section | Implemented in task |
|---|---|
| Architecture (EventBridge → Lambda → S3 flow) | Tasks 1–10 |
| Components (`app.py`, `processor.py`, `downloader.py`, `uploader.py`) | Tasks 2, 3, 6, 7, 8, 9 |
| `optima_shared` reuse (auth, config, dynamodb) | Task 8 (imports), no modification |
| Filename `optima_<proj>_demand_profile_NMI#<NMI.upper()>_...` | Task 3 (Step 3.3) + tested Task 3 (Step 3.1, regex) |
| No URL `nmi=` param | Task 3 (Step 3.1, `assert "nmi" not in params`) |
| Per-site country from DynamoDB | Task 7 (Step 7.1, `test_country_propagates_to_url`) and Task 8 (`country=site.get("country","AU")`) |
| "No data found" sentinel uploaded for audit | Task 4 + Task 7 (`test_no_data_sentinel_treated_as_success_and_uploaded`) |
| Body validation: BOM-tolerant `Commodities:` sniff | Task 3 (Step 3.1, `test_accepts_body_without_csv_content_type_when_starts_with_commodities`) + Task 3 (Step 3.3 implementation) |
| HTML error page rejected | Task 5 (`test_html_error_page_returns_none`) |
| 401/403/404/500/timeout/connection error | Task 5 (all 7 tests) |
| ThreadPoolExecutor parallel | Task 8 (Step 8.3) |
| 200 / 207 / 4xx return codes | Task 8 (`test_partial_failure_returns_207`, `test_login_failure_returns_401`, etc.) |
| Date-range default + OPTIMA_DAYS_BACK | Task 6 (3 tests) |
| Inverted date range → 400 | Task 8 (`test_inverted_date_range_returns_400`) |
| Single-NMI mode via event.nmi | Task 8 (`test_single_nmi_mode_processes_only_that_site`) |
| Lambda handler missing-project → 400 | Task 9 (`test_missing_project_returns_400`) |
| Terraform: log group, Lambda, 2 schedulers, alarm, IAM update | Task 10 |
| GitHub Actions: build + deploy steps | Task 11 |
| CI/CD policy whitelist | Task 12 (docs) + Task 13 (actual policy update) |
| Smoke test 5 steps | Task 14.4–14.9 |
| Coverage ≥ 90% | Task 9.6 (`pytest --cov=src`) |

**2. Placeholder scan:** No "TBD" / "TODO" / "implement later" in any code block. All shell commands shown verbatim. All test code shown in full.

**3. Type consistency:**
- `download_demand_csv` signature: `(cookies, site_id_str, start_date, end_date, project, nmi, *, country="AU") -> tuple[bytes, str] | None` — consistent across Task 3 (definition + tests) and Task 7 (called by `process_site`).
- `process_site` signature: `(cookies, nmi, site_id_str, start_date, end_date, project, country="AU") -> dict[str, Any]` — consistent across Task 7 (definition + tests) and Task 8 (called from `process_export`).
- `process_export` signature: `(project, nmi=None, start_date=None, end_date=None) -> dict[str, Any]` — consistent across Task 8 (definition + tests) and Task 9 (called from `lambda_handler`).
- Filename pattern: `optima_<project.lower()>_demand_profile_NMI#<NMI.upper()>_<start>_<end>_<14digit_ts>.csv` — consistent across Task 3 (downloader), Task 7 (process_site asserts on `s3_key.startswith(...)`), Task 14.5 (smoke test grep).
- `_s3_client` global module-level attr — used in Task 1 conftest reload helper + Task 2 implementation.

---

## Execution Handoff

Plan complete and saved to [`docs/superpowers/plans/2026-05-05-optima-demand-exporter.md`](2026-05-05-optima-demand-exporter.md). Two execution options:

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, two-stage review (spec compliance + code quality) between tasks, fast iteration.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

**Which approach?**
