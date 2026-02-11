"""BidEnergy authentication utilities."""

import requests
from aws_lambda_powertools import Logger

from optima_shared.config import BIDENERGY_BASE_URL

logger = Logger(service="optima-exporter")


def login_bidenergy(username: str, password: str, client_id: str) -> str | None:
    """
    Login to BidEnergy and obtain authentication cookie.

    Args:
        username: BidEnergy username (email)
        password: BidEnergy password
        client_id: Client identifier (e.g., "Visualisation", "BidEnergy")

    Returns:
        Cookie string for subsequent requests, or None if login failed
    """
    login_url = f"{BIDENERGY_BASE_URL}/Account/LogOn"

    params = {
        "ClientId": client_id,
        "UserName": username,
        "Password": password,
    }

    logger.info("Attempting BidEnergy login", extra={"username": username, "client_id": client_id, "url": login_url})

    try:
        # POST with empty body - credentials are in URL params
        response = requests.post(
            login_url,
            params=params,
            headers={"Content-Length": "0"},
            allow_redirects=False,
            timeout=30,
        )

        # Successful login returns 302 redirect with .ASPXAUTH cookie
        if response.status_code == 302:
            cookies = response.cookies
            if ".ASPXAUTH" in cookies:
                cookie_str = "; ".join([f"{c.name}={c.value}" for c in cookies])
                logger.info("BidEnergy login successful", extra={"username": username})
                return cookie_str
            logger.error(
                "BidEnergy login failed: missing .ASPXAUTH cookie",
                extra={
                    "username": username,
                    "cookies_received": list(cookies.keys()),
                    "redirect_location": response.headers.get("Location", "N/A"),
                },
            )
        elif response.status_code == 200:
            # 200 usually means login page returned with error (invalid credentials)
            logger.error(
                "BidEnergy login failed: invalid credentials or account locked",
                extra={
                    "username": username,
                    "status_code": response.status_code,
                    "response_preview": response.text[:500] if response.text else "empty",
                },
            )
        else:
            logger.error(
                "BidEnergy login failed: unexpected response",
                extra={
                    "username": username,
                    "status_code": response.status_code,
                    "response_preview": response.text[:500] if response.text else "empty",
                },
            )

    except requests.Timeout:
        logger.error("BidEnergy login failed: request timeout", extra={"username": username, "timeout_seconds": 30})
    except requests.ConnectionError as e:
        logger.error("BidEnergy login failed: connection error", extra={"username": username, "error": str(e)})
    except requests.RequestException as e:
        logger.error(
            "BidEnergy login failed: request error", exc_info=True, extra={"username": username, "error": str(e)}
        )

    return None
