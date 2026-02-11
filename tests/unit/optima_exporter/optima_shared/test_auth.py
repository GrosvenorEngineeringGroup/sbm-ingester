"""Unit tests for optima_shared/auth.py module.

Tests BidEnergy authentication and cookie extraction.
"""

import requests as req_lib
import responses


class TestLoginBidenergy:
    """Tests for login_bidenergy function."""

    @responses.activate
    def test_successful_login_returns_cookie(self) -> None:
        """Test that successful login returns cookie string."""
        from optima_shared.auth import login_bidenergy

        responses.add(
            responses.POST,
            "https://app.bidenergy.com/Account/LogOn",
            status=302,
            headers={"Set-Cookie": ".ASPXAUTH=token123; path=/"},
        )

        result = login_bidenergy("user@test.com", "password", "ClientId")

        assert result is not None
        assert ".ASPXAUTH=token123" in result

    @responses.activate
    def test_failed_login_returns_none(self) -> None:
        """Test that failed login (200 response) returns None."""
        from optima_shared.auth import login_bidenergy

        responses.add(
            responses.POST,
            "https://app.bidenergy.com/Account/LogOn",
            status=200,
            body="Login failed",
        )

        result = login_bidenergy("user@test.com", "wrong_password", "ClientId")
        assert result is None

    @responses.activate
    def test_missing_aspxauth_cookie_returns_none(self) -> None:
        """Test that missing .ASPXAUTH cookie returns None."""
        from optima_shared.auth import login_bidenergy

        responses.add(
            responses.POST,
            "https://app.bidenergy.com/Account/LogOn",
            status=302,
            headers={"Set-Cookie": "other_cookie=value; path=/"},
        )

        result = login_bidenergy("user@test.com", "password", "ClientId")
        assert result is None

    @responses.activate
    def test_network_error_returns_none(self) -> None:
        """Test that network error returns None."""
        from optima_shared.auth import login_bidenergy

        responses.add(
            responses.POST,
            "https://app.bidenergy.com/Account/LogOn",
            body=req_lib.exceptions.ConnectionError("Network error"),
        )

        result = login_bidenergy("user@test.com", "password", "ClientId")
        assert result is None

    @responses.activate
    def test_non_302_response_returns_none(self) -> None:
        """Test that non-302 response returns None."""
        from optima_shared.auth import login_bidenergy

        responses.add(
            responses.POST,
            "https://app.bidenergy.com/Account/LogOn",
            status=500,
            body="Server error",
        )

        result = login_bidenergy("user@test.com", "password", "ClientId")
        assert result is None

    @responses.activate
    def test_multiple_cookies(self) -> None:
        """Test that multiple cookies from login are combined."""
        from optima_shared.auth import login_bidenergy

        responses.add(
            responses.POST,
            "https://app.bidenergy.com/Account/LogOn",
            status=302,
            headers={
                "Set-Cookie": ".ASPXAUTH=token123; path=/, session=abc; path=/, other=xyz; path=/",
            },
        )

        result = login_bidenergy("user@test.com", "password", "ClientId")

        assert result is not None
        assert ".ASPXAUTH=token123" in result
