"""Unit tests for Bunnings billing parser."""

from __future__ import annotations

import pytest

from shared.billing_parser import bunnings_usage_and_spend_parser


def test_filename_mismatch_raises(tmp_path) -> None:
    """Parser must reject files that are not Bunnings billing reports."""
    f = tmp_path / "20260414-RACV-Usage and Spend Report.csv"
    f.write_bytes(b"irrelevant content")
    with pytest.raises(Exception, match="Not Bunnings Usage and Spend File"):
        bunnings_usage_and_spend_parser(str(f), "dummy-error-log")
