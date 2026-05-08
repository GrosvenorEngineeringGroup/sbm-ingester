"""Tests for SourceFile dataclass — used as Powertools idempotency key payload."""

from __future__ import annotations

import json
from dataclasses import FrozenInstanceError, asdict

import pytest

from shared.source_file import SourceFile


class TestSourceFile:
    def test_constructs_with_bucket_and_key(self) -> None:
        src = SourceFile(bucket="sbm-file-ingester", key="newTBP/foo.csv")
        assert src.bucket == "sbm-file-ingester"
        assert src.key == "newTBP/foo.csv"

    def test_is_frozen(self) -> None:
        src = SourceFile(bucket="b", key="k")
        with pytest.raises(FrozenInstanceError):
            src.bucket = "other"  # type: ignore[misc]

    def test_is_hashable(self) -> None:
        src = SourceFile(bucket="b", key="k")
        # frozen + slots dataclass is hashable by default
        assert hash(src) == hash(SourceFile(bucket="b", key="k"))

    def test_uses_slots_no_dict(self) -> None:
        src = SourceFile(bucket="b", key="k")
        # slots=True removes __dict__
        assert not hasattr(src, "__dict__")

    def test_powertools_prepare_data_compatibility(self) -> None:
        """SourceFile must be JSON-serialisable via dataclasses.asdict.

        This is the path Powertools' _prepare_data takes when an instance has
        __dataclass_fields__ — see aws_lambda_powertools/utilities/idempotency/base.py.
        """
        src = SourceFile(bucket="a", key="b")
        as_dict = asdict(src)
        assert as_dict == {"bucket": "a", "key": "b"}
        # Must round-trip through json.dumps — that is what Powertools hashes.
        assert json.dumps(as_dict) == '{"bucket": "a", "key": "b"}'
