"""Tests for shared.nem_adapter._is_nem_envelope_only."""

from __future__ import annotations

from typing import TYPE_CHECKING

from shared.nem_adapter import _is_nem_envelope_only

if TYPE_CHECKING:
    from pathlib import Path


def _write(tmp_path: Path, name: str, content: bytes) -> str:
    path = tmp_path / name
    path.write_bytes(content)
    return str(path)


class TestIsNemEnvelopeOnly:
    def test_nem12_header(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "a.csv", b"100,NEM12,202605060200,MDP1,Origin\n900\n")
        assert _is_nem_envelope_only(path) is True

    def test_nem13_header(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "a.csv", b"100,NEM13,202605060200,MDP1,Origin\n900\n")
        assert _is_nem_envelope_only(path) is True

    def test_nem12_with_utf8_bom(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "a.csv", b"\xef\xbb\xbf100,NEM12,202605060200,MDP1,Origin\n900\n")
        assert _is_nem_envelope_only(path) is True

    def test_non_nem_csv(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "a.csv", b"Date,Value,Quality\n2026-05-06,1.0,A\n")
        assert _is_nem_envelope_only(path) is False

    def test_missing_file_returns_false(self, tmp_path: Path) -> None:
        assert _is_nem_envelope_only(str(tmp_path / "does-not-exist.csv")) is False

    def test_binary_garbage_returns_false(self, tmp_path: Path) -> None:
        path = _write(tmp_path, "a.bin", b"\x00\x01\x02\x03\x04\xff\xfe\xfd")
        # Either decodes (rare) or raises UnicodeDecodeError; helper must
        # return False without propagating.
        assert _is_nem_envelope_only(path) is False
