"""Scoping guard: _prefix_nmi_in_nem12 must remain internal to optima_exporter.

The Optima_-prefix rewrite is a namespace convention specific to the Optima
pipeline. If this symbol ever gets imported elsewhere in src/, downstream files
that should keep their bare NMIs (e.g. AEMO MDFF pushes, building sensors)
could end up with the prefix.
"""

from pathlib import Path


def _repo_root() -> Path:
    """Locate the repo root by walking up until pyproject.toml is found."""
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "pyproject.toml").exists():
            return parent
    raise RuntimeError("Could not locate repo root (no pyproject.toml found upwards)")


REPO_ROOT = _repo_root()
SRC_ROOT = REPO_ROOT / "src"
ALLOWED_DIR = SRC_ROOT / "functions" / "optima_exporter"
SYMBOL = "_prefix_nmi_in_nem12"


def test_prefix_helper_is_only_referenced_inside_optima_exporter() -> None:
    offenders: list[str] = []

    for path in SRC_ROOT.rglob("*.py"):
        try:
            relative = path.resolve().relative_to(ALLOWED_DIR)
        except ValueError:
            relative = None
        if relative is not None:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        if SYMBOL in text:
            offenders.append(str(path.relative_to(REPO_ROOT)))

    assert not offenders, f"{SYMBOL} leaked outside src/functions/optima_exporter/. Found in: {offenders}"


def test_prefix_helper_exists_inside_optima_exporter() -> None:
    """Sanity check: if this fails, the previous test gives a false negative."""
    matches: list[str] = []
    for path in ALLOWED_DIR.rglob("*.py"):
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        if f"def {SYMBOL}" in text:
            matches.append(str(path.relative_to(REPO_ROOT)))
    assert matches, f"{SYMBOL} definition not found anywhere in {ALLOWED_DIR}"
