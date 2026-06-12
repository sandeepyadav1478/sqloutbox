"""WS-7: OSS packaging assertions — py.typed, types export, tomli marker."""
from __future__ import annotations

from pathlib import Path

import pytest

import sqloutbox


_PKG_DIR = Path(sqloutbox.__file__).parent


def _find_repo_root() -> Path | None:
    """Walk up from this test file to the dir that holds pyproject.toml."""
    for parent in Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").exists():
            return parent
    return None


_REPO_ROOT = _find_repo_root()


def test_py_typed_marker_present():
    """PEP 561 marker ships inside the package so downstream gets types."""
    assert (_PKG_DIR / "py.typed").exists()


def test_signal_types_are_importable_and_typed():
    """The WS-6 signal types are exported with annotations (typed API)."""
    from sqloutbox import NamespaceHealth, health_all

    anns = NamespaceHealth.__annotations__
    for field in ("namespace", "depth", "head_attempts", "is_stuck",
                  "last_error", "last_error_class", "last_attempt_at",
                  "capacity_pct"):
        assert field in anns, f"missing annotation: {field}"

    assert "return" in health_all.__annotations__


def test_pyproject_tomli_conditional_core_dep():
    """tomli is a conditional CORE dependency for Python < 3.11 (F021)."""
    if _REPO_ROOT is None:
        pytest.skip("repo root not found (running from a built dist, not a checkout)")
    text = (_REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert 'tomli>=2.0; python_version < "3.11"' in text \
        or "tomli>=2.0; python_version < '3.11'" in text
    deps_marker = "dependencies = ["
    opt_marker = "[project.optional-dependencies]"
    assert deps_marker in text
    assert opt_marker in text
    core_deps_pos = text.index(deps_marker)
    opt_pos = text.index(opt_marker)
    tomli_pos = text.index("tomli>=2.0")
    assert core_deps_pos < tomli_pos < opt_pos, \
        "tomli must appear in [project] dependencies (core), before extras"


def test_version_is_0_5_0():
    """0.5.0 cut — the release that ships WS-0..WS-7."""
    if _REPO_ROOT is None:
        pytest.skip("repo root not found (running from a built dist, not a checkout)")
    text = (_REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert 'version = "0.5.0"' in text


def test_changelog_exists_and_documents_0_5_0():
    """CHANGELOG.md ships and has a 0.5.0 entry naming the headline changes."""
    if _REPO_ROOT is None:
        pytest.skip("repo root not found (running from a built dist, not a checkout)")
    cl = _REPO_ROOT / "CHANGELOG.md"
    assert cl.exists()
    text = cl.read_text(encoding="utf-8")
    assert "0.5.0" in text
    assert "at-least-once" in text.lower()
    assert "dead-letter" in text.lower() or "dead letter" in text.lower()
    assert "health" in text.lower()


def test_contributing_documents_quality_gates():
    """CONTRIBUTING.md documents the ruff / mypy / pytest gates."""
    if _REPO_ROOT is None:
        pytest.skip("repo root not found (running from a built dist, not a checkout)")
    cg = _REPO_ROOT / "CONTRIBUTING.md"
    assert cg.exists()
    text = cg.read_text(encoding="utf-8").lower()
    assert "ruff" in text
    assert "mypy" in text
    assert "pytest" in text
