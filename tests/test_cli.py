"""Tests for the sqloutbox CLI — init command."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from sqloutbox.cli import cmd_init


def test_init_creates_directory(tmp_path):
    """init creates the config directory with all expected files."""
    target = tmp_path / "myoutbox"
    cmd_init(target)

    assert (target / "outbox_config.py").exists()
    assert (target / "run_service.py").exists()
    assert (target / "logging.json").exists()
    assert (target / "data").is_dir()


def test_init_config_importable(tmp_path):
    """The scaffolded outbox_config.py is valid Python."""
    target = tmp_path / "outbox"
    cmd_init(target)

    config_text = (target / "outbox_config.py").read_text()
    # Should be valid Python (no syntax errors)
    compile(config_text, "outbox_config.py", "exec")


def test_init_service_importable(tmp_path):
    """The scaffolded run_service.py is valid Python."""
    target = tmp_path / "outbox"
    cmd_init(target)

    service_text = (target / "run_service.py").read_text()
    compile(service_text, "run_service.py", "exec")


def test_init_logging_valid_json(tmp_path):
    """The scaffolded logging.json is valid JSON."""
    import json

    target = tmp_path / "outbox"
    cmd_init(target)

    with open(target / "logging.json") as f:
        cfg = json.load(f)
    assert cfg["version"] == 1
    assert "console" in cfg["handlers"]


def test_init_refuses_overwrite(tmp_path):
    """init refuses to overwrite an existing outbox_config.py."""
    target = tmp_path / "outbox"
    cmd_init(target)

    with pytest.raises(SystemExit):
        cmd_init(target)


def test_init_config_has_required_exports(tmp_path):
    """Scaffolded config exports 'config' and 'create_writers'."""
    target = tmp_path / "outbox"
    cmd_init(target)

    config_text = (target / "outbox_config.py").read_text()
    assert "config = OutboxConfig(" in config_text
    assert "def create_writers()" in config_text


def test_init_config_has_turso_writer(tmp_path):
    """Scaffolded config includes a working TursoWriter class."""
    target = tmp_path / "outbox"
    cmd_init(target)

    config_text = (target / "outbox_config.py").read_text()
    assert "class TursoWriter:" in config_text
    assert "async def write_batch" in config_text


def test_init_service_has_signal_handling(tmp_path):
    """Scaffolded service handles SIGTERM and SIGINT."""
    target = tmp_path / "outbox"
    cmd_init(target)

    service_text = (target / "run_service.py").read_text()
    assert "SIGTERM" in service_text
    assert "SIGINT" in service_text


def test_init_preserves_existing_logging_json(tmp_path):
    """init does not overwrite an existing logging.json."""
    target = tmp_path / "outbox"
    target.mkdir()
    (target / "data").mkdir()

    # Pre-create a custom logging.json
    custom = '{"version": 1, "custom": true}'
    (target / "logging.json").write_text(custom)

    cmd_init(target)

    assert (target / "logging.json").read_text() == custom
