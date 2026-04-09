"""Tests for the TOML config loader, .env/Doppler loading, and runservice CLI."""

from __future__ import annotations

import json
import os
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from threading import Thread
from unittest.mock import AsyncMock, patch

import pytest

from sqloutbox._runner import (
    DEFAULT_CONFIG_FILE,
    _import_class,
    _interpolate_dict,
    _interpolate_env,
    _load_env_file,
    _mask_token,
    _parse_tables,
    _prepare_env,
    load_config_toml,
)


# ── Env interpolation ───────────────────────────────────────────────────────


class TestInterpolateEnv:
    def test_simple_replacement(self, monkeypatch):
        monkeypatch.setenv("MY_URL", "https://example.com")
        assert _interpolate_env("${MY_URL}") == "https://example.com"

    def test_multiple_vars(self, monkeypatch):
        monkeypatch.setenv("HOST", "db.example.com")
        monkeypatch.setenv("PORT", "5432")
        result = _interpolate_env("${HOST}:${PORT}")
        assert result == "db.example.com:5432"

    def test_no_vars_passthrough(self):
        assert _interpolate_env("plain text") == "plain text"

    def test_missing_var_raises(self, monkeypatch):
        monkeypatch.delenv("NONEXISTENT_VAR_12345", raising=False)
        with pytest.raises(EnvironmentError, match="NONEXISTENT_VAR_12345"):
            _interpolate_env("${NONEXISTENT_VAR_12345}")

    def test_empty_string(self):
        assert _interpolate_env("") == ""

    def test_var_with_surrounding_text(self, monkeypatch):
        monkeypatch.setenv("TOKEN", "abc123")
        assert _interpolate_env("Bearer ${TOKEN}") == "Bearer abc123"


class TestInterpolateDict:
    def test_simple_dict(self, monkeypatch):
        monkeypatch.setenv("DB_URL", "https://db.example.com")
        result = _interpolate_dict({"url": "${DB_URL}", "timeout": 30})
        assert result == {"url": "https://db.example.com", "timeout": 30}

    def test_nested_dict(self, monkeypatch):
        monkeypatch.setenv("SECRET", "s3cr3t")
        result = _interpolate_dict({"auth": {"token": "${SECRET}"}})
        assert result == {"auth": {"token": "s3cr3t"}}

    def test_non_string_values_untouched(self):
        result = _interpolate_dict({"count": 42, "enabled": True, "items": [1, 2]})
        assert result == {"count": 42, "enabled": True, "items": [1, 2]}


# ── Import helper ────────────────────────────────────────────────────────────


class TestImportClass:
    def test_import_stdlib_class(self):
        cls = _import_class("pathlib:Path")
        assert cls is Path

    def test_missing_colon_raises(self):
        with pytest.raises(ValueError, match="module.path:ClassName"):
            _import_class("pathlib.Path")

    def test_missing_module_raises(self):
        with pytest.raises(ImportError, match="nonexistent_module_xyz"):
            _import_class("nonexistent_module_xyz:Foo")

    def test_missing_class_raises(self):
        with pytest.raises(AttributeError, match="NonexistentClass"):
            _import_class("pathlib:NonexistentClass")


# ── .env file loading ────────────────────────────────────────────────────────


class TestLoadEnvFile:
    def test_simple_key_value(self, tmp_path):
        (tmp_path / ".env").write_text("FOO=bar\nBAZ=qux\n")
        result = _load_env_file(tmp_path / ".env")
        assert result == {"FOO": "bar", "BAZ": "qux"}

    def test_quoted_values(self, tmp_path):
        (tmp_path / ".env").write_text(
            'DB_URL="https://example.com"\n'
            "TOKEN='secret123'\n"
        )
        result = _load_env_file(tmp_path / ".env")
        assert result == {"DB_URL": "https://example.com", "TOKEN": "secret123"}

    def test_comments_and_blank_lines(self, tmp_path):
        (tmp_path / ".env").write_text(
            "# This is a comment\n"
            "\n"
            "KEY=value\n"
            "  # indented comment\n"
        )
        result = _load_env_file(tmp_path / ".env")
        assert result == {"KEY": "value"}

    def test_lines_without_equals_skipped(self, tmp_path):
        (tmp_path / ".env").write_text("VALID=yes\nINVALID_LINE\n")
        result = _load_env_file(tmp_path / ".env")
        assert result == {"VALID": "yes"}

    def test_value_with_equals(self, tmp_path):
        (tmp_path / ".env").write_text("URL=https://x.com?a=1&b=2\n")
        result = _load_env_file(tmp_path / ".env")
        assert result == {"URL": "https://x.com?a=1&b=2"}

    def test_empty_file(self, tmp_path):
        (tmp_path / ".env").write_text("")
        result = _load_env_file(tmp_path / ".env")
        assert result == {}

    def test_whitespace_trimmed(self, tmp_path):
        (tmp_path / ".env").write_text("  KEY  =  value  \n")
        result = _load_env_file(tmp_path / ".env")
        assert result == {"KEY": "value"}


class TestPrepareEnv:
    def test_loads_env_file(self, tmp_path, monkeypatch):
        """_prepare_env loads .env vars into os.environ."""
        (tmp_path / ".env").write_text("TEST_PREP_VAR=hello\n")
        config_file = tmp_path / "outbox.toml"
        config_file.write_text("")
        monkeypatch.delenv("TEST_PREP_VAR", raising=False)
        monkeypatch.delenv("DOPPLER_TOKEN", raising=False)

        _prepare_env(config_file)

        assert os.environ.get("TEST_PREP_VAR") == "hello"
        # Cleanup
        monkeypatch.delenv("TEST_PREP_VAR", raising=False)

    def test_existing_env_not_overwritten(self, tmp_path, monkeypatch):
        """.env does NOT override already-set os.environ."""
        monkeypatch.setenv("TEST_PREP_VAR", "original")
        (tmp_path / ".env").write_text("TEST_PREP_VAR=overridden\n")
        config_file = tmp_path / "outbox.toml"
        config_file.write_text("")
        monkeypatch.delenv("DOPPLER_TOKEN", raising=False)

        _prepare_env(config_file)

        assert os.environ["TEST_PREP_VAR"] == "original"

    def test_no_env_file_is_fine(self, tmp_path, monkeypatch):
        """Missing .env file is not an error."""
        config_file = tmp_path / "outbox.toml"
        config_file.write_text("")
        monkeypatch.delenv("DOPPLER_TOKEN", raising=False)

        _prepare_env(config_file)  # should not raise

    def test_doppler_skipped_without_token(self, tmp_path, monkeypatch):
        """No DOPPLER_TOKEN → Doppler fetch is skipped."""
        config_file = tmp_path / "outbox.toml"
        config_file.write_text("")
        monkeypatch.delenv("DOPPLER_TOKEN", raising=False)

        with patch("sqloutbox._runner._load_doppler") as mock:
            _prepare_env(config_file)
            mock.assert_not_called()

    def test_doppler_called_with_token(self, tmp_path, monkeypatch):
        """DOPPLER_TOKEN present → Doppler secrets fetched."""
        (tmp_path / ".env").write_text("DOPPLER_TOKEN=dp.st.test123\n")
        config_file = tmp_path / "outbox.toml"
        config_file.write_text("")
        monkeypatch.delenv("DOPPLER_TOKEN", raising=False)
        monkeypatch.delenv("DOPPLER_SECRET_A", raising=False)

        with patch("sqloutbox._runner._load_doppler") as mock:
            mock.return_value = {"DOPPLER_SECRET_A": "from_doppler"}
            _prepare_env(config_file)
            mock.assert_called_once_with("dp.st.test123")
            assert os.environ.get("DOPPLER_SECRET_A") == "from_doppler"

        monkeypatch.delenv("DOPPLER_SECRET_A", raising=False)
        monkeypatch.delenv("DOPPLER_TOKEN", raising=False)

    def test_doppler_does_not_override_env(self, tmp_path, monkeypatch):
        """Doppler secrets don't override .env or os.environ."""
        monkeypatch.setenv("DOPPLER_TOKEN", "dp.st.test")
        monkeypatch.setenv("EXISTING_VAR", "from_env")

        config_file = tmp_path / "outbox.toml"
        config_file.write_text("")

        with patch("sqloutbox._runner._load_doppler") as mock:
            mock.return_value = {"EXISTING_VAR": "from_doppler"}
            _prepare_env(config_file)
            assert os.environ["EXISTING_VAR"] == "from_env"

    def test_doppler_failure_halts_service(self, tmp_path, monkeypatch):
        """Doppler failure raises RuntimeError — configured creds must work."""
        monkeypatch.setenv("DOPPLER_TOKEN", "dp.st.bad_token_value")
        config_file = tmp_path / "outbox.toml"
        config_file.write_text("")

        with patch("sqloutbox._runner._load_doppler") as mock:
            mock.side_effect = Exception("connection refused")
            with pytest.raises(RuntimeError, match="DOPPLER_TOKEN is configured"):
                _prepare_env(config_file)


class TestMaskToken:
    def test_long_token(self):
        assert _mask_token("dp.st.abcdef123456") == "dp.st.ab***"

    def test_short_token(self):
        assert _mask_token("short") == "***"


# ── Table parsing ────────────────────────────────────────────────────────────


class TestParseTables:
    def test_all_strings_inject_true(self):
        names, inject, retain = _parse_tables(
            ["events", "metrics"], True, 30, "app", "db",
        )
        assert names == ("events", "metrics")
        assert inject is True
        assert retain == ()

    def test_all_strings_inject_false(self):
        names, inject, retain = _parse_tables(
            ["events", "metrics"], False, 30, "app", "db",
        )
        assert names == ("events", "metrics")
        assert inject is False
        assert retain == ()

    def test_mixed_string_and_dict(self):
        names, inject, retain = _parse_tables(
            ["events", {"name": "raw_log", "inject_outbox_seq": False}],
            True, 30, "app", "db",
        )
        assert names == ("events", "raw_log")
        assert isinstance(inject, frozenset)
        assert inject == frozenset({"events"})

    def test_dict_overrides_default(self):
        names, inject, retain = _parse_tables(
            [
                {"name": "a", "inject_outbox_seq": True},
                {"name": "b", "inject_outbox_seq": False},
            ],
            False, 30, "app", "db",
        )
        assert names == ("a", "b")
        assert inject == frozenset({"a"})

    def test_dict_missing_name_raises(self):
        with pytest.raises(ValueError, match="missing 'name'"):
            _parse_tables(
                [{"inject_outbox_seq": True}], True, 30, "app", "db",
            )

    def test_empty_tables_raises(self):
        with pytest.raises(ValueError, match="must not be empty"):
            _parse_tables([], True, 30, "app", "db")

    def test_invalid_type_raises(self):
        with pytest.raises(ValueError, match="must be strings"):
            _parse_tables([42], True, 30, "app", "db")

    def test_all_dicts_inject_true_simplifies_to_bool(self):
        """When all tables get inject, result is True (not frozenset)."""
        names, inject, retain = _parse_tables(
            [
                {"name": "a", "inject_outbox_seq": True},
                {"name": "b", "inject_outbox_seq": True},
            ],
            False, 30, "app", "db",
        )
        assert inject is True

    def test_all_dicts_inject_false_simplifies_to_bool(self):
        """When no tables get inject, result is False (not frozenset)."""
        names, inject, retain = _parse_tables(
            [
                {"name": "a", "inject_outbox_seq": False},
                {"name": "b", "inject_outbox_seq": False},
            ],
            True, 30, "app", "db",
        )
        assert inject is False

    def test_per_table_retain_log_days(self):
        """Per-table retain_log_days override via inline table."""
        names, inject, retain = _parse_tables(
            [
                "events",
                {"name": "api_calls", "retain_log_days": 7},
            ],
            True, 30, "app", "db",
        )
        assert names == ("events", "api_calls")
        assert retain == (("api_calls", 7),)

    def test_no_retain_override_returns_empty(self):
        """No per-table retain overrides → empty tuple."""
        names, inject, retain = _parse_tables(
            ["events", {"name": "raw_log", "inject_outbox_seq": False}],
            True, 30, "app", "db",
        )
        assert retain == ()


# ── TOML config loading ─────────────────────────────────────────────────────


_MINIMAL_TOML = """\
[app.myapp]
db_dir = "data"

[app.myapp.db.primary]
tables = ["events", "metrics"]
writer_class = "pathlib:Path"
"""

_FULL_TOML = """\
[app.pulseview]
db_dir = "data/pulseview"
batch_size = 250
flush_interval = 2.0
table_flush_threshold = 10
table_max_wait = 3.0
auto_schema = false
cleanup_every = 100
retain_log_days = 3

[app.pulseview.db.analytics]
tables = ["events", "metrics"]
inject_outbox_seq = true
writer_class = "pathlib:Path"

[app.autopulse]
db_dir = "data/autopulse"
batch_size = 100

[app.autopulse.db.main]
tables = ["audit_log"]
inject_outbox_seq = false
writer_class = "pathlib:Path"
"""

_TOML_WITH_ENV = """\
[app.myapp]
db_dir = "data"

[app.myapp.db.primary]
tables = ["events"]
writer_class = "pathlib:Path"

[app.myapp.db.primary.connection]
url = "${TEST_OUTBOX_URL}"
"""

_TOML_PER_TABLE_INJECT = """\
[app.myapp]
db_dir = "data"

[app.myapp.db.mixed]
writer_class = "pathlib:Path"
inject_outbox_seq = true
tables = [
    "events",
    "metrics",
    { name = "raw_log", inject_outbox_seq = false },
]
"""

_TOML_RETAIN_LOG_DAYS = """\
[app.myapp]
db_dir = "data"

[app.myapp.db.primary]
writer_class = "pathlib:Path"
retain_log_days = 14
tables = [
    "events",
    { name = "api_calls", retain_log_days = 7 },
    "metrics",
]
"""


class TestLoadConfigToml:
    def test_minimal_config(self, tmp_path, monkeypatch):
        monkeypatch.delenv("DOPPLER_TOKEN", raising=False)
        config_file = tmp_path / "outbox.toml"
        config_file.write_text(_MINIMAL_TOML)

        config, writers = load_config_toml(config_file)

        assert config.db_dir == tmp_path / "data"
        assert len(config.targets) == 1
        assert config.targets[0].name == "myapp.primary"
        assert config.targets[0].tables == ("events", "metrics")
        assert config.targets[0].inject_outbox_seq is True
        assert config.targets[0].db_dir == tmp_path / "data"
        assert "myapp.primary" in writers
        # Default tuning values
        assert config.batch_size == 500
        assert config.flush_interval == 1.0
        assert config.auto_schema is True

    def test_full_config_multi_app(self, tmp_path, monkeypatch):
        monkeypatch.delenv("DOPPLER_TOKEN", raising=False)
        config_file = tmp_path / "outbox.toml"
        config_file.write_text(_FULL_TOML)

        config, writers = load_config_toml(config_file)

        # Tuning from first app (pulseview)
        assert config.batch_size == 250
        assert config.flush_interval == 2.0
        assert config.table_flush_threshold == 10
        assert config.table_max_wait == 3.0
        assert config.auto_schema is False
        assert config.cleanup_every == 100
        assert config.retain_log_days == 3

        # Two targets from two apps
        assert len(config.targets) == 2

        pv = config.targets[0]
        assert pv.name == "pulseview.analytics"
        assert pv.tables == ("events", "metrics")
        assert pv.inject_outbox_seq is True
        assert pv.db_dir == tmp_path / "data" / "pulseview"
        assert pv.batch_size == 250  # from pulseview app

        ap = config.targets[1]
        assert ap.name == "autopulse.main"
        assert ap.tables == ("audit_log",)
        assert ap.inject_outbox_seq is False
        assert ap.db_dir == tmp_path / "data" / "autopulse"
        assert ap.batch_size == 100  # from autopulse app

        assert "pulseview.analytics" in writers
        assert "autopulse.main" in writers

    def test_per_app_batch_size(self, tmp_path, monkeypatch):
        """Each app's batch_size propagates to its targets."""
        monkeypatch.delenv("DOPPLER_TOKEN", raising=False)
        config_file = tmp_path / "outbox.toml"
        config_file.write_text(_FULL_TOML)

        config, _ = load_config_toml(config_file)

        assert config.targets[0].batch_size == 250  # pulseview
        assert config.targets[1].batch_size == 100  # autopulse

    def test_per_table_inject_outbox_seq(self, tmp_path, monkeypatch):
        """Per-table override of inject_outbox_seq via inline dict."""
        monkeypatch.delenv("DOPPLER_TOKEN", raising=False)
        config_file = tmp_path / "outbox.toml"
        config_file.write_text(_TOML_PER_TABLE_INJECT)

        config, _ = load_config_toml(config_file)

        target = config.targets[0]
        assert target.tables == ("events", "metrics", "raw_log")
        assert isinstance(target.inject_outbox_seq, frozenset)
        assert target.inject_outbox_seq == frozenset({"events", "metrics"})
        assert target.should_inject_seq("events") is True
        assert target.should_inject_seq("metrics") is True
        assert target.should_inject_seq("raw_log") is False

    def test_retain_log_days_db_level_and_per_table(self, tmp_path, monkeypatch):
        """retain_log_days at db-level + per-table override via inline dict."""
        monkeypatch.delenv("DOPPLER_TOKEN", raising=False)
        config_file = tmp_path / "outbox.toml"
        config_file.write_text(_TOML_RETAIN_LOG_DAYS)

        config, _ = load_config_toml(config_file)

        target = config.targets[0]
        assert target.retain_log_days == 14           # db-level default
        assert target.get_retain_days("events") == 14  # uses db default
        assert target.get_retain_days("api_calls") == 7  # per-table override
        assert target.get_retain_days("metrics") == 14  # uses db default
        assert target.table_retain_overrides == (("api_calls", 7),)

    def test_app_isolation_separate_db_dirs(self, tmp_path, monkeypatch):
        monkeypatch.delenv("DOPPLER_TOKEN", raising=False)
        config_file = tmp_path / "outbox.toml"
        config_file.write_text(_FULL_TOML)

        config, _ = load_config_toml(config_file)

        assert config.targets[0].db_dir != config.targets[1].db_dir
        assert config.targets[0].db_dir == tmp_path / "data" / "pulseview"
        assert config.targets[1].db_dir == tmp_path / "data" / "autopulse"

    def test_relative_db_dir_resolved_against_config_parent(
        self, tmp_path, monkeypatch,
    ):
        monkeypatch.delenv("DOPPLER_TOKEN", raising=False)
        subdir = tmp_path / "config"
        subdir.mkdir()
        config_file = subdir / "outbox.toml"
        config_file.write_text(_MINIMAL_TOML)

        config, _ = load_config_toml(config_file)
        assert config.targets[0].db_dir == subdir / "data"

    def test_absolute_db_dir_used_as_is(self, tmp_path, monkeypatch):
        monkeypatch.delenv("DOPPLER_TOKEN", raising=False)
        abs_dir = tmp_path / "absolute" / "outbox"
        toml_text = (
            f'[app.myapp]\ndb_dir = "{abs_dir}"\n\n'
            '[app.myapp.db.primary]\n'
            'tables = ["t"]\nwriter_class = "pathlib:Path"\n'
        )
        config_file = tmp_path / "outbox.toml"
        config_file.write_text(toml_text)

        config, _ = load_config_toml(config_file)
        assert config.targets[0].db_dir == abs_dir

    def test_env_var_interpolation_in_connection(
        self, tmp_path, monkeypatch,
    ):
        monkeypatch.setenv("TEST_OUTBOX_URL", "https://db.test.com")
        monkeypatch.delenv("DOPPLER_TOKEN", raising=False)
        config_file = tmp_path / "outbox.toml"
        config_file.write_text(_TOML_WITH_ENV)

        try:
            load_config_toml(config_file)
        except TypeError:
            pass  # Path(**{"url": ...}) raises — env var was interpolated

    def test_env_loaded_from_dotenv_before_interpolation(
        self, tmp_path, monkeypatch,
    ):
        """${VAR} in TOML resolved via .env file."""
        monkeypatch.delenv("TEST_OUTBOX_URL", raising=False)
        monkeypatch.delenv("DOPPLER_TOKEN", raising=False)
        (tmp_path / ".env").write_text("TEST_OUTBOX_URL=https://from-dotenv.com\n")

        config_file = tmp_path / "outbox.toml"
        config_file.write_text(_TOML_WITH_ENV)

        try:
            load_config_toml(config_file)
        except TypeError:
            pass  # Path(**{...}) raises — the important thing is no EnvironmentError

        # Cleanup
        monkeypatch.delenv("TEST_OUTBOX_URL", raising=False)

    def test_missing_env_var_raises(self, tmp_path, monkeypatch):
        monkeypatch.delenv("TEST_OUTBOX_URL", raising=False)
        monkeypatch.delenv("DOPPLER_TOKEN", raising=False)
        config_file = tmp_path / "outbox.toml"
        config_file.write_text(_TOML_WITH_ENV)

        with pytest.raises(EnvironmentError, match="TEST_OUTBOX_URL"):
            load_config_toml(config_file)

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_config_toml(tmp_path / "nonexistent.toml")

    def test_no_app_section_raises(self, tmp_path, monkeypatch):
        monkeypatch.delenv("DOPPLER_TOKEN", raising=False)
        config_file = tmp_path / "outbox.toml"
        config_file.write_text("# empty config\n")

        with pytest.raises(ValueError, match=r"\[app\.\*\]"):
            load_config_toml(config_file)

    def test_missing_db_dir_raises(self, tmp_path, monkeypatch):
        monkeypatch.delenv("DOPPLER_TOKEN", raising=False)
        config_file = tmp_path / "outbox.toml"
        config_file.write_text(
            '[app.myapp]\n\n'
            '[app.myapp.db.primary]\n'
            'tables = ["t"]\nwriter_class = "pathlib:Path"\n'
        )
        with pytest.raises(ValueError, match="db_dir"):
            load_config_toml(config_file)

    def test_no_db_section_raises(self, tmp_path, monkeypatch):
        monkeypatch.delenv("DOPPLER_TOKEN", raising=False)
        config_file = tmp_path / "outbox.toml"
        config_file.write_text('[app.myapp]\ndb_dir = "data"\n')

        with pytest.raises(ValueError, match=r"\[app\.myapp\.db\.\*\]"):
            load_config_toml(config_file)

    def test_missing_tables_raises(self, tmp_path, monkeypatch):
        monkeypatch.delenv("DOPPLER_TOKEN", raising=False)
        config_file = tmp_path / "outbox.toml"
        config_file.write_text(
            '[app.myapp]\ndb_dir = "data"\n\n'
            '[app.myapp.db.primary]\n'
            'writer_class = "pathlib:Path"\n'
        )
        with pytest.raises(ValueError, match="tables"):
            load_config_toml(config_file)

    def test_missing_writer_class_raises(self, tmp_path, monkeypatch):
        monkeypatch.delenv("DOPPLER_TOKEN", raising=False)
        config_file = tmp_path / "outbox.toml"
        config_file.write_text(
            '[app.myapp]\ndb_dir = "data"\n\n'
            '[app.myapp.db.primary]\n'
            'tables = ["t"]\n'
        )
        with pytest.raises(ValueError, match="writer_class"):
            load_config_toml(config_file)

    def test_target_name_is_app_dot_db(self, tmp_path, monkeypatch):
        monkeypatch.delenv("DOPPLER_TOKEN", raising=False)
        config_file = tmp_path / "outbox.toml"
        config_file.write_text(_FULL_TOML)

        config, writers = load_config_toml(config_file)

        names = [t.name for t in config.targets]
        assert "pulseview.analytics" in names
        assert "autopulse.main" in names
        assert set(writers.keys()) == {"pulseview.analytics", "autopulse.main"}

    def test_tuning_defaults_when_omitted(self, tmp_path, monkeypatch):
        """App with no tuning params uses built-in defaults."""
        monkeypatch.delenv("DOPPLER_TOKEN", raising=False)
        config_file = tmp_path / "outbox.toml"
        config_file.write_text(_MINIMAL_TOML)

        config, _ = load_config_toml(config_file)

        assert config.batch_size == 500
        assert config.flush_interval == 1.0
        assert config.table_flush_threshold == 15
        assert config.table_max_wait == 6.0
        assert config.auto_schema is True
        assert config.cleanup_every == 500
        assert config.retain_log_days == 30


# ── CLI runservice command ───────────────────────────────────────────────────


class TestRunserviceCLI:
    def test_no_config_file_exits(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        from sqloutbox.cli import cmd_runservice

        with pytest.raises(SystemExit) as exc_info:
            cmd_runservice(None)
        assert exc_info.value.code == 1

    def test_explicit_missing_file_exits(self, tmp_path):
        from sqloutbox.cli import cmd_runservice

        with pytest.raises(SystemExit) as exc_info:
            cmd_runservice(tmp_path / "nope.toml")
        assert exc_info.value.code == 1

    def test_cli_parses_runservice(self):
        from sqloutbox.cli import main

        with pytest.raises(SystemExit):
            main(["runservice", "--config", "/nonexistent/path.toml"])
