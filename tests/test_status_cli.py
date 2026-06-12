"""WS-6: `sqloutbox status` CLI — per-namespace depth / stuck, read-only."""
from __future__ import annotations

from pathlib import Path

import pytest

from sqloutbox._outbox import Outbox
from sqloutbox.cli import cmd_status, main


def test_status_db_dir_round_trip(tmp_path: Path, capsys):
    """status --db-dir prints one line per namespace with its depth."""
    Outbox(db_path=tmp_path / "alpha.db", namespace="alpha").enqueue(
        "INSERT INTO alpha (a) VALUES (?)", b"[1]"
    )
    ob = Outbox(db_path=tmp_path / "beta.db", namespace="beta")
    ob.enqueue("INSERT INTO beta (a) VALUES (?)", b"[1]")
    ob.enqueue("INSERT INTO beta (a) VALUES (?)", b"[2]")

    with pytest.raises(SystemExit) as ei:
        cmd_status(config_path=None, db_dir_path=tmp_path)
    assert ei.value.code == 0

    out = capsys.readouterr().out
    assert "alpha" in out
    assert "beta" in out
    assert "depth=1" in out
    assert "depth=2" in out


def test_status_empty_dir(tmp_path: Path, capsys):
    """status on a dir with no .db files prints a friendly message, exit 0."""
    with pytest.raises(SystemExit) as ei:
        cmd_status(config_path=None, db_dir_path=tmp_path)
    assert ei.value.code == 0
    out = capsys.readouterr().out
    assert "no" in out.lower()


def test_status_requires_a_source(capsys):
    """status with neither --config nor --db-dir errors out, exit 1."""
    with pytest.raises(SystemExit) as ei:
        cmd_status(config_path=None, db_dir_path=None)
    assert ei.value.code == 1
    err = capsys.readouterr().err
    assert "--config" in err and "--db-dir" in err


def test_status_marks_stuck_namespace(tmp_path: Path, capsys):
    """A namespace whose head has attempts>0 is flagged STUCK in the output."""
    ob = Outbox(db_path=tmp_path / "evt.db", namespace="evt")
    head = ob.enqueue("INSERT INTO evt (a) VALUES (?)", b"[1]")
    conn = ob._write_conn
    cols = {r[1] for r in conn.execute("PRAGMA table_info(outbox_queue)")}
    if "attempts" not in cols:
        pytest.skip("retry columns not present (Plan 2 not applied)")
    conn.execute(
        "UPDATE outbox_queue SET attempts=5, last_error_class='TRANSIENT' "
        "WHERE seq=?",
        [head],
    )
    conn.commit()

    with pytest.raises(SystemExit) as ei:
        cmd_status(config_path=None, db_dir_path=tmp_path)
    assert ei.value.code == 0
    out = capsys.readouterr().out
    assert "STUCK" in out
    assert "attempts=5" in out


def test_status_main_dispatch(tmp_path: Path, capsys):
    """`main(['status', '--db-dir', ...])` routes to cmd_status."""
    Outbox(db_path=tmp_path / "evt.db", namespace="evt").enqueue(
        "INSERT INTO evt (a) VALUES (?)", b"[1]"
    )
    with pytest.raises(SystemExit) as ei:
        main(["status", "--db-dir", str(tmp_path)])
    assert ei.value.code == 0
    assert "evt" in capsys.readouterr().out


def _write_status_toml(tmp_path: Path, db_dir: Path) -> Path:
    """Write a minimal TOML config that points at db_dir."""
    import os
    toml = tmp_path / "outbox.toml"
    toml.write_text(
        f'[app.myapp]\n'
        f'db_dir = "{db_dir.as_posix()}"\n\n'
        f'[app.myapp.db.primary]\n'
        f'writer_class = "pathlib:Path"\n'
        f'inject_outbox_seq = false\n'
        f'tables = ["evt"]\n'
    )
    return toml


def test_status_config_path_shows_namespaces(tmp_path: Path, capsys, monkeypatch):
    """cmd_status --config reads db_dir(s) from the TOML and prints each namespace.

    This exercises the config_path branch (lines 668-677 in cli.py) which calls
    load_config_toml and derives db_dirs from targets — a path that has zero
    coverage in the existing test suite.
    """
    monkeypatch.delenv("DOPPLER_TOKEN", raising=False)
    db_dir = tmp_path / "data"
    db_dir.mkdir()
    Outbox(db_path=db_dir / "evt.db", namespace="evt").enqueue(
        "INSERT INTO evt (a) VALUES (?)", b"[1]"
    )
    toml = _write_status_toml(tmp_path, db_dir)

    with pytest.raises(SystemExit) as ei:
        cmd_status(config_path=toml, db_dir_path=None)
    assert ei.value.code == 0
    out = capsys.readouterr().out
    assert "evt" in out
    assert "depth=1" in out


def test_status_config_path_multi_app_deduplicates_db_dirs(tmp_path: Path, capsys, monkeypatch):
    """When two TOML targets share the same db_dir, health_all is called once for it.

    The seen-set deduplication in cmd_status (config_path branch) ensures the same
    directory is not scanned twice, which would double-count namespaces.
    """
    monkeypatch.delenv("DOPPLER_TOKEN", raising=False)
    db_dir = tmp_path / "data"
    db_dir.mkdir()
    Outbox(db_path=db_dir / "evt.db", namespace="evt").enqueue(
        "INSERT INTO evt (a) VALUES (?)", b"[1]"
    )
    # Two targets that share the same db_dir.
    toml = tmp_path / "outbox.toml"
    toml.write_text(
        f'[app.myapp]\n'
        f'db_dir = "{db_dir.as_posix()}"\n\n'
        f'[app.myapp.db.primary]\n'
        f'writer_class = "pathlib:Path"\n'
        f'inject_outbox_seq = false\n'
        f'tables = ["evt"]\n\n'
        f'[app.myapp.db.secondary]\n'
        f'writer_class = "pathlib:Path"\n'
        f'inject_outbox_seq = false\n'
        f'tables = ["evt"]\n'
    )

    with pytest.raises(SystemExit) as ei:
        cmd_status(config_path=toml, db_dir_path=None)
    assert ei.value.code == 0
    out = capsys.readouterr().out
    # "evt" must appear exactly once — not doubled.
    assert out.count("evt") == 1


def test_status_main_dispatch_config(tmp_path: Path, capsys, monkeypatch):
    """`main(['status', '--config', ...])` routes to cmd_status via the config branch."""
    monkeypatch.delenv("DOPPLER_TOKEN", raising=False)
    db_dir = tmp_path / "data"
    db_dir.mkdir()
    Outbox(db_path=db_dir / "evt.db", namespace="evt").enqueue(
        "INSERT INTO evt (a) VALUES (?)", b"[1]"
    )
    toml = _write_status_toml(tmp_path, db_dir)

    with pytest.raises(SystemExit) as ei:
        main(["status", "--config", str(toml)])
    assert ei.value.code == 0
    assert "evt" in capsys.readouterr().out
