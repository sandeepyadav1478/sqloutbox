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
