"""WS-3: typed exception hierarchy."""
from __future__ import annotations

import pytest

from sqloutbox.exceptions import (
    ChainIntegrityError,
    ConfigError,
    QueueFullError,
    SqloutboxError,
    UnsupportedStatementError,
)


def test_all_subclass_base():
    """Every typed error is a SqloutboxError (callers can catch one base)."""
    for cls in (ConfigError, QueueFullError, UnsupportedStatementError, ChainIntegrityError):
        assert issubclass(cls, SqloutboxError)
    assert issubclass(SqloutboxError, Exception)


def test_config_error_carries_field_value_reason():
    """ConfigError exposes field, value, reason and a readable message."""
    err = ConfigError(field="batch_size", value=0, reason="must be >= 1")
    assert err.field == "batch_size"
    assert err.value == 0
    assert err.reason == "must be >= 1"
    msg = str(err)
    assert "batch_size" in msg
    assert "must be >= 1" in msg
    assert "0" in msg


def test_queue_full_error_carries_namespace_and_cap():
    """QueueFullError exposes namespace + max_pending and a readable message."""
    err = QueueFullError(namespace="events", max_pending=1000)
    assert err.namespace == "events"
    assert err.max_pending == 1000
    msg = str(err)
    assert "events" in msg
    assert "1000" in msg


def test_unsupported_statement_error_is_message_only():
    """UnsupportedStatementError carries a plain message."""
    err = UnsupportedStatementError("INSERT ... SELECT is not supported")
    assert "SELECT" in str(err)
    assert isinstance(err, SqloutboxError)


def test_chain_integrity_error_is_message_only():
    """ChainIntegrityError carries a plain message (raised by WS-5)."""
    err = ChainIntegrityError("duplicate prev_seq=5")
    assert "prev_seq=5" in str(err)
    assert isinstance(err, SqloutboxError)


def test_exported_from_package_root():
    """The five exceptions are importable from the top-level package."""
    import sqloutbox

    assert sqloutbox.SqloutboxError is SqloutboxError
    assert sqloutbox.ConfigError is ConfigError
    assert sqloutbox.QueueFullError is QueueFullError
    assert sqloutbox.UnsupportedStatementError is UnsupportedStatementError
    assert sqloutbox.ChainIntegrityError is ChainIntegrityError


def test_config_error_raisable_and_catchable_as_base():
    """A ConfigError can be caught as SqloutboxError."""
    with pytest.raises(SqloutboxError):
        raise ConfigError(field="x", value=-1, reason="bad")
