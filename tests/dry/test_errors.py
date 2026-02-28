import pytest

from pgleaderlock.errors import ConnectionError, LockError, PgLeaderLockError, ShutdownError


class TestErrorHierarchy:
    def test_connection_error_is_base(self):
        assert issubclass(ConnectionError, PgLeaderLockError)

    def test_lock_error_is_base(self):
        assert issubclass(LockError, PgLeaderLockError)

    def test_shutdown_error_is_base(self):
        assert issubclass(ShutdownError, PgLeaderLockError)

    def test_base_is_exception(self):
        assert issubclass(PgLeaderLockError, Exception)

    def test_can_catch_as_base(self):
        with pytest.raises(PgLeaderLockError):
            raise ConnectionError("test")

    def test_can_catch_lock_error_as_base(self):
        with pytest.raises(PgLeaderLockError):
            raise LockError("test")

    def test_can_catch_shutdown_error_as_base(self):
        with pytest.raises(PgLeaderLockError):
            raise ShutdownError("test")

    def test_message_preserved(self):
        err = LockError("lock failed")
        assert str(err) == "lock failed"

    def test_connection_error_message(self):
        err = ConnectionError("conn failed")
        assert str(err) == "conn failed"

    def test_shutdown_error_message(self):
        err = ShutdownError("shutdown timeout")
        assert str(err) == "shutdown timeout"
