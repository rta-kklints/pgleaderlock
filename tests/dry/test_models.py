import pytest

from pgleaderlock.models import LockState, RetryContext


class TestLockState:
    def test_all_states_exist(self):
        states = {s.value for s in LockState}
        assert states == {"stopped", "follower", "acquiring", "leader", "reconnecting", "releasing"}

    def test_values(self):
        assert LockState.STOPPED.value == "stopped"
        assert LockState.FOLLOWER.value == "follower"
        assert LockState.ACQUIRING.value == "acquiring"
        assert LockState.LEADER.value == "leader"
        assert LockState.RECONNECTING.value == "reconnecting"
        assert LockState.RELEASING.value == "releasing"


class TestRetryContext:
    def test_creation(self):
        ctx = RetryContext(attempt=1, elapsed_s=0.5, last_error=ValueError("x"))
        assert ctx.attempt == 1
        assert ctx.elapsed_s == 0.5
        assert isinstance(ctx.last_error, ValueError)

    def test_frozen(self):
        ctx = RetryContext(attempt=1, elapsed_s=0.0)
        with pytest.raises(AttributeError):
            ctx.attempt = 2  # type: ignore

    def test_default_last_error(self):
        ctx = RetryContext(attempt=1, elapsed_s=0.0)
        assert ctx.last_error is None
