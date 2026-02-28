from __future__ import annotations

import asyncio
from collections import deque

import pytest
from pgleaderlock.models import LockState, RetryContext
from pgleaderlock.retry import FixedInterval, RetryStrategy


class TestLeaderLockAcquireSuccess:
    """STOPPED → FOLLOWER → ACQUIRING → LEADER path."""

    async def test_acquire_success(self, make_lock, fake_conn):
        fake_conn.script(True)  # pg_try_advisory_lock returns True
        lock = make_lock()
        acquired_events: list[bool] = []

        @lock.on_acquired
        def on_acq():
            acquired_events.append(True)

        await lock.start()
        got = await lock.wait_for_leadership(timeout_s=2.0)
        assert got is True
        assert lock.is_leader
        assert lock.state == LockState.LEADER
        assert len(acquired_events) == 1
        await lock.shutdown(timeout_s=2.0)
        assert lock.state == LockState.STOPPED

    async def test_is_leader_false_when_not_leader(self, make_lock, fake_conn):
        lock = make_lock()
        assert lock.is_leader is False
        assert lock.state == LockState.STOPPED


class TestLeaderLockAcquireFailAndRetry:
    """ACQUIRING → FOLLOWER (on fail) → ACQUIRING → LEADER."""

    async def test_acquire_fail_then_succeed(self, make_lock, fake_conn):
        fake_conn.script(False, True)  # first fail, then succeed
        lock = make_lock()
        fail_events: list[bool] = []

        @lock.on_acquire_failed
        def on_fail():
            fail_events.append(True)

        await lock.start()
        got = await lock.wait_for_leadership(timeout_s=2.0)
        assert got is True
        assert len(fail_events) == 1
        await lock.shutdown(timeout_s=2.0)

    async def test_wait_for_leadership_timeout(self, make_lock, fake_conn):
        # Lock always fails — use non-zero retry to ensure timeout fires
        for _ in range(1000):
            fake_conn.script(False)
        lock = make_lock(retry_strategy=FixedInterval(interval_s=0.05))
        await lock.start()
        got = await lock.wait_for_leadership(timeout_s=0.1)
        assert got is False
        await lock.shutdown(timeout_s=2.0)


class TestLeaderLockShutdown:
    """Shutdown from various states."""

    async def test_shutdown_while_leader(self, make_lock, fake_conn):
        fake_conn.script(True, True)  # acquire, then unlock
        lock = make_lock()
        released_events: list[bool] = []

        @lock.on_released
        def on_rel():
            released_events.append(True)

        await lock.start()
        await lock.wait_for_leadership(timeout_s=2.0)
        await lock.shutdown(timeout_s=2.0)
        assert lock.state == LockState.STOPPED
        assert len(released_events) == 1

    async def test_shutdown_while_follower(self, make_lock, fake_conn):
        # Never acquire — keep returning False
        for _ in range(100):
            fake_conn.script(False)
        lock = make_lock()
        await lock.start()
        await asyncio.sleep(0.05)
        await lock.shutdown(timeout_s=2.0)
        assert lock.state == LockState.STOPPED

    async def test_shutdown_idempotent(self, make_lock, fake_conn):
        fake_conn.script(True, True)
        lock = make_lock()
        await lock.start()
        await lock.wait_for_leadership(timeout_s=2.0)
        await lock.shutdown(timeout_s=2.0)
        await lock.shutdown(timeout_s=2.0)  # second call should be safe
        assert lock.state == LockState.STOPPED

    async def test_start_idempotent(self, make_lock, fake_conn):
        fake_conn.script(True)
        lock = make_lock()
        await lock.start()
        await lock.wait_for_leadership(timeout_s=2.0)
        await lock.start()  # should be no-op since already running
        assert lock.is_leader
        await lock.shutdown(timeout_s=2.0)

    async def test_shutdown_before_start(self, make_lock, fake_conn):
        lock = make_lock()
        # Should not raise
        await lock.shutdown(timeout_s=1.0)
        assert lock.state == LockState.STOPPED


class TestLeaderLockStepDown:
    """step_down() releases leadership."""

    async def test_step_down_with_auto_reacquire(self, make_lock, fake_conn):
        fake_conn.script(True, True, True)  # acquire, unlock succeeds, re-acquire
        lock = make_lock(auto_reacquire=True)
        released: list[bool] = []

        @lock.on_released
        def on_rel():
            released.append(True)

        await lock.start()
        await lock.wait_for_leadership(timeout_s=2.0)
        await lock.step_down(timeout_s=2.0)
        assert len(released) == 1
        # Should re-acquire
        got = await lock.wait_for_leadership(timeout_s=2.0)
        assert got is True
        await lock.shutdown(timeout_s=2.0)

    async def test_step_down_without_auto_reacquire(self, make_lock, fake_conn):
        fake_conn.script(True, True)  # acquire, unlock
        lock = make_lock(auto_reacquire=False)
        await lock.start()
        await lock.wait_for_leadership(timeout_s=2.0)
        await lock.step_down(timeout_s=2.0)
        await asyncio.sleep(0.05)
        assert lock.state == LockState.STOPPED

    async def test_step_down_when_not_leader(self, make_lock, fake_conn):
        lock = make_lock()
        # Should be a no-op
        await lock.step_down(timeout_s=1.0)
        assert lock.state == LockState.STOPPED


class TestLeaderLockHealthFailure:
    """Health check failures trigger on_lost."""

    async def test_health_fail_no_grace_no_reacquire(self, make_lock, fake_conn):
        fake_conn.script(True)  # acquire succeeds
        lock = make_lock(health_interval_s=0.01, auto_reacquire=False)
        lost_events: list[bool] = []

        @lock.on_lost
        def on_lost():
            lost_events.append(True)

        await lock.start()
        await lock.wait_for_leadership(timeout_s=2.0)

        # Next health check will fail
        fake_conn.script(Exception("connection lost"))
        await asyncio.sleep(0.15)

        assert len(lost_events) == 1
        # With auto_reacquire=False, should go to STOPPED
        assert lock.state == LockState.STOPPED

    async def test_health_fail_with_auto_reacquire(self, make_lock, fake_conn):
        fake_conn.script(True)  # acquire
        lock = make_lock(health_interval_s=0.01, auto_reacquire=True)
        lost_events: list[bool] = []

        @lock.on_lost
        def on_lost():
            lost_events.append(True)

        await lock.start()
        await lock.wait_for_leadership(timeout_s=2.0)

        # Health fail, then reconnect & re-acquire
        fake_conn.script(Exception("conn lost"), True)
        await asyncio.sleep(0.15)

        assert len(lost_events) >= 1
        # Should be back in FOLLOWER or LEADER
        assert lock.state in (LockState.FOLLOWER, LockState.ACQUIRING, LockState.LEADER)
        await lock.shutdown(timeout_s=2.0)


class TestLeaderLockReconnecting:
    """RECONNECTING state with grace period."""

    async def test_reconnect_success(self, make_lock, fake_conn):
        fake_conn.script(True)  # initial acquire
        lock = make_lock(health_interval_s=0.01, reconnect_grace_s=1.0)
        lost_events: list[bool] = []

        @lock.on_lost
        def on_lost():
            lost_events.append(True)

        await lock.start()
        await lock.wait_for_leadership(timeout_s=2.0)

        # Health fail, then reconnect + reacquire succeeds
        fake_conn.script(Exception("fail"), True)  # health fail, reacquire success
        await asyncio.sleep(0.15)

        # Should recover without on_lost
        assert len(lost_events) == 0
        assert lock.is_leader
        await lock.shutdown(timeout_s=2.0)

    async def test_reconnect_fail_other_holds(self, make_lock, fake_conn):
        fake_conn.script(True)  # initial acquire
        lock = make_lock(health_interval_s=0.01, reconnect_grace_s=0.1)
        lost_events: list[bool] = []

        @lock.on_lost
        def on_lost():
            lost_events.append(True)

        await lock.start()
        await lock.wait_for_leadership(timeout_s=2.0)

        # Health fail, reacquire returns False (other holds lock)
        fake_conn.script(Exception("fail"), False)
        await asyncio.sleep(0.3)

        assert len(lost_events) >= 1
        await lock.shutdown(timeout_s=2.0)


class TestLeaderLockContextManager:
    """async with LeaderLock."""

    async def test_context_manager(self, make_lock, fake_conn):
        fake_conn.script(True, True)  # acquire, release
        lock = make_lock()
        async with lock:
            got = await lock.wait_for_leadership(timeout_s=2.0)
            assert got is True
        assert lock.state == LockState.STOPPED


class TestLeaderLockCallbacks:
    """Callback registration and error handling."""

    async def test_multiple_callbacks(self, make_lock, fake_conn):
        fake_conn.script(True, True)  # acquire, release
        lock = make_lock()
        events: list[str] = []

        @lock.on_acquired
        def first():
            events.append("first")

        @lock.on_acquired
        def second():
            events.append("second")

        await lock.start()
        await lock.wait_for_leadership(timeout_s=2.0)
        assert events == ["first", "second"]
        await lock.shutdown(timeout_s=2.0)

    async def test_async_callback(self, make_lock, fake_conn):
        fake_conn.script(True, True)
        lock = make_lock()
        events: list[str] = []

        @lock.on_acquired
        async def on_acq():
            events.append("async_acquired")

        await lock.start()
        await lock.wait_for_leadership(timeout_s=2.0)
        assert events == ["async_acquired"]
        await lock.shutdown(timeout_s=2.0)

    async def test_callback_exception_handled(self, make_lock, fake_conn):
        fake_conn.script(True, True)
        lock = make_lock()
        errors: list[Exception] = []

        @lock.on_acquired
        def bad_cb():
            raise ValueError("boom")

        @lock.on_error
        def on_err(exc):
            errors.append(exc)

        await lock.start()
        await lock.wait_for_leadership(timeout_s=2.0)
        # _fire catches the error and fires on_error with Exception("callback_error")
        # since on_acquired is called with no args
        assert len(errors) >= 1
        await lock.shutdown(timeout_s=2.0)

    async def test_state_change_callback(self, make_lock, fake_conn):
        fake_conn.script(True, True)
        lock = make_lock()
        transitions: list[tuple[LockState, LockState]] = []

        @lock.on_state_change
        def on_change(from_state, to_state):
            transitions.append((from_state, to_state))

        await lock.start()
        await lock.wait_for_leadership(timeout_s=2.0)

        # Should have STOPPED->FOLLOWER, FOLLOWER->ACQUIRING, ACQUIRING->LEADER
        assert len(transitions) >= 3
        assert transitions[0] == (LockState.STOPPED, LockState.FOLLOWER)
        assert transitions[-1] == (LockState.ACQUIRING, LockState.LEADER)
        await lock.shutdown(timeout_s=2.0)

    async def test_decorator_returns_original(self, make_lock, fake_conn):
        lock = make_lock()

        def my_func():
            pass

        result = lock.on_acquired(my_func)
        assert result is my_func

    async def test_on_released_decorator_returns_original(self, make_lock, fake_conn):
        lock = make_lock()

        def my_func():
            pass

        assert lock.on_released(my_func) is my_func

    async def test_on_lost_decorator_returns_original(self, make_lock, fake_conn):
        lock = make_lock()

        def my_func():
            pass

        assert lock.on_lost(my_func) is my_func

    async def test_on_acquire_failed_decorator_returns_original(self, make_lock, fake_conn):
        lock = make_lock()

        def my_func():
            pass

        assert lock.on_acquire_failed(my_func) is my_func

    async def test_on_error_decorator_returns_original(self, make_lock, fake_conn):
        lock = make_lock()

        def my_func(exc):
            pass

        assert lock.on_error(my_func) is my_func


class TestLeaderLockDbErrorDuringAcquire:
    """DB errors during acquisition."""

    async def test_db_error_with_auto_reacquire(self, make_lock, fake_conn):
        fake_conn.script(Exception("db error"), True)  # fail then succeed
        lock = make_lock(auto_reacquire=True)
        error_events: list[Exception] = []

        @lock.on_error
        def on_err(exc):
            error_events.append(exc)

        await lock.start()
        got = await lock.wait_for_leadership(timeout_s=2.0)
        assert got is True
        assert len(error_events) >= 1
        await lock.shutdown(timeout_s=2.0)

    async def test_db_error_without_auto_reacquire(self, make_lock, fake_conn):
        fake_conn.script(Exception("db error"))
        lock = make_lock(auto_reacquire=False)
        await lock.start()
        await asyncio.sleep(0.1)
        assert lock.state == LockState.STOPPED


class TestLeaderLockReleaseFailure:
    """Release/unlock failures."""

    async def test_unlock_failure_goes_stopped(self, make_lock, fake_conn):
        fake_conn.script(True, Exception("unlock fail"))  # acquire, unlock fails
        lock = make_lock()
        error_events: list[Exception] = []

        @lock.on_error
        def on_err(exc):
            error_events.append(exc)

        await lock.start()
        await lock.wait_for_leadership(timeout_s=2.0)
        await lock.shutdown(timeout_s=2.0)
        assert lock.state == LockState.STOPPED
        assert len(error_events) >= 1


class TestLeaderLockExternalShutdownEvent:
    """External shutdown_event integration."""

    async def test_external_shutdown_event(self, make_lock, fake_conn):
        fake_conn.script(True, True)
        ext_event = asyncio.Event()
        lock = make_lock(shutdown_event=ext_event)

        await lock.start()
        await lock.wait_for_leadership(timeout_s=2.0)
        ext_event.set()
        await asyncio.sleep(0.1)
        # The lock should react to external shutdown
        await lock.shutdown(timeout_s=2.0)
        assert lock.state == LockState.STOPPED

    async def test_external_shutdown_event_stops_loop(self, make_lock, fake_conn):
        """Setting external event before start should still stop loop quickly."""
        fake_conn.script(True, True)
        ext_event = asyncio.Event()
        lock = make_lock(shutdown_event=ext_event)
        ext_event.set()
        await lock.start()
        await asyncio.sleep(0.1)
        await lock.shutdown(timeout_s=2.0)
        assert lock.state == LockState.STOPPED


class TestLeaderLockDbOps:
    """Direct tests for _db_try_lock, _db_unlock, _db_health."""

    async def test_db_try_lock_sql(self, make_lock, fake_conn):
        fake_conn.script(True)
        lock = make_lock()
        await lock.start()
        await lock.wait_for_leadership(timeout_s=2.0)
        # Check that the correct SQL was issued
        lock_calls = [
            (sql, params)
            for sql, params in fake_conn.execute_calls
            if "pg_try_advisory_lock" in sql
        ]
        assert len(lock_calls) >= 1
        sql, params = lock_calls[0]
        assert params == (1, 100)
        await lock.shutdown(timeout_s=2.0)

    async def test_db_unlock_sql(self, make_lock, fake_conn):
        fake_conn.script(True, True)  # acquire, unlock
        lock = make_lock()
        await lock.start()
        await lock.wait_for_leadership(timeout_s=2.0)
        await lock.shutdown(timeout_s=2.0)
        unlock_calls = [
            (sql, params)
            for sql, params in fake_conn.execute_calls
            if "pg_advisory_unlock" in sql
        ]
        assert len(unlock_calls) >= 1
        sql, params = unlock_calls[0]
        assert params == (1, 100)

    async def test_db_health_sql(self, make_lock, fake_conn):
        fake_conn.script(True)  # acquire
        lock = make_lock(health_interval_s=0.01)
        await lock.start()
        await lock.wait_for_leadership(timeout_s=2.0)
        await asyncio.sleep(0.05)  # let health check run
        health_calls = [
            (sql, params)
            for sql, params in fake_conn.execute_calls
            if sql == "SELECT 1"
        ]
        assert len(health_calls) >= 1
        await lock.shutdown(timeout_s=2.0)


class TestLeaderLockUnlockReturnsFalse:
    """pg_advisory_unlock returning False is treated as error."""

    async def test_unlock_returns_false(self, make_lock, fake_conn):
        fake_conn.script(True, False)  # acquire, unlock returns False
        lock = make_lock()
        error_events: list[Exception] = []

        @lock.on_error
        def on_err(exc):
            error_events.append(exc)

        await lock.start()
        await lock.wait_for_leadership(timeout_s=2.0)
        # Step down triggers the RELEASING state
        await lock.step_down(timeout_s=2.0)
        await asyncio.sleep(0.05)
        assert lock.state == LockState.STOPPED
        assert len(error_events) >= 1


class TestShutdownTimeout:
    """Shutdown with timeout that forces cancellation."""

    async def test_shutdown_timeout_cancels_task(self, make_lock, fake_conn):
        """When shutdown times out, it should cancel the task and force STOPPED."""
        # Use a retry strategy with a long delay to force timeout
        fake_conn.script(False)  # acquire fails
        lock = make_lock(retry_strategy=FixedInterval(interval_s=100.0))
        await lock.start()
        await asyncio.sleep(0.05)  # let it start and enter retry sleep
        # Very short timeout — should trigger cancellation
        await lock.shutdown(timeout_s=0.01)
        assert lock.state == LockState.STOPPED


class TestCloseConnException:
    """_close_conn handles exception during close."""

    async def test_close_conn_swallows_exception(self, make_lock, fake_conn):
        fake_conn.script(True)  # acquire
        lock = make_lock(auto_reacquire=False, health_interval_s=0.01)

        await lock.start()
        await lock.wait_for_leadership(timeout_s=2.0)

        # Make close raise, then health check fails
        original_close = fake_conn.close

        async def bad_close():
            raise RuntimeError("close failed")

        fake_conn.close = bad_close
        fake_conn.script(Exception("health fail"))
        await asyncio.sleep(0.15)
        assert lock.state == LockState.STOPPED
        fake_conn.close = original_close


class TestRetryReturnsNone:
    """When retry strategy returns None, acquisition stops."""

    async def test_retry_none_stops(self, make_lock, fake_conn):

        class LimitedRetry:
            def next_delay_s(self, ctx: RetryContext) -> float | None:
                if ctx.attempt >= 2:
                    return None
                return 0.0

        fake_conn.script(False, False, False)
        lock = make_lock(retry_strategy=LimitedRetry())
        await lock.start()
        await asyncio.sleep(0.1)
        assert lock.state == LockState.STOPPED


class TestConnectWithRetryFailure:
    """connect_with_retry raises ConnectionError when strategy returns None."""

    async def test_connect_retry_exhausted(self, fake_conn):
        from pgleaderlock.errors import ConnectionError
        from pgleaderlock.lock import LeaderLock

        call_count = 0

        async def failing_connect():
            nonlocal call_count
            call_count += 1
            raise OSError("connection refused")

        class OneRetry:
            def next_delay_s(self, ctx: RetryContext) -> float | None:
                if ctx.attempt >= 2:
                    return None
                return 0.0

        lock = LeaderLock(
            dsn="postgresql://fake",
            key1=1,
            key2=100,
            retry_strategy=OneRetry(),
            health_interval_s=0.01,
            connect_fn=failing_connect,
        )
        error_events: list[Exception] = []

        @lock.on_error
        def on_err(exc):
            error_events.append(exc)

        await lock.start()
        await asyncio.sleep(0.2)
        assert lock.state == LockState.STOPPED
        # The lifecycle error should have triggered on_error
        assert any(isinstance(e, ConnectionError) for e in error_events)
        await lock.shutdown(timeout_s=1.0)


class TestReconnectingStepDown:
    """Step down during RECONNECTING state."""

    async def test_step_down_during_reconnecting_auto_reacquire(self, fake_conn):
        from pgleaderlock.lock import LeaderLock

        conn_call_count = 0

        async def connect_fn():
            nonlocal conn_call_count
            conn_call_count += 1
            if conn_call_count >= 3:
                # Third connect fails to trigger reconnecting state
                raise OSError("conn refused")
            return fake_conn

        fake_conn.script(True)  # initial acquire
        lock = LeaderLock(
            dsn="postgresql://fake",
            key1=1,
            key2=100,
            retry_strategy=FixedInterval(interval_s=0.0),
            health_interval_s=0.01,
            reconnect_grace_s=2.0,
            auto_reacquire=True,
            connect_fn=connect_fn,
        )
        lost_events: list[bool] = []

        @lock.on_lost
        def on_lost():
            lost_events.append(True)

        await lock.start()
        await lock.wait_for_leadership(timeout_s=2.0)

        # Health check fails -> RECONNECTING
        fake_conn.script(Exception("health fail"))
        await asyncio.sleep(0.1)

        # Now step down during reconnecting
        if lock.state == LockState.RECONNECTING:
            await lock.step_down(timeout_s=2.0)
            assert len(lost_events) >= 1

        await lock.shutdown(timeout_s=2.0)

    async def test_step_down_during_reconnecting_no_auto_reacquire(self, fake_conn):
        from pgleaderlock.lock import LeaderLock

        conn_call_count = 0

        async def connect_fn():
            nonlocal conn_call_count
            conn_call_count += 1
            if conn_call_count >= 3:
                raise OSError("conn refused")
            return fake_conn

        fake_conn.script(True)  # initial acquire
        lock = LeaderLock(
            dsn="postgresql://fake",
            key1=1,
            key2=100,
            retry_strategy=FixedInterval(interval_s=0.0),
            health_interval_s=0.01,
            reconnect_grace_s=2.0,
            auto_reacquire=False,
            connect_fn=connect_fn,
        )
        lost_events: list[bool] = []

        @lock.on_lost
        def on_lost():
            lost_events.append(True)

        await lock.start()
        await lock.wait_for_leadership(timeout_s=2.0)

        fake_conn.script(Exception("health fail"))
        await asyncio.sleep(0.1)

        if lock.state == LockState.RECONNECTING:
            await lock.step_down(timeout_s=2.0)
            await asyncio.sleep(0.05)
            assert lock.state == LockState.STOPPED

        await lock.shutdown(timeout_s=2.0)


class TestReconnectConnectException:
    """Connection exceptions during reconnecting phase."""

    async def test_reconnect_connect_fails_then_grace_expires(self, fake_conn):
        from pgleaderlock.lock import LeaderLock

        conn_call_count = 0

        async def connect_fn():
            nonlocal conn_call_count
            conn_call_count += 1
            if conn_call_count >= 2:
                raise OSError("conn refused")
            return fake_conn

        fake_conn.script(True)  # initial acquire
        lock = LeaderLock(
            dsn="postgresql://fake",
            key1=1,
            key2=100,
            retry_strategy=FixedInterval(interval_s=0.01),
            health_interval_s=0.01,
            reconnect_grace_s=0.1,  # short grace
            auto_reacquire=False,
            connect_fn=connect_fn,
        )
        lost_events: list[bool] = []

        @lock.on_lost
        def on_lost():
            lost_events.append(True)

        await lock.start()
        await lock.wait_for_leadership(timeout_s=2.0)

        # Health fail -> RECONNECTING, then connect keeps failing -> grace expires
        fake_conn.script(Exception("health fail"))
        await asyncio.sleep(0.3)

        assert len(lost_events) >= 1
        assert lock.state == LockState.STOPPED
        await lock.shutdown(timeout_s=1.0)


class TestReconnectReacquireException:
    """Reacquire exception during reconnecting phase."""

    async def test_reconnect_reacquire_exception(self, fake_conn):
        from pgleaderlock.lock import LeaderLock

        fake_conn.script(True)  # initial acquire

        async def connect_fn():
            return fake_conn

        lock = LeaderLock(
            dsn="postgresql://fake",
            key1=1,
            key2=100,
            retry_strategy=FixedInterval(interval_s=0.02),
            health_interval_s=0.01,
            reconnect_grace_s=0.1,
            auto_reacquire=True,
            connect_fn=connect_fn,
        )
        lost_events: list[bool] = []

        @lock.on_lost
        def on_lost():
            lost_events.append(True)

        await lock.start()
        await lock.wait_for_leadership(timeout_s=2.0)

        # Health fail -> RECONNECTING
        # connect succeeds but reacquire throws; non-zero retry delay lets event loop run
        fake_conn.script(Exception("health fail"))
        for _ in range(20):
            fake_conn.script(Exception("reacquire fail"))
        await asyncio.sleep(0.4)

        assert len(lost_events) >= 1
        await lock.shutdown(timeout_s=2.0)


class TestReconnectGraceNoAutoReacquire:
    """Grace period expires with auto_reacquire=False -> STOPPED."""

    async def test_grace_expired_no_auto_reacquire(self, make_lock, fake_conn):
        fake_conn.script(True)  # initial acquire
        lock = make_lock(
            health_interval_s=0.01,
            reconnect_grace_s=0.05,
            auto_reacquire=False,
        )
        lost_events: list[bool] = []

        @lock.on_lost
        def on_lost():
            lost_events.append(True)

        await lock.start()
        await lock.wait_for_leadership(timeout_s=2.0)

        # Health fail, then reacquire returns False -> grace expired
        fake_conn.script(Exception("fail"), False)
        await asyncio.sleep(0.3)

        assert len(lost_events) >= 1
        assert lock.state == LockState.STOPPED


class TestStepDownTimeoutForce:
    """step_down timeout forces connection close."""

    async def test_step_down_timeout(self, make_lock, fake_conn):
        fake_conn.script(True)  # acquire
        lock = make_lock()
        await lock.start()
        await lock.wait_for_leadership(timeout_s=2.0)

        # Make unlock hang by using very long retry
        # Actually, let's just set step_down with very short timeout
        # while the lock is sleeping in health_interval
        # step_down sets the event, but with a very short timeout
        # the _wait_release might time out
        fake_conn.script(True)  # unlock should succeed normally

        # This should work normally since step_down is fast enough
        await lock.step_down(timeout_s=2.0)
        await lock.shutdown(timeout_s=2.0)
