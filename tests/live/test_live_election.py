"""Live integration tests requiring a real PostgreSQL instance.

Run with: pytest tests/live/ --pg-dsn postgresql://user:pass@host/db
Or set PGLEADERLOCK_TEST_DSN environment variable.
"""

from __future__ import annotations

import asyncio

import psycopg
import pytest

from pgleaderlock.lock import LeaderLock
from pgleaderlock.models import LockState
from pgleaderlock.retry import FixedInterval

pytestmark = pytest.mark.live


class TestLiveElection:
    """Basic election with real PostgreSQL."""

    async def test_single_lock_acquires(self, pg_dsn, lock_keys):
        k1, k2 = lock_keys
        lock = LeaderLock(
            dsn=pg_dsn,
            key1=k1,
            key2=k2,
            retry_strategy=FixedInterval(interval_s=0.1),
            health_interval_s=0.5,
        )
        async with lock:
            got = await lock.wait_for_leadership(timeout_s=5.0)
            assert got is True
            assert lock.is_leader
            assert lock.state == LockState.LEADER
        assert lock.state == LockState.STOPPED

    async def test_mutual_exclusion(self, pg_dsn, lock_keys):
        """Only one of two locks can be leader at once."""
        k1, k2 = lock_keys
        lock1 = LeaderLock(
            dsn=pg_dsn,
            key1=k1,
            key2=k2,
            retry_strategy=FixedInterval(interval_s=0.1),
            health_interval_s=0.5,
        )
        lock2 = LeaderLock(
            dsn=pg_dsn,
            key1=k1,
            key2=k2,
            retry_strategy=FixedInterval(interval_s=0.1),
            health_interval_s=0.5,
        )

        await lock1.start()
        await lock2.start()

        # Wait for one to acquire
        await lock1.wait_for_leadership(timeout_s=5.0)
        await asyncio.sleep(0.5)

        # Exactly one should be leader
        assert lock1.is_leader != lock2.is_leader, (
            f"lock1.is_leader={lock1.is_leader}, lock2.is_leader={lock2.is_leader}"
        )

        await lock1.shutdown(timeout_s=5.0)
        await lock2.shutdown(timeout_s=5.0)


class TestLiveStepDown:
    """Step-down and handoff with real PostgreSQL."""

    async def test_step_down_handoff(self, pg_dsn, lock_keys):
        """When leader steps down, follower should acquire."""
        k1, k2 = lock_keys
        lock1 = LeaderLock(
            dsn=pg_dsn,
            key1=k1,
            key2=k2,
            retry_strategy=FixedInterval(interval_s=0.1),
            health_interval_s=0.5,
            auto_reacquire=False,
        )
        lock2 = LeaderLock(
            dsn=pg_dsn,
            key1=k1,
            key2=k2,
            retry_strategy=FixedInterval(interval_s=0.1),
            health_interval_s=0.5,
        )

        await lock1.start()
        await lock1.wait_for_leadership(timeout_s=5.0)
        assert lock1.is_leader

        await lock2.start()
        await asyncio.sleep(0.3)
        assert not lock2.is_leader

        # Leader steps down
        await lock1.step_down(timeout_s=5.0)
        await asyncio.sleep(0.1)
        assert not lock1.is_leader

        # Lock2 should pick up leadership
        got2 = await lock2.wait_for_leadership(timeout_s=5.0)
        assert got2 is True
        assert lock2.is_leader

        await lock1.shutdown(timeout_s=5.0)
        await lock2.shutdown(timeout_s=5.0)


class TestLiveCallbacks:
    """Verify callbacks fire with real PostgreSQL."""

    async def test_callbacks_fire(self, pg_dsn, lock_keys):
        k1, k2 = lock_keys
        lock = LeaderLock(
            dsn=pg_dsn,
            key1=k1,
            key2=k2,
            retry_strategy=FixedInterval(interval_s=0.1),
            health_interval_s=0.5,
        )
        events = []

        @lock.on_acquired
        def on_acq():
            events.append("acquired")

        @lock.on_released
        def on_rel():
            events.append("released")

        @lock.on_state_change
        def on_sc(f, t):
            events.append(f"state:{f.value}->{t.value}")

        async with lock:
            await lock.wait_for_leadership(timeout_s=5.0)

        assert "acquired" in events
        assert "released" in events
        assert any("state:" in e for e in events)


async def _terminate_backend(dsn: str, victim_conn: psycopg.AsyncConnection) -> None:
    """Kill a connection's backend via pg_terminate_backend."""
    cur = await victim_conn.execute("SELECT pg_backend_pid()")
    row = await cur.fetchone()
    pid = row[0]
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as admin:
        await admin.execute("SELECT pg_terminate_backend(%s)", (pid,))


class TestLiveHardKillFailover:
    """Simulate kill -9 via pg_terminate_backend and verify failover."""

    async def test_follower_becomes_leader_after_hard_kill(self, pg_dsn, lock_keys):
        """Kill the leader's backend; the follower should acquire leadership."""
        k1, k2 = lock_keys
        lock1 = LeaderLock(
            dsn=pg_dsn,
            key1=k1,
            key2=k2,
            retry_strategy=FixedInterval(interval_s=0.2),
            health_interval_s=0.5,
        )
        lock2 = LeaderLock(
            dsn=pg_dsn,
            key1=k1,
            key2=k2,
            retry_strategy=FixedInterval(interval_s=0.2),
            health_interval_s=0.5,
        )

        await lock1.start()
        assert await lock1.wait_for_leadership(timeout_s=5.0)

        await lock2.start()
        await asyncio.sleep(0.5)
        assert not lock2.is_leader

        # Hard-kill the leader's backend
        await _terminate_backend(pg_dsn, lock1._conn)

        # Follower should pick up leadership
        assert await lock2.wait_for_leadership(timeout_s=15.0)
        assert lock2.is_leader

        await lock1.shutdown(timeout_s=5.0)
        await lock2.shutdown(timeout_s=5.0)

    async def test_three_way_failover(self, pg_dsn, lock_keys):
        """Kill leader, follower takes over, kill again, third takes over."""
        k1, k2 = lock_keys
        lock1 = LeaderLock(
            dsn=pg_dsn,
            key1=k1,
            key2=k2,
            retry_strategy=FixedInterval(interval_s=0.2),
            health_interval_s=0.5,
            auto_reacquire=False,  # don't let killed leader compete again
        )
        lock2 = LeaderLock(
            dsn=pg_dsn,
            key1=k1,
            key2=k2,
            retry_strategy=FixedInterval(interval_s=0.2),
            health_interval_s=0.5,
            auto_reacquire=False,
        )
        lock3 = LeaderLock(
            dsn=pg_dsn,
            key1=k1,
            key2=k2,
            retry_strategy=FixedInterval(interval_s=0.2),
            health_interval_s=0.5,
        )

        await lock1.start()
        assert await lock1.wait_for_leadership(timeout_s=5.0)

        await lock2.start()
        await lock3.start()
        await asyncio.sleep(0.5)
        assert not lock2.is_leader
        assert not lock3.is_leader

        # Kill first leader — lock1 stops (auto_reacquire=False)
        await _terminate_backend(pg_dsn, lock1._conn)

        # Wait for one of lock2/lock3 to become leader
        done, _ = await asyncio.wait(
            [
                asyncio.create_task(lock2.wait_for_leadership(timeout_s=15.0)),
                asyncio.create_task(lock3.wait_for_leadership(timeout_s=15.0)),
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )
        assert any(t.result() for t in done)
        assert lock2.is_leader != lock3.is_leader

        # Kill the new leader — it stops (auto_reacquire=False)
        new_leader = lock2 if lock2.is_leader else lock3
        remaining = lock3 if lock2.is_leader else lock2
        await _terminate_backend(pg_dsn, new_leader._conn)

        assert await remaining.wait_for_leadership(timeout_s=15.0)
        assert remaining.is_leader

        await lock1.shutdown(timeout_s=5.0)
        await lock2.shutdown(timeout_s=5.0)
        await lock3.shutdown(timeout_s=5.0)


class TestLiveGracePeriod:
    """Test reconnect_grace_s with real connection kills."""

    async def test_grace_period_recovers_leadership(self, pg_dsn, lock_keys):
        """Leader whose backend is killed recovers within grace period."""
        k1, k2 = lock_keys
        lost_events = []

        lock = LeaderLock(
            dsn=pg_dsn,
            key1=k1,
            key2=k2,
            retry_strategy=FixedInterval(interval_s=0.2),
            health_interval_s=0.5,
            reconnect_grace_s=10.0,  # generous grace window
        )

        @lock.on_lost
        def _lost():
            lost_events.append("lost")

        await lock.start()
        assert await lock.wait_for_leadership(timeout_s=5.0)

        # Kill the backend — lock should enter RECONNECTING
        await _terminate_backend(pg_dsn, lock._conn)

        # Wait for it to detect the failure and attempt reconnect
        await asyncio.sleep(2.0)

        # It should have recovered leadership (no other contender)
        assert lock.is_leader
        assert len(lost_events) == 0, "on_lost should NOT fire when grace recovery succeeds"

        await lock.shutdown(timeout_s=5.0)

    async def test_grace_period_lost_to_competitor(self, pg_dsn, lock_keys):
        """Leader loses lock during grace when a competitor grabs it."""
        k1, k2 = lock_keys
        lost_events = []

        lock1 = LeaderLock(
            dsn=pg_dsn,
            key1=k1,
            key2=k2,
            retry_strategy=FixedInterval(interval_s=0.2),
            health_interval_s=0.5,
            reconnect_grace_s=10.0,
        )

        @lock1.on_lost
        def _lost():
            lost_events.append("lost")

        await lock1.start()
        assert await lock1.wait_for_leadership(timeout_s=5.0)

        # Kill the leader backend
        await _terminate_backend(pg_dsn, lock1._conn)

        # Immediately start a competitor that will grab the lock
        # while lock1 is reconnecting
        lock2 = LeaderLock(
            dsn=pg_dsn,
            key1=k1,
            key2=k2,
            retry_strategy=FixedInterval(interval_s=0.1),
            health_interval_s=0.5,
        )
        await lock2.start()
        assert await lock2.wait_for_leadership(timeout_s=10.0)

        # lock1 should have lost leadership
        await asyncio.sleep(2.0)
        assert not lock1.is_leader
        assert lock2.is_leader
        assert len(lost_events) > 0, "on_lost should fire when competitor takes lock"

        await lock1.shutdown(timeout_s=5.0)
        await lock2.shutdown(timeout_s=5.0)

    async def test_no_grace_fires_lost_immediately(self, pg_dsn, lock_keys):
        """Without reconnect_grace_s, on_lost fires immediately on failure."""
        k1, k2 = lock_keys
        lost_events = []

        lock = LeaderLock(
            dsn=pg_dsn,
            key1=k1,
            key2=k2,
            retry_strategy=FixedInterval(interval_s=0.2),
            health_interval_s=0.5,
            reconnect_grace_s=None,  # no grace
        )

        @lock.on_lost
        def _lost():
            lost_events.append("lost")

        await lock.start()
        assert await lock.wait_for_leadership(timeout_s=5.0)

        await _terminate_backend(pg_dsn, lock._conn)

        # Wait for health check to detect and fire on_lost
        await asyncio.sleep(3.0)
        assert len(lost_events) > 0, "on_lost should fire without grace period"

        await lock.shutdown(timeout_s=5.0)


class TestLiveDifferentKeys:
    """Different key pairs should not interfere."""

    async def test_different_keys_independent(self, pg_dsn):
        lock1 = LeaderLock(
            dsn=pg_dsn,
            key1=99999,
            key2=1,
            retry_strategy=FixedInterval(interval_s=0.1),
            health_interval_s=0.5,
        )
        lock2 = LeaderLock(
            dsn=pg_dsn,
            key1=99999,
            key2=2,
            retry_strategy=FixedInterval(interval_s=0.1),
            health_interval_s=0.5,
        )

        await lock1.start()
        await lock2.start()

        got1 = await lock1.wait_for_leadership(timeout_s=5.0)
        got2 = await lock2.wait_for_leadership(timeout_s=5.0)

        # Both should be leaders since they use different keys
        assert got1 is True
        assert got2 is True
        assert lock1.is_leader
        assert lock2.is_leader

        await lock1.shutdown(timeout_s=5.0)
        await lock2.shutdown(timeout_s=5.0)
