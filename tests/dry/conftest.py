from __future__ import annotations

import asyncio
from collections import deque
from typing import Any

import pytest

from pgleaderlock.lock import LeaderLock
from pgleaderlock.retry import FixedInterval


class FakeCursor:
    """Simulates a psycopg cursor with scripted results."""

    def __init__(self, result: Any = None) -> None:
        self._result = result

    async def fetchone(self) -> tuple[Any, ...] | None:
        if self._result is None:
            return None
        return (self._result,)


class FakeConnection:
    """Simulates a psycopg.AsyncConnection with scripted behavior."""

    def __init__(self) -> None:
        self._execute_results: deque[Any] = deque()  # True/False/Exception
        self._closed = False
        self.execute_calls: list[tuple[str, tuple | None]] = []

    def script(self, *results: Any) -> None:
        """Queue results. True/False for lock ops, None for health, Exception to raise."""
        self._execute_results.extend(results)

    async def execute(self, sql: str, params: tuple | None = None) -> FakeCursor:
        self.execute_calls.append((sql, params))
        if not self._execute_results:
            return FakeCursor(True)
        result = self._execute_results.popleft()
        if isinstance(result, Exception):
            raise result
        return FakeCursor(result)

    async def close(self) -> None:
        self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed


@pytest.fixture
def fake_conn() -> FakeConnection:
    return FakeConnection()


@pytest.fixture
def make_lock(fake_conn: FakeConnection):
    """Factory to create a LeaderLock wired to a FakeConnection."""

    def _make(
        *,
        health_interval_s: float = 0.01,
        reconnect_grace_s: float | None = None,
        auto_reacquire: bool = True,
        retry_strategy=None,
        shutdown_event: asyncio.Event | None = None,
    ) -> LeaderLock:
        async def connect_fn():
            return fake_conn

        return LeaderLock(
            dsn="postgresql://fake",
            key1=1,
            key2=100,
            retry_strategy=retry_strategy or FixedInterval(interval_s=0.0),
            health_interval_s=health_interval_s,
            reconnect_grace_s=reconnect_grace_s,
            auto_reacquire=auto_reacquire,
            shutdown_event=shutdown_event,
            connect_fn=connect_fn,
        )

    return _make
