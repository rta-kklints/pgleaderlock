from __future__ import annotations

import asyncio
import inspect
import time
from typing import Any, Awaitable, Callable, Self

import psycopg

from pgleaderlock._logging import get_logger
from pgleaderlock.errors import ConnectionError, LockError, ShutdownError
from pgleaderlock.models import (
    Callback,
    ErrorCallback,
    LockState,
    RetryContext,
    StateChangeCallback,
)
from pgleaderlock.retry import ExponentialBackoff, RetryStrategy

logger = get_logger("pgleaderlock")


class LeaderLock:
    """Distributed leader election using PostgreSQL session-level advisory locks."""

    def __init__(
        self,
        dsn: str,
        key1: int,
        key2: int,
        *,
        retry_strategy: RetryStrategy | None = None,
        health_interval_s: float = 5.0,
        reconnect_grace_s: float | None = None,
        auto_reacquire: bool = True,
        shutdown_event: asyncio.Event | None = None,
        connect_fn: Callable[[], Awaitable[psycopg.AsyncConnection[Any]]] | None = None,
    ) -> None:
        self._dsn = dsn
        self._key1 = key1
        self._key2 = key2
        self._health_interval_s = health_interval_s
        self._reconnect_grace_s = reconnect_grace_s
        self._auto_reacquire = auto_reacquire
        self._shutdown_event = shutdown_event
        self._connect_fn = connect_fn

        self._state = LockState.STOPPED
        self._task: asyncio.Task[None] | None = None
        self._conn: psycopg.AsyncConnection[Any] | None = None
        self._stop_event = asyncio.Event()
        self._step_down_event = asyncio.Event()
        self._leadership_event = asyncio.Event()
        self._retry_strategy: RetryStrategy = retry_strategy or ExponentialBackoff()

        self._retry_attempt: int = 0
        self._retry_start: float | None = None
        self._releasing_from_step_down: bool = False

        # Callback lists
        self._on_acquired: list[Callback] = []
        self._on_released: list[Callback] = []
        self._on_lost: list[Callback] = []
        self._on_acquire_failed: list[Callback] = []
        self._on_state_change: list[StateChangeCallback] = []
        self._on_error: list[ErrorCallback] = []

        self._log = logger.bind(key1=key1, key2=key2)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def state(self) -> LockState:
        return self._state

    @property
    def is_leader(self) -> bool:
        return self._state == LockState.LEADER

    # ------------------------------------------------------------------
    # Event registration decorators
    # ------------------------------------------------------------------

    def on_acquired(self, fn: Callback) -> Callback:
        self._on_acquired.append(fn)
        return fn

    def on_released(self, fn: Callback) -> Callback:
        self._on_released.append(fn)
        return fn

    def on_lost(self, fn: Callback) -> Callback:
        self._on_lost.append(fn)
        return fn

    def on_acquire_failed(self, fn: Callback) -> Callback:
        self._on_acquire_failed.append(fn)
        return fn

    def on_state_change(self, fn: StateChangeCallback) -> StateChangeCallback:
        self._on_state_change.append(fn)
        return fn

    def on_error(self, fn: ErrorCallback) -> ErrorCallback:
        self._on_error.append(fn)
        return fn

    # ------------------------------------------------------------------
    # Callback invocation helper
    # ------------------------------------------------------------------

    async def _fire(self, callbacks: list, *args: Any) -> None:
        for cb in callbacks:
            try:
                result = cb(*args)
                if inspect.isawaitable(result):
                    await result
            except Exception:
                self._log.exception("callback_error", callback=cb.__name__)
                if callbacks is not self._on_error:
                    await self._fire(self._on_error, args[0] if args else Exception("callback_error"))

    # ------------------------------------------------------------------
    # Lifecycle methods
    # ------------------------------------------------------------------

    async def start(self) -> None:
        if self._state != LockState.STOPPED:
            return
        self._stop_event.clear()
        self._step_down_event.clear()
        self._leadership_event.clear()
        self._retry_attempt = 0
        self._retry_start = None
        self._releasing_from_step_down = False
        self._task = asyncio.create_task(self._run_loop())

    async def shutdown(self, timeout_s: float | None = None) -> None:
        self._stop_event.set()
        if self._task is None:
            return
        try:
            await asyncio.wait_for(asyncio.shield(self._task), timeout=timeout_s)
        except asyncio.TimeoutError:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            await self._close_conn()
            if self._state != LockState.STOPPED:
                await self._transition(LockState.STOPPED)
        except asyncio.CancelledError:
            pass
        self._task = None

    async def step_down(self, timeout_s: float | None = None) -> None:
        if self._state not in (LockState.LEADER, LockState.RECONNECTING):
            return
        self._step_down_event.set()
        if self._task is None:
            return

        async def _wait_release() -> None:
            while self._state in (
                LockState.LEADER,
                LockState.RELEASING,
                LockState.RECONNECTING,
            ):
                await asyncio.sleep(0.01)

        try:
            await asyncio.wait_for(_wait_release(), timeout=timeout_s)
        except asyncio.TimeoutError:
            self._log.warning("step_down_timeout_force_close")
            await self._close_conn()

    async def wait_for_leadership(self, timeout_s: float | None = None) -> bool:
        try:
            await asyncio.wait_for(self._leadership_event.wait(), timeout=timeout_s)
            return True
        except asyncio.TimeoutError:
            return False

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.shutdown(timeout_s=10.0)

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    async def _connect(self) -> psycopg.AsyncConnection[Any]:
        if self._connect_fn:
            return await self._connect_fn()
        conn = await psycopg.AsyncConnection.connect(self._dsn, autocommit=True)
        return conn

    async def _close_conn(self) -> None:
        if self._conn is not None:
            try:
                await self._conn.close()
            except Exception:
                pass
            self._conn = None

    # ------------------------------------------------------------------
    # DB operations
    # ------------------------------------------------------------------

    async def _db_try_lock(self) -> bool:
        assert self._conn is not None
        cur = await self._conn.execute(
            "SELECT pg_try_advisory_lock(%s, %s)", (self._key1, self._key2)
        )
        row = await cur.fetchone()
        return bool(row and row[0])

    async def _db_unlock(self) -> bool:
        assert self._conn is not None
        cur = await self._conn.execute(
            "SELECT pg_advisory_unlock(%s, %s)", (self._key1, self._key2)
        )
        row = await cur.fetchone()
        return bool(row and row[0])

    async def _db_health(self) -> None:
        assert self._conn is not None
        await self._conn.execute("SELECT 1")

    # ------------------------------------------------------------------
    # State transition helper
    # ------------------------------------------------------------------

    async def _transition(self, new_state: LockState) -> None:
        old = self._state
        self._state = new_state
        self._log.info("state_change", **{"from": old.value, "to": new_state.value})
        if new_state == LockState.LEADER:
            self._leadership_event.set()
        else:
            self._leadership_event.clear()
        await self._fire(self._on_state_change, old, new_state)

    # ------------------------------------------------------------------
    # Stop check
    # ------------------------------------------------------------------

    def _should_stop(self) -> bool:
        return self._stop_event.is_set() or (
            self._shutdown_event is not None and self._shutdown_event.is_set()
        )

    # ------------------------------------------------------------------
    # Interruptible sleep
    # ------------------------------------------------------------------

    async def _interruptible_sleep(self, seconds: float) -> None:
        if seconds <= 0:
            return
        stop_fut = asyncio.ensure_future(self._stop_event.wait())
        try:
            await asyncio.wait_for(stop_fut, timeout=seconds)
        except asyncio.TimeoutError:
            pass
        finally:
            if not stop_fut.done():
                stop_fut.cancel()
                try:
                    await stop_fut
                except asyncio.CancelledError:
                    pass

    # ------------------------------------------------------------------
    # Connect with retry
    # ------------------------------------------------------------------

    async def _connect_with_retry(self) -> None:
        attempt = 0
        start_time = time.monotonic()
        while True:
            if self._should_stop():
                return
            try:
                self._conn = await self._connect()
                return
            except Exception as exc:
                attempt += 1
                elapsed = time.monotonic() - start_time
                ctx = RetryContext(attempt=attempt, elapsed_s=elapsed, last_error=exc)
                delay = self._retry_strategy.next_delay_s(ctx)
                self._log.warning("connect_failed", attempt=attempt, error=str(exc))
                if delay is None:
                    raise ConnectionError(
                        f"Failed to connect after {attempt} attempts"
                    ) from exc
                await self._interruptible_sleep(delay)

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def _run_loop(self) -> None:
        try:
            await self._connect_with_retry()
            if self._should_stop():
                return
            await self._transition(LockState.FOLLOWER)

            while self._state != LockState.STOPPED:
                if self._should_stop():
                    await self._handle_stop()
                    break

                if self._state == LockState.FOLLOWER:
                    await self._state_follower()
                elif self._state == LockState.ACQUIRING:
                    await self._state_acquiring()
                elif self._state == LockState.LEADER:
                    await self._state_leader()
                elif self._state == LockState.RECONNECTING:
                    await self._state_reconnecting()
                elif self._state == LockState.RELEASING:
                    await self._state_releasing()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            self._log.exception("lifecycle_error")
            await self._fire(self._on_error, exc)
        finally:
            await self._close_conn()
            if self._state != LockState.STOPPED:
                await self._transition(LockState.STOPPED)

    # ------------------------------------------------------------------
    # State handlers
    # ------------------------------------------------------------------

    async def _state_follower(self) -> None:
        if self._should_stop():
            return
        await self._transition(LockState.ACQUIRING)

    async def _state_acquiring(self) -> None:
        try:
            acquired = await self._db_try_lock()
        except Exception as exc:
            self._log.error("acquire_db_error", error=str(exc))
            await self._fire(self._on_error, exc)
            await self._close_conn()
            if self._auto_reacquire:
                await self._connect_with_retry()
                if self._should_stop():
                    return
                await self._transition(LockState.FOLLOWER)
            else:
                await self._transition(LockState.STOPPED)
            return

        if acquired:
            self._log.info("lock_acquired")
            self._retry_attempt = 0
            self._retry_start = None
            await self._transition(LockState.LEADER)
            await self._fire(self._on_acquired)
        else:
            self._retry_attempt += 1
            if self._retry_start is None:
                self._retry_start = time.monotonic()
            elapsed = time.monotonic() - self._retry_start
            ctx = RetryContext(
                attempt=self._retry_attempt,
                elapsed_s=elapsed,
                last_error=None,
            )
            delay = self._retry_strategy.next_delay_s(ctx)
            self._log.info(
                "acquire_failed", attempt=self._retry_attempt, next_delay=delay
            )
            await self._fire(self._on_acquire_failed)
            if delay is None:
                await self._transition(LockState.STOPPED)
                return
            await self._interruptible_sleep(delay)
            await self._transition(LockState.FOLLOWER)

    async def _state_leader(self) -> None:
        step_down_task = asyncio.ensure_future(self._step_down_event.wait())
        stop_task = asyncio.ensure_future(self._stop_event.wait())
        sleep_task = asyncio.ensure_future(asyncio.sleep(self._health_interval_s))

        tasks = [step_down_task, stop_task, sleep_task]
        if self._shutdown_event is not None:
            ext_stop_task = asyncio.ensure_future(self._shutdown_event.wait())
            tasks.append(ext_stop_task)
        else:
            ext_stop_task = None

        done, pending = await asyncio.wait(
            tasks, return_when=asyncio.FIRST_COMPLETED
        )
        for t in pending:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass

        if stop_task in done or (ext_stop_task is not None and ext_stop_task in done):
            return  # main loop will handle stop

        if step_down_task in done:
            self._step_down_event.clear()
            self._releasing_from_step_down = True
            await self._transition(LockState.RELEASING)
            return

        # Health check
        try:
            await self._db_health()
            self._log.debug("health_ok")
        except Exception as exc:
            self._log.warning("health_check_failed", error=str(exc))
            await self._close_conn()
            if self._reconnect_grace_s is not None:
                await self._transition(LockState.RECONNECTING)
            else:
                await self._fire(self._on_lost)
                if self._auto_reacquire:
                    await self._connect_with_retry()
                    if self._should_stop():
                        return
                    await self._transition(LockState.FOLLOWER)
                else:
                    await self._transition(LockState.STOPPED)

    async def _state_reconnecting(self) -> None:
        assert self._reconnect_grace_s is not None
        deadline = time.monotonic() + self._reconnect_grace_s
        attempt = 0
        start_time = time.monotonic()

        while time.monotonic() < deadline:
            if self._should_stop():
                return

            if self._step_down_event.is_set():
                self._step_down_event.clear()
                self._releasing_from_step_down = True
                await self._fire(self._on_lost)
                if self._auto_reacquire:
                    if self._conn is None:
                        await self._connect_with_retry()
                    if self._should_stop():
                        return
                    await self._transition(LockState.FOLLOWER)
                else:
                    await self._transition(LockState.STOPPED)
                return

            # Try to reconnect
            try:
                self._conn = await self._connect()
            except Exception:
                attempt += 1
                elapsed = time.monotonic() - start_time
                ctx = RetryContext(attempt=attempt, elapsed_s=elapsed)
                delay = self._retry_strategy.next_delay_s(ctx)
                if delay is None or time.monotonic() + delay > deadline:
                    break
                await self._interruptible_sleep(min(delay, deadline - time.monotonic()))
                continue

            # Try to re-acquire
            try:
                acquired = await self._db_try_lock()
                if acquired:
                    self._log.info("leadership_recovered")
                    await self._transition(LockState.LEADER)
                    return
                else:
                    self._log.info("reacquire_failed_held_by_other")
                    break
            except Exception:
                await self._close_conn()
                attempt += 1
                elapsed = time.monotonic() - start_time
                ctx = RetryContext(attempt=attempt, elapsed_s=elapsed)
                delay = self._retry_strategy.next_delay_s(ctx)
                if delay is None or time.monotonic() + delay > deadline:
                    break
                await self._interruptible_sleep(min(delay, deadline - time.monotonic()))

        # Grace expired or reacquire failed
        self._log.warning("grace_period_exhausted")
        await self._fire(self._on_lost)
        if self._auto_reacquire:
            if self._conn is None:
                await self._connect_with_retry()
            if self._should_stop():
                return
            await self._transition(LockState.FOLLOWER)
        else:
            await self._transition(LockState.STOPPED)

    async def _state_releasing(self) -> None:
        try:
            success = await self._db_unlock()
            if success:
                self._log.info("lock_released")
                await self._fire(self._on_released)
                if self._releasing_from_step_down and self._auto_reacquire:
                    self._releasing_from_step_down = False
                    await self._transition(LockState.FOLLOWER)
                else:
                    self._releasing_from_step_down = False
                    await self._transition(LockState.STOPPED)
            else:
                raise LockError("pg_advisory_unlock returned false")
        except Exception as exc:
            self._log.error("release_failed", error=str(exc))
            await self._fire(self._on_error, exc)
            self._releasing_from_step_down = False
            await self._close_conn()
            await self._transition(LockState.STOPPED)

    async def _handle_stop(self) -> None:
        if self._state == LockState.LEADER:
            await self._transition(LockState.RELEASING)
            try:
                await self._db_unlock()
                self._log.info("lock_released")
                await self._fire(self._on_released)
            except Exception as exc:
                self._log.error("release_on_stop_failed", error=str(exc))
                await self._fire(self._on_error, exc)
        await self._close_conn()
        await self._transition(LockState.STOPPED)
