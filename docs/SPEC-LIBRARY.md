# pgleaderlock — Specification

## 1. Purpose

A Python library for **distributed leader election** using PostgreSQL **session-level advisory locks** with the two-bigint key pattern (`pg_try_advisory_lock(key1, key2)`).

A single leader is elected among competing processes. The library manages the full lock lifecycle — acquisition with configurable retry, health monitoring, voluntary release, involuntary loss detection, and shutdown — exposing state transitions via user-defined callbacks.

The library is **async-native**, using `psycopg` (psycopg 3) in async mode with `asyncio`. The lock lifecycle runs as an `asyncio.Task` within the caller's event loop.

### 1.1 Non-Goals

- **Fencing tokens.** Session advisory locks provide mutual exclusion but no mechanism to prevent a stale leader (partitioned from downstream systems but still holding the lock) from performing side effects. Fencing is out of scope for v1.
- **Multi-process coordination within a single host.** Each process runs its own `LeaderLock` instance with its own DB connection.

---

## 2. Advisory Lock Semantics

PostgreSQL session-level advisory locks have properties that constrain the design:

1. **Session-scoped.** The lock is held until explicitly released or the session (connection) terminates.
2. **No TTL / no renewal.** There is nothing to "renew" — the lock persists as long as the session lives.
3. **Re-entrant per session.** Calling `pg_try_advisory_lock(k1, k2)` again on the *same* session succeeds and increments an internal hold count, requiring a matching number of `pg_advisory_unlock` calls. **The library must never re-acquire while already leader.**
4. **No notification on loss.** The only way to detect loss is to detect that the session (connection) has died.

### 2.1 Lock Identity

The lock is identified by a pair of 32-bit integers `(key1, key2)`, passed to `pg_try_advisory_lock(key1, key2)` and `pg_advisory_unlock(key1, key2)`. The caller chooses values that represent the resource being protected (e.g., a hash of a service name).

---

## 3. Finite State Machine

The lock lifecycle is modelled as a **deterministic finite automaton (DFA)** with six states. The distinction between *voluntary release* and *involuntary loss* is semantically significant: applications react differently (e.g., loss triggers immediate cessation of leader-only work, while release is a graceful handoff).

### 3.1 States

| State | Description |
|---|---|
| **STOPPED** | Terminal/initial. No lifecycle task, no connection. |
| **FOLLOWER** | Running but not currently attempting acquisition. Waiting for next retry tick (back-off delay). |
| **ACQUIRING** | Actively executing `pg_try_advisory_lock`. |
| **LEADER** | Lock held. Connection health loop running. |
| **RECONNECTING** | Grace period after health-check failure. Attempting to reconnect and re-acquire before declaring loss. Lock is **not held** during this state. |
| **RELEASING** | Voluntary release in progress (`pg_advisory_unlock`). |

> **Note on LOST:** Involuntary loss is not a durable state — it is a *transient event* that triggers either a transition to FOLLOWER (auto-reacquire) or STOPPED (!auto-reacquire). The `on_lost` callback fires on the transition itself.
>
> **Flap damping (RECONNECTING):** When `reconnect_grace_s` is set and a health check fails while LEADER, the FSM enters RECONNECTING instead of immediately emitting `on_lost`. During this state the library silently attempts to open a new connection and re-acquire the lock. If successful within the grace window, it transitions back to LEADER with only a log notice — no `on_lost`/`on_acquired` events fire. If the grace period expires or re-acquisition fails (another process holds the lock), it transitions normally to FOLLOWER with `on_lost`. This avoids spurious event churn from transient network interruptions or PG connection timeouts. **`is_leader` returns `False` during RECONNECTING** — the lock is genuinely not held and callers must not assume exclusivity.

### 3.2 Inputs (Events)

| Input | Source |
|---|---|
| `Start` | User calls `start()` |
| `StopRequested` | Shutdown event set or `shutdown()` called |
| `AcquireAttempt` | Internal: retry timer fires |
| `AcquireSucceeded` | DB: `pg_try_advisory_lock` returned `true` |
| `AcquireFailed` | DB: `pg_try_advisory_lock` returned `false` |
| `DbError` | DB: connection/query error during any operation |
| `HealthOk` | DB: health check query succeeded |
| `HealthFailed` | DB: health check query failed |
| `ReacquireSucceeded` | DB: re-acquisition during grace period succeeded |
| `ReacquireFailed` | DB: re-acquisition during grace period failed (lock held by another, or connection failed) |
| `GraceExpired` | Internal: `reconnect_grace_s` timer elapsed without successful re-acquisition |
| `ReleaseRequested` | User calls `step_down()` |
| `ReleaseSucceeded` | DB: `pg_advisory_unlock` returned `true` |
| `ReleaseFailed` | DB: `pg_advisory_unlock` failed |

### 3.3 Transition Table

| From | Input | To | Actions |
|---|---|---|---|
| STOPPED | `Start` | FOLLOWER | Open connection; schedule immediate `AcquireAttempt` |
| FOLLOWER | `AcquireAttempt` | ACQUIRING | Execute `pg_try_advisory_lock(k1, k2)` |
| FOLLOWER | `StopRequested` | STOPPED | Close connection; emit `on_stopped` |
| ACQUIRING | `AcquireSucceeded` | LEADER | Emit `on_acquired`; reset retry state; begin health loop |
| ACQUIRING | `AcquireFailed` | FOLLOWER | Compute backoff delay; emit `on_acquire_failed`; sleep; schedule next `AcquireAttempt` |
| ACQUIRING | `DbError` | FOLLOWER¹ | Emit `on_lost`; close connection; reconnect; schedule `AcquireAttempt` |
| ACQUIRING | `StopRequested` | STOPPED | Close connection; emit `on_stopped` |
| LEADER | `HealthOk` | LEADER | *(no transition)* |
| LEADER | `HealthFailed` / `DbError` | RECONNECTING³ | Log warning; close connection; start grace timer; attempt reconnect + re-acquire |
| LEADER | `StopRequested` | RELEASING | Execute `pg_advisory_unlock(k1, k2)` |
| LEADER | `ReleaseRequested` | RELEASING | Execute `pg_advisory_unlock(k1, k2)` |
| RECONNECTING | `ReacquireSucceeded` | LEADER | Log notice ("leadership recovered"); **no events emitted** |
| RECONNECTING | `ReacquireFailed` | FOLLOWER¹ | Emit `on_lost`; schedule `AcquireAttempt` |
| RECONNECTING | `GraceExpired` | FOLLOWER¹ | Emit `on_lost`; schedule `AcquireAttempt` |
| RECONNECTING | `StopRequested` | STOPPED | Close connection; emit `on_stopped` |
| RELEASING | `ReleaseSucceeded` | STOPPED² | Emit `on_released`; close connection |
| RELEASING | `ReleaseFailed` / `DbError` | STOPPED | Emit `on_error`; close connection |

¹ Transitions to STOPPED instead if `auto_reacquire=False`.
² Transitions to FOLLOWER instead if triggered by `step_down()` and `auto_reacquire=True`.
³ Falls through directly to FOLLOWER¹ (emitting `on_lost`) if `reconnect_grace_s` is `None` (flap damping disabled).

### 3.4 State Diagram

```
                         ┌─────────────────────────────────────┐
                         │              STOPPED                │
                         │  (initial / terminal, no connection)│
                         └──────┬────────────────▲─────▲───────┘
                       Start    │                │     │
                                ▼                │     │
                         ┌──────────────┐        │     │
                    ┌───▶│   FOLLOWER   │────────┘     │
                    │    │ (back-off    │  StopReq      │
    AcquireFailed   │    │  waiting)    │               │
                    │    └──────┬───────┘               │
                    │    AcquireAttempt   ▲              │
                    │           ▼         │ on_lost      │
                    │    ┌──────────────┐ │              │
                    └────│  ACQUIRING   │─┘ (DbError)   │
                         └──────┬───────┘               │
                     AcquireSucceeded                   │
                                ▼                       │
                    ┌──────────────────────┐            │
               ┌───▶│       LEADER        │            │
               │    │  (lock held,        │            │
               │    │   health loop)      │            │
               │    └──┬───┬──────┬───────┘            │
               │       │   │      │ HealthFailed       │
            HealthOk   │   │      ▼ (grace enabled)    │
                       │   │  ┌────────────────┐       │
                       │   │  │  RECONNECTING  │───────│──▶ FOLLOWER
                       │   │  │  (grace period,│       │    (on_lost if
                       │   │  │   lock NOT held)│      │     grace fails)
                       │   │  └───────┬────────┘       │
                       │   │     ReacquireSucceeded     │
                       │   │    (no events, log only)   │
                       │   │          │                 │
                       │   │          └─────────────────┘──▶ back to LEADER
                       │   │ StopReq /
                       │   │ ReleaseReq
                       │   ▼
                       │ ┌──────────────┐
                       │ │  RELEASING   │──────────────────▶ STOPPED
                       │ └──────────────┘
                       │   HealthFailed (no grace)
                       └──────────────────────────────────▶ FOLLOWER (on_lost)
```

---

## 4. Public API

### 4.1 Design: Separation of Configuration and Events

The constructor accepts only **infrastructure concerns** (DSN, lock keys, timing, retry). Event callbacks are registered separately via **decorator methods**, keeping the constructor small and allowing callbacks to be added incrementally.

### 4.2 `LeaderLock`

The primary class. Manages the full lock lifecycle as an `asyncio.Task`.

```python
class LeaderLock:
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
        connect_fn: Callable[[], Awaitable[AsyncConnection]] | None = None,
    ) -> None: ...

    # --- Lifecycle ---
    async def start(self) -> None: ...
    async def shutdown(self, timeout_s: float | None = None) -> None: ...
    async def step_down(self, timeout_s: float | None = None) -> None: ...
    async def wait_for_leadership(self, timeout_s: float | None = None) -> bool: ...

    # --- Context manager ---
    async def __aenter__(self) -> Self: ...
    async def __aexit__(self, *exc: object) -> None: ...

    # --- State ---
    @property
    def state(self) -> LockState: ...

    @property
    def is_leader(self) -> bool: ...

    # --- Event registration (decorator style) ---
    def on_acquired(self, fn: Callback) -> Callback: ...
    def on_released(self, fn: Callback) -> Callback: ...
    def on_lost(self, fn: Callback) -> Callback: ...
    def on_acquire_failed(self, fn: Callback) -> Callback: ...
    def on_state_change(self, fn: StateChangeCallback) -> StateChangeCallback: ...
    def on_error(self, fn: ErrorCallback) -> ErrorCallback: ...
```

#### Usage Example

```python
lock = LeaderLock(dsn="postgresql://...", key1=1, key2=100)

@lock.on_acquired
def handle_acquired():
    logger.info("I am the leader now")

@lock.on_lost
def handle_lost():
    logger.info("Leadership lost, stopping work")

async with lock:
    await lock.wait_for_leadership()
    # ... do leader work ...
```

#### Constructor Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dsn` | `str` | *(required)* | PostgreSQL connection string. |
| `key1` | `int` | *(required)* | First advisory lock key (32-bit signed integer). |
| `key2` | `int` | *(required)* | Second advisory lock key (32-bit signed integer). |
| `retry_strategy` | `RetryStrategy \| None` | `ExponentialBackoff()` | Back-off algorithm for acquisition retries (also used for reconnection). |
| `health_interval_s` | `float` | `5.0` | Seconds between health-check queries while LEADER. |
| `reconnect_grace_s` | `float \| None` | `None` | Flap damping: max seconds to silently attempt reconnect + re-acquire after health-check failure before emitting `on_lost`. `None` disables (immediate `on_lost`). |
| `auto_reacquire` | `bool` | `True` | Whether to cycle back to FOLLOWER after loss/release. |
| `shutdown_event` | `asyncio.Event \| None` | `None` | External shutdown signal. Library also creates an internal one. |
| `connect_fn` | `Callable \| None` | `None` | Custom async connection factory (for testing / custom drivers). Overrides `dsn`. |

#### Methods

- **`start()`** — Spawns the lifecycle task and begins lock acquisition. Transitions STOPPED → FOLLOWER. Idempotent.
- **`shutdown(timeout_s=None)`** — Signals the lifecycle task to release the lock and stop. Awaits until the task completes (or `timeout_s` elapses). Idempotent.
- **`step_down(timeout_s=None)`** — Voluntarily release leadership without stopping. Blocks until RELEASING completes or `timeout_s` elapses; on timeout, cycles the connection (force-release). If `auto_reacquire=True`, resumes acquisition attempts.
- **`wait_for_leadership(timeout_s=None)`** — Awaits until the lock transitions to LEADER. Returns `True` if acquired, `False` on timeout.

#### Context Manager

`async with lock:` is sugar for `start()` on enter and `shutdown()` on exit. Exceptions during exit trigger shutdown with a default timeout.

#### Properties

- **`state`** — Current `LockState` (read-only).
- **`is_leader`** — Shorthand for `self.state == LockState.LEADER`.

#### Event Registration

Callbacks are registered via **decorator methods** that return the original function unchanged. Multiple callbacks can be registered per event — they are invoked in registration order.

```python
@lock.on_acquired
def first(): ...

@lock.on_acquired
def second(): ...   # both fire, in order
```

### 4.3 `LockState` (Enum)

```python
class LockState(enum.Enum):
    STOPPED = "stopped"
    FOLLOWER = "follower"
    ACQUIRING = "acquiring"
    LEADER = "leader"
    RECONNECTING = "reconnecting"
    RELEASING = "releasing"
```

### 4.4 Callback Types

```python
Callback = Callable[[], None]
StateChangeCallback = Callable[[LockState, LockState], None]   # (from_state, to_state)
ErrorCallback = Callable[[Exception], None]
```

#### Callback Contract

1. Callbacks are invoked **on the lifecycle task** (in the event loop), serialised in transition order.
2. Callbacks **must be fast and non-blocking**. Long-running work should be dispatched via `asyncio.create_task()` or similar.
3. Exceptions raised by callbacks are **caught, logged**, and forwarded to `on_error` (if set). They **never crash the lifecycle task**.
4. Callbacks may be either sync functions or async coroutines — the library awaits coroutines and calls sync functions directly.

### 4.5 `RetryStrategy` (Protocol)

```python
@dataclass(frozen=True)
class RetryContext:
    attempt: int            # 1-based attempt number
    elapsed_s: float        # total time since first attempt in this cycle
    last_error: Exception | None

class RetryStrategy(Protocol):
    def next_delay_s(self, ctx: RetryContext) -> float | None:
        """Return seconds to sleep before next attempt, or None to stop retrying."""
```

Returning `None` signals "give up" — the lock transitions to STOPPED.

#### Bundled Implementations

| Class | Behaviour |
|---|---|
| `ExponentialBackoff(base_s=1.0, max_s=30.0, multiplier=2.0)` | `min(base * multiplier^(attempt-1), max_s)` |
| `FixedInterval(interval_s=5.0)` | Constant delay. |
| `DecorrelatedJitter(base_s=1.0, max_s=30.0)` | `min(max_s, random(base_s, prev_delay * 3))` (AWS-style). |

---

## 5. Concurrency Model

### 5.1 Lifecycle Task

The lock lifecycle runs as a single `asyncio.Task` within the caller's event loop. This task owns:

- The PostgreSQL async connection (opened/closed exclusively by this task).
- The FSM state variable.
- The retry timer and health-check loop.
- All `pg_try_advisory_lock` / `pg_advisory_unlock` / health-check calls.

### 5.2 Thread Safety

- The library is designed for **single-threaded asyncio** use. All public methods are coroutines and must be called from the same event loop.
- **`shutdown()` and `step_down()`** communicate with the lifecycle task via `asyncio.Event` objects.
- **State** is stored as a single `LockState` enum value. Since all mutations happen on the same event loop, no explicit locking is needed.
- **Callbacks** are invoked on the lifecycle task within the event loop. If a callback needs to run in a thread, it should use `asyncio.to_thread()`.

### 5.3 Connection Ownership

The library **owns a dedicated async connection**. Rationale:

- Advisory locks are session-scoped; sharing a connection risks accidental release or re-entrant acquisition from other code.
- The lifecycle task is the sole user of the connection, eliminating concurrency concerns.

Default driver: **`psycopg`** (psycopg 3) in **async mode** (`psycopg.AsyncConnection`). The `connect_fn` parameter allows substitution for testing.

### 5.4 Lock Loss Detection

While in LEADER state, the lifecycle task runs a **connection health-check loop**:

```sql
SELECT 1
```

at `health_interval_s` intervals (using `asyncio.sleep`). If the query raises an `OperationalError` (or any connection-level error), the FSM transitions via `HealthFailed`.

This is **not** a "lock renewal" — session advisory locks have no TTL. It is purely a **session liveness probe**.

---

## 6. Error Handling

### 6.1 Exception Hierarchy

```python
class PgLeaderLockError(Exception):
    """Base exception for all pgleaderlock errors."""

class ConnectionError(PgLeaderLockError):
    """Failed to establish or maintain a database connection."""

class LockError(PgLeaderLockError):
    """Failed to acquire or release the advisory lock."""

class ShutdownError(PgLeaderLockError):
    """Error during shutdown (e.g., timeout waiting for worker thread)."""
```

### 6.2 Error Handling Policy

- **DB errors during acquisition:** Transition to FOLLOWER (auto-reacquire) or STOPPED. Emit `on_lost` + `on_error`.
- **DB errors during health check:** Same as above.
- **DB errors during release:** Transition to STOPPED. Emit `on_error`. Connection is closed regardless.
- **Callback exceptions:** Caught, logged, forwarded to `on_error`. Never propagate to the worker loop.
- **Connection establishment failures:** Retried using the same `RetryStrategy`.

---

## 7. Logging

The library uses **stdlib `logging`** via a thin structured wrapper (following the `NDLogger` pattern from the reference codebase). No external logging dependencies.

```python
# pgleaderlock/_logging.py
from pgleaderlock._logging import get_logger
logger = get_logger("pgleaderlock")
```

Log messages include structured context:

```
pgleaderlock | state_change from=follower to=acquiring key1=1234 key2=5678
pgleaderlock | lock_acquired key1=1234 key2=5678 attempt=3
pgleaderlock | health_check_failed error="connection closed" key1=1234 key2=5678
```

Logger name: `"pgleaderlock"`.

---

## 8. Project Structure

```
pgleaderlock/
├── docs/
│   └── SPEC-LIBRARY.md          # this file
├── src/
│   └── pgleaderlock/
│       ├── __init__.py           # public API re-exports
│       ├── _logging.py           # stdlib logging wrapper
│       ├── errors.py             # exception hierarchy
│       ├── lock.py               # LeaderLock (FSM + worker thread)
│       ├── models.py             # LockState enum, RetryContext, callback types
│       ├── retry.py              # RetryStrategy protocol + bundled implementations
│       └── py.typed              # PEP 561 marker
├── tests/
│   ├── test_lock.py
│   ├── test_retry.py
│   └── conftest.py               # fixtures (mock connection, etc.)
├── pyproject.toml
└── README.md
```

Build system: **hatchling** with `src/` layout.

---

## 9. CLI Tool (`pgleaderlock-cli`)

A **separate CLI entrypoint** for testing and operational verification. Uses **click** + **loguru**.

### 9.1 Commands

#### `pgleaderlock run`

Connect, participate in leader election, and log all state transitions until interrupted.

```
pgleaderlock run --dsn postgresql://... --key1 1234 --key2 5678 \
    --health-interval 5.0 --retry-base 1.0 --retry-max 30.0
```

| Option | Default | Description |
|---|---|---|
| `--dsn` | `$PG_DSN` | PostgreSQL connection string |
| `--key1` | *(required)* | Advisory lock key 1 |
| `--key2` | *(required)* | Advisory lock key 2 |
| `--health-interval` | `5.0` | Health check interval (seconds) |
| `--retry-base` | `1.0` | Base delay for exponential backoff |
| `--retry-max` | `30.0` | Maximum delay for exponential backoff |
| `--reconnect-grace` | `None` | Flap damping grace period (seconds); omit to disable |
| `--no-auto-reacquire` | `False` | Disable re-acquisition after loss |

**Behaviour:**
- Registers SIGTERM and SIGINT handlers that call `lock.shutdown()`.
- Logs all state transitions and callback events via loguru.
- Exit code 0 on clean shutdown.

#### `pgleaderlock acquire` *(optional, v1+)*

Single-shot acquisition attempt. Exits 0 if acquired, 1 if not.

#### `pgleaderlock status` *(optional, v1+)*

Queries `pg_locks` for the given advisory lock key. Informational only — not authoritative for "who is leader".

### 9.2 CLI Logging

The CLI configures loguru to intercept the `pgleaderlock` stdlib logger (same pattern as the reference ndmatter app):

```python
import logging
from loguru import logger

class InterceptHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None: ...

pg_logger = logging.getLogger("pgleaderlock")
pg_logger.handlers = [InterceptHandler()]
pg_logger.propagate = False
```

### 9.3 CLI Project Placement

The CLI lives within the same package as a `cli` module:

```
src/pgleaderlock/cli.py
```

Entrypoint in `pyproject.toml`:

```toml
[project.scripts]
pgleaderlock = "pgleaderlock.cli:main"
```

---

## 10. Dependencies

### Library (core)

| Package | Purpose |
|---|---|
| `psycopg[binary]` >= 3.1 | PostgreSQL driver (async mode) |

### CLI (optional extra)

| Package | Purpose |
|---|---|
| `click` >= 8.1 | CLI framework |
| `loguru` >= 0.7 | CLI logging |

```toml
[project.optional-dependencies]
cli = ["click>=8.1", "loguru>=0.7"]
```

---

## 11. Testing Strategy

- **Unit tests:** FSM transitions tested with a mock async connection (no real DB). Inject `connect_fn` that returns a mock.
- **Retry tests:** Deterministic via injectable `time_fn` / `sleep_fn` (or by patching `asyncio.sleep`).
- **Integration tests:** Against a real PostgreSQL instance (dockerised). Verify mutual exclusion with two competing `LeaderLock` instances.
- **Concurrency tests:** Concurrent `start()` / `shutdown()` / `step_down()` calls within the event loop.

```bash
uv run pytest tests/
```

---

## 12. Resolved Design Decisions

1. **`step_down()` blocks with timeout.** Awaits until RELEASING completes. On timeout, cycles the connection (force-release via session termination) to guarantee the lock is freed.
2. **`wait_for_leadership(timeout)` is provided.** Returns `True` if LEADER state reached, `False` on timeout. Useful for startup synchronisation.
3. **Reconnection uses the same `RetryStrategy`.** No separate reconnection config — the same back-off governs both acquisition retries and reconnection attempts.
4. **Context manager supported.** `async with lock:` calls `start()` on enter and `shutdown()` on exit.
