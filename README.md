# pgleaderlock

Distributed leader election using PostgreSQL session-level advisory locks.

When a process acquires the lock, it becomes the leader. When the connection
drops, PostgreSQL automatically releases the lock, letting another process
take over. No tables, no polling queries against shared state -- just
`pg_try_advisory_lock`.

## Install

```
pip install pgleaderlock            # library only
pip install pgleaderlock[cli]       # includes CLI tool
```

Requires Python 3.13+ and PostgreSQL. Uses [psycopg 3](https://www.psycopg.org/psycopg3/)
for async connections.

## Quick start

```python
import asyncio
from pgleaderlock import LeaderLock

async def main():
    lock = LeaderLock(
        dsn="host=localhost dbname=myapp user=myuser",
        key1=1000,
        key2=1,
    )

    @lock.on_acquired
    def became_leader():
        print("I am the leader now")

    @lock.on_lost
    def lost_leadership():
        print("No longer the leader")

    async with lock:
        # runs until interrupted
        await asyncio.Event().wait()

asyncio.run(main())
```

## How it works

The lock goes through a state machine:

```
stopped -> follower -> acquiring -> leader -> releasing -> stopped
                                      |
                                      +-> reconnecting -> leader (recovered)
                                                       -> follower (lost)
```

- **stopped**: not running.
- **follower**: connected, waiting to attempt acquisition.
- **acquiring**: calling `pg_try_advisory_lock(key1, key2)`.
- **leader**: lock held. Periodic health checks (`SELECT 1`) confirm the connection is alive.
- **reconnecting**: connection lost while leader. Tries to reconnect within the grace period and re-acquire.
- **releasing**: explicitly unlocking via `pg_advisory_unlock`.

## Lock keys

Advisory locks use a pair of 32-bit integers `(key1, key2)` as the lock
identifier. Pick a convention -- for example, `key1` identifies the service
and `key2` identifies the resource:

```python
SCHEDULER_LOCK = (5001, 1)
CLEANUP_LOCK   = (5001, 2)

lock = LeaderLock(dsn=dsn, key1=SCHEDULER_LOCK[0], key2=SCHEDULER_LOCK[1])
```

Any process using the same `(key1, key2)` on the same database competes for
the same lock.

## Constructor parameters

```python
LeaderLock(
    dsn="host=localhost dbname=myapp",
    key1=1000,
    key2=1,
    retry_strategy=ExponentialBackoff(base_s=1.0, max_s=30.0),
    health_interval_s=5.0,
    reconnect_grace_s=10.0,
    auto_reacquire=True,
    shutdown_event=some_event,
    connect_fn=my_connect,
)
```

| Parameter | Default | Description |
|---|---|---|
| `dsn` | required | psycopg connection string |
| `key1`, `key2` | required | Advisory lock key pair (int32) |
| `retry_strategy` | `ExponentialBackoff()` | Controls delay between retries |
| `health_interval_s` | `5.0` | Seconds between health checks while leader |
| `reconnect_grace_s` | `None` | If set, time allowed to reconnect and re-acquire before declaring leadership lost |
| `auto_reacquire` | `True` | After losing leadership, go back to follower and try again |
| `shutdown_event` | `None` | External `asyncio.Event` to trigger shutdown |
| `connect_fn` | `None` | Custom async callable returning a `psycopg.AsyncConnection` (bypasses `dsn`) |

## Callbacks

Register callbacks as decorators or by calling the method directly. Callbacks
can be sync or async.

```python
lock = LeaderLock(dsn=dsn, key1=1, key2=1)

@lock.on_acquired
def start_work():
    print("acquired")

@lock.on_released
def stop_work():
    print("released cleanly")

@lock.on_lost
def handle_loss():
    # connection died or lock taken by another process
    print("lost leadership")

@lock.on_acquire_failed
def not_yet():
    print("someone else holds the lock, will retry")

@lock.on_state_change
def log_state(from_state, to_state):
    print(f"{from_state.value} -> {to_state.value}")

@lock.on_error
def on_error(exc):
    print(f"error: {exc}")
```

## Retry strategies

Three built-in strategies. All implement the `RetryStrategy` protocol.

```python
from pgleaderlock import ExponentialBackoff, FixedInterval, DecorrelatedJitter

# Exponential: base * multiplier^(attempt-1), capped at max_s
ExponentialBackoff(base_s=1.0, max_s=30.0, multiplier=2.0)

# Fixed: constant delay
FixedInterval(interval_s=5.0)

# Decorrelated jitter (AWS-style): uniform(base, prev_delay * 3), capped
DecorrelatedJitter(base_s=1.0, max_s=30.0)
```

Write your own by implementing:

```python
from pgleaderlock import RetryStrategy, RetryContext

class MyStrategy:
    def next_delay_s(self, ctx: RetryContext) -> float | None:
        if ctx.attempt > 10:
            return None  # give up
        return 2.0
```

Returning `None` stops retrying.

## Reconnect grace period

By default, a failed health check means leadership is immediately lost. Set
`reconnect_grace_s` to allow time to reconnect and re-acquire the lock without
triggering `on_lost`:

```python
lock = LeaderLock(
    dsn=dsn, key1=1, key2=1,
    reconnect_grace_s=15.0,   # 15 seconds to recover
    health_interval_s=3.0,
)
```

If the connection comes back and the lock is still available within the grace
period, leadership continues uninterrupted.

## Voluntary step-down

A leader can give up the lock without shutting down:

```python
async with lock:
    await lock.wait_for_leadership()
    # do some work...
    await lock.step_down()
    # lock transitions: leader -> releasing -> follower -> acquiring -> ...
```

With `auto_reacquire=True` (the default), the process re-enters the election
after stepping down.

## Custom connection function

If your application already manages database connections or needs specific
connection parameters, pass a `connect_fn`:

```python
import psycopg

async def my_connect():
    return await psycopg.AsyncConnection.connect(
        "host=/var/run/postgresql dbname=myapp",
        autocommit=True,
    )

lock = LeaderLock(dsn="", key1=1, key2=1, connect_fn=my_connect)
```

The function must return a `psycopg.AsyncConnection` with autocommit enabled.

## Shutdown

Three ways to stop:

```python
# 1. Context manager (calls shutdown with 10s timeout)
async with lock:
    await some_event.wait()

# 2. Explicit shutdown
await lock.shutdown(timeout_s=5.0)

# 3. External event
shutdown = asyncio.Event()
lock = LeaderLock(dsn=dsn, key1=1, key2=1, shutdown_event=shutdown)
async with lock:
    shutdown.set()  # triggers clean shutdown
```

Shutdown releases the lock if held, closes the connection, and transitions
to `stopped`.

## CLI

The `pgleaderlock[cli]` extra installs a `pgleaderlock` command for testing
and monitoring:

```
pgleaderlock run \
    --dsn "host=localhost dbname=myapp user=myuser" \
    --key1 1000 --key2 1 \
    --health-interval 3.0 \
    --retry-base 1.0 --retry-max 30.0 \
    --reconnect-grace 10.0
```

The DSN can also be set via the `PG_DSN` environment variable:

```
export PG_DSN="host=/var/run/postgresql dbname=myapp user=myuser"
pgleaderlock run --key1 1000 --key2 1
```

For Unix socket connections:

```
pgleaderlock run \
    --dsn "host=/var/run/postgresql dbname=myapp user=myuser" \
    --key1 1000 --key2 1
```

## Exceptions

All exceptions inherit from `PgLeaderLockError`:

- `ConnectionError` -- failed to connect after retries exhausted
- `LockError` -- `pg_advisory_unlock` returned false (lock wasn't held)
- `ShutdownError` -- timeout during shutdown

## Properties

```python
lock.state       # LockState enum: STOPPED, FOLLOWER, ACQUIRING, LEADER, RECONNECTING, RELEASING
lock.is_leader   # True when state is LEADER
```
