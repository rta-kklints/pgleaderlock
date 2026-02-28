from __future__ import annotations

import asyncio
import logging
import signal
import sys

try:
    import click
    from loguru import logger
except ImportError:
    raise SystemExit(
        "CLI dependencies not installed. Install with: pip install pgleaderlock[cli]"
    )

from pgleaderlock import LeaderLock, LockState
from pgleaderlock.retry import ExponentialBackoff


class InterceptHandler(logging.Handler):
    """Route stdlib logging to loguru."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        frame, depth = logging.currentframe(), 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1
        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def _setup_logging() -> None:
    pg_logger = logging.getLogger("pgleaderlock")
    pg_logger.handlers = [InterceptHandler()]
    pg_logger.setLevel(logging.DEBUG)
    pg_logger.propagate = False


@click.group()
def main() -> None:
    """pgleaderlock — distributed leader election via PostgreSQL advisory locks."""
    pass


@main.command()
@click.option("--dsn", envvar="PG_DSN", required=True, help="PostgreSQL connection string")
@click.option("--key1", type=int, required=True, help="Advisory lock key 1")
@click.option("--key2", type=int, required=True, help="Advisory lock key 2")
@click.option("--health-interval", type=float, default=5.0, help="Health check interval (seconds)")
@click.option("--retry-base", type=float, default=1.0, help="Base delay for exponential backoff")
@click.option("--retry-max", type=float, default=30.0, help="Maximum delay for exponential backoff")
@click.option("--reconnect-grace", type=float, default=None, help="Flap damping grace period (seconds)")
@click.option("--no-auto-reacquire", is_flag=True, default=False, help="Disable re-acquisition after loss")
def run(
    dsn: str,
    key1: int,
    key2: int,
    health_interval: float,
    retry_base: float,
    retry_max: float,
    reconnect_grace: float | None,
    no_auto_reacquire: bool,
) -> None:
    """Connect, participate in leader election, and log state transitions."""
    _setup_logging()

    async def _run() -> None:
        shutdown_event = asyncio.Event()
        loop = asyncio.get_running_loop()

        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, shutdown_event.set)

        lock = LeaderLock(
            dsn=dsn,
            key1=key1,
            key2=key2,
            retry_strategy=ExponentialBackoff(base_s=retry_base, max_s=retry_max),
            health_interval_s=health_interval,
            reconnect_grace_s=reconnect_grace,
            auto_reacquire=not no_auto_reacquire,
            shutdown_event=shutdown_event,
        )

        @lock.on_acquired
        def _acquired() -> None:
            logger.info("Leadership acquired")

        @lock.on_released
        def _released() -> None:
            logger.info("Leadership released")

        @lock.on_lost
        def _lost() -> None:
            logger.warning("Leadership lost")

        @lock.on_acquire_failed
        def _acquire_failed() -> None:
            logger.info("Acquire attempt failed, will retry")

        @lock.on_state_change
        def _state_change(from_state: LockState, to_state: LockState) -> None:
            logger.info(f"State: {from_state.value} → {to_state.value}")

        @lock.on_error
        def _error(exc: Exception) -> None:
            logger.error(f"Error: {exc}")

        async with lock:
            await shutdown_event.wait()
            logger.info("Shutdown signal received")

        logger.info("Clean shutdown complete")

    asyncio.run(_run())
