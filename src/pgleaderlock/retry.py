from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from pgleaderlock.models import RetryContext


@runtime_checkable
class RetryStrategy(Protocol):
    """Protocol for retry back-off strategies."""

    def next_delay_s(self, ctx: RetryContext) -> float | None:
        """Return seconds to sleep before next attempt, or None to stop retrying."""
        ...


@dataclass(frozen=True, slots=True)
class ExponentialBackoff:
    """Exponential back-off: min(base * multiplier^(attempt-1), max_s)."""

    base_s: float = 1.0
    max_s: float = 30.0
    multiplier: float = 2.0

    def next_delay_s(self, ctx: RetryContext) -> float:
        return min(self.base_s * self.multiplier ** (ctx.attempt - 1), self.max_s)


@dataclass(frozen=True, slots=True)
class FixedInterval:
    """Constant delay between retries."""

    interval_s: float = 5.0

    def next_delay_s(self, ctx: RetryContext) -> float:
        return self.interval_s


@dataclass(slots=True)
class DecorrelatedJitter:
    """AWS-style decorrelated jitter: min(max_s, uniform(base_s, prev_delay * 3))."""

    base_s: float = 1.0
    max_s: float = 30.0
    _rng: random.Random = field(default_factory=random.Random)
    _prev_delay: float | None = field(default=None, init=False)

    def next_delay_s(self, ctx: RetryContext) -> float:
        prev = self._prev_delay if self._prev_delay is not None else self.base_s
        delay = min(self.max_s, self._rng.uniform(self.base_s, prev * 3))
        self._prev_delay = delay
        return delay
