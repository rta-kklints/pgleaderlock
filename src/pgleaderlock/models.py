from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import Any, Callable


class LockState(enum.Enum):
    """States of the leader lock finite state machine."""

    STOPPED = "stopped"
    FOLLOWER = "follower"
    ACQUIRING = "acquiring"
    LEADER = "leader"
    RECONNECTING = "reconnecting"
    RELEASING = "releasing"


@dataclass(frozen=True, slots=True)
class RetryContext:
    """Context passed to RetryStrategy.next_delay_s."""

    attempt: int
    elapsed_s: float
    last_error: Exception | None = None


Callback = Callable[[], Any]
StateChangeCallback = Callable[[LockState, LockState], Any]
ErrorCallback = Callable[[Exception], Any]
