class PgLeaderLockError(Exception):
    """Base exception for all pgleaderlock errors."""


class ConnectionError(PgLeaderLockError):
    """Failed to establish or maintain a database connection."""


class LockError(PgLeaderLockError):
    """Failed to acquire or release the advisory lock."""


class ShutdownError(PgLeaderLockError):
    """Error during shutdown (e.g., timeout waiting for lifecycle task)."""
