from __future__ import annotations

import os
import random

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--pg-dsn",
        action="store",
        default=None,
        help="PostgreSQL DSN for live tests",
    )


@pytest.fixture(scope="session")
def pg_dsn(request):
    """Get PostgreSQL DSN from CLI option or environment."""
    dsn = request.config.getoption("--pg-dsn") or os.environ.get("PGLEADERLOCK_TEST_DSN")
    if not dsn:
        pytest.skip("No PostgreSQL DSN provided (use --pg-dsn or PGLEADERLOCK_TEST_DSN)")
    return dsn


@pytest.fixture
def lock_keys():
    """Generate random lock key pair to avoid collisions between tests."""
    return (random.randint(1, 2**31 - 1), random.randint(1, 2**31 - 1))
