import random

import pytest
from pgleaderlock.models import RetryContext
from pgleaderlock.retry import DecorrelatedJitter, ExponentialBackoff, FixedInterval, RetryStrategy


class TestExponentialBackoff:
    def test_first_attempt(self):
        s = ExponentialBackoff(base_s=1.0, max_s=30.0, multiplier=2.0)
        ctx = RetryContext(attempt=1, elapsed_s=0.0)
        assert s.next_delay_s(ctx) == 1.0

    def test_second_attempt(self):
        s = ExponentialBackoff(base_s=1.0, max_s=30.0, multiplier=2.0)
        ctx = RetryContext(attempt=2, elapsed_s=1.0)
        assert s.next_delay_s(ctx) == 2.0

    def test_third_attempt(self):
        s = ExponentialBackoff(base_s=1.0, max_s=30.0, multiplier=2.0)
        ctx = RetryContext(attempt=3, elapsed_s=3.0)
        assert s.next_delay_s(ctx) == 4.0

    def test_capped_at_max(self):
        s = ExponentialBackoff(base_s=1.0, max_s=10.0, multiplier=2.0)
        ctx = RetryContext(attempt=100, elapsed_s=100.0)
        assert s.next_delay_s(ctx) == 10.0

    def test_custom_params(self):
        s = ExponentialBackoff(base_s=0.5, max_s=5.0, multiplier=3.0)
        ctx = RetryContext(attempt=1, elapsed_s=0.0)
        assert s.next_delay_s(ctx) == 0.5
        ctx2 = RetryContext(attempt=2, elapsed_s=0.5)
        assert s.next_delay_s(ctx2) == 1.5
        ctx3 = RetryContext(attempt=3, elapsed_s=2.0)
        assert s.next_delay_s(ctx3) == 4.5
        ctx4 = RetryContext(attempt=4, elapsed_s=6.5)
        assert s.next_delay_s(ctx4) == 5.0  # capped

    def test_conforms_to_protocol(self):
        assert isinstance(ExponentialBackoff(), RetryStrategy)


class TestFixedInterval:
    def test_constant_delay(self):
        s = FixedInterval(interval_s=3.0)
        for attempt in range(1, 10):
            ctx = RetryContext(attempt=attempt, elapsed_s=float(attempt))
            assert s.next_delay_s(ctx) == 3.0

    def test_default(self):
        s = FixedInterval()
        ctx = RetryContext(attempt=1, elapsed_s=0.0)
        assert s.next_delay_s(ctx) == 5.0

    def test_conforms_to_protocol(self):
        assert isinstance(FixedInterval(), RetryStrategy)


class TestDecorrelatedJitter:
    def test_deterministic_with_seed(self):
        rng = random.Random(42)
        s = DecorrelatedJitter(base_s=1.0, max_s=30.0, _rng=rng)
        ctx = RetryContext(attempt=1, elapsed_s=0.0)
        d1 = s.next_delay_s(ctx)
        assert 1.0 <= d1 <= 3.0  # uniform(1.0, 1.0*3)

    def test_within_bounds(self):
        rng = random.Random(123)
        s = DecorrelatedJitter(base_s=1.0, max_s=5.0, _rng=rng)
        for i in range(1, 20):
            ctx = RetryContext(attempt=i, elapsed_s=float(i))
            delay = s.next_delay_s(ctx)
            assert 1.0 <= delay <= 5.0

    def test_prev_delay_tracking(self):
        rng = random.Random(0)
        s = DecorrelatedJitter(base_s=1.0, max_s=100.0, _rng=rng)
        ctx = RetryContext(attempt=1, elapsed_s=0.0)
        d1 = s.next_delay_s(ctx)
        assert d1 >= 1.0
        # Now _prev_delay should be d1
        assert s._prev_delay == d1

    def test_conforms_to_protocol(self):
        assert isinstance(DecorrelatedJitter(), RetryStrategy)

    def test_reproducible_sequence(self):
        """Same seed produces same sequence."""
        delays_a = []
        delays_b = []
        for seed_delays in [delays_a, delays_b]:
            rng = random.Random(99)
            s = DecorrelatedJitter(base_s=1.0, max_s=30.0, _rng=rng)
            for i in range(1, 6):
                ctx = RetryContext(attempt=i, elapsed_s=float(i))
                seed_delays.append(s.next_delay_s(ctx))
        assert delays_a == delays_b


class TestRetryContext:
    def test_frozen(self):
        ctx = RetryContext(attempt=1, elapsed_s=0.0)
        with pytest.raises(AttributeError):
            ctx.attempt = 2  # type: ignore[misc]

    def test_defaults(self):
        ctx = RetryContext(attempt=1, elapsed_s=0.0)
        assert ctx.last_error is None
