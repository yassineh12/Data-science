import random
import threading

import pytest

from dsops.resilience import (
    Bulkhead,
    BulkheadFullError,
    CircuitBreaker,
    CircuitOpenError,
    RetryError,
    RetryPolicy,
    TokenBucket,
    retry,
)


class FakeClock:
    def __init__(self, t: float = 0.0):
        self.t = t

    def __call__(self) -> float:
        return self.t

    def sleep(self, s: float) -> None:
        self.t += s


# ------------------------------------------------------------------ retry


def test_retry_succeeds_after_transient_failures():
    calls, sleeps = [], []

    @retry(max_attempts=4, base_delay=0.5, jitter="none", sleep=sleeps.append)
    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise ConnectionError("boom")
        return "ok"

    assert flaky() == "ok"
    assert len(calls) == 3
    assert sleeps == [0.5, 1.0]  # exponential


def test_retry_gives_up_with_retry_error():
    @retry(max_attempts=2, base_delay=0, sleep=lambda s: None)
    def always():
        raise TimeoutError("nope")

    with pytest.raises(RetryError) as ei:
        always()
    assert ei.value.attempts == 2
    assert isinstance(ei.value.last_exception, TimeoutError)


def test_retry_does_not_retry_unlisted_exceptions():
    calls = []

    @retry(max_attempts=5, retry_on=(ConnectionError,), sleep=lambda s: None)
    def bad():
        calls.append(1)
        raise ValueError("bug, not transient")

    with pytest.raises(ValueError):
        bad()
    assert len(calls) == 1


def test_retry_giveup_predicate():
    calls = []

    @retry(max_attempts=5, giveup=lambda e: "fatal" in str(e), sleep=lambda s: None)
    def f():
        calls.append(1)
        raise RuntimeError("fatal")

    with pytest.raises(RuntimeError):
        f()
    assert len(calls) == 1


def test_backoff_caps_and_full_jitter_bounds():
    pol = RetryPolicy(max_attempts=10, base_delay=1, max_delay=5, jitter="none")
    assert [pol.backoff(a) for a in range(1, 6)] == [1, 2, 4, 5, 5]
    jit = RetryPolicy(base_delay=1, max_delay=5, rng=random.Random(42))
    for a in range(1, 10):
        assert 0 <= jit.backoff(a) <= 5


async def test_retry_async():
    calls = []

    async def no_sleep(_):
        return None

    @retry(max_attempts=3, base_delay=0, sleep=no_sleep)
    async def flaky():
        calls.append(1)
        if len(calls) < 2:
            raise ConnectionError
        return 42

    assert await flaky() == 42


def test_retry_rejects_conflicting_args():
    with pytest.raises(TypeError):
        retry(policy=RetryPolicy(), max_attempts=3)


# -------------------------------------------------------- circuit breaker


def test_circuit_opens_then_half_opens_then_closes():
    clock = FakeClock()
    cb = CircuitBreaker("svc", failure_threshold=2, recovery_timeout=10, clock=clock)

    def fail():
        raise ConnectionError

    for _ in range(2):
        with pytest.raises(ConnectionError):
            cb.call(fail)
    assert cb.state == "open"

    with pytest.raises(CircuitOpenError) as ei:
        cb.call(lambda: "never called")
    assert ei.value.retry_after == pytest.approx(10)

    clock.t = 10
    assert cb.state == "half_open"
    assert cb.call(lambda: "probe ok") == "probe ok"
    assert cb.state == "closed"
    assert cb.failure_count == 0


def test_circuit_half_open_failure_reopens():
    clock = FakeClock()
    cb = CircuitBreaker(failure_threshold=1, recovery_timeout=5, clock=clock)
    with pytest.raises(ZeroDivisionError):
        cb.call(lambda: 1 / 0)
    clock.t = 5
    with pytest.raises(ZeroDivisionError):
        cb.call(lambda: 1 / 0)
    assert cb.state == "open"


def test_circuit_ignores_unexpected_exceptions():
    cb = CircuitBreaker(failure_threshold=1, expected_exceptions=(ConnectionError,))

    @cb
    def f():
        raise KeyError("caller bug")

    with pytest.raises(KeyError):
        f()
    assert cb.state == "closed"


def test_circuit_state_change_hook():
    changes = []
    cb = CircuitBreaker(failure_threshold=1, on_state_change=lambda a, b: changes.append((a, b)))
    with pytest.raises(ValueError):
        cb.call(lambda: (_ for _ in ()).throw(ValueError()))
    cb.reset()
    assert changes == [("closed", "open"), ("open", "closed")]


async def test_circuit_async_decorator():
    cb = CircuitBreaker(failure_threshold=1)

    @cb
    async def f():
        raise ConnectionError

    with pytest.raises(ConnectionError):
        await f()
    with pytest.raises(CircuitOpenError):
        await f()


# ------------------------------------------------------------ token bucket


def test_token_bucket_burst_and_refill():
    clock = FakeClock()
    tb = TokenBucket(rate=2, capacity=4, clock=clock, sleep=clock.sleep)
    assert all(tb.try_acquire() for _ in range(4))
    assert not tb.try_acquire()
    clock.t += 1.0  # 2 tokens back
    assert tb.try_acquire() and tb.try_acquire()
    assert not tb.try_acquire()


def test_token_bucket_blocking_acquire_waits():
    clock = FakeClock()
    tb = TokenBucket(rate=10, capacity=1, clock=clock, sleep=clock.sleep)
    assert tb.acquire()
    assert tb.acquire()
    assert clock.t == pytest.approx(0.1)


def test_token_bucket_timeout():
    clock = FakeClock()
    tb = TokenBucket(rate=1, capacity=1, clock=clock, sleep=clock.sleep)
    tb.acquire()
    assert tb.acquire(timeout=0.5) is False


def test_token_bucket_rejects_oversized_request():
    with pytest.raises(ValueError):
        TokenBucket(rate=1, capacity=2).try_acquire(3)


# ---------------------------------------------------------------- bulkhead


def test_bulkhead_limits_concurrency():
    bh = Bulkhead(1)
    entered, release = threading.Event(), threading.Event()

    def hold():
        with bh:
            entered.set()
            release.wait(2)

    t = threading.Thread(target=hold)
    t.start()
    entered.wait(2)
    with pytest.raises(BulkheadFullError):
        with bh:
            pass
    release.set()
    t.join()
    with bh:  # free again
        pass
