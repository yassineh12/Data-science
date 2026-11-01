"""Resilience primitives for calls that cross a network boundary.

Everything here accepts an injectable ``clock`` / ``sleep`` so behaviour can be
tested deterministically without real waiting.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import logging
import random
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator, TypeVar

from .errors import DsopsError

log = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


# --------------------------------------------------------------------------- #
# Retry with exponential backoff
# --------------------------------------------------------------------------- #


class RetryError(DsopsError):
    """Raised when every attempt failed. ``last_exception`` holds the final cause."""

    def __init__(self, attempts: int, last_exception: BaseException):
        super().__init__(f"gave up after {attempts} attempt(s): {last_exception!r}")
        self.attempts = attempts
        self.last_exception = last_exception


@dataclass
class RetryPolicy:
    """How to retry.

    ``jitter="full"`` uses the AWS "full jitter" strategy: a uniform delay in
    ``[0, computed_backoff]``. It spreads retries out so a fleet of clients does
    not hammer a recovering service in lockstep.
    """

    max_attempts: int = 3
    base_delay: float = 0.1
    max_delay: float = 30.0
    multiplier: float = 2.0
    jitter: str = "full"  # "full" | "none"
    retry_on: tuple[type[BaseException], ...] = (Exception,)
    giveup: Callable[[BaseException], bool] | None = None
    rng: random.Random = field(default_factory=random.Random, repr=False)

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.jitter not in ("full", "none"):
            raise ValueError("jitter must be 'full' or 'none'")

    def backoff(self, attempt: int) -> float:
        """Delay to wait after the ``attempt``-th failure (1-based)."""
        raw = min(self.max_delay, self.base_delay * (self.multiplier ** (attempt - 1)))
        return self.rng.uniform(0, raw) if self.jitter == "full" else raw

    def should_retry(self, exc: BaseException) -> bool:
        if not isinstance(exc, self.retry_on):
            return False
        return not (self.giveup and self.giveup(exc))

    def delays(self) -> Iterator[float]:
        for attempt in range(1, self.max_attempts):
            yield self.backoff(attempt)


def retry(
    _func: F | None = None,
    *,
    policy: RetryPolicy | None = None,
    sleep: Callable[[float], Any] | None = None,
    on_retry: Callable[[int, BaseException, float], None] | None = None,
    **policy_kwargs: Any,
) -> Any:
    """Retry a sync or async callable.

    Usable bare (``@retry``) or configured (``@retry(max_attempts=5, retry_on=(IOError,))``).
    Exceptions not matched by ``retry_on`` propagate immediately and unchanged.
    When attempts run out a :class:`RetryError` is raised from the last exception.
    """
    if policy is not None and policy_kwargs:
        raise TypeError("pass either policy= or policy keyword arguments, not both")
    pol = policy or RetryPolicy(**policy_kwargs)

    def decorate(func: F) -> F:
        if inspect.iscoroutinefunction(func):
            do_sleep = sleep or asyncio.sleep

            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                for attempt in range(1, pol.max_attempts + 1):
                    try:
                        return await func(*args, **kwargs)
                    except BaseException as exc:  # noqa: BLE001 - filtered below
                        if not pol.should_retry(exc):
                            raise
                        if attempt == pol.max_attempts:
                            raise RetryError(attempt, exc) from exc
                        delay = pol.backoff(attempt)
                        _notify(func, attempt, exc, delay, on_retry)
                        res = do_sleep(delay)
                        if inspect.isawaitable(res):
                            await res
                raise AssertionError("unreachable")

            return async_wrapper  # type: ignore[return-value]

        do_sleep_sync = sleep or time.sleep

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            for attempt in range(1, pol.max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except BaseException as exc:  # noqa: BLE001 - filtered below
                    if not pol.should_retry(exc):
                        raise
                    if attempt == pol.max_attempts:
                        raise RetryError(attempt, exc) from exc
                    delay = pol.backoff(attempt)
                    _notify(func, attempt, exc, delay, on_retry)
                    do_sleep_sync(delay)
            raise AssertionError("unreachable")

        return wrapper  # type: ignore[return-value]

    return decorate(_func) if _func is not None else decorate


def _notify(
    func: Callable[..., Any],
    attempt: int,
    exc: BaseException,
    delay: float,
    hook: Callable[[int, BaseException, float], None] | None,
) -> None:
    log.warning(
        "retrying %s after attempt %d failed (%r); sleeping %.3fs",
        getattr(func, "__qualname__", func), attempt, exc, delay,
    )
    if hook:
        hook(attempt, exc, delay)


# --------------------------------------------------------------------------- #
# Circuit breaker
# --------------------------------------------------------------------------- #


class CircuitOpenError(DsopsError):
    """Raised instead of calling the protected function while the circuit is open."""

    def __init__(self, name: str, retry_after: float):
        super().__init__(f"circuit {name!r} is open; retry in {retry_after:.2f}s")
        self.name = name
        self.retry_after = retry_after


class CircuitBreaker:
    """Stop calling a dependency that keeps failing, then probe it cautiously.

    closed     calls pass through; consecutive failures are counted
    open       calls fail fast with :class:`CircuitOpenError` for ``recovery_timeout``
    half_open  up to ``half_open_max_calls`` trial calls; one success closes, one failure re-opens
    """

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

    def __init__(
        self,
        name: str = "default",
        *,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        half_open_max_calls: int = 1,
        expected_exceptions: tuple[type[BaseException], ...] = (Exception,),
        clock: Callable[[], float] = time.monotonic,
        on_state_change: Callable[[str, str], None] | None = None,
    ):
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls
        self.expected_exceptions = expected_exceptions
        self._clock = clock
        self._on_state_change = on_state_change
        self._lock = threading.Lock()
        self._state = self.CLOSED
        self._failures = 0
        self._opened_at = 0.0
        self._half_open_inflight = 0

    @property
    def state(self) -> str:
        with self._lock:
            self._maybe_half_open()
            return self._state

    @property
    def failure_count(self) -> int:
        return self._failures

    def _transition(self, new: str) -> None:
        old, self._state = self._state, new
        if old != new:
            log.info("circuit %s: %s -> %s", self.name, old, new)
            if self._on_state_change:
                self._on_state_change(old, new)

    def _maybe_half_open(self) -> None:
        if self._state == self.OPEN and self._clock() - self._opened_at >= self.recovery_timeout:
            self._transition(self.HALF_OPEN)
            self._half_open_inflight = 0

    def _before_call(self) -> None:
        with self._lock:
            self._maybe_half_open()
            if self._state == self.OPEN:
                remaining = self.recovery_timeout - (self._clock() - self._opened_at)
                raise CircuitOpenError(self.name, max(0.0, remaining))
            if self._state == self.HALF_OPEN:
                if self._half_open_inflight >= self.half_open_max_calls:
                    raise CircuitOpenError(self.name, 0.0)
                self._half_open_inflight += 1

    def _on_success(self) -> None:
        with self._lock:
            self._failures = 0
            if self._state == self.HALF_OPEN:
                self._half_open_inflight = max(0, self._half_open_inflight - 1)
            self._transition(self.CLOSED)

    def _on_failure(self) -> None:
        with self._lock:
            self._failures += 1
            if self._state == self.HALF_OPEN or self._failures >= self.failure_threshold:
                self._opened_at = self._clock()
                self._half_open_inflight = 0
                self._transition(self.OPEN)

    def _on_ignored(self) -> None:
        with self._lock:
            if self._state == self.HALF_OPEN:
                self._half_open_inflight = max(0, self._half_open_inflight - 1)

    def reset(self) -> None:
        with self._lock:
            self._failures = 0
            self._half_open_inflight = 0
            self._transition(self.CLOSED)

    def call(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        self._before_call()
        try:
            result = func(*args, **kwargs)
        except self.expected_exceptions:
            self._on_failure()
            raise
        except BaseException:
            self._on_ignored()
            raise
        self._on_success()
        return result

    async def call_async(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        self._before_call()
        try:
            result = await func(*args, **kwargs)
        except self.expected_exceptions:
            self._on_failure()
            raise
        except BaseException:
            self._on_ignored()
            raise
        self._on_success()
        return result

    def __call__(self, func: F) -> F:
        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                return await self.call_async(func, *args, **kwargs)

            return async_wrapper  # type: ignore[return-value]

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return self.call(func, *args, **kwargs)

        return wrapper  # type: ignore[return-value]


# --------------------------------------------------------------------------- #
# Token-bucket rate limiter
# --------------------------------------------------------------------------- #


class TokenBucket:
    """Thread-safe token bucket: ``rate`` tokens/second, bursts up to ``capacity``."""

    def __init__(
        self,
        rate: float,
        capacity: float | None = None,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ):
        if rate <= 0:
            raise ValueError("rate must be > 0")
        self.rate = float(rate)
        self.capacity = float(capacity if capacity is not None else rate)
        self._tokens = self.capacity
        self._clock = clock
        self._sleep = sleep
        self._last = clock()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        now = self._clock()
        self._tokens = min(self.capacity, self._tokens + (now - self._last) * self.rate)
        self._last = now

    @property
    def available(self) -> float:
        with self._lock:
            self._refill()
            return self._tokens

    def try_acquire(self, tokens: float = 1.0) -> bool:
        if tokens > self.capacity:
            raise ValueError("requested more tokens than bucket capacity")
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    def acquire(self, tokens: float = 1.0, timeout: float | None = None) -> bool:
        """Block until ``tokens`` are available. Returns False if ``timeout`` elapses first."""
        if tokens > self.capacity:
            raise ValueError("requested more tokens than bucket capacity")
        deadline = None if timeout is None else self._clock() + timeout
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return True
                wait = (tokens - self._tokens) / self.rate
            if deadline is not None:
                remaining = deadline - self._clock()
                if remaining <= 0:
                    return False
                wait = min(wait, remaining)
            self._sleep(wait)

    def limit(self, func: F) -> F:
        """Decorator: every call consumes one token, blocking if necessary."""

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            self.acquire()
            return func(*args, **kwargs)

        return wrapper  # type: ignore[return-value]


# --------------------------------------------------------------------------- #
# Bulkhead (concurrency limiter)
# --------------------------------------------------------------------------- #


class BulkheadFullError(DsopsError):
    pass


class Bulkhead:
    """Cap concurrent calls into a dependency so one slow service cannot exhaust all workers."""

    def __init__(self, max_concurrent: int, *, timeout: float | None = 0.0, name: str = "default"):
        if max_concurrent < 1:
            raise ValueError("max_concurrent must be >= 1")
        self.name = name
        self.max_concurrent = max_concurrent
        self.timeout = timeout
        self._sem = threading.BoundedSemaphore(max_concurrent)

    def __enter__(self) -> "Bulkhead":
        acquired = (
            self._sem.acquire(blocking=False)
            if self.timeout == 0
            else self._sem.acquire(timeout=self.timeout)
        )
        if not acquired:
            raise BulkheadFullError(f"bulkhead {self.name!r} full ({self.max_concurrent} in flight)")
        return self

    def __exit__(self, *exc: Any) -> None:
        self._sem.release()

    def __call__(self, func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            with self:
                return func(*args, **kwargs)

        return wrapper  # type: ignore[return-value]
