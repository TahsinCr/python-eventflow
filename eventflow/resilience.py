"""Retry, cooldown, and circuit-breaker primitives."""

from __future__ import annotations

import asyncio
import inspect
import math
import random
import threading
import time
from collections.abc import Awaitable, Callable, Hashable
from dataclasses import dataclass
from typing import Generic, Literal, TypeVar

from ._callbacks import close_awaitable, synchronous_result
from .base import (
    validate_finite_duration,
    validate_nonnegative_timeout,
    validate_positive_capacity,
)
from .compatibility import ExceptionGroup
from .errors import CircuitOpenError
from .timing import Deadline

K = TypeVar("K", bound=Hashable)
T = TypeVar("T")
__all__ = [
    "RetryPolicy",
    "RetryObservation",
    "CooldownRegistry",
    "CooldownStats",
    "CircuitBreaker",
    "CircuitBreakerStats",
]


class _UnexpectedAwaitable(TypeError):
    pass


@dataclass(frozen=True, slots=True)
class RetryObservation:
    attempt: int
    elapsed: float
    status: Literal["success", "retry", "error"]
    error: Exception | None = None


class RetryPolicy:
    """Explicit sync/async retries with bounded backoff and optional deadline."""

    def __init__(
        self,
        max_attempts: int = 3,
        *,
        backoff: Literal["fixed", "exponential"] = "exponential",
        initial_delay: float = 0.0,
        max_delay: float = 60.0,
        jitter: float = 0.0,
        observer: Callable[[RetryObservation], None] | None = None,
    ) -> None:
        self._attempts = validate_positive_capacity(max_attempts, name="Retry max attempts")
        if backoff not in ("fixed", "exponential"):
            raise ValueError("backoff must be 'fixed' or 'exponential'")
        self._backoff = backoff
        self._initial = validate_finite_duration(
            initial_delay, name="initial_delay", allow_zero=True
        )
        self._maximum = validate_finite_duration(max_delay, name="max_delay", allow_zero=True)
        if self._maximum < self._initial:
            raise ValueError("max_delay must be at least initial_delay")
        self._jitter = validate_finite_duration(jitter, name="jitter", allow_zero=True)
        if self._jitter > 1:
            raise ValueError("jitter must be between 0 and 1")
        if observer is not None and not callable(observer):
            raise TypeError("observer must be callable")
        self._observer = observer

    def _delay(self, attempt: int) -> float:
        delay = self._initial
        if self._backoff == "exponential" and attempt > 1 and delay > 0:
            if self._maximum == 0 or attempt - 1 >= (math.log2(self._maximum) - math.log2(delay)):
                delay = self._maximum
            else:
                delay = math.ldexp(delay, attempt - 1)
        delay = min(delay, self._maximum)
        if self._jitter:
            delay *= random.uniform(1.0 - self._jitter, 1.0 + self._jitter)
        return min(delay, self._maximum)

    def _observe(self, observation: RetryObservation, primary: Exception | None = None) -> None:
        if self._observer is None:
            return
        try:
            synchronous_result(self._observer(observation))
        except Exception as observer_error:
            if primary is None:
                raise
            raise ExceptionGroup(
                "Retry operation and observer both failed", [primary, observer_error]
            ) from primary

    @staticmethod
    def _should_retry(predicate: Callable[[Exception], bool] | None, error: Exception) -> bool:
        if predicate is None:
            return False
        result = synchronous_result(predicate(error))
        if type(result) is not bool:
            raise TypeError("should_retry must return bool")
        return result

    def run(
        self,
        operation: Callable[[], T],
        *,
        should_retry: Callable[[Exception], bool] | None = None,
        deadline: Deadline | None = None,
    ) -> T:
        """Run sync attempts within a retry budget; running work is not interrupted."""
        if not callable(operation):
            raise TypeError("operation must be callable")
        if should_retry is not None and not callable(should_retry):
            raise TypeError("should_retry must be callable")
        for attempt in range(1, self._attempts + 1):
            if deadline is not None and deadline.expired:
                raise TimeoutError("Retry deadline expired before the next attempt")
            started = time.monotonic()
            try:
                result = operation()
                if inspect.isawaitable(result):
                    close_awaitable(result)
                    raise _UnexpectedAwaitable("RetryPolicy.run requires a synchronous operation")
            except _UnexpectedAwaitable:
                raise
            except Exception as error:
                elapsed = time.monotonic() - started
                retry = attempt < self._attempts and self._should_retry(should_retry, error)
                self._observe(
                    RetryObservation(attempt, elapsed, "retry" if retry else "error", error), error
                )
                if not retry:
                    raise
                delay = self._delay(attempt)
                if deadline is not None:
                    remaining = deadline.remaining
                    if remaining is not None:
                        if remaining <= delay:
                            time.sleep(remaining)
                            raise TimeoutError(
                                "Retry deadline expired before another attempt"
                            ) from error
                        delay = min(delay, remaining)
                time.sleep(delay)
            else:
                self._observe(RetryObservation(attempt, time.monotonic() - started, "success"))
                return result
        raise AssertionError("unreachable retry state")

    async def run_async(
        self,
        operation: Callable[[], Awaitable[T]],
        *,
        should_retry: Callable[[Exception], bool] | None = None,
        deadline: Deadline | None = None,
    ) -> T:
        """Run async attempts within a retry budget; cancellation is never retried.

        The deadline gates new attempts and caps retry delays. It does not
        interrupt an operation that has already started.
        """
        if not callable(operation):
            raise TypeError("operation must be callable")
        if should_retry is not None and not callable(should_retry):
            raise TypeError("should_retry must be callable")
        for attempt in range(1, self._attempts + 1):
            if deadline is not None and deadline.expired:
                raise TimeoutError("Retry deadline expired before the next attempt")
            started = time.monotonic()
            try:
                pending = operation()
                if not inspect.isawaitable(pending):
                    raise _UnexpectedAwaitable(
                        "RetryPolicy.run_async requires an awaitable operation"
                    )
                result = await pending
            except _UnexpectedAwaitable:
                raise
            except asyncio.CancelledError:
                try:
                    self._observe(RetryObservation(attempt, time.monotonic() - started, "error"))
                except BaseException as observer_error:
                    raise asyncio.CancelledError from observer_error
                raise
            except Exception as error:
                elapsed = time.monotonic() - started
                retry = attempt < self._attempts and self._should_retry(should_retry, error)
                self._observe(
                    RetryObservation(attempt, elapsed, "retry" if retry else "error", error), error
                )
                if not retry:
                    raise
                delay = self._delay(attempt)
                if deadline is not None:
                    remaining = deadline.remaining
                    if remaining is not None:
                        if remaining <= delay:
                            await asyncio.sleep(remaining)
                            raise TimeoutError(
                                "Retry deadline expired before another attempt"
                            ) from error
                        delay = min(delay, remaining)
                await asyncio.sleep(delay)
            else:
                self._observe(RetryObservation(attempt, time.monotonic() - started, "success"))
                return result
        raise AssertionError("unreachable retry state")


@dataclass(frozen=True, slots=True)
class CooldownStats:
    keys: int
    active: int
    waiters: int


class CooldownRegistry(Generic[K]):
    """Thread-safe per-key monotonic cooldowns with sync and cancellable async waits."""

    def __init__(
        self,
        *,
        max_entries: int = 4096,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._max_entries = validate_positive_capacity(max_entries, name="Cooldown max entries")
        if not callable(clock):
            raise TypeError("clock must be callable")
        self._clock = clock
        self._condition = threading.Condition()
        self._deadlines: dict[K, float] = {}
        self._async_waiters: dict[
            K, set[tuple[asyncio.AbstractEventLoop, asyncio.Future[None]]]
        ] = {}

    def _prune_locked(self, now: float) -> int:
        expired = [key for key, deadline in self._deadlines.items() if deadline <= now]
        for key in expired:
            self._deadlines.pop(key, None)
        return len(expired)

    def _notify_async(self, key: K) -> None:
        waiters = tuple(self._async_waiters.get(key, ()))
        for loop, future in waiters:
            if not loop.is_closed():
                try:
                    loop.call_soon_threadsafe(self._resolve, future)
                except RuntimeError:
                    pass

    @staticmethod
    def _resolve(future: asyncio.Future[None]) -> None:
        if not future.done():
            future.set_result(None)

    def block(self, key: K, duration: float, *, extend: bool = True) -> float:
        """Block a key; by default, a new block only extends the current deadline."""
        seconds = validate_finite_duration(duration, name="duration", allow_zero=True)
        if not isinstance(extend, bool):
            raise TypeError("extend must be a bool")
        now = self._clock()
        with self._condition:
            self._prune_locked(now)
            if key not in self._deadlines and len(self._deadlines) >= self._max_entries:
                raise RuntimeError("CooldownRegistry entry capacity is full")
            deadline = now + seconds
            current = self._deadlines.get(key)
            if current is not None:
                deadline = max(current, deadline) if extend else deadline
            if deadline <= now:
                self._deadlines.pop(key, None)
            else:
                self._deadlines[key] = deadline
            self._condition.notify_all()
            self._notify_async(key)
            return max(0.0, deadline - now)

    def remaining(self, key: K) -> float:
        now = self._clock()
        with self._condition:
            deadline = self._deadlines.get(key)
            if deadline is None:
                return 0.0
            remaining = deadline - now
            if remaining <= 0:
                self._deadlines.pop(key, None)
                return 0.0
            return remaining

    def is_blocked(self, key: K) -> bool:
        return self.remaining(key) > 0.0

    def clear(self, key: K) -> bool:
        with self._condition:
            removed = self._deadlines.pop(key, None) is not None
            self._condition.notify_all()
            self._notify_async(key)
            return removed

    def prune(self) -> int:
        with self._condition:
            return self._prune_locked(self._clock())

    def wait(self, key: K, timeout: float | None = None) -> bool:
        """Block the calling thread until cooldown ends; return False on timeout."""
        timeout = validate_nonnegative_timeout(timeout, name="cooldown wait timeout")
        end = None if timeout is None else self._clock() + timeout
        with self._condition:
            while True:
                now = self._clock()
                deadline = self._deadlines.get(key)
                if deadline is None or deadline <= now:
                    self._deadlines.pop(key, None)
                    return True
                remaining = deadline - now
                if end is not None:
                    remaining_timeout = end - now
                    if remaining_timeout <= 0:
                        return False
                    remaining = min(remaining, remaining_timeout)
                self._condition.wait(remaining)

    async def wait_async(self, key: K, timeout: float | None = None) -> bool:
        """Wait without blocking the loop; cancellation unregisters the waiter."""
        timeout = validate_nonnegative_timeout(timeout, name="cooldown wait timeout")
        loop = asyncio.get_running_loop()
        end = None if timeout is None else loop.time() + timeout
        while True:
            future: asyncio.Future[None] = loop.create_future()
            with self._condition:
                now = self._clock()
                deadline = self._deadlines.get(key)
                if deadline is None or deadline <= now:
                    self._deadlines.pop(key, None)
                    return True
                delay = deadline - now
                if end is not None:
                    remaining_timeout = end - loop.time()
                    if remaining_timeout <= 0:
                        return False
                    delay = min(delay, remaining_timeout)
                self._async_waiters.setdefault(key, set()).add((loop, future))
            try:
                await asyncio.wait_for(future, timeout=delay)
            except TimeoutError:
                pass
            finally:
                with self._condition:
                    waiters = self._async_waiters.get(key)
                    if waiters is not None:
                        waiters.discard((loop, future))
                        if not waiters:
                            self._async_waiters.pop(key, None)

    def stats(self) -> CooldownStats:
        with self._condition:
            now = self._clock()
            self._prune_locked(now)
            return CooldownStats(
                len(self._deadlines),
                sum(deadline > now for deadline in self._deadlines.values()),
                sum(len(waiters) for waiters in self._async_waiters.values()),
            )


@dataclass(frozen=True, slots=True)
class CircuitBreakerStats:
    state: Literal["closed", "open", "half_open"]
    failures: int
    probes: int
    calls: int
    rejected: int


class CircuitBreaker:
    """Protect sync or async operations with closed/open/half-open transitions."""

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        *,
        half_open_max: int = 1,
        should_count: Callable[[Exception], bool] | None = None,
        observer: Callable[[CircuitBreakerStats], object] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._threshold = validate_positive_capacity(failure_threshold, name="failure_threshold")
        self._recovery = validate_finite_duration(recovery_timeout, name="recovery_timeout")
        if self._recovery == 0:
            raise ValueError("recovery_timeout must be positive")
        self._half_open_max = validate_positive_capacity(half_open_max, name="half_open_max")
        if should_count is not None and not callable(should_count):
            raise TypeError("should_count must be callable")
        if observer is not None and not callable(observer):
            raise TypeError("observer must be callable")
        if not callable(clock):
            raise TypeError("clock must be callable")
        self._should_count = should_count or (lambda _error: True)
        self._observer = observer
        self._clock = clock
        self._lock = threading.Lock()
        self._state: Literal["closed", "open", "half_open"] = "closed"
        self._failures = 0
        self._probes = 0
        self._opened_at = 0.0
        self._probe_epoch = 0
        self._calls = 0
        self._rejected = 0

    @property
    def state(self) -> str:
        with self._lock:
            self._advance()
            return self._state

    def _advance(self) -> None:
        if self._state == "open" and self._clock() - self._opened_at >= self._recovery:
            self._state = "half_open"
            self._probes = 0
            self._probe_epoch += 1

    def _enter(self) -> tuple[bool, int]:
        with self._lock:
            self._advance()
            if self._state == "open" or (
                self._state == "half_open" and self._probes >= self._half_open_max
            ):
                self._rejected += 1
                raise CircuitOpenError(f"Circuit breaker is {self._state}")
            self._calls += 1
            probe = self._state == "half_open"
            if probe:
                self._probes += 1
            return probe, self._probe_epoch

    def _finish(
        self,
        probe: bool,
        epoch: int,
        error: Exception | None,
        cancelled: bool = False,
    ) -> None:
        counted = False
        classification_error: Exception | None = None
        if error is not None:
            try:
                decision = self._should_count(error)
                if inspect.isawaitable(decision):
                    close_awaitable(decision)
                    raise TypeError("CircuitBreaker should_count must be synchronous")
                if type(decision) is not bool:
                    raise TypeError("should_count must return bool")
                counted = decision
            except Exception as exc:
                counted = True
                classification_error = exc
        with self._lock:
            if probe and epoch != self._probe_epoch:
                return
            if cancelled:
                if probe:
                    self._probes = max(0, self._probes - 1)
                return
            if probe:
                self._probes = max(0, self._probes - 1)
                if counted:
                    self._state = "open"
                    self._opened_at = self._clock()
                    self._failures = self._threshold
                    self._probes = 0
                    self._probe_epoch += 1
                elif self._probes == 0:
                    self._state = "closed"
                    self._failures = 0
                    self._probe_epoch += 1
            elif self._state == "closed":
                self._failures = self._failures + 1 if counted else 0
                if self._failures >= self._threshold:
                    self._state = "open"
                    self._opened_at = self._clock()
        try:
            self._notify()
        except Exception as observer_error:
            if error is not None:
                raise ExceptionGroup(
                    "Circuit operation and observer both failed", [error, observer_error]
                ) from error
            raise
        if classification_error is not None and error is not None:
            raise ExceptionGroup(
                "Circuit failure classifier failed", [error, classification_error]
            ) from error

    def _notify(self) -> None:
        if self._observer is not None:
            result = self._observer(self.stats())
            if inspect.isawaitable(result):
                close_awaitable(result)
                raise TypeError("CircuitBreaker observer must be synchronous")

    def stats(self) -> CircuitBreakerStats:
        with self._lock:
            self._advance()
            return CircuitBreakerStats(
                self._state, self._failures, self._probes, self._calls, self._rejected
            )

    def reset(self) -> None:
        with self._lock:
            self._state = "closed"
            self._failures = self._probes = 0
            self._opened_at = 0.0
            self._probe_epoch += 1
        self._notify()

    def call(self, operation: Callable[[], T]) -> T:
        if not callable(operation):
            raise TypeError("operation must be callable")
        probe, epoch = self._enter()
        try:
            result = operation()
            if inspect.isawaitable(result):
                close_awaitable(result)
                raise _UnexpectedAwaitable("CircuitBreaker.call requires a synchronous operation")
        except _UnexpectedAwaitable:
            self._finish(probe, epoch, None, cancelled=True)
            raise
        except Exception as error:
            self._finish(probe, epoch, error)
            raise
        except BaseException:
            self._finish(probe, epoch, None, cancelled=True)
            raise
        self._finish(probe, epoch, None)
        return result

    async def call_async(self, operation: Callable[[], Awaitable[T]]) -> T:
        if not callable(operation):
            raise TypeError("operation must be callable")
        probe, epoch = self._enter()
        try:
            pending = operation()
            if not inspect.isawaitable(pending):
                raise _UnexpectedAwaitable(
                    "CircuitBreaker.call_async requires an awaitable operation"
                )
            result = await pending
        except _UnexpectedAwaitable:
            self._finish(probe, epoch, None, cancelled=True)
            raise
        except asyncio.CancelledError:
            self._finish(probe, epoch, None, cancelled=True)
            raise
        except Exception as error:
            self._finish(probe, epoch, error)
            raise
        except BaseException:
            self._finish(probe, epoch, None, cancelled=True)
            raise
        self._finish(probe, epoch, None)
        return result
