"""Loop-owned keyed concurrency and rate controls."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Hashable
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

from ._lifecycle import bind_running_loop, finish_cleanup
from .base import validate_finite_duration
from .observability import OperationStats, _require_operation_stats

K = TypeVar("K", bound=Hashable)
T = TypeVar("T")
__all__ = [
    "KeyedConcurrencyLimiter",
    "KeyedConcurrencyKeyStats",
    "KeyedConcurrencyStats",
    "SingleFlight",
    "SingleFlightStats",
    "KeyedRateLimiter",
    "KeyedRateStats",
]


@dataclass(slots=True)
class _ConcurrencyEntry:
    active: int = 0
    waiters: dict[asyncio.Future[None], None] = field(default_factory=dict)
    users: int = 0


@dataclass(frozen=True, slots=True)
class KeyedConcurrencyKeyStats:
    active: int
    waiters: int


@dataclass(frozen=True, slots=True)
class KeyedConcurrencyStats:
    keys: int
    active: int
    waiters: int
    closed: bool


class _Permit:
    def __init__(self, limiter: KeyedConcurrencyLimiter[Any], key: Hashable) -> None:
        self._limiter = limiter
        self._key = key
        self._entered = False

    async def __aenter__(self) -> None:
        if self._entered:
            raise RuntimeError("A keyed concurrency permit can only be entered once")
        started = asyncio.get_running_loop().time()
        try:
            await self._limiter._acquire(self._key)
        except asyncio.CancelledError:
            self._limiter._record("cancelled", asyncio.get_running_loop().time() - started)
            raise
        except Exception:
            self._limiter._record("error", asyncio.get_running_loop().time() - started)
            raise
        self._limiter._record("success", asyncio.get_running_loop().time() - started)
        self._entered = True
        return None

    async def __aexit__(self, *_args: object) -> None:
        if self._entered:
            self._entered = False
            self._limiter._release(self._key)


class KeyedConcurrencyLimiter(Generic[K]):
    """Limit simultaneous async work independently for each hashable key."""

    def __init__(self, max_concurrency: int, *, observer: OperationStats | None = None) -> None:
        if isinstance(max_concurrency, bool) or not isinstance(max_concurrency, int):
            raise TypeError("max_concurrency must be an integer")
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be positive")
        _require_operation_stats(observer)
        self._limit = max_concurrency
        self._observer = observer
        self._loop: asyncio.AbstractEventLoop | None = None
        self._entries: dict[K, _ConcurrencyEntry] = {}
        self._waiter_tasks: set[asyncio.Task[Any]] = set()
        self._total_waiters = 0
        self._closed = False

    def _bind_loop(self) -> asyncio.AbstractEventLoop:
        loop = bind_running_loop(self._loop, "KeyedConcurrencyLimiter")
        self._loop = loop
        return loop

    def _entry(self, key: K) -> _ConcurrencyEntry:
        entry = self._entries.get(key)
        if entry is None:
            entry = _ConcurrencyEntry()
            self._entries[key] = entry
        return entry

    def acquire(self, key: K) -> _Permit:
        """Return an async context manager that releases its permit on exit."""
        return _Permit(self, key)

    def _record(self, status: str, elapsed: float) -> None:
        if self._observer is not None:
            self._observer.record(
                "keyed_concurrency.acquire",
                status=status,
                elapsed=elapsed,
                queue_depth=self._total_waiters,
            )

    async def _acquire(self, key: K) -> None:
        loop = self._bind_loop()
        if self._closed:
            raise RuntimeError("KeyedConcurrencyLimiter is closed")
        entry = self._entry(key)
        entry.users += 1
        acquired = False
        waiter: asyncio.Future[None] | None = None
        task = asyncio.current_task()
        if task is not None:
            self._waiter_tasks.add(task)
        try:
            if self._closed:
                raise RuntimeError("KeyedConcurrencyLimiter is closed")
            if entry.active < self._limit and not entry.waiters:
                entry.active += 1
                acquired = True
            else:
                waiter = loop.create_future()
                entry.waiters[waiter] = None
                self._total_waiters += 1
                self._wake_waiters(entry)
                while True:
                    if self._closed:
                        raise RuntimeError("KeyedConcurrencyLimiter is closed")
                    if next(iter(entry.waiters)) is waiter and entry.active < self._limit:
                        entry.waiters.pop(waiter)
                        self._total_waiters -= 1
                        waiter = None
                        entry.active += 1
                        acquired = True
                        self._wake_waiters(entry)
                        break
                    await asyncio.shield(waiter)
        finally:
            if waiter is not None:
                is_first = next(iter(entry.waiters)) is waiter
                entry.waiters.pop(waiter)
                self._total_waiters -= 1
                if not waiter.done():
                    waiter.cancel()
                if is_first:
                    self._wake_waiters(entry)
            if task is not None:
                self._waiter_tasks.discard(task)
            if not acquired:
                entry.users -= 1
                self._prune(key, entry)

    def _wake_waiters(self, entry: _ConcurrencyEntry) -> None:
        if not entry.waiters:
            return
        if self._closed:
            for waiter in tuple(entry.waiters):
                if not waiter.done():
                    waiter.set_result(None)
        elif entry.active < self._limit:
            waiter = next(iter(entry.waiters))
            if not waiter.done():
                waiter.set_result(None)

    def _release(self, key: K) -> None:
        self._bind_loop()
        entry = self._entries.get(key)
        if entry is None or entry.active == 0:
            raise RuntimeError("No active permit exists for this key")
        entry.active -= 1
        entry.users -= 1
        self._wake_waiters(entry)
        self._prune(key, entry)

    def _prune(self, key: K, entry: _ConcurrencyEntry) -> None:
        if entry.users == 0 and entry.active == 0 and not entry.waiters:
            self._entries.pop(key, None)

    def acquire_nowait(self, key: K) -> bool:
        """Reserve a slot immediately; pair a successful call with release(key)."""
        self._bind_loop()
        if self._closed:
            return False
        entry = self._entry(key)
        if entry.active >= self._limit or entry.waiters:
            self._prune(key, entry)
            return False
        entry.active += 1
        entry.users += 1
        return True

    async def release(self, key: K) -> None:
        """Release a slot previously acquired with acquire_nowait()."""
        self._release(key)

    def stats(self) -> KeyedConcurrencyStats:
        self._bind_loop()
        return KeyedConcurrencyStats(
            len(self._entries),
            sum(entry.active for entry in self._entries.values()),
            sum(len(entry.waiters) for entry in self._entries.values()),
            self._closed,
        )

    def key_stats(self, key: K) -> KeyedConcurrencyKeyStats:
        """Return active and waiting counts for one key, or zeroes if unused."""
        self._bind_loop()
        entry = self._entries.get(key)
        return KeyedConcurrencyKeyStats(
            0 if entry is None else entry.active,
            0 if entry is None else len(entry.waiters),
        )

    async def aclose(self) -> None:
        self._bind_loop()
        self._closed = True
        for entry in tuple(self._entries.values()):
            self._wake_waiters(entry)
        for key, entry in tuple(self._entries.items()):
            self._prune(key, entry)
        waiters = tuple(task for task in self._waiter_tasks if task is not asyncio.current_task())
        if waiters:
            await finish_cleanup(asyncio.gather(*waiters, return_exceptions=True))


@dataclass(frozen=True, slots=True)
class SingleFlightStats:
    active: int
    shared_waiters: int


@dataclass(slots=True)
class _Flight(Generic[T]):
    task: asyncio.Task[T]
    waiters: int = 0


class SingleFlight(Generic[K]):
    """Share one in-flight async factory result among callers with the same key."""

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._flights: dict[K, _Flight[Any]] = {}
        self._closed = False
        self._close_task: asyncio.Task[None] | None = None

    def _bind_loop(self) -> asyncio.AbstractEventLoop:
        loop = bind_running_loop(self._loop, "SingleFlight")
        self._loop = loop
        return loop

    async def run(self, key: K, factory: Callable[[], Awaitable[T]]) -> T:
        """Join the active operation; cancelling one waiter leaves shared work running."""
        loop = self._bind_loop()
        if not callable(factory):
            raise TypeError("factory must be callable")
        if self._closed:
            raise RuntimeError("SingleFlight is closed")
        flight = self._flights.get(key)
        if flight is None or flight.task.done():

            async def execute() -> T:
                return await factory()

            task = loop.create_task(execute(), name="EventFlowSingleFlight")
            flight = _Flight(task)
            self._flights[key] = flight
            task.add_done_callback(
                lambda completed, flight_key=key, record=flight: self._finished(
                    flight_key, record, completed
                )
            )
        flight.waiters += 1
        try:
            return await asyncio.shield(flight.task)
        finally:
            flight.waiters -= 1

    async def aclose(self) -> None:
        """Cancel and drain shared operations owned by this instance.

        Factories must cooperate with cancellation for shutdown to finish.
        Cancelling one waiter still leaves shared work running; call ``aclose``
        when the instance is no longer needed.
        """
        loop = self._bind_loop()
        current = asyncio.current_task()
        if any(flight.task is current for flight in self._flights.values()):
            raise RuntimeError("A SingleFlight operation cannot close its own instance")
        if self._close_task is None:
            self._closed = True
            tasks = tuple(flight.task for flight in self._flights.values())

            async def close_tasks() -> None:
                for task in tasks:
                    task.cancel()
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)

            self._close_task = loop.create_task(close_tasks(), name="EventFlowSingleFlightClose")
        await asyncio.shield(self._close_task)

    def _finished(self, key: K, flight: _Flight[Any], task: asyncio.Task[Any]) -> None:
        if self._flights.get(key) is flight:
            self._flights.pop(key, None)
        if not task.cancelled():
            error = task.exception()
            if error is not None and flight.waiters == 0:
                loop = self._loop
                if loop is not None:
                    loop.call_exception_handler(
                        {
                            "message": "SingleFlight operation failed after every waiter left",
                            "exception": error,
                            "task": task,
                        }
                    )

    def stats(self) -> SingleFlightStats:
        self._bind_loop()
        return SingleFlightStats(
            len(self._flights), sum(flight.waiters for flight in self._flights.values())
        )


@dataclass(slots=True)
class _RateEntry:
    tokens: float
    updated: float
    waiters: int = 0
    signals: set[asyncio.Event] | None = None


@dataclass(frozen=True, slots=True)
class KeyedRateStats:
    keys: int
    waiters: int
    closed: bool


class KeyedRateLimiter(Generic[K]):
    """Async token bucket limiter with independent buckets and monotonic refill."""

    def __init__(
        self,
        rate: float,
        period: float = 1.0,
        burst: float | None = None,
        *,
        max_keys: int = 4096,
        observer: OperationStats | None = None,
    ) -> None:
        self._rate = validate_finite_duration(rate, name="rate")
        self._period = validate_finite_duration(period, name="period")
        default_burst = max(self._rate, 1.0)
        self._burst = validate_finite_duration(
            burst if burst is not None else default_burst, name="burst"
        )
        if self._burst < 1:
            raise ValueError("burst must be at least one token")
        if isinstance(max_keys, bool) or not isinstance(max_keys, int):
            raise TypeError("max_keys must be an integer")
        if max_keys < 1:
            raise ValueError("max_keys must be positive")
        _require_operation_stats(observer)
        self._max_keys = max_keys
        self._observer = observer
        self._loop: asyncio.AbstractEventLoop | None = None
        self._entries: dict[K, _RateEntry] = {}
        self._waiter_tasks: set[asyncio.Task[Any]] = set()
        self._closed = False

    def _bind_loop(self) -> asyncio.AbstractEventLoop:
        loop = bind_running_loop(self._loop, "KeyedRateLimiter")
        self._loop = loop
        return loop

    def _refill(self, entry: _RateEntry, now: float) -> None:
        entry.tokens = min(
            self._burst, entry.tokens + (now - entry.updated) * self._rate / self._period
        )
        entry.updated = now

    def _entry(self, key: K, now: float) -> _RateEntry:
        entry = self._entries.get(key)
        if entry is None:
            if len(self._entries) >= self._max_keys:
                self.prune()
            if len(self._entries) >= self._max_keys:
                raise RuntimeError("KeyedRateLimiter key capacity is full")
            entry = _RateEntry(self._burst, now)
            self._entries[key] = entry
        else:
            self._refill(entry, now)
        return entry

    async def acquire(self, key: K) -> None:
        loop = self._bind_loop()
        started = loop.time()
        if self._closed:
            if self._observer is not None:
                self._observer.record("keyed_rate.acquire", status="error")
            raise RuntimeError("KeyedRateLimiter is closed")
        entry = self._entry(key, loop.time())
        entry.waiters += 1
        task = asyncio.current_task()
        if task is not None:
            self._waiter_tasks.add(task)
        if entry.signals is None:
            entry.signals = set()
        try:
            while True:
                if self._closed:
                    raise RuntimeError("KeyedRateLimiter is closed")
                now = loop.time()
                self._refill(entry, now)
                if entry.tokens >= 1.0:
                    entry.tokens -= 1.0
                    if self._observer is not None:
                        self._observer.record(
                            "keyed_rate.acquire",
                            elapsed=loop.time() - started,
                            queue_depth=entry.waiters - 1,
                        )
                    return
                delay = (1.0 - entry.tokens) * self._period / self._rate
                changed = asyncio.Event()
                entry.signals.add(changed)
                try:
                    await asyncio.wait_for(changed.wait(), timeout=delay)
                except TimeoutError:
                    pass
                finally:
                    entry.signals.discard(changed)
        finally:
            entry.waiters -= 1
            if task is not None:
                self._waiter_tasks.discard(task)
            self._prune(key, entry)

    def try_acquire(self, key: K) -> bool:
        loop = self._bind_loop()
        if self._closed:
            if self._observer is not None:
                self._observer.record("keyed_rate.try_acquire", status="error", rejected=1)
            return False
        entry = self._entry(key, loop.time())
        if entry.tokens < 1.0:
            self._prune(key, entry)
            if self._observer is not None:
                self._observer.record("keyed_rate.try_acquire", status="error", rejected=1)
            return False
        entry.tokens -= 1.0
        self._prune(key, entry)
        if self._observer is not None:
            self._observer.record("keyed_rate.try_acquire")
        return True

    def available(self, key: K) -> float:
        loop = self._bind_loop()
        entry = self._entries.get(key)
        if entry is None:
            return self._burst
        self._refill(entry, loop.time())
        self._prune(key, entry)
        return entry.tokens

    def _prune(self, key: K, entry: _RateEntry) -> None:
        if entry.waiters == 0 and entry.tokens >= self._burst:
            self._entries.pop(key, None)

    def prune(self) -> int:
        """Remove idle buckets that have refilled completely."""
        loop = self._bind_loop()
        now = loop.time()
        before = len(self._entries)
        for key, entry in tuple(self._entries.items()):
            self._refill(entry, now)
            self._prune(key, entry)
        return before - len(self._entries)

    def stats(self) -> KeyedRateStats:
        self._bind_loop()
        return KeyedRateStats(
            len(self._entries), sum(entry.waiters for entry in self._entries.values()), self._closed
        )

    async def aclose(self) -> None:
        self._bind_loop()
        self._closed = True
        for entry in self._entries.values():
            if entry.signals:
                for signal in tuple(entry.signals):
                    signal.set()
        waiters = tuple(task for task in self._waiter_tasks if task is not asyncio.current_task())
        if waiters:
            await finish_cleanup(asyncio.gather(*waiters, return_exceptions=True))
        for key, entry in tuple(self._entries.items()):
            self._prune(key, entry)
