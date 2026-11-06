"""Keyed coalescing and loop-owned async scheduling."""

from __future__ import annotations

import asyncio
import inspect
import math
import threading
import time
from collections import deque
from collections.abc import Callable, Hashable
from dataclasses import dataclass
from typing import Any, Generic, Literal, TypeVar

from ._callbacks import close_awaitable, synchronous_result
from ._lifecycle import (
    bind_running_loop,
    create_task_safely,
    finish_cleanup,
    request_task_cancellation,
)
from .base import (
    validate_finite_duration,
    validate_nonnegative_timeout,
    validate_positive_capacity,
)
from .compatibility import BaseExceptionGroup, ExceptionGroup
from .observability import OperationStats, _require_operation_stats

K = TypeVar("K", bound=Hashable)
T = TypeVar("T")
__all__ = [
    "KeyedCoalescer",
    "CoalescerStats",
    "AsyncScheduler",
    "ScheduledHandle",
    "SchedulerStats",
]


@dataclass(frozen=True, slots=True)
class CoalescerStats:
    pending_keys: int
    submitted: int
    merged: int
    delivered: int
    failures: int
    rejected: int
    discarded: int
    unreported_failures: int
    closed: bool


@dataclass(slots=True)
class _Coalesced(Generic[T]):
    value: T
    deadline: float


class KeyedCoalescer(Generic[K, T]):
    """Merge repeated keyed updates and deliver them from one owned worker thread.

    The first update for a key starts its coalescing window. User key and merge
    callbacks run on the calling thread; they should be quick and non-reentrant.
    Delivery always runs on the coalescer's worker thread.
    """

    def __init__(
        self,
        *,
        interval: float,
        key: Callable[[T], K],
        merge: Callable[[T, T], T],
        callback: Callable[[T], object],
        max_pending_keys: int = 128,
        on_error: Callable[[BaseException], object] | None = None,
        observer: OperationStats | None = None,
    ) -> None:
        self._interval = validate_finite_duration(interval, name="interval")
        self._key = key
        self._merge = merge
        self._callback = callback
        self._capacity = validate_positive_capacity(max_pending_keys, name="max_pending_keys")
        if not all(callable(item) for item in (key, merge, callback)):
            raise TypeError("key, merge and callback must be callable")
        if on_error is not None and not callable(on_error):
            raise TypeError("on_error must be callable")
        _require_operation_stats(observer)
        self._on_error = on_error
        self._observer = observer
        self._condition = threading.Condition()
        self._pending: dict[K, _Coalesced[T]] = {}
        self._closed = False
        self._flushing = False
        self._flush_requests = 0
        self._close_flush = False
        self._active = 0
        self._submitted = self._merged = self._delivered = self._failures = self._rejected = 0
        self._errors: deque[BaseException] = deque(maxlen=32)
        self._unreported_failures = 0
        self._discarded = 0
        self._thread = threading.Thread(target=self._run, name="EventFlowCoalescer", daemon=True)
        self._thread_started = False

    def push(self, value: T) -> None:
        key = synchronous_result(self._key(value))
        now = time.monotonic()
        with self._condition:
            if self._closed:
                raise RuntimeError("KeyedCoalescer is closed")
            entry = self._pending.get(key)
            if entry is None:
                if len(self._pending) >= self._capacity:
                    self._rejected += 1
                    raise RuntimeError("KeyedCoalescer pending-key capacity is full")
                if not self._thread_started:
                    self._thread.start()
                    self._thread_started = True
                self._pending[key] = _Coalesced(value, now + self._interval)
                self._submitted += 1
            else:
                entry.value = synchronous_result(self._merge(entry.value, value))
                self._merged += 1
            self._condition.notify()

    def _next_due(self) -> tuple[K, T] | None:
        if self._flushing and self._pending:
            key = next(iter(self._pending))
            entry = self._pending.pop(key)
            return key, entry.value
        if not self._pending:
            return None
        key, entry = min(self._pending.items(), key=lambda pair: pair[1].deadline)
        delay = entry.deadline - time.monotonic()
        if delay > 0:
            self._condition.wait(delay)
            return None
        self._pending.pop(key)
        return key, entry.value

    def _run(self) -> None:
        while True:
            with self._condition:
                while True:
                    item = self._next_due()
                    if item is not None:
                        self._active += 1
                        break
                    if self._closed and not self._pending:
                        self._condition.notify_all()
                        return
                    if not self._pending:
                        self._condition.wait()
            started = time.monotonic()
            status = "success"
            try:
                result = self._callback(item[1])
                if inspect.isawaitable(result):
                    close_awaitable(result)
                    raise TypeError("KeyedCoalescer callback must be synchronous")
            except BaseException as error:
                status = "error"
                with self._condition:
                    self._failures += 1
                    self._capture_error(error)
                if self._on_error is not None:
                    try:
                        result = self._on_error(error)
                        if inspect.isawaitable(result):
                            close_awaitable(result)
                            raise TypeError("KeyedCoalescer on_error must be synchronous")
                    except BaseException as observer_error:
                        with self._condition:
                            self._capture_error(observer_error)
            else:
                with self._condition:
                    self._delivered += 1
            finally:
                with self._condition:
                    self._active -= 1
                    self._condition.notify_all()
                if self._observer is not None:
                    self._observer.record(
                        "keyed_coalescer.delivery",
                        status=status,
                        elapsed=time.monotonic() - started,
                        queue_depth=self.pending_count,
                    )

    def _capture_error(self, error: BaseException) -> None:
        if len(self._errors) == self._errors.maxlen:
            self._unreported_failures += 1
        self._errors.append(error)

    @property
    def pending_count(self) -> int:
        with self._condition:
            return len(self._pending)

    def stats(self) -> CoalescerStats:
        with self._condition:
            return CoalescerStats(
                len(self._pending),
                self._submitted,
                self._merged,
                self._delivered,
                self._failures,
                self._rejected,
                self._discarded,
                self._unreported_failures,
                self._closed,
            )

    def _raise_errors(self) -> None:
        with self._condition:
            errors = list(self._errors)
            self._errors.clear()
            unreported, self._unreported_failures = self._unreported_failures, 0
        if unreported:
            errors.append(
                RuntimeError(f"{unreported} earlier coalescer error(s) were not retained")
            )
        if errors:
            if all(isinstance(error, Exception) for error in errors):
                raise ExceptionGroup(
                    "KeyedCoalescer callbacks failed",
                    [error for error in errors if isinstance(error, Exception)],
                )
            raise BaseExceptionGroup("KeyedCoalescer callbacks failed", errors)

    def flush(self, timeout: float | None = None) -> None:
        """Deliver all pending values and wait for the worker to become idle."""
        if threading.current_thread() is self._thread:
            raise RuntimeError("KeyedCoalescer worker cannot flush itself")
        timeout = validate_nonnegative_timeout(timeout, name="coalescer flush timeout")
        end = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            discarded = self._discarded
            self._flush_requests += 1
            self._flushing = True
            self._condition.notify_all()
            try:
                while self._pending or self._active:
                    remaining = None if end is None else end - time.monotonic()
                    if remaining is not None and remaining <= 0:
                        raise TimeoutError("KeyedCoalescer flush did not finish before the timeout")
                    self._condition.wait(remaining)
                if self._discarded != discarded:
                    raise RuntimeError("KeyedCoalescer closed and discarded pending values")
            finally:
                self._flush_requests -= 1
                self._flushing = self._flush_requests > 0 or self._close_flush
                self._condition.notify_all()
        self._raise_errors()

    def close(self, *, flush: bool = True, timeout: float | None = None) -> None:
        """Reject updates and apply the first close call's flush/discard policy."""
        if not isinstance(flush, bool):
            raise TypeError("flush must be a bool")
        timeout = validate_nonnegative_timeout(timeout, name="coalescer close timeout")
        if threading.current_thread() is self._thread:
            raise RuntimeError("KeyedCoalescer worker cannot close itself")
        end = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            if not self._closed:
                self._closed = True
                self._close_flush = flush
                if not flush:
                    self._discarded += len(self._pending)
                    self._pending.clear()
            self._flushing = self._flush_requests > 0 or self._close_flush
            self._condition.notify_all()
        if self._thread_started:
            remaining = None if end is None else max(0.0, end - time.monotonic())
            self._thread.join(remaining)
            if self._thread.is_alive():
                raise TimeoutError("KeyedCoalescer callback is still running")
        self._raise_errors()

    def __enter__(self) -> KeyedCoalescer[K, T]:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


@dataclass(frozen=True, slots=True)
class SchedulerStats:
    active: int
    completed: int
    failed: int
    cancelled: int
    closed: bool


class ScheduledHandle:
    """Cancellation handle for one scheduled job; cancellation is cooperative."""

    def __init__(self, task: asyncio.Task[Any], loop: asyncio.AbstractEventLoop) -> None:
        self._task = task
        self._loop = loop

    @property
    def done(self) -> bool:
        return self._task.done()

    def cancel(self) -> bool:
        return request_task_cancellation(self._task, self._loop)

    async def join(self, timeout: float | None = None) -> Any:
        timeout = validate_nonnegative_timeout(timeout, name="scheduled task timeout")
        if asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("ScheduledHandle belongs to a different event loop")
        done, _ = await asyncio.wait((self._task,), timeout=timeout)
        if not done:
            raise TimeoutError("Scheduled task did not finish before the timeout")
        return self._task.result()


class AsyncScheduler:
    """Schedule one-shot and periodic async or synchronous callables on one loop."""

    def __init__(
        self,
        *,
        on_error: Callable[[BaseException], object] | None = None,
        observer: OperationStats | None = None,
    ) -> None:
        if on_error is not None and not callable(on_error):
            raise TypeError("on_error must be callable")
        _require_operation_stats(observer)
        self._on_error = on_error
        self._observer = observer
        self._loop: asyncio.AbstractEventLoop | None = None
        self._tasks: set[asyncio.Task[Any]] = set()
        self._closed = False
        self._closing: asyncio.Task[None] | None = None
        self._completed = self._failed = self._cancelled = 0

    def _bind_loop(self) -> asyncio.AbstractEventLoop:
        loop = bind_running_loop(self._loop, "AsyncScheduler")
        self._loop = loop
        return loop

    def _track(self, coroutine: Any, name: str) -> ScheduledHandle:
        loop = self._bind_loop()
        if self._closed:
            coroutine.close()
            raise RuntimeError("AsyncScheduler is closed")
        try:
            task = loop.create_task(coroutine, name=name)
        except BaseException:
            coroutine.close()
            raise
        self._tasks.add(task)
        task.add_done_callback(self._finished)
        return ScheduledHandle(task, loop)

    def _finished(self, task: asyncio.Task[Any]) -> None:
        self._tasks.discard(task)
        if task.cancelled():
            self._cancelled += 1
            return
        error = task.exception()
        if error is None:
            self._completed += 1
            return
        self._failed += 1
        if self._on_error is not None:
            try:
                result = self._on_error(error)
                if inspect.isawaitable(result):
                    close_awaitable(result)
                    raise TypeError("AsyncScheduler on_error must be synchronous")
                return
            except BaseException as observer_error:
                error = BaseExceptionGroup(
                    "Scheduled job and error observer both failed", [error, observer_error]
                )
        loop = self._loop
        if loop is not None:
            loop.call_exception_handler(
                {
                    "message": "Unhandled failure in EventFlow scheduled job",
                    "exception": error,
                    "task": task,
                }
            )

    @staticmethod
    async def _invoke(
        callback: Callable[..., object], args: tuple[object, ...], kwargs: dict[str, object]
    ) -> object:
        result = callback(*args, **kwargs)
        return await result if inspect.isawaitable(result) else result

    def schedule_once(
        self,
        callback: Callable[..., object],
        *args: object,
        delay: float = 0.0,
        name: str | None = None,
        **kwargs: object,
    ) -> ScheduledHandle:
        if not callable(callback):
            raise TypeError("callback must be callable")
        wait = validate_nonnegative_timeout(delay, name="schedule delay")

        async def run() -> object:
            if wait:
                await asyncio.sleep(wait)
            started = time.monotonic()
            try:
                result = await self._invoke(callback, args, dict(kwargs))
            except BaseException as error:
                if self._observer is not None:
                    self._observer.record(
                        "scheduler.once",
                        status="cancelled"
                        if isinstance(error, asyncio.CancelledError)
                        else "error",
                        elapsed=time.monotonic() - started,
                    )
                raise
            if self._observer is not None:
                self._observer.record("scheduler.once", elapsed=time.monotonic() - started)
            return result

        return self._track(run(), name or "EventFlowScheduledOnce")

    def periodic(
        self,
        callback: Callable[..., object],
        *args: object,
        interval: float,
        overlap: Literal["skip", "queue", "parallel"] = "skip",
        initial_delay: float | None = None,
        max_parallel: int = 16,
        name: str | None = None,
        **kwargs: object,
    ) -> ScheduledHandle:
        if not callable(callback):
            raise TypeError("callback must be callable")
        period = validate_finite_duration(interval, name="interval")
        if overlap not in ("skip", "queue", "parallel"):
            raise ValueError("overlap must be 'skip', 'queue', or 'parallel'")
        first = period
        if initial_delay is not None:
            validated = validate_nonnegative_timeout(initial_delay, name="initial delay")
            assert validated is not None
            first = validated
        parallel_limit = validate_positive_capacity(max_parallel, name="max_parallel")
        loop = self._bind_loop()

        async def run_tick() -> None:
            started = time.monotonic()
            try:
                await self._invoke(callback, args, dict(kwargs))
            except BaseException as error:
                if self._observer is not None:
                    self._observer.record(
                        "scheduler.periodic",
                        status="cancelled"
                        if isinstance(error, asyncio.CancelledError)
                        else "error",
                        elapsed=time.monotonic() - started,
                    )
                raise
            if self._observer is not None:
                self._observer.record("scheduler.periodic", elapsed=time.monotonic() - started)

        async def run() -> None:
            next_tick = loop.time() + first
            children: set[asyncio.Task[None]] = set()
            queue_one = False
            try:
                while True:
                    if self._closed:
                        return
                    await asyncio.sleep(max(0.0, next_tick - loop.time()))
                    if self._closed:
                        return
                    if overlap == "parallel":
                        if len(children) < parallel_limit:
                            child = create_task_safely(
                                run_tick(), name=f"{name or 'EventFlowPeriodic'}:tick", loop=loop
                            )
                            children.add(child)
                            child.add_done_callback(children.discard)
                            self._tasks.add(child)
                            child.add_done_callback(self._finished)
                        elif self._observer is not None:
                            self._observer.record("scheduler.periodic", status="error", dropped=1)
                    else:
                        await run_tick()
                    now = loop.time()
                    next_tick += period
                    if next_tick <= now:
                        if overlap == "skip":
                            missed = math.floor((now - next_tick) / period) + 1
                            next_tick += missed * period
                        elif overlap == "queue" and not queue_one:
                            next_tick = now
                            queue_one = True
                        else:
                            next_tick = now + period
                            queue_one = False
            finally:
                if children:
                    for child in children:
                        child.cancel()
                    await asyncio.gather(*children, return_exceptions=True)

        return self._track(run(), name or "EventFlowPeriodic")

    def cancel(self, handle: ScheduledHandle) -> bool:
        if not isinstance(handle, ScheduledHandle):
            raise TypeError("handle must be a ScheduledHandle")
        if handle._task not in self._tasks:
            return False
        return handle.cancel()

    async def _close_tasks(self, timeout: float | None) -> None:
        tasks = tuple(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            _, pending = await asyncio.wait(tasks, timeout=timeout)
            if pending:
                raise TimeoutError(f"{len(pending)} scheduled task(s) remain after shutdown")

    async def aclose(self, timeout: float | None = None) -> None:
        self._bind_loop()
        current = asyncio.current_task()
        if current is self._closing or current in self._tasks:
            raise RuntimeError("A scheduled job cannot close its own scheduler")
        timeout = validate_nonnegative_timeout(timeout, name="scheduler close timeout")
        self._closed = True
        if self._closing is None or (
            self._closing.done()
            and (self._tasks or self._closing.cancelled() or self._closing.exception() is not None)
        ):
            self._closing = create_task_safely(
                self._close_tasks(timeout), name="EventFlowSchedulerClose"
            )
        await finish_cleanup(self._closing)

    def snapshot(self) -> SchedulerStats:
        self._bind_loop()
        return SchedulerStats(
            len(self._tasks), self._completed, self._failed, self._cancelled, self._closed
        )

    async def __aenter__(self) -> AsyncScheduler:
        self._bind_loop()
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.aclose()
