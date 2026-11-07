"""Thread work pools with explicit bounds on accepted work."""

from __future__ import annotations

import threading
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, TypeVar

from ._callbacks import is_async_callable, synchronous_result
from .base import validate_positive_capacity
from .observability import OperationStats, _require_operation_stats

T = TypeVar("T")
__all__ = ["BoundedWorkPool", "WorkPoolStats"]


@dataclass(frozen=True, slots=True)
class WorkPoolStats:
    workers: int
    pending: int
    running: int
    completed: int
    rejected: int
    closed: bool


class BoundedWorkPool:
    """Own a thread pool and reject submissions beyond workers plus pending slots.

    ``max_pending`` counts accepted work that is waiting for a worker. Running
    calls cannot be forcibly stopped; cancelling a Future only succeeds before
    its callable begins.
    """

    def __init__(
        self, *, max_workers: int, max_pending: int, observer: OperationStats | None = None
    ) -> None:
        self._workers = validate_positive_capacity(max_workers, name="max_workers")
        if isinstance(max_pending, bool) or not isinstance(max_pending, int):
            raise TypeError("max_pending must be an integer")
        if max_pending < 0:
            raise ValueError("max_pending cannot be negative")
        _require_operation_stats(observer)
        self._pending_limit = max_pending
        self._observer = observer
        self._slots = threading.BoundedSemaphore(self._workers + max_pending)
        self._executor = ThreadPoolExecutor(
            max_workers=self._workers, thread_name_prefix="EventFlowWorker"
        )
        self._lock = threading.Lock()
        self._pending = 0
        self._running = 0
        self._completed = 0
        self._rejected = 0
        self._closed = False
        self._worker_ids: set[int] = set()

    def submit(self, operation: Callable[..., T], /, *args: Any, **kwargs: Any) -> Future[T]:
        """Submit work immediately or raise when all accepted slots are occupied."""
        if not callable(operation):
            raise TypeError("operation must be callable")
        if is_async_callable(operation):
            raise TypeError("BoundedWorkPool accepts synchronous operations only")
        future: Future[T] | None = None
        with self._lock:
            if self._closed:
                raise RuntimeError("BoundedWorkPool is shut down")
            if not self._slots.acquire(blocking=False):
                self._rejected += 1
            else:
                self._pending += 1
                started = False

                def run() -> T:
                    nonlocal started
                    with self._lock:
                        started = True
                        self._pending -= 1
                        self._running += 1
                        self._worker_ids.add(threading.get_ident())
                    try:
                        return synchronous_result(operation(*args, **kwargs))
                    finally:
                        with self._lock:
                            self._worker_ids.discard(threading.get_ident())

                try:
                    future = self._executor.submit(run)
                except BaseException:
                    self._pending -= 1
                    self._slots.release()
                    raise

        if future is None:
            capacity_error = RuntimeError("BoundedWorkPool capacity is full")
            if self._observer is not None:
                try:
                    self._observer.record("work_pool.submit", status="error", rejected=1)
                except BaseException as observer_error:
                    raise capacity_error from observer_error
            raise capacity_error

        def finished(_future: Future[T]) -> None:
            with self._lock:
                if started:
                    self._running -= 1
                else:
                    self._pending -= 1
                self._completed += 1
            self._slots.release()
            if self._observer is not None:
                status = (
                    "cancelled"
                    if _future.cancelled()
                    else ("error" if _future.exception() is not None else "success")
                )
                self._observer.record("work_pool.job", status=status)

        future.add_done_callback(finished)
        return future

    def stats(self) -> WorkPoolStats:
        with self._lock:
            return WorkPoolStats(
                self._workers,
                self._pending,
                self._running,
                self._completed,
                self._rejected,
                self._closed,
            )

    def shutdown(self, *, wait: bool = True, cancel_futures: bool = False) -> None:
        """Reject new jobs and optionally wait for accepted work to finish."""
        if not isinstance(wait, bool) or not isinstance(cancel_futures, bool):
            raise TypeError("wait and cancel_futures must be bools")
        with self._lock:
            if wait and threading.get_ident() in self._worker_ids:
                raise RuntimeError("A BoundedWorkPool worker cannot wait for its own shutdown")
            self._closed = True
        self._executor.shutdown(wait=wait, cancel_futures=cancel_futures)

    close = shutdown

    def __enter__(self) -> BoundedWorkPool:
        return self

    def __exit__(self, *_args: object) -> None:
        self.shutdown()
