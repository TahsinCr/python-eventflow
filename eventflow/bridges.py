"""Thread-to-async bridges with explicit loop and shutdown ownership."""

from __future__ import annotations

import asyncio
import concurrent.futures
import inspect
import threading
import time
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable, Hashable
from dataclasses import dataclass
from typing import Any, Generic, Literal, TypeVar, cast

from ._callbacks import close_awaitable
from ._lifecycle import bind_running_loop
from .base import validate_nonnegative_timeout, validate_positive_capacity
from .errors import EventQueueClosedError, LoopPortalClosedError, QueueOverflowError
from .event_bus import EventBus
from .filtering import EventFilter, EventType
from .observability import OperationStats, _require_operation_stats
from .subscription import Subscription

T = TypeVar("T")
__all__ = ["AsyncEventQueue", "AsyncEventQueueStats", "LoopPortal"]


@dataclass(frozen=True, slots=True)
class AsyncEventQueueStats:
    size: int
    capacity: int
    received: int
    dropped: int
    overflow_errors: int
    closed: bool


class AsyncEventQueue(Generic[T]):
    """Bounded cross-thread queue subscribed to a synchronous EventBus.

    One lock-protected deque covers every accepted event, including events
    published before the first loop consumer is attached. A publish never
    creates an asyncio task.
    """

    def __init__(
        self,
        bus: EventBus[T],
        *,
        event_filter: EventFilter[T] | None = None,
        event_type: EventType | None = None,
        predicate: Callable[[T], bool] | None = None,
        routing_key: Hashable | None = None,
        capacity: int = 128,
        overflow: Literal["drop_oldest", "drop_newest", "error"] = "error",
        observer: OperationStats | None = None,
    ) -> None:
        if not isinstance(bus, EventBus):
            raise TypeError("AsyncEventQueue requires a synchronous EventBus")
        _require_operation_stats(observer)
        self._capacity = validate_positive_capacity(capacity, name="Event queue capacity")
        if overflow not in ("drop_oldest", "drop_newest", "error"):
            raise ValueError("overflow must be 'drop_oldest', 'drop_newest', or 'error'")
        self._overflow = overflow
        self._observer = observer
        self._events: deque[T] = deque()
        self._lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread_id: int | None = None
        self._waiter: asyncio.Future[None] | None = None
        self._consumer: asyncio.Task[Any] | None = None
        self._received = 0
        self._dropped = 0
        self._overflow_errors = 0
        self._closed = False
        self._subscription: Subscription | None = bus.subscribe(
            self._on_event,
            event_filter=event_filter,
            event_type=event_type,
            predicate=predicate,
            routing_key=routing_key,
        )

    def _bind_loop(self) -> asyncio.AbstractEventLoop:
        loop = bind_running_loop(self._loop, "AsyncEventQueue")
        if self._loop_thread_id is None:
            self._loop_thread_id = threading.get_ident()
        self._loop = loop
        return loop

    @property
    def closed(self) -> bool:
        with self._lock:
            return self._closed

    def _on_event(self, event: T) -> None:
        waiter: asyncio.Future[None] | None
        dropped = 0
        overflow_error = False
        accepted = True
        with self._lock:
            if self._closed:
                return
            if len(self._events) < self._capacity:
                self._events.append(event)
            elif self._overflow == "drop_newest":
                self._dropped += 1
                dropped = 1
                accepted = False
            elif self._overflow == "drop_oldest":
                self._events.popleft()
                self._dropped += 1
                dropped = 1
                self._events.append(event)
            else:
                self._overflow_errors += 1
                overflow_error = True
                accepted = False
            waiter, self._waiter = self._waiter, None
            loop = self._loop
            loop_thread_id = self._loop_thread_id
            depth = len(self._events)
        try:
            if self._observer is not None:
                self._observer.record(
                    "event_queue.publish",
                    status="error" if overflow_error else "success",
                    dropped=dropped,
                    rejected=int(overflow_error),
                    queue_depth=depth,
                )
        finally:
            if accepted and waiter is not None and loop is not None:
                try:
                    if threading.get_ident() == loop_thread_id:
                        self._wake(waiter)
                    else:
                        loop.call_soon_threadsafe(self._wake, waiter)
                except RuntimeError:
                    # A closing loop no longer has a consumer; accepted data stays queued.
                    pass
        if overflow_error:
            raise QueueOverflowError("AsyncEventQueue capacity is full")
        if dropped and self._overflow == "drop_newest":
            return

    @staticmethod
    def _wake(waiter: asyncio.Future[None]) -> None:
        if not waiter.done():
            waiter.set_result(None)

    async def receive(self) -> T:
        """Receive one event; events already accepted are drained after close."""
        loop = self._bind_loop()
        task = asyncio.current_task()
        if self._consumer is None or self._consumer.done():
            self._consumer = task
        elif self._consumer is not task:
            raise RuntimeError("AsyncEventQueue supports one consumer task")
        while True:
            with self._lock:
                if self._events:
                    self._received += 1
                    event = self._events.popleft()
                    depth = len(self._events)
                    break
                if self._closed:
                    raise EventQueueClosedError("AsyncEventQueue is closed and drained")
                if self._waiter is not None:
                    raise RuntimeError("Concurrent receives are not supported")
                waiter = loop.create_future()
                self._waiter = waiter
            try:
                await waiter
            except BaseException:
                with self._lock:
                    if self._waiter is waiter:
                        self._waiter = None
                raise
        if self._observer is not None:
            self._observer.record("event_queue.receive", queue_depth=depth)
        return event

    def __aiter__(self) -> AsyncIterator[T]:
        self._bind_loop()
        return self

    async def __anext__(self) -> T:
        try:
            return await self.receive()
        except EventQueueClosedError as error:
            raise StopAsyncIteration from error

    def stats(self) -> AsyncEventQueueStats:
        with self._lock:
            return AsyncEventQueueStats(
                len(self._events),
                self._capacity,
                self._received,
                self._dropped,
                self._overflow_errors,
                self._closed,
            )

    async def aclose(self) -> None:
        owner = self._loop
        if owner is not None and owner.is_closed():
            loop = None
        else:
            loop = self._bind_loop()
        with self._lock:
            self._closed = True
            waiter, self._waiter = self._waiter, None
            subscription = self._subscription
        error: BaseException | None = None
        if subscription is not None:
            try:
                subscription.cancel()
            except BaseException as exc:
                error = exc
            else:
                self._subscription = None
        if waiter is not None and loop is not None:
            loop.call_soon(self._wake, waiter)
        if error is not None:
            raise error

    async def __aenter__(self) -> AsyncEventQueue[T]:
        self._bind_loop()
        if self.closed:
            raise RuntimeError("AsyncEventQueue is closed")
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.aclose()


class LoopPortal:
    """Own a background event loop and submit async operations from other threads."""

    def __init__(self, *, name: str = "EventFlowLoopPortal") -> None:
        if not isinstance(name, str) or not name:
            raise ValueError("portal thread name must be a non-empty string")
        self._name = name
        self._lock = threading.Lock()
        self._close_lock = threading.Lock()
        self._ready = threading.Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._startup_error: BaseException | None = None
        self._futures: set[concurrent.futures.Future[Any]] = set()
        self._closing = False
        self._stopping = False
        self._closed = False

    @property
    def running(self) -> bool:
        with self._lock:
            return (
                self._loop is not None
                and self._loop.is_running()
                and not self._closing
                and not self._stopping
            )

    def start(self) -> LoopPortal:
        """Start the loop thread and wait until its event loop is running."""
        with self._lock:
            if self._closed:
                raise LoopPortalClosedError("LoopPortal cannot be restarted after close")
            if self._closing or self._stopping:
                raise LoopPortalClosedError("LoopPortal is closing")
            if self._thread is None:
                self._ready.clear()
                self._startup_error = None
                thread = threading.Thread(target=self._run, name=self._name, daemon=True)
                self._thread = thread
                try:
                    thread.start()
                except BaseException as error:
                    self._thread = None
                    self._startup_error = error
                    self._ready.set()
                    raise
        self._ready.wait()
        with self._lock:
            if self._startup_error is not None:
                raise LoopPortalClosedError("LoopPortal event loop failed to start") from (
                    self._startup_error
                )
            if self._closing or self._stopping or self._closed:
                raise LoopPortalClosedError("LoopPortal is closing")
        return self

    def _run(self) -> None:
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        except BaseException as error:
            with self._lock:
                self._startup_error = error
                self._ready.set()
            return

        def mark_started() -> None:
            with self._lock:
                self._loop = loop
                self._ready.set()

        try:
            loop.call_soon(mark_started)
            loop.run_forever()
        except BaseException as error:
            with self._lock:
                if not self._ready.is_set():
                    self._startup_error = error
                    self._ready.set()
            raise
        finally:
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            finally:
                try:
                    loop.run_until_complete(loop.shutdown_default_executor())
                finally:
                    asyncio.set_event_loop(None)
                    loop.close()

    def submit(
        self,
        operation: Callable[..., Awaitable[T]] | Awaitable[T],
        *args: object,
        **kwargs: object,
    ) -> concurrent.futures.Future[T]:
        """Submit an awaitable or async callable; caller ContextVars propagate."""
        with self._lock:
            if (
                self._thread is None
                or self._loop is None
                or not self._loop.is_running()
                or self._closing
                or self._stopping
            ):
                if not callable(operation):
                    close_awaitable(operation)
                raise LoopPortalClosedError("LoopPortal is not accepting work")
            loop = self._loop
            future: concurrent.futures.Future[T] = concurrent.futures.Future()
            self._futures.add(future)
            task: asyncio.Task[T] | None = None

            def forget(completed: concurrent.futures.Future[T]) -> None:
                with self._lock:
                    self._futures.discard(completed)

            def cancel_on_loop() -> None:
                if task is not None and not task.done():
                    task.cancel()

            def cancel_task(completed: concurrent.futures.Future[T]) -> None:
                if completed.cancelled():
                    try:
                        loop.call_soon_threadsafe(cancel_on_loop)
                    except RuntimeError:
                        pass

            future.add_done_callback(forget)
            future.add_done_callback(cancel_task)

            def close_input() -> None:
                if not callable(operation):
                    close_awaitable(operation)

            def schedule() -> None:
                nonlocal task
                if future.cancelled():
                    close_input()
                    return
                started = False

                async def run() -> T:
                    nonlocal started
                    started = True
                    result = operation(*args, **kwargs) if callable(operation) else operation
                    if not inspect.isawaitable(result):
                        raise TypeError("portal operation must return an awaitable")
                    return await cast(Awaitable[T], result)

                coroutine = run()
                try:
                    task = loop.create_task(coroutine, name="EventFlowPortalCall")
                except BaseException as error:
                    coroutine.close()
                    close_input()
                    if not future.done():
                        future.set_exception(error)
                    return
                if future.cancelled():
                    task.cancel()

                def transfer_result(completed: asyncio.Task[T]) -> None:
                    if not started:
                        close_input()
                    if future.done():
                        return
                    if completed.cancelled():
                        future.cancel()
                        return
                    try:
                        future.set_result(completed.result())
                    except BaseException as error:
                        future.set_exception(error)

                task.add_done_callback(transfer_result)

            try:
                loop.call_soon_threadsafe(schedule)
            except RuntimeError as error:
                self._futures.discard(future)
                close_input()
                raise LoopPortalClosedError("LoopPortal event loop is not accepting work") from (
                    error
                )
        return future

    def call(
        self,
        operation: Callable[..., Awaitable[T]] | Awaitable[T],
        *args: object,
        timeout: float | None = None,
        **kwargs: object,
    ) -> T:
        """Block for a result; calls from the portal loop thread are rejected."""
        if self._thread is threading.current_thread():
            if not callable(operation):
                close_awaitable(operation)
            raise RuntimeError("LoopPortal.call cannot block its own event-loop thread")
        timeout = validate_nonnegative_timeout(timeout, name="portal call timeout")
        future = self.submit(operation, *args, **kwargs)
        try:
            return future.result(timeout=timeout)
        except concurrent.futures.TimeoutError as error:
            if future.done() and not future.cancelled() and future.exception() is error:
                raise
            future.cancel()
            raise TimeoutError(
                "LoopPortal call exceeded its timeout; cancellation is cooperative"
            ) from error

    def cancel(self, future: concurrent.futures.Future[Any]) -> bool:
        if not isinstance(future, concurrent.futures.Future):
            raise TypeError("future must be a concurrent.futures.Future")
        with self._lock:
            if future not in self._futures:
                return False
        return future.cancel()

    def _restore_close_state(self) -> None:
        with self._lock:
            self._closing = False

    def close(self, timeout: float | None = None) -> None:
        """Cancel portal tasks and stop only after they have actually finished."""
        if self._thread is threading.current_thread():
            raise RuntimeError("LoopPortal.close cannot block its own event-loop thread")
        timeout = validate_nonnegative_timeout(timeout, name="portal close timeout")
        end = None if timeout is None else time.monotonic() + timeout
        with self._close_lock:
            with self._lock:
                if self._closed:
                    return
                thread = self._thread
                if thread is None:
                    self._closed = True
                    return
                self._closing = True
                starting = self._loop is None

            if starting:
                remaining = None if end is None else max(0.0, end - time.monotonic())
                if not self._ready.wait(remaining):
                    self._restore_close_state()
                    raise TimeoutError("LoopPortal event loop has not started")

            with self._lock:
                loop = self._loop
                startup_error = self._startup_error
                stopping = self._stopping

            if loop is None:
                remaining = None if end is None else max(0.0, end - time.monotonic())
                thread.join(remaining)
                if thread.is_alive():
                    self._restore_close_state()
                    raise TimeoutError("LoopPortal startup thread has not exited")
                with self._lock:
                    self._closed = True
                    self._closing = False
                    self._thread = None
                if startup_error is not None:
                    raise LoopPortalClosedError("LoopPortal event loop failed to start") from (
                        startup_error
                    )
                return

            if stopping or not loop.is_running():
                remaining = None if end is None else max(0.0, end - time.monotonic())
                thread.join(remaining)
                if thread.is_alive():
                    self._restore_close_state()
                    raise TimeoutError("LoopPortal thread is stopping but has not exited")
                with self._lock:
                    self._closed = True
                    self._closing = False
                    self._loop = None
                    self._thread = None
                return

            async def cancel_owned_tasks() -> None:
                current = asyncio.current_task()
                tasks = tuple(task for task in asyncio.all_tasks(loop) if task is not current)
                for task in tasks:
                    task.cancel()
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)

            while True:
                shutdown = asyncio.run_coroutine_threadsafe(cancel_owned_tasks(), loop)
                try:
                    remaining = None if end is None else max(0.0, end - time.monotonic())
                    shutdown.result(timeout=remaining)
                except concurrent.futures.CancelledError as error:
                    remaining = None if end is None else end - time.monotonic()
                    if remaining is None or remaining > 0:
                        continue
                    self._restore_close_state()
                    raise TimeoutError(
                        "LoopPortal shutdown was cancelled; loop ownership was retained"
                    ) from error
                except concurrent.futures.TimeoutError as error:
                    shutdown.cancel()
                    self._restore_close_state()
                    raise TimeoutError(
                        "LoopPortal tasks remain active; loop ownership was retained"
                    ) from error
                break
            with self._lock:
                self._stopping = True
            loop.call_soon_threadsafe(loop.stop)
            remaining = None if end is None else max(0.0, end - time.monotonic())
            thread.join(remaining)
            if thread.is_alive():
                self._restore_close_state()
                raise TimeoutError("LoopPortal thread did not stop before the timeout")
            with self._lock:
                self._closed = True
                self._closing = False
                self._loop = None
                self._thread = None

    def __enter__(self) -> LoopPortal:
        return self.start()

    def __exit__(self, *_args: object) -> None:
        self.close()
