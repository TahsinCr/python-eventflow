"""Lazy single-consumer async streams with opt-in bounded prefetch."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable
from typing import Any, Generic, Literal, NoReturn, TypeVar, cast

from ._lifecycle import bind_running_loop, create_task_safely, finish_cleanup
from .base import validate_finite_duration, validate_positive_capacity

T = TypeVar("T")
U = TypeVar("U")
_StreamOperator = tuple[Literal["map", "filter"], Callable[[Any], object]]
__all__ = ["AsyncStream"]


def _raise_with_cleanup(primary: BaseException, cleanup: BaseException) -> NoReturn:
    if cleanup is primary:
        raise primary
    if isinstance(cleanup, Exception):
        raise primary from cleanup
    raise cleanup from primary


class AsyncStream(AsyncIterator[T], Generic[T]):
    """Own one async source and consume it once on one event loop.

    map/filter/batch are lazy and create no worker tasks. Use ``async with``
    around the terminal stream for deterministic cleanup on an early break.
    aclose cancels an outstanding read, then closes sources and producers.
    """

    def __init__(
        self,
        source: AsyncIterable[T],
        *,
        _upstream: AsyncStream[Any] | None = None,
    ) -> None:
        if not isinstance(source, AsyncIterable):
            raise TypeError("AsyncStream source must be async iterable")
        self._source = source
        self._upstream = _upstream
        self._operator_source: AsyncStream[Any] | None = None
        self._operators: tuple[_StreamOperator, ...] = ()
        self._iterator: AsyncIterator[T] | None = None
        self._claimed = False
        self._closed = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._consumer: asyncio.Task[Any] | None = None
        self._reader: asyncio.Task[Any] | None = None
        self._read_finished: asyncio.Event | None = None
        self._cleanup: asyncio.Task[None] | None = None

    def _bind_loop(self) -> None:
        self._loop = bind_running_loop(self._loop, "AsyncStream")

    @property
    def closed(self) -> bool:
        """Whether the stream no longer accepts reads."""
        return self._closed

    def __aiter__(self) -> AsyncStream[T]:
        self._bind_loop()
        if self._claimed:
            raise RuntimeError("AsyncStream supports one consumption only")
        if self._closed:
            raise RuntimeError("AsyncStream is closed")
        self._claimed = True
        return self

    async def __anext__(self) -> T:
        task = asyncio.current_task()
        if self._consumer is not task:
            self._bind_loop()
        if self._closed:
            raise StopAsyncIteration
        if self._reader is not None:
            raise RuntimeError("Concurrent reads from one AsyncStream are not supported")
        if self._consumer is None:
            self._consumer = task
        elif self._consumer is not task:
            raise RuntimeError("AsyncStream must be consumed by one task")
        self._claimed = True
        self._reader = task
        try:
            source = self._operator_source
            if source is not None:
                use_fast_path = (
                    type(source).__anext__ is AsyncStream.__anext__
                    and "__anext__" not in source.__dict__
                )
                while True:
                    if use_fast_path:
                        if source._consumer is not task:
                            source._bind_loop()
                        if source._closed:
                            raise StopAsyncIteration
                        if source._reader is not None:
                            raise RuntimeError(
                                "Concurrent reads from one AsyncStream are not supported"
                            )
                        if source._consumer is None:
                            source._consumer = task
                        elif source._consumer is not task:
                            raise RuntimeError("AsyncStream must be consumed by one task")
                        source._claimed = True
                        source._reader = task
                        try:
                            if source._iterator is None:
                                source._iterator = source._source.__aiter__()
                            value: Any = await source._iterator.__anext__()
                        finally:
                            source._reader = None
                            if source._read_finished is not None:
                                source._read_finished.set()
                            source._read_finished = None
                    else:
                        value = await source.__anext__()
                    for operation, transform in self._operators:
                        result = transform(value)
                        result_type = type(result)
                        if (
                            result_type is not int
                            and result_type is not bool
                            and inspect.isawaitable(result)
                        ):
                            result = await result
                        if operation == "filter":
                            if type(result) is not bool:
                                raise TypeError("Stream filter predicates must return bool")
                            if not result:
                                break
                        else:
                            value = result
                    else:
                        return cast(T, value)
            if self._iterator is None:
                self._iterator = self._source.__aiter__()
            return await self._iterator.__anext__()
        except BaseException as error:
            self._reader = None
            if not self._closed:
                try:
                    await self.aclose()
                except BaseException as cleanup_error:
                    if isinstance(error, StopAsyncIteration):
                        raise cleanup_error
                    _raise_with_cleanup(error, cleanup_error)
            raise
        finally:
            self._reader = None
            if self._read_finished is not None:
                self._read_finished.set()
            self._read_finished = None

    def map(self, callback: Callable[[T], U | Awaitable[U]]) -> AsyncStream[U]:
        """Transform each item with a sync or awaitable callback, without threads."""
        if not callable(callback):
            raise TypeError("Stream map callback must be callable")
        return cast(AsyncStream[U], self._transform("map", callback))

    def filter(self, predicate: Callable[[T], bool | Awaitable[bool]]) -> AsyncStream[T]:
        """Keep matching items using a synchronous or awaitable predicate."""
        if not callable(predicate):
            raise TypeError("Stream filter predicate must be callable")

        return cast(AsyncStream[T], self._transform("filter", predicate))

    def _transform(
        self,
        kind: Literal["map", "filter"],
        callback: Callable[[Any], object],
    ) -> AsyncStream[Any]:
        upstream = self._operator_source if self._operator_source is not None else self
        operator: _StreamOperator = (kind, callback)
        operators = (*self._operators, operator)
        stream = AsyncStream(upstream, _upstream=upstream)
        stream._operator_source = upstream
        stream._operators = operators
        return stream

    def batch(self, size: int, *, partial: bool = True) -> AsyncStream[tuple[T, ...]]:
        """Yield fixed-size tuples and optionally the final partial tuple."""
        size = validate_positive_capacity(size, name="Stream batch size")
        if not isinstance(partial, bool):
            raise TypeError("Stream partial flag must be a bool")

        async def batched() -> AsyncIterator[tuple[T, ...]]:
            items = []
            async for item in self:
                items.append(item)
                if len(items) == size:
                    yield tuple(items)
                    items.clear()
            if items and partial:
                yield tuple(items)

        return AsyncStream(batched(), _upstream=self)

    def batch_within(
        self,
        max_items: int,
        max_delay: float,
        *,
        partial: bool = True,
    ) -> AsyncStream[tuple[T, ...]]:
        """Emit a batch when full or when max_delay elapses after its first item.

        One producer accumulates a bounded batch and transfers it through one
        pending slot. Demand-gated handoff bounds internal buffering to one
        batch and one source item in flight. A single monotonic timer serves
        each batch; neither tasks nor timer handles are created per item.
        """
        max_items = validate_positive_capacity(max_items, name="Timed batch max_items")
        delay = validate_finite_duration(max_delay, name="Timed batch max_delay")
        if not isinstance(partial, bool):
            raise TypeError("Stream partial flag must be a bool")

        async def batched() -> AsyncIterator[tuple[T, ...]]:
            loop = asyncio.get_running_loop()
            available = asyncio.Event()
            space = asyncio.Event()
            space.set()
            values: list[T] = []
            pending: tuple[T, ...] | None = None
            producer_done = False
            stopped = False
            source_error: BaseException | None = None
            timer: asyncio.TimerHandle | None = None
            deadline: float | None = None
            end = object()

            def cancel_timer() -> None:
                nonlocal timer, deadline
                if timer is not None:
                    timer.cancel()
                    timer = None
                deadline = None

            def flush() -> None:
                nonlocal pending
                if not values:
                    return
                pending = tuple(values)
                values.clear()
                cancel_timer()
                space.clear()
                available.set()

            def expire() -> None:
                nonlocal timer
                timer = None
                flush()

            async def produce() -> None:
                nonlocal producer_done, source_error, pending, timer, deadline
                try:
                    async for item in self:
                        if not space.is_set():
                            await space.wait()
                        if stopped:
                            return
                        arrived = loop.time()
                        if values and deadline is not None and arrived >= deadline:
                            flush()
                            await space.wait()
                            if stopped:
                                return
                            arrived = loop.time()
                        values.append(item)
                        if len(values) == 1:
                            deadline = arrived + delay
                            timer = loop.call_at(deadline, expire)
                        if len(values) == max_items:
                            flush()
                except BaseException as error:
                    if not (stopped and isinstance(error, asyncio.CancelledError)):
                        source_error = error
                finally:
                    if stopped:
                        values.clear()
                        pending = None
                        cancel_timer()
                    elif values and partial:
                        flush()
                    else:
                        values.clear()
                        cancel_timer()
                    producer_done = True
                    available.set()

            def take_batch() -> tuple[T, ...] | None:
                nonlocal pending
                value = pending
                if value is None:
                    return None
                pending = None
                if not producer_done:
                    available.clear()
                return value

            async def next_batch() -> tuple[T, ...] | object:
                space.set()
                while True:
                    value = take_batch()
                    if value is not None:
                        return value
                    if producer_done:
                        return end
                    available.clear()
                    await available.wait()

            producer = create_task_safely(produce(), name="EventFlowTimedBatchProducer")
            try:
                while True:
                    item = await next_batch()
                    if item is end:
                        if source_error is not None:
                            raise source_error
                        break
                    yield cast(tuple[T, ...], item)
            finally:
                cancel_timer()
                stopped = True
                producer.cancel()
                await finish_cleanup(asyncio.gather(producer, return_exceptions=True))

        return AsyncStream(batched(), _upstream=self)

    def buffer(self, capacity: int) -> AsyncStream[T]:
        """Prefetch at most capacity queued items plus one in-flight source item.

        A full queue blocks its single producer. Values are never dropped.
        Source failures propagate after already queued items have been read.
        """
        capacity = validate_positive_capacity(capacity, name="Stream buffer capacity")

        async def buffered() -> AsyncIterator[T]:
            queue: asyncio.Queue[object] = asyncio.Queue(maxsize=capacity)
            end = object()
            error: BaseException | None = None
            closing = False

            async def produce() -> None:
                nonlocal error
                try:
                    async for item in self:
                        await queue.put(item)
                except BaseException as exc:
                    if closing and isinstance(exc, asyncio.CancelledError):
                        raise
                    error = exc
                await queue.put(end)

            producer = create_task_safely(produce(), name="EventFlowStreamProducer")
            try:
                while True:
                    item = await queue.get()
                    if item is end:
                        if error is not None:
                            raise error
                        break
                    yield cast(T, item)
            finally:
                closing = True
                producer.cancel()
                await finish_cleanup(asyncio.gather(producer, return_exceptions=True))

        return AsyncStream(buffered(), _upstream=self)

    async def _shutdown(
        self, reader: asyncio.Task[Any] | None, read_finished: asyncio.Event | None
    ) -> None:
        if reader is not None and not reader.done():
            reader.cancel()
            if read_finished is not None:
                await read_finished.wait()
        source_error: BaseException | None = None
        if self._operator_source is None:
            try:
                source = self._iterator if self._iterator is not None else self._source
                close = getattr(source, "aclose", None)
                if close is not None:
                    await close()
            except BaseException as error:
                source_error = error
        if self._upstream is not None:
            try:
                await self._upstream.aclose()
            except BaseException as cleanup_error:
                if source_error is not None:
                    _raise_with_cleanup(source_error, cleanup_error)
                raise
        if source_error is not None:
            raise source_error

    async def aclose(self) -> None:
        """Stop reads and await producer/source cleanup, even during repeated cancellation."""
        self._bind_loop()
        if self._reader is asyncio.current_task():
            raise RuntimeError("Cannot close an AsyncStream from its active read task")
        if self._cleanup is None:
            self._closed = True
            reader = self._reader
            if reader is not None:
                # Only external concurrent close needs a read-completion event.
                self._read_finished = asyncio.Event()
            self._cleanup = create_task_safely(
                self._shutdown(reader, self._read_finished),
                name="EventFlowStreamCleanup",
            )
        await finish_cleanup(self._cleanup)

    async def __aenter__(self) -> AsyncStream[T]:
        self._bind_loop()
        if self._closed:
            raise RuntimeError("AsyncStream is closed")
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.aclose()
