"""Lazy single-consumer async streams with opt-in bounded prefetch."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable
from typing import Any, Generic, TypeVar, cast

from ._lifecycle import finish_cleanup
from .base import validate_positive_capacity

T = TypeVar("T")
U = TypeVar("U")
__all__ = ["AsyncStream"]


class AsyncStream(AsyncIterator[T], Generic[T]):
    """Own one async source and consume it once on one event loop.

    map/filter/batch are lazy and create no worker tasks. Use ``async with``
    around the terminal stream for deterministic cleanup on an early break.
    aclose cancels an outstanding read, then closes sources and producers.
    """

    def __init__(
        self, source: AsyncIterable[T], *, _upstream: AsyncStream[Any] | None = None
    ) -> None:
        if not isinstance(source, AsyncIterable):
            raise TypeError("AsyncStream source must be async iterable")
        self._source = source
        self._upstream = _upstream
        self._iterator: AsyncIterator[T] | None = None
        self._claimed = False
        self._closed = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._reader: asyncio.Task[Any] | None = None
        self._read_finished: asyncio.Event | None = None
        self._cleanup: asyncio.Task[None] | None = None

    def _bind_loop(self) -> None:
        loop = asyncio.get_running_loop()
        if self._loop is None:
            self._loop = loop
        elif self._loop is not loop:
            raise RuntimeError("AsyncStream belongs to a different event loop")

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
        self._bind_loop()
        if self._closed:
            raise StopAsyncIteration
        if self._reader is not None:
            raise RuntimeError("Concurrent reads from one AsyncStream are not supported")
        self._claimed = True
        self._reader = asyncio.current_task()
        try:
            if self._iterator is None:
                self._iterator = self._source.__aiter__()
            return await self._iterator.__anext__()
        except BaseException:
            self._reader = None
            if not self._closed:
                await self.aclose()
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

        async def transformed() -> AsyncIterator[U]:
            async for item in self:
                result = callback(item)
                yield await result if inspect.isawaitable(result) else cast(U, result)

        return AsyncStream(transformed(), _upstream=self)

    def filter(self, predicate: Callable[[T], bool | Awaitable[bool]]) -> AsyncStream[T]:
        """Keep matching items using a synchronous or awaitable predicate."""
        if not callable(predicate):
            raise TypeError("Stream filter predicate must be callable")

        async def filtered() -> AsyncIterator[T]:
            async for item in self:
                accepted = predicate(item)
                if await accepted if inspect.isawaitable(accepted) else accepted:
                    yield item

        return AsyncStream(filtered(), _upstream=self)

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

            producer = asyncio.create_task(produce(), name="EventFlowStreamProducer")
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
        try:
            source = self._iterator if self._iterator is not None else self._source
            close = getattr(source, "aclose", None)
            if close is not None:
                await close()
        finally:
            if self._upstream is not None:
                await self._upstream.aclose()

    async def aclose(self) -> None:
        """Stop reads and await producer/source cleanup, even during repeated cancellation."""
        self._bind_loop()
        if self._cleanup is None:
            self._closed = True
            reader = self._reader
            if reader is not None:
                # Only external concurrent close needs a read-completion event.
                self._read_finished = asyncio.Event()
            self._cleanup = asyncio.create_task(
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
