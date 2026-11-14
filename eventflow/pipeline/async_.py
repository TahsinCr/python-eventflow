"""Asyncio pipeline execution using the same registry and observer contracts."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Coroutine
from time import perf_counter
from typing import Any, Generic, Literal, TypeVar

from .._callbacks import _NON_AWAITABLE_TYPES, is_awaitable, synchronous_result
from ..observability import PipelineObserver
from .base import _PipelineBase
from .registry import _Entry

T = TypeVar("T")
Stage = Callable[[T], Awaitable[T]]
Middleware = Callable[[T, Callable[[T], Awaitable[T]]], Awaitable[T]]


class _AsyncContinuation(Generic[T]):
    __slots__ = ("_following", "_owner", "_used", "_active", "_started", "_continuation")

    def __init__(
        self,
        following: Callable[[T], Awaitable[T]],
        owner: asyncio.Task[Any] | None,
    ) -> None:
        self._following = following
        self._owner = owner
        self._used = False
        self._active = True
        self._started = False
        self._continuation: Coroutine[Any, Any, T] | None = None

    async def _advance(self, value: T) -> T:
        self._started = True
        if self._used or not self._active or asyncio.current_task() is not self._owner:
            raise RuntimeError("Middleware continuation is single-use and owned by its call")
        self._used = True
        return await self._following(value)

    def close_unawaited(self) -> None:
        continuation = self._continuation
        if continuation is not None and not self._started:
            continuation.close()
            self._continuation = None

    def __call__(self, value: T) -> Coroutine[Any, Any, T]:
        self.close_unawaited()
        self._started = False
        continuation = self._advance(value)
        self._continuation = continuation
        return continuation


class AsyncPipeline(_PipelineBase[Stage[T], Middleware[T]], Generic[T]):
    """Same-type async transformations on one owning event loop.

    Concurrent runs share configuration but have independent continuations and
    caller-owned payloads. Cancellation propagates directly into the active stage.
    """

    _asynchronous = True

    def __init__(self, *, observer: PipelineObserver | None = None) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        super().__init__(observer=observer)

    async def _invoke(
        self,
        name: str,
        phase: Literal["stage", "middleware"],
        callback: Callable[..., Awaitable[T]],
        context: T,
        continuation: Callable[[T], Awaitable[T]] | None = None,
    ) -> T:
        started = perf_counter() if self._observer is not None else 0.0
        try:
            operation = (
                callback(context) if continuation is None else callback(context, continuation)
            )
            if not is_awaitable(operation):
                raise TypeError("Async pipeline callbacks must return awaitables")
            result = await operation
            if type(result) not in _NON_AWAITABLE_TYPES:
                result = synchronous_result(result)
        except BaseException as error:
            self._raise(error, self._observe(phase, name, started, error), name, phase, context)
        if self._observer is not None:
            diagnostic = self._observe(phase, name, started)
            if diagnostic is not None:
                self._raise(diagnostic, None, name, "observer", context)
        return result

    def _compile(self) -> None:
        stages = self._stages.plan
        if self._observer is None:
            pairs = tuple((entry.name, entry.callback) for entry in stages)
            non_awaitable = _NON_AWAITABLE_TYPES

            async def execute(context: T) -> T:
                for name, callback in pairs:
                    try:
                        operation = callback(context)
                        if not is_awaitable(operation):
                            raise TypeError("Async pipeline callbacks must return awaitables")
                        result = await operation
                        context = (
                            result if type(result) in non_awaitable else synchronous_result(result)
                        )
                    except BaseException as error:
                        self._raise(error, None, name, "stage", context)
                return context

            chain: Callable[[T], Awaitable[T]] = execute
        else:

            async def observed_execute(context: T) -> T:
                for entry in stages:
                    context = await self._invoke(entry.name, "stage", entry.callback, context)
                return context

            chain = observed_execute
        for entry in reversed(self._middleware.plan):
            chain = self._layer(entry, chain)
        self._execution = chain

    def _layer(
        self, entry: _Entry[Middleware[T]], following: Callable[[T], Awaitable[T]]
    ) -> Callable[[T], Awaitable[T]]:
        name = entry.name
        callback = entry.callback
        if self._observer is None:
            non_awaitable = _NON_AWAITABLE_TYPES

            async def execute(context: T) -> T:
                continuation = _AsyncContinuation(following, asyncio.current_task())
                try:
                    operation = callback(context, continuation)
                    if not is_awaitable(operation):
                        raise TypeError("Async pipeline callbacks must return awaitables")
                    result = await operation
                    return result if type(result) in non_awaitable else synchronous_result(result)
                except BaseException as error:
                    self._raise(error, None, name, "middleware", context)
                finally:
                    continuation._active = False
                    continuation.close_unawaited()

            return execute

        async def observed_execute(context: T) -> T:
            continuation = _AsyncContinuation(following, asyncio.current_task())
            try:
                return await self._invoke(name, "middleware", callback, context, continuation)
            finally:
                continuation._active = False
                continuation.close_unawaited()

        return observed_execute

    async def run(self, context: T) -> T:
        """Execute a snapshot without starting worker tasks or swallowing cancellation."""
        loop = asyncio.get_running_loop()
        bound_loop = self._loop
        if bound_loop is None:
            with self._lock:
                if self._loop is None:
                    self._loop = loop
                elif self._loop is not loop:
                    raise RuntimeError("AsyncPipeline belongs to a different event loop")
                execute = self._execution
        elif bound_loop is not loop:
            raise RuntimeError("AsyncPipeline belongs to a different event loop")
        else:
            execute = self._execution
        if self._observer is None:
            return await execute(context)
        started = perf_counter()
        try:
            result = await execute(context)
        except BaseException as error:
            self._raise(error, self._observe("run", None, started, error), None, "run", context)
        diagnostic = self._observe("run", None, started)
        if diagnostic is not None:
            self._raise(diagnostic, None, None, "observer", context)
        return result
