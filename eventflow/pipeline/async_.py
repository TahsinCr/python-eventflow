"""Asyncio pipeline execution using the same registry and observer contracts."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from time import perf_counter
from typing import Generic, Literal, TypeVar

from .._callbacks import synchronous_result
from ..observability import PipelineObserver
from .base import _PipelineBase
from .registry import _Entry

T = TypeVar("T")
Stage = Callable[[T], Awaitable[T]]
Middleware = Callable[[T, Callable[[T], Awaitable[T]]], Awaitable[T]]


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
            if not inspect.isawaitable(operation):
                raise TypeError("Async pipeline callbacks must return awaitables")
            result = synchronous_result(await operation)
        except BaseException as error:
            self._raise(error, self._observe(phase, name, started, error), name, phase, context)
        diagnostic = self._observe(phase, name, started)
        if diagnostic is not None:
            self._raise(diagnostic, None, name, "observer", context)
        return result

    def _compile(self) -> None:
        stages = self._stages.plan

        async def execute(context: T) -> T:
            for entry in stages:
                context = await self._invoke(entry.name, "stage", entry.callback, context)
            return context

        chain = execute
        for entry in reversed(self._middleware.plan):
            chain = self._layer(entry, chain)
        self._execution = chain

    def _layer(
        self, entry: _Entry[Middleware[T]], following: Callable[[T], Awaitable[T]]
    ) -> Callable[[T], Awaitable[T]]:
        async def execute(context: T) -> T:
            used = False
            active = True
            owner = asyncio.current_task()

            async def next_(value: T) -> T:
                nonlocal used
                if used or not active or asyncio.current_task() is not owner:
                    raise RuntimeError(
                        "Middleware continuation is single-use and owned by its call"
                    )
                used = True
                return await following(value)

            try:
                return await self._invoke(entry.name, "middleware", entry.callback, context, next_)
            finally:
                active = False

        return execute

    async def run(self, context: T) -> T:
        """Execute a snapshot without starting worker tasks or swallowing cancellation."""
        loop = asyncio.get_running_loop()
        with self._lock:
            if self._loop is None:
                self._loop = loop
            elif self._loop is not loop:
                raise RuntimeError("AsyncPipeline belongs to a different event loop")
            execute = self._execution
        started = perf_counter() if self._observer is not None else 0.0
        try:
            result = await execute(context)
        except BaseException as error:
            self._raise(error, self._observe("run", None, started, error), None, "run", context)
        diagnostic = self._observe("run", None, started)
        if diagnostic is not None:
            self._raise(diagnostic, None, None, "observer", context)
        return result
