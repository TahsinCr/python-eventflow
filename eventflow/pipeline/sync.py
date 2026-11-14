"""Thread-safe configuration and snapshot execution for synchronous pipelines."""

from __future__ import annotations

from collections.abc import Callable
from threading import get_ident
from time import perf_counter
from typing import Generic, Literal, TypeVar

from .._callbacks import _NON_AWAITABLE_TYPES, synchronous_result
from .base import _PipelineBase
from .registry import _Entry

T = TypeVar("T")
Stage = Callable[[T], T]
Middleware = Callable[[T, Callable[[T], T]], T]


class _SyncContinuation(Generic[T]):
    __slots__ = ("_following", "_owner", "_used", "_active")

    def __init__(self, following: Callable[[T], T], owner: int) -> None:
        self._following = following
        self._owner = owner
        self._used = False
        self._active = True

    def __call__(self, value: T) -> T:
        if self._used or not self._active or get_ident() != self._owner:
            raise RuntimeError("Middleware continuation is single-use and scoped to its call")
        self._used = True
        return self._following(value)


class Pipeline(_PipelineBase[Stage[T], Middleware[T]], Generic[T]):
    """Run same-type transformations with optional around middleware.

    Each run uses a compiled immutable plan. Configuration changes apply to
    future runs. Payload mutation is the caller's responsibility.
    """

    def _invoke(
        self,
        name: str,
        phase: Literal["stage", "middleware"],
        callback: Callable[..., T],
        context: T,
        continuation: Callable[[T], T] | None = None,
    ) -> T:
        started = perf_counter() if self._observer is not None else 0.0
        try:
            result = callback(context) if continuation is None else callback(context, continuation)
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

            def execute(context: T) -> T:
                for name, callback in pairs:
                    try:
                        result = callback(context)
                        context = (
                            result if type(result) in non_awaitable else synchronous_result(result)
                        )
                    except BaseException as error:
                        self._raise(error, None, name, "stage", context)
                return context

            chain: Callable[[T], T] = execute
        else:

            def observed_execute(context: T) -> T:
                for entry in stages:
                    context = self._invoke(entry.name, "stage", entry.callback, context)
                return context

            chain = observed_execute
        for entry in reversed(self._middleware.plan):
            chain = self._layer(entry, chain)
        self._execution = chain

    def _layer(self, entry: _Entry[Middleware[T]], following: Callable[[T], T]) -> Callable[[T], T]:
        name = entry.name
        callback = entry.callback
        if self._observer is None:
            non_awaitable = _NON_AWAITABLE_TYPES

            def execute(context: T) -> T:
                continuation = _SyncContinuation(following, get_ident())
                try:
                    result = callback(context, continuation)
                    return result if type(result) in non_awaitable else synchronous_result(result)
                except BaseException as error:
                    self._raise(error, None, name, "middleware", context)
                finally:
                    continuation._active = False

            return execute

        def observed_execute(context: T) -> T:
            continuation = _SyncContinuation(following, get_ident())
            try:
                return self._invoke(name, "middleware", callback, context, continuation)
            finally:
                continuation._active = False

        return observed_execute

    def run(self, context: T) -> T:
        """Execute one snapshot; failures preserve their cause and stop later stages."""
        execute = self._execution
        if self._observer is None:
            return execute(context)
        started = perf_counter()
        try:
            result = execute(context)
        except BaseException as error:
            self._raise(error, self._observe("run", None, started, error), None, "run", context)
        diagnostic = self._observe("run", None, started)
        if diagnostic is not None:
            self._raise(diagnostic, None, None, "observer", context)
        return result
