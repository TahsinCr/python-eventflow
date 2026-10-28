"""Thread-safe configuration and snapshot execution for synchronous pipelines."""

from __future__ import annotations

from collections.abc import Callable
from threading import get_ident
from time import perf_counter
from typing import Generic, Literal, TypeVar

from .._callbacks import synchronous_result
from .base import _PipelineBase
from .registry import _Entry

T = TypeVar("T")
Stage = Callable[[T], T]
Middleware = Callable[[T, Callable[[T], T]], T]


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
            result = synchronous_result(result)
        except BaseException as error:
            self._raise(error, self._observe(phase, name, started, error), name, phase, context)
        diagnostic = self._observe(phase, name, started)
        if diagnostic is not None:
            self._raise(diagnostic, None, name, "observer", context)
        return result

    def _compile(self) -> None:
        stages = self._stages.plan

        def execute(context: T) -> T:
            for entry in stages:
                context = self._invoke(entry.name, "stage", entry.callback, context)
            return context

        chain = execute
        for entry in reversed(self._middleware.plan):
            chain = self._layer(entry, chain)
        self._execution = chain

    def _layer(self, entry: _Entry[Middleware[T]], following: Callable[[T], T]) -> Callable[[T], T]:
        def execute(context: T) -> T:
            used = False
            active = True
            owner = get_ident()

            def next_(value: T) -> T:
                nonlocal used
                if used or not active or get_ident() != owner:
                    raise RuntimeError(
                        "Middleware continuation is single-use and scoped to its call"
                    )
                used = True
                return following(value)

            try:
                return self._invoke(entry.name, "middleware", entry.callback, context, next_)
            finally:
                active = False

        return execute

    def run(self, context: T) -> T:
        """Execute one snapshot; failures preserve their cause and stop later stages."""
        with self._lock:
            execute = self._execution
        started = perf_counter() if self._observer is not None else 0.0
        try:
            result = execute(context)
        except BaseException as error:
            self._raise(error, self._observe("run", None, started, error), None, "run", context)
        diagnostic = self._observe("run", None, started)
        if diagnostic is not None:
            self._raise(diagnostic, None, None, "observer", context)
        return result
