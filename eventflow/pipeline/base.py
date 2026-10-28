"""Shared registry, decorators, snapshots and observer error policy."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from time import perf_counter
from typing import Generic, Literal, NoReturn, TypeVar

from .._callbacks import is_async_callable, synchronous_result
from ..context import current_correlation_id
from ..observability import ObservationKind, PipelineObservation, PipelineObserver
from .contracts import PipelineExecutionError
from .registry import _Registry

C = TypeVar("C")
M = TypeVar("M")


class _PipelineBase(Generic[C, M]):
    _asynchronous = False

    def __init__(self, *, observer: PipelineObserver | None = None) -> None:
        if observer is not None and (not callable(observer) or is_async_callable(observer)):
            raise TypeError("Pipeline observer must be a synchronous callable")
        self._observer = observer
        self._lock = threading.RLock()
        self._stages: _Registry[C] = _Registry()
        self._middleware: _Registry[M] = _Registry()
        self._compile()

    def _compile(self) -> None:
        raise NotImplementedError

    def _validate(self, callback: object) -> None:
        if not callable(callback):
            raise TypeError("Pipeline callback must be callable")
        if not self._asynchronous and is_async_callable(callback):
            raise TypeError("Synchronous pipeline requires synchronous callbacks")

    @property
    def stage_names(self) -> tuple[str, ...]:
        """Names in execution order, including disabled stages."""
        with self._lock:
            return tuple(entry.name for entry in self._stages.ordered)

    @property
    def middleware_names(self) -> tuple[str, ...]:
        """Names from outermost to innermost, including disabled middleware."""
        with self._lock:
            return tuple(entry.name for entry in self._middleware.ordered)

    def add(
        self, name: str, callback: C, *, before: str | None = None, after: str | None = None
    ) -> None:
        """Add a stage atomically; references must already exist and be acyclic."""
        self._validate(callback)
        with self._lock:
            self._stages.add(name, callback, before, after)
            self._compile()

    def stage(
        self, name: str, *, before: str | None = None, after: str | None = None
    ) -> Callable[[C], C]:
        """Register a stage while returning the original callable unchanged."""

        def register(callback: C) -> C:
            self.add(name, callback, before=before, after=after)
            return callback

        return register

    def remove(self, name: str) -> C:
        """Remove a stage and references to it; return its original callback."""
        with self._lock:
            callback = self._stages.remove(name)
            self._compile()
            return callback

    def replace(self, name: str, callback: C) -> None:
        """Replace a callback, retaining its name, order and enabled state."""
        self._validate(callback)
        with self._lock:
            self._stages.replace(name, callback)
            self._compile()

    def enable(self, name: str) -> None:
        """Enable a stage for future runs."""
        with self._lock:
            self._stages.enable(name, True)
            self._compile()

    def disable(self, name: str) -> None:
        """Disable a stage for future runs; ordering constraints remain intact."""
        with self._lock:
            self._stages.enable(name, False)
            self._compile()

    def add_middleware(
        self, name: str, callback: M, *, before: str | None = None, after: str | None = None
    ) -> None:
        """Register around middleware in a separate, ordered registry."""
        self._validate(callback)
        with self._lock:
            self._middleware.add(name, callback, before, after)
            self._compile()

    def middleware(
        self, name: str, *, before: str | None = None, after: str | None = None
    ) -> Callable[[M], M]:
        """Register middleware and return its original callable unchanged."""

        def register(callback: M) -> M:
            self.add_middleware(name, callback, before=before, after=after)
            return callback

        return register

    def remove_middleware(self, name: str) -> M:
        """Remove middleware and its ordering references."""
        with self._lock:
            callback = self._middleware.remove(name)
            self._compile()
            return callback

    def replace_middleware(self, name: str, callback: M) -> None:
        """Replace middleware without changing its position."""
        self._validate(callback)
        with self._lock:
            self._middleware.replace(name, callback)
            self._compile()

    def enable_middleware(self, name: str) -> None:
        """Enable middleware for future runs."""
        with self._lock:
            self._middleware.enable(name, True)
            self._compile()

    def disable_middleware(self, name: str) -> None:
        """Disable middleware for future runs."""
        with self._lock:
            self._middleware.enable(name, False)
            self._compile()

    def _observe(
        self,
        kind: ObservationKind,
        name: str | None,
        started: float,
        error: BaseException | None = None,
    ) -> Exception | None:
        if self._observer is None:
            return None
        status: Literal["success", "error", "cancelled"] = (
            "cancelled"
            if isinstance(error, asyncio.CancelledError)
            else "error"
            if error is not None
            else "success"
        )
        try:
            synchronous_result(
                self._observer(
                    PipelineObservation(
                        kind,
                        name,
                        perf_counter() - started,
                        status,
                        current_correlation_id(),
                        error,
                    )
                )
            )
        except Exception as observer_error:
            return observer_error
        return None

    @staticmethod
    def _raise(
        error: BaseException,
        diagnostic: Exception | None,
        name: str | None,
        phase: Literal["stage", "middleware", "run", "observer"],
        context: object,
    ) -> NoReturn:
        if isinstance(error, PipelineExecutionError):
            if diagnostic is not None:
                error.observer_errors += (diagnostic,)
            raise error
        if isinstance(error, Exception):
            failure = PipelineExecutionError(name, context, phase=phase)
            if diagnostic is not None:
                failure.observer_errors = (diagnostic,)
            raise failure from error
        if diagnostic is not None:
            raise error from diagnostic
        raise error
