"""Explicit ownership of existing subscription and cancellation handles."""

from __future__ import annotations

import asyncio
import inspect
import threading
from collections.abc import Awaitable
from typing import Protocol, TypeVar

from ._callbacks import is_async_callable, synchronous_result
from ._lifecycle import finish_cleanup
from .compatibility import BaseExceptionGroup, ExceptionGroup

__all__ = ["CancelHandle", "AsyncCancelHandle", "SubscriptionScope", "AsyncSubscriptionScope"]


class CancelHandle(Protocol):
    """A synchronous, preferably idempotent cancellation handle."""

    def cancel(self) -> None: ...


class AsyncCancelHandle(Protocol):
    """A handle whose cancel method returns an awaitable, preferably idempotently."""

    def cancel(self) -> Awaitable[None]: ...


H = TypeVar("H", bound=CancelHandle)
A = TypeVar("A", bound=AsyncCancelHandle)


def _raise_cleanup_errors(errors: list[BaseException]) -> None:
    if not errors:
        return
    for index, error in enumerate(errors):
        if not isinstance(error, Exception):
            remaining = errors[:index] + errors[index + 1 :]
            if remaining:
                raise error from BaseExceptionGroup("Subscription scope cleanup failed", remaining)
            raise error
    ordinary = [error for error in errors if isinstance(error, Exception)]
    raise ExceptionGroup("Subscription scope cleanup failed", ordinary)


class SubscriptionScope:
    """Cancel owned handles in reverse registration order on exit.

    All handles are attempted. Failed handles remain owned for a later retry.
    A closed scope rejects additions, including after an unsuccessful close.
    """

    def __init__(self) -> None:
        self._handles: list[CancelHandle] = []
        self._lock = threading.RLock()
        self._close_lock = threading.RLock()
        self._closed = False

    @property
    def closed(self) -> bool:
        """Whether the scope has stopped accepting handles."""
        with self._lock:
            return self._closed

    def add(self, handle: H) -> H:
        """Own a handle and return it unchanged; duplicate identities are ignored."""
        cancel = getattr(handle, "cancel", None)
        if not callable(cancel) or is_async_callable(cancel):
            raise TypeError("SubscriptionScope requires a synchronous cancel handle")
        with self._lock:
            if self._closed:
                raise RuntimeError("Cannot add a handle to a closed subscription scope")
            if not any(item is handle for item in self._handles):
                self._handles.append(handle)
        return handle

    def close(self) -> None:
        """Attempt every cancellation, retaining failures and reporting them together."""
        with self._close_lock:
            with self._lock:
                self._closed = True
                pending, self._handles = list(reversed(self._handles)), []
            failed: list[CancelHandle] = []
            errors: list[BaseException] = []
            for handle in pending:
                try:
                    synchronous_result(handle.cancel())
                except BaseException as error:
                    failed.append(handle)
                    errors.append(error)
            with self._lock:
                self._handles = list(reversed(failed))
            _raise_cleanup_errors(errors)

    def __enter__(self) -> SubscriptionScope:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


class AsyncSubscriptionScope:
    """Own async handles on one loop and finish cleanup despite caller cancellation.

    Cleanup runs sequentially in reverse registration order. Concurrent aclose
    callers await the same cleanup task. Failed handles can be retried.
    """

    def __init__(self) -> None:
        self._handles: list[AsyncCancelHandle] = []
        self._closed = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._cleanup: asyncio.Task[list[BaseException]] | None = None

    def _bind_loop(self) -> None:
        loop = asyncio.get_running_loop()
        if self._loop is None:
            self._loop = loop
        elif self._loop is not loop:
            raise RuntimeError("AsyncSubscriptionScope belongs to a different event loop")

    @property
    def closed(self) -> bool:
        """Whether the scope has stopped accepting handles."""
        return self._closed

    def add(self, handle: A) -> A:
        """Own an awaitable cancel handle and return it unchanged.

        The cancel method is not called here because doing so would cancel the
        handle. Its return value is validated when the scope closes.
        """
        if not callable(getattr(handle, "cancel", None)):
            raise TypeError("AsyncSubscriptionScope requires an awaitable cancel handle")
        if self._loop is not None:
            self._bind_loop()
        if self._closed:
            raise RuntimeError("Cannot add a handle to a closed subscription scope")
        if not any(item is handle for item in self._handles):
            self._handles.append(handle)
        return handle

    async def _cancel_handles(self) -> list[BaseException]:
        pending, self._handles = list(reversed(self._handles)), []
        failed: list[AsyncCancelHandle] = []
        errors: list[BaseException] = []
        for handle in pending:
            try:
                operation = handle.cancel()
                if not inspect.isawaitable(operation):
                    raise TypeError("Async cancel handle cancel() must return an awaitable")
                synchronous_result(await operation)
            except BaseException as error:
                failed.append(handle)
                errors.append(error)
        self._handles = list(reversed(failed))
        return errors

    async def aclose(self) -> None:
        """Finish all cancellations; caller cancellation propagates after cleanup."""
        self._bind_loop()
        self._closed = True
        if self._cleanup is None or (self._cleanup.done() and self._handles):
            self._cleanup = asyncio.create_task(
                self._cancel_handles(), name="EventFlowScopeCleanup"
            )
        try:
            errors = await finish_cleanup(self._cleanup)
        except asyncio.CancelledError as cancellation:
            if self._cleanup.done() and not self._cleanup.cancelled():
                errors = self._cleanup.result()
                if errors:
                    raise cancellation from BaseExceptionGroup(
                        "Subscription scope cleanup failed", errors
                    )
            raise
        _raise_cleanup_errors(errors)

    async def __aenter__(self) -> AsyncSubscriptionScope:
        self._bind_loop()
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.aclose()
