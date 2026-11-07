"""Explicit ownership of existing subscription and cancellation handles."""

from __future__ import annotations

import asyncio
import inspect
import threading
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol, TypeVar

from ._callbacks import is_async_callable, synchronous_result
from ._lifecycle import (
    bind_running_loop,
    create_task_safely,
    finish_cleanup_errors,
    raise_cleanup_errors,
)

__all__ = [
    "CancelHandle",
    "AsyncCancelHandle",
    "SubscriptionScope",
    "AsyncSubscriptionScope",
    "ResourceScope",
    "AsyncResourceScope",
]


class CancelHandle(Protocol):
    """A synchronous, preferably idempotent cancellation handle."""

    def cancel(self) -> None: ...


class AsyncCancelHandle(Protocol):
    """A handle whose cancel method returns an awaitable, preferably idempotently."""

    def cancel(self) -> Awaitable[None]: ...


H = TypeVar("H", bound=CancelHandle)
A = TypeVar("A", bound=AsyncCancelHandle)


class SubscriptionScope:
    """Cancel owned handles in reverse registration order on exit.

    All handles are attempted. Failed handles remain owned for a later retry.
    A closed scope rejects additions, including after an unsuccessful close.
    """

    def __init__(self) -> None:
        self._handles: list[CancelHandle] = []
        self._handle_ids: set[int] = set()
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
            handle_id = id(handle)
            if handle_id not in self._handle_ids:
                self._handles.append(handle)
                self._handle_ids.add(handle_id)
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
                self._handle_ids = {id(handle) for handle in failed}
            raise_cleanup_errors(errors, message="Subscription scope cleanup failed")

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
        self._handle_ids: set[int] = set()
        self._closed = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._cleanup: asyncio.Task[list[BaseException]] | None = None

    def _bind_loop(self) -> None:
        self._loop = bind_running_loop(self._loop, "AsyncSubscriptionScope")

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
        handle_id = id(handle)
        if handle_id not in self._handle_ids:
            self._handles.append(handle)
            self._handle_ids.add(handle_id)
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
        self._handle_ids = {id(handle) for handle in failed}
        return errors

    async def aclose(self) -> None:
        """Finish all cancellations; caller cancellation propagates after cleanup."""
        self._bind_loop()
        if asyncio.current_task() is self._cleanup:
            raise RuntimeError("A subscription cleanup cannot wait for its own scope")
        self._closed = True
        cleanup = self._cleanup
        if cleanup is None or (cleanup.done() and self._handles):
            cleanup = create_task_safely(self._cancel_handles(), name="EventFlowScopeCleanup")
            self._cleanup = cleanup
        errors = await finish_cleanup_errors(cleanup, message="Subscription scope cleanup failed")
        raise_cleanup_errors(errors, message="Subscription scope cleanup failed")

    async def __aenter__(self) -> AsyncSubscriptionScope:
        self._bind_loop()
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.aclose()


@dataclass(slots=True)
class _ResourceEntry:
    resource: object
    cleanup: Callable[[], object] | None


def _resolve_cleanup(
    resource: object, close: str | Callable[[object], object]
) -> Callable[[], object]:
    if isinstance(close, str):
        cleanup = getattr(resource, close, None)
        if not callable(cleanup):
            raise TypeError(f"resource must provide callable {close!r}")
        return cleanup
    if not callable(close):
        raise TypeError("close must be a method name or callable")
    return lambda: close(resource)


class ResourceScope:
    """Own sync resources and deferred cleanup in reverse registration order."""

    def __init__(self) -> None:
        self._entries: list[_ResourceEntry] = []
        self._resource_ids: set[int] = set()
        self._lock = threading.RLock()
        self._close_lock = threading.RLock()
        self._closed = False
        self._inflight = 0
        self._closing_thread: int | None = None

    @property
    def closed(self) -> bool:
        with self._lock:
            return self._closed

    def add(
        self,
        resource: object,
        *,
        close: str | Callable[[object], object] = "close",
        owned: bool = True,
    ) -> object:
        """Register a resource; borrowed resources are tracked but never closed."""
        if not isinstance(owned, bool):
            raise TypeError("owned must be a bool")
        cleanup = _resolve_cleanup(resource, close) if owned else None
        if cleanup is not None and is_async_callable(cleanup):
            raise TypeError("ResourceScope requires synchronous cleanup")
        with self._lock:
            if self._closed:
                raise RuntimeError("Cannot add a resource to a closed scope")
            resource_id = id(resource)
            if resource_id not in self._resource_ids:
                self._entries.append(_ResourceEntry(resource, cleanup))
                self._resource_ids.add(resource_id)
        return resource

    def defer(self, cleanup: Callable[[], object]) -> Callable[[], object]:
        """Register a zero-argument cleanup function."""
        if not callable(cleanup) or is_async_callable(cleanup):
            raise TypeError("ResourceScope.defer requires a synchronous callable")
        with self._lock:
            if self._closed:
                raise RuntimeError("Cannot add cleanup to a closed scope")
            self._entries.append(_ResourceEntry(cleanup, cleanup))
        return cleanup

    def detach(self, resource: object) -> object:
        """Remove ownership and return the resource without running its cleanup."""
        with self._lock:
            for index in range(len(self._entries) - 1, -1, -1):
                if self._entries[index].resource is resource:
                    detached = self._entries.pop(index).resource
                    self._resource_ids.discard(id(detached))
                    return detached
        raise KeyError("resource is not registered in this scope")

    def pending_cleanup(self) -> int:
        with self._lock:
            return self._inflight + sum(entry.cleanup is not None for entry in self._entries)

    def close(self) -> None:
        """Attempt all cleanups; failed entries remain available for a later retry."""
        with self._close_lock:
            with self._lock:
                current_thread = threading.get_ident()
                if self._closing_thread == current_thread:
                    return
                self._closing_thread = current_thread
                self._closed = True
                pending, self._entries = list(reversed(self._entries)), []
                self._inflight = sum(entry.cleanup is not None for entry in pending)
            failed: list[_ResourceEntry] = []
            errors: list[BaseException] = []
            try:
                for entry in pending:
                    if entry.cleanup is None:
                        continue
                    try:
                        synchronous_result(entry.cleanup())
                    except BaseException as error:
                        failed.append(entry)
                        errors.append(error)
                    finally:
                        with self._lock:
                            self._inflight -= 1
                with self._lock:
                    self._entries = list(reversed(failed))
                    self._resource_ids = {id(entry.resource) for entry in failed}
                raise_cleanup_errors(errors, message="Resource scope cleanup failed")
            finally:
                with self._lock:
                    self._closing_thread = None

    def __enter__(self) -> ResourceScope:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


class AsyncResourceScope:
    """Own sync or async resources and finish cleanup despite caller cancellation."""

    def __init__(self) -> None:
        self._entries: list[_ResourceEntry] = []
        self._resource_ids: set[int] = set()
        self._closed = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._cleanup: asyncio.Task[list[BaseException]] | None = None
        self._inflight = 0

    def _bind_loop(self) -> None:
        self._loop = bind_running_loop(self._loop, "AsyncResourceScope")

    @property
    def closed(self) -> bool:
        return self._closed

    def add(
        self,
        resource: object,
        *,
        close: str | Callable[[object], object] = "aclose",
        owned: bool = True,
    ) -> object:
        """Register a resource; cleanup may be sync or async and remains sequential."""
        if not isinstance(owned, bool):
            raise TypeError("owned must be a bool")
        cleanup = _resolve_cleanup(resource, close) if owned else None
        if self._loop is not None:
            self._bind_loop()
        if self._closed:
            raise RuntimeError("Cannot add a resource to a closed scope")
        resource_id = id(resource)
        if resource_id not in self._resource_ids:
            self._entries.append(_ResourceEntry(resource, cleanup))
            self._resource_ids.add(resource_id)
        return resource

    def defer(self, cleanup: Callable[[], object]) -> Callable[[], object]:
        """Register a zero-argument sync or async cleanup function."""
        if not callable(cleanup):
            raise TypeError("cleanup must be callable")
        if self._loop is not None:
            self._bind_loop()
        if self._closed:
            raise RuntimeError("Cannot add cleanup to a closed scope")
        self._entries.append(_ResourceEntry(cleanup, cleanup))
        return cleanup

    def detach(self, resource: object) -> object:
        if self._loop is not None:
            self._bind_loop()
        for index in range(len(self._entries) - 1, -1, -1):
            if self._entries[index].resource is resource:
                detached = self._entries.pop(index).resource
                self._resource_ids.discard(id(detached))
                return detached
        raise KeyError("resource is not registered in this scope")

    def pending_cleanup(self) -> int:
        return self._inflight + sum(entry.cleanup is not None for entry in self._entries)

    async def _run_cleanups(self) -> list[BaseException]:
        pending, self._entries = list(reversed(self._entries)), []
        self._inflight = sum(entry.cleanup is not None for entry in pending)
        failed: list[_ResourceEntry] = []
        errors: list[BaseException] = []
        for entry in pending:
            if entry.cleanup is None:
                continue
            try:
                result = entry.cleanup()
                if inspect.isawaitable(result):
                    await result
                else:
                    synchronous_result(result)
            except BaseException as error:
                failed.append(entry)
                errors.append(error)
            finally:
                self._inflight -= 1
        self._entries = list(reversed(failed))
        self._resource_ids = {id(entry.resource) for entry in failed}
        return errors

    async def aclose(self) -> None:
        self._bind_loop()
        if asyncio.current_task() is self._cleanup:
            raise RuntimeError("A resource cleanup cannot wait for its own scope")
        self._closed = True
        cleanup = self._cleanup
        if cleanup is None or (cleanup.done() and self._entries):
            cleanup = create_task_safely(self._run_cleanups(), name="EventFlowResourceCleanup")
            self._cleanup = cleanup
        errors = await finish_cleanup_errors(cleanup, message="Resource scope cleanup failed")
        raise_cleanup_errors(errors, message="Resource scope cleanup failed")

    async def __aenter__(self) -> AsyncResourceScope:
        self._bind_loop()
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.aclose()
