"""Small owner-aware registry for explicitly constructed application services."""

from __future__ import annotations

import inspect
import threading
from collections.abc import Callable, Hashable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Literal, TypeVar

from ._callbacks import close_awaitable, synchronous_result
from ._lifecycle import raise_cleanup_errors

K = TypeVar("K", bound=Hashable)
T = TypeVar("T")
__all__ = ["RegistryChange", "ServiceRegistry"]


@dataclass(frozen=True, slots=True)
class RegistryChange:
    action: Literal["registered", "unregistered"]
    name: str
    owner: Hashable


def _normalize_name(name: str) -> str:
    if not isinstance(name, str):
        raise TypeError("service name must be a string")
    normalized = name.strip()
    if not normalized:
        raise ValueError("service name must be a non-empty string")
    return normalized


@dataclass(slots=True)
class _ServiceEntry:
    value: object
    owner: Hashable
    cleanup: Callable[[], object] | None


class ServiceRegistry:
    """Register services by explicit name and owner, with optional sync cleanup.

    Change observers run synchronously after the registry mutation and outside
    its lock. An observer failure never rolls back a completed mutation.
    """

    def __init__(self, *, observer: Callable[[RegistryChange], object] | None = None) -> None:
        if observer is not None and not callable(observer):
            raise TypeError("observer must be callable")
        self._observer = observer
        self._entries: dict[str, _ServiceEntry] = {}
        self._closing_owners: set[Hashable] = set()
        self._lock = threading.RLock()

    def register(
        self,
        name: str,
        service: T,
        *,
        owner: Hashable,
        close: str | Callable[[T], object] | None = None,
    ) -> T:
        name = _normalize_name(name)
        if close is None:
            cleanup = None
        elif isinstance(close, str):
            method = getattr(service, close, None)
            if not callable(method) or inspect.iscoroutinefunction(method):
                raise TypeError(f"service must provide synchronous {close!r}")
            cleanup = method
        elif callable(close):

            def cleanup_service() -> object:
                return close(service)

            cleanup = cleanup_service
        else:
            raise TypeError("close must be a method name, callable, or None")
        with self._lock:
            if owner in self._closing_owners:
                raise RuntimeError("Cannot register a service for an owner that is closing")
            if name in self._entries:
                raise KeyError(f"Service {name!r} is already registered")
            self._entries[name] = _ServiceEntry(service, owner, cleanup)
        self._notify(RegistryChange("registered", name, owner))
        return service

    def _notify(self, change: RegistryChange) -> None:
        if self._observer is not None:
            synchronous_result(self._observer(change))

    @staticmethod
    def _cleanup_entry(entry: _ServiceEntry) -> None:
        cleanup = entry.cleanup
        if cleanup is None:
            return
        result = cleanup()
        if inspect.isawaitable(result):
            close_awaitable(result)
            raise TypeError("ServiceRegistry only accepts synchronous cleanup")
        synchronous_result(result)

    def resolve(self, name: str) -> Any:
        name = _normalize_name(name)
        with self._lock:
            try:
                return self._entries[name].value
            except KeyError:
                raise KeyError(f"Service {name!r} is not registered") from None

    def snapshot(self) -> Mapping[str, object]:
        with self._lock:
            return MappingProxyType({name: entry.value for name, entry in self._entries.items()})

    def unregister(self, name: str, *, owner: Hashable) -> object:
        name = _normalize_name(name)
        with self._lock:
            if owner in self._closing_owners:
                raise RuntimeError("Service owner is already closing")
            entry = self._entries.get(name)
            if entry is None:
                raise KeyError(f"Service {name!r} is not registered")
            if entry.owner != owner:
                raise PermissionError(f"Owner {owner!r} does not own service {name!r}")
            self._closing_owners.add(owner)
        try:
            self._cleanup_entry(entry)
        except BaseException as error:
            with self._lock:
                self._closing_owners.discard(owner)
            raise_cleanup_errors(
                [error], message="Service cleanup failed", exclude_repeated_cause=True
            )
        with self._lock:
            if self._entries.get(name) is entry:
                self._entries.pop(name)
            self._closing_owners.discard(owner)
        self._notify(RegistryChange("unregistered", name, owner))
        return entry.value

    def close_owner(self, owner: Hashable) -> None:
        """Close owned services in reverse registration order and retain failures."""
        with self._lock:
            if owner in self._closing_owners:
                raise RuntimeError("Service owner is already closing")
            self._closing_owners.add(owner)
            pending = [
                (name, entry)
                for name, entry in reversed(tuple(self._entries.items()))
                if entry.owner == owner
            ]
        errors: list[BaseException] = []
        closed: list[tuple[str, _ServiceEntry]] = []
        for name, entry in pending:
            try:
                self._cleanup_entry(entry)
            except BaseException as error:
                errors.append(error)
            else:
                closed.append((name, entry))
        with self._lock:
            for name, entry in closed:
                if self._entries.get(name) is entry:
                    self._entries.pop(name)
            self._closing_owners.discard(owner)
        for name, entry in closed:
            try:
                self._notify(RegistryChange("unregistered", name, entry.owner))
            except BaseException as error:
                errors.append(error)
        raise_cleanup_errors(errors, message="Service cleanup failed", exclude_repeated_cause=True)

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)
