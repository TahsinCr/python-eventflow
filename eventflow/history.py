"""Optional bounded in-memory history for event buses."""

from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from collections import deque
from itertools import islice
from typing import Any, Generic, TypeVar

from .filtering import EventFilter, EventType

T = TypeVar("T")


def _are_plain_types(event_type: EventType) -> bool:
    if isinstance(event_type, tuple):
        return all(type(candidate) is type for candidate in event_type)
    return type(event_type) is type


def _validate_query_limit(limit: int | None) -> None:
    if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0):
        raise ValueError("Event history query limit must be a positive integer")


def _query_by_type(
    events: tuple[T, ...],
    event_type: EventType,
    limit: int | None,
) -> tuple[T, ...]:
    if limit is None:
        return tuple(event for event in events if isinstance(event, event_type))

    matched: list[T] = []
    for event in reversed(events):
        if isinstance(event, event_type):
            matched.append(event)
            if len(matched) == limit:
                break
    matched.reverse()
    return tuple(matched)


class EventHistory(Generic[T], ABC):
    """Storage contract for custom histories used by event buses.

    Implementations must make ``append``, ``latest``, ``query``, ``clear``, and
    ``__len__`` safe under concurrent calls and return query results from a
    consistent snapshot. A history shared by multiple buses must provide the
    same guarantees across those buses; a bus lock only protects its own writes.
    """

    @abstractmethod
    def append(self, event: T) -> None: ...

    @abstractmethod
    def latest(self, event_filter: EventFilter[T] | None = None) -> T | None: ...

    @abstractmethod
    def query(
        self,
        event_filter: EventFilter[T] | None = None,
        *,
        limit: int | None = None,
    ) -> tuple[T, ...]: ...

    @abstractmethod
    def clear(self) -> None: ...

    @abstractmethod
    def __len__(self) -> int: ...


class MemoryEventHistory(EventHistory[T]):
    """Thread-safe bounded history preserving publication order."""

    def __init__(self, capacity: int = 1_000) -> None:
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity <= 0:
            raise ValueError("Event history capacity must be a positive integer")
        self._events: deque[T] = deque(maxlen=capacity)
        self._lock = threading.RLock()
        self._snapshot_cache: tuple[T, ...] | None = None
        self._snapshot_source: object = self._events
        self._type_results: dict[Any, tuple[T, ...]] = {}
        self._limited_type_results: dict[tuple[Any, int], tuple[T, ...]] = {}

    @property
    def capacity(self) -> int:
        return self._events.maxlen or 0

    def _snapshot_locked(self) -> tuple[T, ...]:
        if self._snapshot_source is self._events and self._snapshot_cache is not None:
            return self._snapshot_cache
        snapshot = tuple(self._events)
        if self._snapshot_source is not self._events:
            self._snapshot_source = self._events
            if self._type_results:
                self._type_results.clear()
            if self._limited_type_results:
                self._limited_type_results.clear()
        self._snapshot_cache = snapshot
        return snapshot

    def append(self, event: T) -> None:
        with self._lock:
            self._events.append(event)
            if self._snapshot_cache is not None:
                self._snapshot_cache = None
            if self._type_results:
                self._type_results.clear()
            if self._limited_type_results:
                self._limited_type_results.clear()

    def latest(self, event_filter: EventFilter[T] | None = None) -> T | None:
        with self._lock:
            if event_filter is None:
                return self._events[-1] if self._events else None
            if (
                type(event_filter) is EventFilter
                and event_filter.predicate is None
                and event_filter.event_type is not None
                and _are_plain_types(event_filter.event_type)
            ):
                event_type = event_filter.event_type
                for event in reversed(self._events):
                    if isinstance(event, event_type):
                        return event
                return None
            events = self._snapshot_locked()
        return next(
            (event for event in reversed(events) if event_filter.matches(event)),
            None,
        )

    def query(
        self,
        event_filter: EventFilter[T] | None = None,
        *,
        limit: int | None = None,
    ) -> tuple[T, ...]:
        _validate_query_limit(limit)
        with self._lock:
            if event_filter is None:
                if limit is None:
                    return self._snapshot_locked()
                recent = list(islice(reversed(self._events), limit))
                recent.reverse()
                return tuple(recent)
            if (
                type(event_filter) is EventFilter
                and event_filter.predicate is None
                and event_filter.event_type is not None
            ):
                event_type = event_filter.event_type
                if self._snapshot_source is not self._events:
                    self._snapshot_cache = None
                    self._snapshot_source = self._events
                    if self._type_results:
                        self._type_results.clear()
                    if self._limited_type_results:
                        self._limited_type_results.clear()
                plain_types = _are_plain_types(event_type)
                if plain_types:
                    cached_all = self._type_results.get(event_type)
                    if cached_all is not None:
                        return cached_all if limit is None else cached_all[-limit:]
                    if limit is not None:
                        cached_limited = self._limited_type_results.get((event_type, limit))
                        if cached_limited is not None:
                            return cached_limited
                if limit is not None and plain_types:
                    matched_limited: list[T] = []
                    for event in reversed(self._events):
                        if isinstance(event, event_type):
                            matched_limited.append(event)
                            if len(matched_limited) == limit:
                                break
                    matched_limited.reverse()
                    result = tuple(matched_limited)
                    if len(self._limited_type_results) < 64:
                        self._limited_type_results[(event_type, limit)] = result
                    return result
                events = self._snapshot_locked()
                result = _query_by_type(events, event_type, limit)
                if limit is None and plain_types:
                    self._type_results[event_type] = result
                return result
            events = self._snapshot_locked()
        if limit is None:
            return tuple(event for event in events if event_filter.matches(event))
        matched: list[T] = []
        for event in reversed(events):
            if event_filter.matches(event):
                matched.append(event)
                if len(matched) == limit:
                    break
        matched.reverse()
        return tuple(matched)

    def clear(self) -> None:
        with self._lock:
            self._events.clear()
            if self._snapshot_cache is not None:
                self._snapshot_cache = None
            if self._type_results:
                self._type_results.clear()
            if self._limited_type_results:
                self._limited_type_results.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._events)
