"""Optional bounded in-memory history for event buses."""

from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from collections import deque
from itertools import islice
from typing import Generic, TypeVar, cast

from ._native import HISTORY_TYPE_FILTER
from .filtering import EventFilter, EventType

T = TypeVar("T")


def _validate_query_limit(limit: int | None) -> None:
    if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0):
        raise ValueError("Event history query limit must be a positive integer")


def _query_by_type_python(
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


def _query_by_type_native(
    events: tuple[T, ...],
    event_type: EventType,
    limit: int | None,
) -> tuple[T, ...]:
    if limit is not None:
        return _query_by_type_python(events, event_type, limit)
    native_filter = HISTORY_TYPE_FILTER
    if native_filter is None:
        raise RuntimeError("the experimental native history filter was not loaded")
    return cast(tuple[T, ...], native_filter(events, event_type, None))


_query_by_type = _query_by_type_python
if HISTORY_TYPE_FILTER is not None:
    _query_by_type = _query_by_type_native


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

    @property
    def capacity(self) -> int:
        return self._events.maxlen or 0

    def append(self, event: T) -> None:
        with self._lock:
            self._events.append(event)

    def latest(self, event_filter: EventFilter[T] | None = None) -> T | None:
        with self._lock:
            if event_filter is None:
                return self._events[-1] if self._events else None
            events = tuple(self._events)
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
                    return tuple(self._events)
                recent = list(islice(reversed(self._events), limit))
                recent.reverse()
                return tuple(recent)
            events = tuple(self._events)
        if (
            type(event_filter) is EventFilter
            and event_filter.predicate is None
            and event_filter.event_type is not None
        ):
            return _query_by_type(events, event_filter.event_type, limit)
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

    def __len__(self) -> int:
        with self._lock:
            return len(self._events)
