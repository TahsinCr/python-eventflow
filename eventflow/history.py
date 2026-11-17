"""Optional bounded in-memory history for event buses."""

from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Callable
from itertools import islice
from typing import Generic, TypeVar

from .filtering import EventFilter, EventType, _matches_all

T = TypeVar("T")


def _recent_matches(
    events: tuple[T, ...], limit: int, matches: Callable[[T], bool]
) -> tuple[T, ...]:
    matched: list[T] = []
    for event in reversed(events):
        if matches(event):
            matched.append(event)
            if len(matched) == limit:
                break
    matched.reverse()
    return tuple(matched)


def _exact_type_match(event: object, target: EventType | None) -> bool:
    # A later tuple member cannot bypass earlier instance checks.
    return type(event) is (target[0] if type(target) is tuple else target)


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

    @property
    def capacity(self) -> int:
        return self._events.maxlen or 0

    def _snapshot_locked(self) -> tuple[T, ...]:
        if self._snapshot_cache is None:
            self._snapshot_cache = tuple(self._events)
        return self._snapshot_cache

    def append(self, event: T) -> None:
        with self._lock:
            self._events.append(event)
            self._snapshot_cache = None

    def latest(self, event_filter: EventFilter[T] | None = None) -> T | None:
        event_type = (
            event_filter.event_type
            if type(event_filter) is EventFilter and event_filter.predicate is None
            else None
        )
        with self._lock:
            if event_filter is None or _matches_all(event_filter):
                return self._events[-1] if self._events else None
            if event_type is not None and self._events:
                last = self._events[-1]
                if _exact_type_match(last, event_type):
                    return last
            events = self._snapshot_locked()
        if event_type is not None:
            return next(
                (event for event in reversed(events) if isinstance(event, event_type)), None
            )
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
        event_type = (
            event_filter.event_type
            if type(event_filter) is EventFilter
            and event_filter.predicate is None
            else None
        )
        with self._lock:
            if event_filter is None or _matches_all(event_filter):
                if limit is None:
                    return self._snapshot_locked()
                recent = list(islice(reversed(self._events), limit))
                recent.reverse()
                return tuple(recent)
            if limit == 1 and event_type is not None and self._events:
                last = self._events[-1]
                if _exact_type_match(last, event_type):
                    return (last,)
            events = self._snapshot_locked()
        # Event class membership can change without modifying the stored sequence.
        if event_type is not None:
            return _query_by_type(events, event_type, limit)
        if limit is None:
            return tuple(event for event in events if event_filter.matches(event))
        return _recent_matches(events, limit, event_filter.matches)

    def clear(self) -> None:
        with self._lock:
            self._events.clear()
            self._snapshot_cache = None

    def __len__(self) -> int:
        with self._lock:
            return len(self._events)
