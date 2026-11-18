"""Optional bounded in-memory history for event buses."""

from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Callable, Iterator, Reversible
from itertools import chain, islice
from operator import index
from sys import maxsize
from typing import Generic, TypeVar

from .filtering import EventFilter, EventType, _matches_all

T = TypeVar("T")


def _recent_matches(
    events: Reversible[T], limit: int, matches: Callable[[T], bool]
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


def _validate_query_limit(limit: int | None) -> int | None:
    if limit is not None and (
        isinstance(limit, bool) or not isinstance(limit, int) or (limit := index(limit)) <= 0
    ):
        raise ValueError("Event history query limit must be a positive integer")
    return limit


def _query_by_type(
    events: Reversible[T],
    event_type: EventType,
    limit: int | None,
) -> tuple[T, ...]:
    if limit is None:
        matches = [event for event in events if isinstance(event, event_type)]
        return tuple(matches)

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


class _BlockSnapshot(Generic[T]):
    def __init__(self, blocks: tuple[tuple[T, ...], ...]) -> None:
        self._blocks = blocks

    def __iter__(self) -> Iterator[T]:
        return chain.from_iterable(self._blocks)

    def __reversed__(self) -> Iterator[T]:
        return chain.from_iterable(map(reversed, reversed(self._blocks)))


class _Blocks(Generic[T]):
    """Keep sealed blocks immutable and copy only the two mutable edges on capture."""

    def __init__(self, capacity: int, block_size: int) -> None:
        self.capacity = capacity
        self.block_size = block_size
        self.size = 0
        self.head: deque[T] = deque()
        self.blocks: deque[tuple[T, ...]] = deque()
        self.tail: list[T] = []
        self.snapshot: _BlockSnapshot[T] | None = None

    def append(self, event: T) -> None:
        full = self.size == self.capacity
        new_head = deque(self.blocks[0]) if full and not self.head else None
        self.tail.append(event)
        if len(self.tail) == self.block_size:
            try:
                self.blocks.append(tuple(self.tail))
            except MemoryError:
                self.tail.pop()
                raise
            self.tail.clear()
        if not full:
            self.size += 1
        else:
            if new_head is not None:
                self.head = new_head
                self.blocks.popleft()
            self.head.popleft()
        self.snapshot = None

    def capture(self) -> _BlockSnapshot[T]:
        if self.snapshot is None:
            self.snapshot = _BlockSnapshot((tuple(self.head), *self.blocks, tuple(self.tail)))
        return self.snapshot

    def latest(self) -> T:
        if self.tail:
            return self.tail[-1]
        if self.blocks:
            return self.blocks[-1][-1]
        return self.head[-1]

    def __iter__(self) -> Iterator[T]:
        return chain(self.head, chain.from_iterable(self.blocks), self.tail)

    def __reversed__(self) -> Iterator[T]:
        return chain(
            reversed(self.tail),
            chain.from_iterable(map(reversed, reversed(self.blocks))),
            reversed(self.head),
        )

    def __len__(self) -> int:
        return self.size


class MemoryEventHistory(EventHistory[T]):
    """Thread-safe bounded history preserving publication order."""

    def __init__(self, capacity: int = 1_000, *, block_size: int | None = None) -> None:
        if (
            isinstance(capacity, bool)
            or not isinstance(capacity, int)
            or (capacity := index(capacity)) <= 0
        ):
            raise ValueError("Event history capacity must be a positive integer")
        if block_size is not None and (
            isinstance(block_size, bool)
            or not isinstance(block_size, int)
            or not 1 <= (block_size := index(block_size)) <= capacity
        ):
            raise ValueError("Event history block size must be an integer between 1 and capacity")
        self._capacity = capacity
        self._block_size = block_size
        self._events: deque[T] | _Blocks[T] = (
            deque(maxlen=capacity) if block_size is None else _Blocks(capacity, block_size)
        )
        self._lock = threading.RLock()
        self._snapshot_cache: tuple[T, ...] | None = None

    @property
    def capacity(self) -> int:
        return self._capacity

    def _capture_locked(self) -> Reversible[T]:
        events = self._events
        return events.capture() if isinstance(events, _Blocks) else self._snapshot_locked()

    def _replay_snapshot(self) -> Reversible[T]:
        with self._lock:
            return self._capture_locked()

    def _last_locked(self) -> T:
        events = self._events
        return events.latest() if isinstance(events, _Blocks) else events[-1]

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
                return self._last_locked() if self._events else None
            if event_type is not None and self._events:
                last = self._last_locked()
                if _exact_type_match(last, event_type):
                    return last
            events = self._capture_locked()
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
        limit = _validate_query_limit(limit)
        event_type = (
            event_filter.event_type
            if type(event_filter) is EventFilter and event_filter.predicate is None
            else None
        )
        if event_filter is None or _matches_all(event_filter):
            with self._lock:
                if limit is not None:
                    recent = list(islice(reversed(self._events), min(limit, maxsize)))
                    recent.reverse()
                    return tuple(recent)
                if not isinstance(self._events, _Blocks):
                    return self._snapshot_locked()
                unfiltered = self._events.capture()
            return tuple(unfiltered)
        with self._lock:
            if limit == 1 and event_type is not None and self._events:
                last = self._last_locked()
                if _exact_type_match(last, event_type):
                    return (last,)
            events = self._capture_locked()
        # Event class membership can change without modifying the stored sequence.
        if event_type is not None:
            return _query_by_type(events, event_type, limit)
        if limit is None:
            return tuple(event for event in events if event_filter.matches(event))
        return _recent_matches(events, limit, event_filter.matches)

    def clear(self) -> None:
        with self._lock:
            events = self._events
            if isinstance(events, _Blocks):
                self._events = _Blocks(events.capacity, events.block_size)
            else:
                events.clear()
            self._snapshot_cache = None

    def __len__(self) -> int:
        with self._lock:
            return len(self._events)
