"""Common contracts and pure helpers shared by both event buses."""

from __future__ import annotations

import inspect
import math
import threading
import weakref
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any, Generic, TypeVar

from .contracts import ErrorPolicy, EventBusStats
from .errors import EventBusClosedError
from .filtering import EventFilter, EventType, coerce_event_filter
from .history import EventHistory, MemoryEventHistory, _validate_query_limit


T = TypeVar("T")


def validate_positive_capacity(value: int, *, name: str) -> int:
    """Validate a bounded collection/schedule capacity."""

    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def normalize_replay_capacity(value: int) -> int:
    """Validate a replay buffer capacity as a positive integer."""

    return validate_positive_capacity(value, name="Replay buffer limit")


def _validate_timeout(value: float, *, name: str, allow_zero: bool = False) -> float:
    qualifier = "non-negative" if allow_zero else "positive"
    message = f"{name} must be {qualifier} and finite"
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(message)
    try:
        timeout = float(value)
    except OverflowError as exc:
        if allow_zero:
            raise ValueError(message) from exc
        raise ValueError(message) from None
    if not math.isfinite(timeout) or timeout < 0 or (timeout == 0 and not allow_zero):
        raise ValueError(message)
    return timeout


def validate_nonnegative_count(value: int, *, name: str) -> int:
    """Validate a count that may be zero."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


class BaseEventBus(Generic[T], ABC):
    def __init__(
        self,
        *,
        history: EventHistory[T] | int | None = None,
        error_policy: ErrorPolicy = ErrorPolicy.ISOLATE,
    ) -> None:
        if history is None or isinstance(history, EventHistory):
            self._history = history
        elif isinstance(history, bool) or not isinstance(history, int):
            raise TypeError("Event history must be a positive capacity or EventHistory")
        else:
            self._history = MemoryEventHistory[T](history)
        self._error_policy = ErrorPolicy(error_policy)
        self._engine_owner_lock = threading.Lock()
        self._engine_owner: weakref.ReferenceType[Any] | None = None
        self._published = 0
        self._delivered = 0
        self._failed = 0

    def _claim_engine_ownership(self, owner: Any) -> bool:
        """Atomically assign this bus to one live engine without retaining it."""
        with self._engine_owner_lock:
            current_owner = self._engine_owner() if self._engine_owner is not None else None
            if current_owner is not None and current_owner is not owner:
                return False
            self._engine_owner = weakref.ref(owner)
            return True

    @property
    def history(self) -> EventHistory[T] | None:
        return self._history

    @property
    def error_policy(self) -> ErrorPolicy:
        return self._error_policy

    @property
    @abstractmethod
    def closed(self) -> bool: ...

    @property
    @abstractmethod
    def subscriber_count(self) -> int: ...

    @property
    @abstractmethod
    def stats(self) -> EventBusStats: ...

    def _ensure_open(self) -> None:
        if self.closed:
            raise EventBusClosedError("Event bus is closed")

    def _record(self, event: T) -> None:
        if self._history is not None:
            self._history.append(event)

    @staticmethod
    def _query_replay(
        history: EventHistory[T] | None,
        replay: int,
        has_filter: bool,
    ) -> tuple[T, ...]:
        if not replay or history is None:
            return ()
        if has_filter:
            return history.query()
        return history.query(limit=replay)

    @staticmethod
    def _delivery_limit(*, once: bool, times: int | None) -> int | None:
        if times is not None:
            if isinstance(times, bool) or not isinstance(times, int) or times <= 0:
                raise ValueError("Subscription times must be a positive integer")
            if once and times != 1:
                raise ValueError("Use once=True or times, not both")
            return times
        return 1 if once else None

    def latest(
        self,
        *,
        event_filter: EventFilter[T] | None = None,
        event_type: EventType | None = None,
        predicate: Callable[[T], bool] | None = None,
    ) -> T | None:
        normalized_filter = coerce_event_filter(
            event_filter,
            event_type=event_type,
            predicate=predicate,
        )
        if self._history is None:
            return None
        if event_filter is None and event_type is None and predicate is None:
            return self._history.latest()
        return self._history.latest(normalized_filter)

    def query(
        self,
        *,
        event_filter: EventFilter[T] | None = None,
        event_type: EventType | None = None,
        predicate: Callable[[T], bool] | None = None,
        limit: int | None = None,
    ) -> tuple[T, ...]:
        _validate_query_limit(limit)
        normalized_filter = coerce_event_filter(
            event_filter,
            event_type=event_type,
            predicate=predicate,
        )
        if self._history is None:
            return ()
        if event_filter is None and event_type is None and predicate is None:
            return self._history.query(limit=limit)
        return self._history.query(normalized_filter, limit=limit)

    @staticmethod
    def _is_async_handler(handler: Any) -> bool:
        if inspect.iscoroutinefunction(handler):
            return True
        return callable(handler) and inspect.iscoroutinefunction(type(handler).__call__)
