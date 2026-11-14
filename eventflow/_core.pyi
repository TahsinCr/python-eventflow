from __future__ import annotations

import threading
from collections import deque
from collections.abc import Callable, Hashable
from typing import Any

class Flag:
    def __init__(self, value: bool = True) -> None: ...
    def get(self) -> bool: ...
    def set(self, value: object) -> None: ...

class Subscriber:
    callback: Callable[[Any], object]
    event_filter: Any
    remaining: int | None
    subscription: Any
    pending: deque[Any] | None
    def __init__(
        self,
        callback: Callable[[Any], object],
        event_filter: Any,
        remaining: int | None,
        subscription: Any,
        match_all: bool = ...,
        replaying: bool = ...,
        pending: deque[Any] | None = ...,
    ) -> None: ...
    @property
    def replaying(self) -> bool: ...
    @replaying.setter
    def replaying(self, value: object) -> None: ...
    @property
    def lock(self) -> threading.Lock: ...
    @property
    def match_all(self) -> bool: ...
    def claim(self) -> tuple[bool, bool]: ...
    def restore_claim(self) -> None: ...
    def buffer_if_replaying(self, event: object) -> bool: ...
    def finish_replay(self) -> tuple[Any, ...] | None: ...

class BusCore:
    def __init__(
        self,
        lock: threading.RLock,
        condition: threading.Condition,
        subscribers: dict[int, Subscriber],
        unkeyed: dict[int, Subscriber],
        keyed: dict[Hashable, dict[int, Subscriber]],
        route_snapshots: dict[Hashable, tuple[Subscriber, ...]],
        history: deque[Any] | None,
        routing_key: Callable[[Any], Hashable] | None,
        executor: Any,
        on_before: Callable[[Any], object] | None,
        on_after: Callable[[Any, Any], object] | None,
        on_error: Callable[[Any], object] | None,
        error_raise: bool = ...,
        fast_snapshot: bool = ...,
    ) -> None: ...
    def invalidate_snapshots(self, has_subscribers: bool, has_keyed: bool) -> None: ...
    def set_closed(self, closed: object) -> None: ...
    def add_stats(self, published: int, delivered: int, failed: int) -> None: ...
    def inc_waiters(self) -> None: ...
    def dec_waiters(self) -> None: ...
    def stats(self) -> Any: ...
    def publish(self, bus: Any, event: Any) -> Any: ...

class OperationStatsCore:
    def __init__(self, max_operations: int = ...) -> None: ...
    def record(
        self,
        name: str,
        status: str,
        elapsed: float,
        queue_depth: int | None,
        dropped: int,
        retries: int,
        rejected: int,
        timeouts: int,
        shutdown_seconds: float,
    ) -> None: ...
    def snapshot(self) -> Any: ...
    def reset(self) -> None: ...

def configure(
    publish_result: type[Any],
    stats_cls: type[Any],
    filter_cls: type[Any],
    closed_error: type[BaseException],
    invalid_handler_error: type[BaseException],
    error_context: type[Any],
    exception_group: type[BaseException],
    lock_fn: Callable[[], threading.Lock],
    rlock_type: type[threading.RLock],
    is_awaitable_fn: Callable[[object], bool],
    close_awaitable_fn: Callable[[object], None],
) -> None: ...
def configure_observability(
    operation_metrics: type[Any],
    operation_stats_snapshot: type[Any],
    mapping_proxy: type[Any],
) -> None: ...
