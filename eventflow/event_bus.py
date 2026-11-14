"""Thread-safe synchronous event bus."""

from __future__ import annotations

import inspect
import threading
import time
from collections import deque
from collections.abc import Callable, Hashable
from concurrent.futures import Future, ThreadPoolExecutor
from contextvars import copy_context
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar, cast

from ._callbacks import (
    _NON_AWAITABLE_TYPES,
    close_awaitable,
    is_async_callable,
    is_awaitable,
    synchronous_result,
)
from ._compatibility import ExceptionGroup
from ._native import CORE
from .actions import EventBusActions, EventErrorContext, EventTimeoutContext
from .base import (
    BaseEventBus,
    normalize_replay_capacity,
    validate_finite_duration,
    validate_nonnegative_count,
    validate_positive_capacity,
)
from .contracts import ErrorPolicy, EventBusStats, PublishResult
from .errors import (
    EventBusClosedError,
    EventBusError,
    EventShutdownTimeoutError,
    InvalidEventHandlerError,
)
from .filtering import EventFilter, EventType, coerce_event_filter
from .history import EventHistory
from .subscription import Subscription

T = TypeVar("T")
_executor_context = threading.local()
if CORE is not None:
    CORE.configure(
        PublishResult,
        EventBusStats,
        EventFilter,
        EventBusClosedError,
        InvalidEventHandlerError,
        EventErrorContext,
        ExceptionGroup,
        threading.Lock,
        threading.RLock,
        is_awaitable,
        close_awaitable,
    )


def _validate_executor(executor: object) -> ThreadPoolExecutor | None:
    if executor is not None and not isinstance(executor, ThreadPoolExecutor):
        raise TypeError("executor must be a ThreadPoolExecutor or None")
    return executor


def _invoke_sync_callback(callback: Callable[[T], None], event: T) -> None:
    result = callback(event)
    if result is None or type(result) in _NON_AWAITABLE_TYPES:
        return
    if inspect.isawaitable(result):
        if inspect.iscoroutine(result):
            close_awaitable(result)
        raise InvalidEventHandlerError(
            "Synchronous handlers must not return awaitables; use AsyncEventBus"
        )


@dataclass(slots=True)
class _Subscriber(Generic[T]):
    callback: Callable[[T], None]
    event_filter: EventFilter[T]
    remaining: int | None
    subscription: Subscription
    match_all: bool = False
    lock: threading.Lock = field(default_factory=threading.Lock)
    replaying: bool = False
    pending: deque[T] | None = None

    def claim(self) -> tuple[bool, bool]:
        """Reserve one delivery and report whether it is the final one."""

        if self.remaining is None:
            return self.subscription._active, False
        with self.lock:
            if not self.subscription._active:
                return False, False
            if self.remaining <= 0:
                return False, False
            self.remaining -= 1
            return True, self.remaining == 0

    def restore_claim(self) -> None:
        if self.remaining is None:
            return
        with self.lock:
            self.remaining += 1

    def buffer_if_replaying(self, event: T) -> bool:
        if not self.replaying:
            return False
        with self.lock:
            if not self.replaying:
                return False
            pending = self.pending
            assert pending is not None
            if pending.maxlen is not None and len(pending) >= pending.maxlen:
                raise BufferError("Event replay buffer is full")
            pending.append(event)
            return True

    def finish_replay(self) -> tuple[T, ...] | None:
        with self.lock:
            pending = self.pending
            if pending:
                events = tuple(pending)
                pending.clear()
                return events
            self.replaying = False
            self.pending = None
            return None


class EventBus(BaseEventBus[T]):
    """Publish events safely from any thread to synchronous subscribers.

    A routing key extractor enables indexed subscriptions by key. Publishing
    visits matching keyed subscribers and wildcard subscribers in registration
    order, without scanning unrelated keyed subscriptions. Extractors must
    return a hashable non-``None`` value; ``None`` denotes a wildcard
    subscription.
    """

    def __init__(
        self,
        *,
        history: EventHistory[T] | int | None = None,
        routing_key: Callable[[T], Hashable] | None = None,
        error_policy: ErrorPolicy = ErrorPolicy.ISOLATE,
        executor: ThreadPoolExecutor | None = None,
        replay_buffer_limit: int = 1_000,
        max_schedules: int = 64,
        shutdown_timeout: float = 5.0,
        actions: EventBusActions[T] | None = None,
        on_before: Callable[[T], None] | None = None,
        on_after: Callable[[T, PublishResult], None] | None = None,
        on_error: Callable[[EventErrorContext[T]], None] | None = None,
        on_timeout: Callable[[EventTimeoutContext[T]], None] | None = None,
    ) -> None:
        super().__init__(history=history, error_policy=error_policy)
        replay_buffer_limit = normalize_replay_capacity(replay_buffer_limit)
        max_schedules = validate_positive_capacity(
            max_schedules,
            name="Maximum periodic schedules",
        )
        shutdown_timeout = validate_finite_duration(shutdown_timeout, name="Shutdown timeout")
        direct_actions = (on_before, on_after, on_error, on_timeout)
        if actions is not None and any(action is not None for action in direct_actions):
            raise ValueError("Use actions or direct on_* callbacks, not both")
        self._actions = (
            actions
            if actions is not None
            else EventBusActions(
                on_before=on_before,
                on_after=on_after,
                on_error=on_error,
                on_timeout=on_timeout,
            )
        )
        for action in (
            self._actions.on_before,
            self._actions.on_after,
            self._actions.on_error,
            self._actions.on_timeout,
        ):
            if action is not None and is_async_callable(action):
                raise InvalidEventHandlerError("EventBus actions must be synchronous")
        self._executor = _validate_executor(executor)
        if routing_key is not None and not callable(routing_key):
            raise TypeError("routing_key must be callable or None")
        self._routing_key = routing_key
        self._replay_buffer_limit = replay_buffer_limit
        self._max_schedules = max_schedules
        self._shutdown_timeout = shutdown_timeout
        self._subscribers: dict[int, _Subscriber[T]] = {}
        self._unkeyed_subscribers: dict[int, _Subscriber[T]] = {}
        self._keyed_subscribers: dict[Hashable, dict[int, _Subscriber[T]]] = {}
        self._all_snapshot: tuple[_Subscriber[T], ...] | None = None
        self._unkeyed_snapshot: tuple[_Subscriber[T], ...] | None = None
        self._route_snapshots: dict[Hashable, tuple[_Subscriber[T], ...]] = {}
        self._next_id = 0
        self._closed = False
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._waiter_count = 0
        self._clear_generation = 0
        self._schedules: dict[int, tuple[Subscription, threading.Thread]] = {}
        self._core: Any = (
            CORE.BusCore(
                self._lock,
                self._condition,
                self._subscribers,
                self._unkeyed_subscribers,
                self._keyed_subscribers,
                self._route_snapshots,
                self._history,
                self._routing_key,
                self._executor,
                self._actions.on_before,
                self._actions.on_after,
                self._actions.on_error,
                self._error_policy is ErrorPolicy.RAISE,
                type(self)._subscriber_snapshot is EventBus._subscriber_snapshot,
            )
            if CORE is not None
            else None
        )

    def _invalidate_snapshots_locked(self) -> None:
        self._all_snapshot = None
        self._unkeyed_snapshot = None
        if self._route_snapshots:
            self._route_snapshots.clear()
        if self._core is not None:
            self._core.invalidate_snapshots(bool(self._subscribers), bool(self._keyed_subscribers))

    def _add_stats(self, published: int = 0, delivered: int = 0, failed: int = 0) -> None:
        if self._core is not None:
            self._core.add_stats(published, delivered, failed)
        else:
            self._published += published
            self._delivered += delivered
            self._failed += failed

    @property
    def closed(self) -> bool:
        with self._lock:
            return self._closed

    @property
    def subscriber_count(self) -> int:
        with self._lock:
            return len(self._subscribers)

    @property
    def stats(self) -> EventBusStats:
        with self._lock:
            if self._core is not None:
                return self._core.stats()
            return EventBusStats(self._published, self._delivered, self._failed)

    def subscribe(
        self,
        callback: Callable[[T], None],
        *,
        event_filter: EventFilter[T] | None = None,
        event_type: EventType | None = None,
        predicate: Callable[[T], bool] | None = None,
        routing_key: Hashable | None = None,
        once: bool = False,
        times: int | None = None,
        replay: int = 0,
    ) -> Subscription:
        if not callable(callback):
            raise InvalidEventHandlerError("Event handler must be callable")
        if is_async_callable(callback):
            raise InvalidEventHandlerError(
                "EventBus accepts synchronous handlers; use AsyncEventBus"
            )
        replay = validate_nonnegative_count(replay, name="Replay count")
        delivery_limit = self._delivery_limit(once=once, times=times)
        normalized_filter = coerce_event_filter(
            event_filter,
            event_type=event_type,
            predicate=predicate,
        )
        if routing_key is not None:
            if self._routing_key is None:
                raise ValueError("routing_key requires a bus routing_key function")
            try:
                hash(routing_key)
            except TypeError as exc:
                raise TypeError("routing_key values must be hashable") from exc
        with self._lock:
            self._ensure_open()
            history = self._history
            subscription_id = self._next_id
            self._next_id += 1
            has_filter = any(option is not None for option in (event_filter, event_type, predicate))
            if replay and routing_key is not None and history is not None:
                replay_snapshot = history.query(
                    EventFilter(
                        predicate=lambda event: self._event_routing_key(event) == routing_key
                    ),
                    limit=replay,
                )
            else:
                replay_snapshot = self._query_replay(
                    history,
                    replay,
                    has_filter or routing_key is not None,
                )

            def cancel() -> None:
                with self._lock:
                    self._subscribers.pop(subscription_id, None)
                    if routing_key is None:
                        self._unkeyed_subscribers.pop(subscription_id, None)
                    else:
                        subscribers = self._keyed_subscribers.get(routing_key)
                        if subscribers is not None:
                            subscribers.pop(subscription_id, None)
                            if not subscribers:
                                self._keyed_subscribers.pop(routing_key, None)
                    self._invalidate_snapshots_locked()

            is_replaying = bool(replay and history is not None)
            match_all = (
                type(normalized_filter) is EventFilter
                and normalized_filter.event_type is None
                and normalized_filter.predicate is None
            )
            subscription = Subscription(subscription_id, cancel)
            if self._core is not None and CORE is not None:
                subscription._active_flag = CORE.Flag(True)
                subscriber_cls: Any = CORE.Subscriber
            else:
                subscriber_cls = _Subscriber
            subscriber = subscriber_cls(
                callback,
                normalized_filter,
                delivery_limit,
                subscription,
                match_all=match_all,
                replaying=is_replaying,
                pending=deque(maxlen=self._replay_buffer_limit) if is_replaying else None,
            )
            self._subscribers[subscription_id] = subscriber
            if routing_key is None:
                self._unkeyed_subscribers[subscription_id] = subscriber
            else:
                self._keyed_subscribers.setdefault(routing_key, {})[subscription_id] = subscriber
            self._invalidate_snapshots_locked()

        try:
            replay_events = tuple(
                event for event in replay_snapshot if normalized_filter.matches(event)
            )[-replay:]

            if subscriber.replaying:
                pending: tuple[T, ...] | None = replay_events
                while pending is not None:
                    for event in pending:
                        claimed, final = subscriber.claim()
                        if not claimed:
                            break
                        if final:
                            subscription._consume()
                        try:
                            _invoke_sync_callback(callback, event)
                        except Exception as exc:
                            action_errors = self._notify_errors(event, (exc,))
                            with self._lock:
                                self._add_stats(failed=1 + len(action_errors))
                            if self._error_policy is ErrorPolicy.RAISE:
                                subscription.cancel()
                                raise ExceptionGroup(
                                    "Event replay failed",
                                    [exc, *action_errors],
                                ) from exc
                        else:
                            with self._lock:
                                self._add_stats(delivered=1)
                            if final:
                                break
                    pending = subscriber.finish_replay()
        except BaseException:
            subscription.cancel()
            raise
        return subscription

    def once(
        self,
        callback: Callable[[T], None],
        *,
        event_filter: EventFilter[T] | None = None,
        event_type: EventType | None = None,
        predicate: Callable[[T], bool] | None = None,
        routing_key: Hashable | None = None,
        replay: int = 0,
    ) -> Subscription:
        """Subscribe a handler for its first matching event only."""

        return self.subscribe(
            callback,
            event_filter=event_filter,
            event_type=event_type,
            predicate=predicate,
            routing_key=routing_key,
            once=True,
            replay=replay,
        )

    def publish(self, event: T) -> PublishResult:
        core = self._core
        if core is not None:
            return cast(PublishResult, core.publish(self, event))
        return self._publish_python(event)

    def _publish_python(self, event: T) -> PublishResult:
        with self._lock:
            if self._closed:
                raise EventBusClosedError("Event bus is closed")
            if (
                not self._subscribers
                and self._actions.on_before is None
                and self._actions.on_after is None
            ):
                self._record(event)
                self._add_stats(published=1)
                return PublishResult()
            event_key = self._event_routing_key(event) if self._keyed_subscribers else None
            subscribers = self._subscriber_snapshot(event_key)
            self._record(event)
            self._add_stats(published=1)

        before_error: Exception | None = None
        if self._actions.on_before is not None:
            try:
                synchronous_result(self._actions.on_before(event))
            except Exception as exc:
                before_error = exc

        matched, errors = self._match_subscribers(subscribers, event)
        if before_error is not None:
            errors.insert(0, before_error)

        delivered = 0
        executor = self._executor
        if executor is None:
            for subscriber in matched:
                if subscriber.remaining is None:
                    if not subscriber.subscription._active:
                        continue
                    try:
                        _invoke_sync_callback(subscriber.callback, event)
                    except Exception as exc:
                        errors.append(exc)
                    else:
                        delivered += 1
                else:
                    was_delivered, error = self._deliver(subscriber, event)
                    if was_delivered:
                        delivered += 1
                    if error is not None:
                        errors.append(error)
        else:
            active_executors = getattr(_executor_context, "active", frozenset())
            if id(executor) in active_executors:
                for subscriber in matched:
                    was_delivered, error = self._deliver(subscriber, event)
                    if was_delivered:
                        delivered += 1
                    if error is not None:
                        errors.append(error)
            else:
                futures: list[tuple[_Subscriber[T], Future[None]]] = []
                dispatch_context = copy_context()
                for subscriber in matched:
                    claimed, final = subscriber.claim()
                    if not claimed:
                        continue
                    try:
                        future = executor.submit(
                            dispatch_context.copy().run,
                            self._invoke_executor_callback,
                            subscriber,
                            event,
                            active_executors,
                            final,
                        )
                    except Exception as exc:
                        subscriber.restore_claim()
                        errors.append(exc)
                    except BaseException:
                        subscriber.restore_claim()
                        raise
                    else:
                        futures.append((subscriber, future))
                for subscriber, future in futures:
                    try:
                        future.result()
                    except Exception as exc:
                        if future.cancelled():
                            subscriber.restore_claim()
                        errors.append(exc)
                    else:
                        delivered += 1

        if errors and self._actions.on_error is not None:
            errors.extend(self._notify_errors(event, tuple(errors)))
        result = PublishResult(
            matched=len(matched),
            delivered=delivered,
            failed=len(errors),
            errors=tuple(errors),
        )
        if self._actions.on_after is not None:
            try:
                synchronous_result(self._actions.on_after(event, result))
            except Exception as exc:
                errors.append(exc)
                errors.extend(self._notify_errors(event, (exc,)))
                result = PublishResult(
                    matched=len(matched),
                    delivered=delivered,
                    failed=len(errors),
                    errors=tuple(errors),
                )
        if delivered or errors or self._waiter_count:
            with self._condition:
                self._add_stats(delivered=delivered, failed=len(errors))
                if self._waiter_count:
                    self._condition.notify_all()
        if errors and self._error_policy is ErrorPolicy.RAISE:
            raise ExceptionGroup("Event delivery failed", errors)
        return result

    def _subscriber_snapshot(self, event_key: Hashable | None) -> tuple[_Subscriber[T], ...]:
        if self._routing_key is None:
            snapshot = self._all_snapshot
            if snapshot is None:
                snapshot = tuple(self._subscribers.values())
                self._all_snapshot = snapshot
            return snapshot
        keyed = self._keyed_subscribers.get(event_key) if event_key is not None else None
        if not keyed:
            snapshot = self._unkeyed_snapshot
            if snapshot is None:
                snapshot = tuple(self._unkeyed_subscribers.values())
                self._unkeyed_snapshot = snapshot
            return snapshot
        snapshot = self._route_snapshots.get(event_key)
        if snapshot is not None:
            return snapshot
        unkeyed = self._unkeyed_subscribers
        if not unkeyed:
            snapshot = tuple(keyed.values())
            self._route_snapshots[event_key] = snapshot
            return snapshot

        unkeyed_items = iter(unkeyed.items())
        keyed_items = iter(keyed.items())
        unkeyed_item = next(unkeyed_items, None)
        keyed_item = next(keyed_items, None)
        merged: list[_Subscriber[T]] = []
        while unkeyed_item is not None and keyed_item is not None:
            if unkeyed_item[0] <= keyed_item[0]:
                merged.append(unkeyed_item[1])
                unkeyed_item = next(unkeyed_items, None)
            else:
                merged.append(keyed_item[1])
                keyed_item = next(keyed_items, None)

        if unkeyed_item is not None:
            merged.append(unkeyed_item[1])
            merged.extend(subscriber for _identifier, subscriber in unkeyed_items)
        if keyed_item is not None:
            merged.append(keyed_item[1])
            merged.extend(subscriber for _identifier, subscriber in keyed_items)
        snapshot = tuple(merged)
        self._route_snapshots[event_key] = snapshot
        return snapshot

    def _event_routing_key(self, event: T) -> Hashable | None:
        if self._routing_key is None:
            return None
        key = self._routing_key(event)
        if key is None:
            raise TypeError("routing_key function must return a non-None hashable value")
        try:
            hash(key)
        except TypeError as exc:
            raise TypeError("routing_key function must return a hashable value") from exc
        return key

    def _match_subscribers(
        self,
        subscribers: tuple[_Subscriber[T], ...],
        event: T,
    ) -> tuple[list[_Subscriber[T]], list[Exception]]:
        matched: list[_Subscriber[T]] = []
        errors: list[Exception] = []
        for subscriber in subscribers:
            if not subscriber.subscription._active:
                continue
            if not subscriber.match_all:
                try:
                    matches = subscriber.event_filter.matches(event)
                except Exception as exc:
                    errors.append(exc)
                    continue
                if not matches:
                    continue
            if subscriber.replaying:
                try:
                    if subscriber.buffer_if_replaying(event):
                        continue
                except BufferError as exc:
                    subscriber.subscription.cancel()
                    errors.append(exc)
                    continue
            matched.append(subscriber)
        return matched, errors

    def _invoke_executor_callback(
        self,
        subscriber: _Subscriber[T],
        event: T,
        inherited_executors: frozenset[int],
        final: bool,
    ) -> None:
        if final:
            try:
                subscriber.subscription._consume()
            except BaseException:
                subscriber.restore_claim()
                raise
        previous = getattr(_executor_context, "active", None)
        active = set(inherited_executors)
        if previous is not None:
            active.update(previous)
        if self._executor is not None:
            active.add(id(self._executor))
        _executor_context.active = frozenset(active)
        try:
            _invoke_sync_callback(subscriber.callback, event)
        finally:
            if previous is None:
                del _executor_context.active
            else:
                _executor_context.active = previous

    def _notify_errors(
        self,
        event: T,
        errors: tuple[Exception, ...],
    ) -> list[Exception]:
        action = self._actions.on_error
        if action is None:
            return []
        action_errors: list[Exception] = []
        for error in errors:
            try:
                synchronous_result(action(EventErrorContext(event, error)))
            except Exception as exc:
                action_errors.append(exc)
        return action_errors

    def _deliver(
        self,
        subscriber: _Subscriber[T],
        event: T,
    ) -> tuple[bool, Exception | None]:
        claimed, final = subscriber.claim()
        if not claimed:
            return False, None
        if final:
            try:
                subscriber.subscription._consume()
            except BaseException:
                subscriber.restore_claim()
                raise
        try:
            _invoke_sync_callback(subscriber.callback, event)
        except Exception as exc:
            return False, exc
        return True, None

    def publish_every(
        self,
        event: T,
        interval: float,
        *,
        times: int | None = None,
        immediately: bool = True,
    ) -> Subscription:
        """Publish the same event periodically until cancelled or completed."""

        interval = validate_finite_duration(interval, name="Event publish interval")
        if times is not None and (
            isinstance(times, bool) or not isinstance(times, int) or times <= 0
        ):
            raise ValueError("Periodic publish times must be a positive integer")

        stopped = threading.Event()
        with self._lock:
            self._ensure_open()
            if len(self._schedules) >= self._max_schedules:
                raise EventBusError("Maximum periodic schedule capacity reached")
            schedule_id = self._next_id
            self._next_id += 1

            def cancel() -> None:
                stopped.set()

            subscription = Subscription(schedule_id, cancel)

        def run() -> None:
            completed = 0
            try:
                if not immediately and stopped.wait(interval):
                    return
                while not stopped.is_set() and not self.closed:
                    try:
                        self.publish(event)
                    except Exception:
                        if self._error_policy is ErrorPolicy.RAISE:
                            break
                    completed += 1
                    if times is not None and completed >= times:
                        break
                    if stopped.is_set():
                        break
                    if stopped.wait(interval):
                        break
            finally:
                subscription._deactivate()
                with self._lock:
                    self._schedules.pop(schedule_id, None)

        thread = threading.Thread(
            target=run,
            name=f"EventBusPeriodic-{schedule_id}",
            daemon=True,
        )
        with self._lock:
            self._ensure_open()
            self._schedules[schedule_id] = (subscription, thread)
            try:
                thread.start()
            except BaseException:
                self._schedules.pop(schedule_id, None)
                subscription._deactivate()
                stopped.set()
                raise
        return subscription

    def wait_for(
        self,
        *,
        event_filter: EventFilter[T] | None = None,
        event_type: EventType | None = None,
        predicate: Callable[[T], bool] | None = None,
        routing_key: Hashable | None = None,
        timeout: float | None = None,
    ) -> T | None:
        if timeout is not None:
            timeout = validate_finite_duration(
                timeout,
                name="Event wait timeout",
                allow_zero=True,
            )
        normalized_filter = coerce_event_filter(
            event_filter,
            event_type=event_type,
            predicate=predicate,
        )
        received: list[T] = []

        def capture(event: T) -> None:
            with self._condition:
                received.append(event)
                self._condition.notify_all()

        with self._condition:
            # One-shot claim deactivates before capture; only clear/close abort a wait.
            clear_generation = self._clear_generation
            subscription = self.subscribe(
                capture,
                event_filter=normalized_filter,
                routing_key=routing_key,
                once=True,
            )
        timed_out = False
        try:
            with self._condition:
                self._waiter_count += 1
                if self._core is not None:
                    self._core.inc_waiters()
                try:
                    ready = self._condition.wait_for(
                        lambda: (
                            bool(received)
                            or self._closed
                            or self._clear_generation != clear_generation
                        ),
                        timeout=timeout,
                    )
                finally:
                    self._waiter_count -= 1
                    if self._core is not None:
                        self._core.dec_waiters()
                timed_out = not ready
                value = received[0] if ready and received else None
        finally:
            subscription.cancel()
        if timed_out and self._actions.on_timeout is not None:
            synchronous_result(
                self._actions.on_timeout(EventTimeoutContext(normalized_filter, timeout))
            )
        return value

    def clear(self) -> None:
        """Remove subscribers and wake waiters; retain history and schedules."""
        with self._lock:
            self._clear_generation += 1
            subscriptions = tuple(
                subscriber.subscription for subscriber in self._subscribers.values()
            )
            self._subscribers.clear()
            self._unkeyed_subscribers.clear()
            self._keyed_subscribers.clear()
            self._invalidate_snapshots_locked()
        for subscription in subscriptions:
            subscription._deactivate()
        with self._condition:
            self._condition.notify_all()

    def close(self) -> None:
        with self._condition:
            if self._closed and not self._schedules:
                return
            self._closed = True
            if self._core is not None:
                self._core.set_closed(True)
            subscriptions = tuple(
                subscriber.subscription for subscriber in self._subscribers.values()
            )
            schedules = tuple(self._schedules.values())
            self._subscribers.clear()
            self._unkeyed_subscribers.clear()
            self._keyed_subscribers.clear()
            self._invalidate_snapshots_locked()
            self._condition.notify_all()
        for subscription in subscriptions:
            subscription._deactivate()
        for schedule, _thread in schedules:
            schedule.cancel()
        deadline = time.monotonic() + self._shutdown_timeout
        pending: list[str] = []
        for _schedule, thread in schedules:
            if thread is not threading.current_thread():
                thread.join(max(0.0, deadline - time.monotonic()))
                if thread.is_alive():
                    pending.append(thread.name)
        with self._lock:
            for schedule_id, (_schedule, thread) in tuple(self._schedules.items()):
                if not thread.is_alive():
                    self._schedules.pop(schedule_id, None)
        if pending:
            raise EventShutdownTimeoutError(
                "Periodic event schedules did not stop in time: " + ", ".join(pending)
            )

    def __len__(self) -> int:
        return self.subscriber_count

    def __enter__(self) -> EventBus[T]:
        self._ensure_open()
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()
