"""Application-level engines for managing named event channels."""

from __future__ import annotations

import threading
from collections.abc import Awaitable, Callable, Hashable
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Generic, TypeVar

from ._compatibility import ExceptionGroup
from .actions import (
    AsyncEventBusActions,
    EventBusActions,
    EventErrorContext,
    EventTimeoutContext,
)
from .async_event_bus import AsyncEventBus
from .base import BaseEventBus, validate_positive_capacity
from .contracts import DeliveryMode, ErrorPolicy, PublishResult
from .errors import EventBusClosedError
from .event_bus import EventBus, _validate_executor
from .filtering import EventFilter, EventType
from .subscription import AsyncSubscription, Subscription

BusT = TypeVar("BusT", bound=BaseEventBus[Any])


def _channel_name(name: str) -> str:
    if not isinstance(name, str):
        raise TypeError("Event channel name must be a string")
    normalized = name.strip().lower()
    if not normalized:
        raise ValueError("Event channel name cannot be empty")
    return normalized


def _validate_history_capacity(history: object) -> int | None:
    if history is None:
        return None
    if isinstance(history, bool) or not isinstance(history, int):
        raise TypeError("Engine history must be a positive integer capacity or None")
    if history <= 0:
        raise ValueError("Engine history capacity must be positive")
    return history


class _ChannelRegistry(Generic[BusT]):
    """Own channel maps and close retries shared by both engine types."""

    def __init__(self, owner: object) -> None:
        self._owner = owner
        self._channels: dict[str, BusT] = {}
        self._pending_close: dict[int, BusT] = {}
        self._bus_ids: set[int] = set()
        self._lock = threading.RLock()
        self._running = True

    @property
    def running(self) -> bool:
        with self._lock:
            return self._running

    @property
    def names(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(self._channels))

    def get_fast(self, name: str) -> BusT | None:
        if not self._running:
            return None
        return self._channels.get(name)

    def get_or_create(self, name: str, factory: Callable[[], BusT]) -> BusT:
        with self._lock:
            bus = self._channels.get(name)
            if bus is None:
                bus = factory()
                bus._claim_engine_ownership(self._owner)
                self._channels[name] = bus
                self._bus_ids.add(id(bus))
            self._running = True
            return bus

    def add(
        self,
        name: str,
        bus: BusT,
        *,
        bus_type: str,
        channel_type: str,
    ) -> None:
        with self._lock:
            if name in self._channels:
                raise ValueError(f"{channel_type} channel already exists: {name}")
            bus_id = id(bus)
            if bus_id in self._bus_ids:
                raise ValueError(f"An {bus_type} instance can belong to only one channel")
            self._channels[name] = bus
            if not bus._claim_engine_ownership(self._owner):
                del self._channels[name]
                raise ValueError(f"An {bus_type} instance can belong to only one engine")
            self._bus_ids.add(bus_id)
            self._running = True

    def begin_remove(self, name: str) -> BusT | None:
        with self._lock:
            bus = self._channels.pop(name, None)
            if bus is not None:
                self._pending_close[id(bus)] = bus
            return bus

    def begin_stop(self) -> tuple[BusT, ...]:
        with self._lock:
            channels = tuple(self._channels.values())
            self._channels.clear()
            for channel in channels:
                self._pending_close[id(channel)] = channel
            self._running = False
            return tuple(self._pending_close.values())

    def close_succeeded(self, bus: BusT) -> None:
        with self._lock:
            self._pending_close.pop(id(bus), None)
            self._bus_ids.discard(id(bus))

    def start(self) -> None:
        with self._lock:
            self._running = True


class EventEngine:
    """Create and manage thread-safe event buses by a readable channel name."""

    def __init__(
        self,
        *,
        history: int | None = None,
        error_policy: ErrorPolicy = ErrorPolicy.ISOLATE,
        executor: ThreadPoolExecutor | None = None,
        actions: EventBusActions[Any] | None = None,
        on_before: Callable[[Any], None] | None = None,
        on_after: Callable[[Any, PublishResult], None] | None = None,
        on_error: Callable[[EventErrorContext[Any]], None] | None = None,
        on_timeout: Callable[[EventTimeoutContext[Any]], None] | None = None,
    ) -> None:
        self._history = _validate_history_capacity(history)
        self._error_policy = ErrorPolicy(error_policy)
        self._executor = _validate_executor(executor)
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
        self._channel_registry = _ChannelRegistry[EventBus[Any]](self)

    @property
    def running(self) -> bool:
        return self._channel_registry.running

    @property
    def channel_names(self) -> tuple[str, ...]:
        return self._channel_registry.names

    def _create_channel_bus(self) -> EventBus[Any]:
        return EventBus(
            history=self._history,
            error_policy=self._error_policy,
            executor=self._executor,
            actions=self._actions,
        )

    def channel(self, name: str) -> EventBus[Any]:
        """Return a channel, creating it with engine defaults when necessary."""

        name = _channel_name(name)
        return self._channel_registry.get_or_create(name, self._create_channel_bus)

    def add(self, name: str, bus: EventBus[Any]) -> EventBus[Any]:
        """Register a bus owned by this engine under a channel name.

        An open bus cannot be claimed by another engine; that registration raises
        ValueError.
        """

        if not isinstance(bus, EventBus):
            raise TypeError("Event engine channels must be EventBus instances")
        if bus.closed:
            raise EventBusClosedError("Cannot add a closed EventBus to an engine")
        name = _channel_name(name)
        self._channel_registry.add(name, bus, bus_type="EventBus", channel_type="Event")
        return bus

    def publish(self, channel: str, event: Any) -> PublishResult:
        if type(channel) is str:
            bus = self._channel_registry.get_fast(channel)
            if bus is not None:
                return bus.publish(event)
        return self.channel(channel).publish(event)

    def subscribe(
        self,
        channel: str,
        callback: Callable[[Any], None],
        *,
        event_filter: EventFilter[Any] | None = None,
        event_type: EventType | None = None,
        predicate: Callable[[Any], bool] | None = None,
        routing_key: Hashable | None = None,
        once: bool = False,
        times: int | None = None,
        replay: int = 0,
    ) -> Subscription:
        return self.channel(channel).subscribe(
            callback,
            event_filter=event_filter,
            event_type=event_type,
            predicate=predicate,
            routing_key=routing_key,
            once=once,
            times=times,
            replay=replay,
        )

    def once(
        self,
        channel: str,
        callback: Callable[[Any], None],
        *,
        event_filter: EventFilter[Any] | None = None,
        event_type: EventType | None = None,
        predicate: Callable[[Any], bool] | None = None,
        routing_key: Hashable | None = None,
        replay: int = 0,
    ) -> Subscription:
        return self.channel(channel).once(
            callback,
            event_filter=event_filter,
            event_type=event_type,
            predicate=predicate,
            routing_key=routing_key,
            replay=replay,
        )

    def publish_every(
        self,
        channel: str,
        event: Any,
        interval: float,
        *,
        times: int | None = None,
        immediately: bool = True,
    ) -> Subscription:
        return self.channel(channel).publish_every(
            event,
            interval,
            times=times,
            immediately=immediately,
        )

    def wait_for(
        self,
        channel: str,
        *,
        event_filter: EventFilter[Any] | None = None,
        event_type: EventType | None = None,
        predicate: Callable[[Any], bool] | None = None,
        routing_key: Hashable | None = None,
        timeout: float | None = None,
    ) -> Any | None:
        return self.channel(channel).wait_for(
            event_filter=event_filter,
            event_type=event_type,
            predicate=predicate,
            routing_key=routing_key,
            timeout=timeout,
        )

    def remove(self, name: str) -> bool:
        name = _channel_name(name)
        bus = self._channel_registry.begin_remove(name)
        if bus is None:
            return False
        bus.close()
        self._channel_registry.close_succeeded(bus)
        return True

    def start(self) -> None:
        """Keep Service-style lifecycle symmetry; channels start lazily."""

        self._channel_registry.start()

    def stop(self) -> None:
        pending = self._channel_registry.begin_stop()
        errors: list[Exception] = []
        for channel in pending:
            try:
                channel.close()
            except Exception as exc:
                errors.append(exc)
            else:
                self._channel_registry.close_succeeded(channel)
        if errors:
            raise ExceptionGroup("One or more event channels failed to close", errors)

    close = stop

    def __enter__(self) -> EventEngine:
        return self

    def __exit__(self, *_args: object) -> None:
        self.stop()


class AsyncEventEngine:
    """Create and manage asyncio-native event buses by channel name."""

    def __init__(
        self,
        *,
        history: int | None = None,
        error_policy: ErrorPolicy = ErrorPolicy.ISOLATE,
        delivery_mode: DeliveryMode = DeliveryMode.SEQUENTIAL,
        max_concurrency: int | None = None,
        actions: AsyncEventBusActions[Any] | None = None,
        on_before: Callable[[Any], Awaitable[None]] | None = None,
        on_after: Callable[[Any, PublishResult], Awaitable[None]] | None = None,
        on_error: Callable[[EventErrorContext[Any]], Awaitable[None]] | None = None,
        on_timeout: Callable[[EventTimeoutContext[Any]], Awaitable[None]] | None = None,
    ) -> None:
        self._history = _validate_history_capacity(history)
        self._error_policy = ErrorPolicy(error_policy)
        self._delivery_mode = DeliveryMode(delivery_mode)
        self._max_concurrency = (
            None
            if max_concurrency is None
            else validate_positive_capacity(
                max_concurrency,
                name="Maximum concurrent deliveries",
            )
        )
        if self._max_concurrency is not None and self._delivery_mode is not DeliveryMode.CONCURRENT:
            raise ValueError("max_concurrency requires DeliveryMode.CONCURRENT")
        direct_actions = (on_before, on_after, on_error, on_timeout)
        if actions is not None and any(action is not None for action in direct_actions):
            raise ValueError("Use actions or direct on_* callbacks, not both")
        self._actions = (
            actions
            if actions is not None
            else AsyncEventBusActions(
                on_before=on_before,
                on_after=on_after,
                on_error=on_error,
                on_timeout=on_timeout,
            )
        )
        self._channel_registry = _ChannelRegistry[AsyncEventBus[Any]](self)

    @property
    def running(self) -> bool:
        return self._channel_registry.running

    @property
    def channel_names(self) -> tuple[str, ...]:
        return self._channel_registry.names

    def _create_channel_bus(self) -> AsyncEventBus[Any]:
        return AsyncEventBus(
            history=self._history,
            error_policy=self._error_policy,
            delivery_mode=self._delivery_mode,
            max_concurrency=self._max_concurrency,
            actions=self._actions,
        )

    def channel(self, name: str) -> AsyncEventBus[Any]:
        name = _channel_name(name)
        return self._channel_registry.get_or_create(name, self._create_channel_bus)

    def add(self, name: str, bus: AsyncEventBus[Any]) -> AsyncEventBus[Any]:
        """Register a bus owned by this engine under a channel name.

        An open bus cannot be claimed by another engine; that registration raises
        ValueError.
        """

        if not isinstance(bus, AsyncEventBus):
            raise TypeError("Async event engine channels must be AsyncEventBus instances")
        if bus.closed:
            raise EventBusClosedError("Cannot add a closed AsyncEventBus to an engine")
        name = _channel_name(name)
        self._channel_registry.add(name, bus, bus_type="AsyncEventBus", channel_type="Async event")
        return bus

    async def publish(self, channel: str, event: Any) -> PublishResult:
        if type(channel) is str:
            bus = self._channel_registry.get_fast(channel)
            if bus is not None:
                return await bus.publish(event)
        return await self.channel(channel).publish(event)

    async def subscribe(
        self,
        channel: str,
        callback: Callable[[Any], Awaitable[None]],
        *,
        event_filter: EventFilter[Any] | None = None,
        event_type: EventType | None = None,
        predicate: Callable[[Any], bool] | None = None,
        once: bool = False,
        times: int | None = None,
        replay: int = 0,
    ) -> AsyncSubscription:
        return await self.channel(channel).subscribe(
            callback,
            event_filter=event_filter,
            event_type=event_type,
            predicate=predicate,
            once=once,
            times=times,
            replay=replay,
        )

    async def once(
        self,
        channel: str,
        callback: Callable[[Any], Awaitable[None]],
        *,
        event_filter: EventFilter[Any] | None = None,
        event_type: EventType | None = None,
        predicate: Callable[[Any], bool] | None = None,
        replay: int = 0,
    ) -> AsyncSubscription:
        return await self.channel(channel).once(
            callback,
            event_filter=event_filter,
            event_type=event_type,
            predicate=predicate,
            replay=replay,
        )

    async def publish_every(
        self,
        channel: str,
        event: Any,
        interval: float,
        *,
        times: int | None = None,
        immediately: bool = True,
    ) -> AsyncSubscription:
        return await self.channel(channel).publish_every(
            event,
            interval,
            times=times,
            immediately=immediately,
        )

    async def wait_for(
        self,
        channel: str,
        *,
        event_filter: EventFilter[Any] | None = None,
        event_type: EventType | None = None,
        predicate: Callable[[Any], bool] | None = None,
        timeout: float | None = None,
    ) -> Any | None:
        return await self.channel(channel).wait_for(
            event_filter=event_filter,
            event_type=event_type,
            predicate=predicate,
            timeout=timeout,
        )

    async def remove(self, name: str) -> bool:
        name = _channel_name(name)
        bus = self._channel_registry.begin_remove(name)
        if bus is None:
            return False
        await bus.close()
        self._channel_registry.close_succeeded(bus)
        return True

    async def start(self) -> None:
        """Keep lifecycle symmetry; channels bind to a loop when first used."""

        self._channel_registry.start()

    async def stop(self) -> None:
        pending = self._channel_registry.begin_stop()
        errors: list[Exception] = []
        for channel in pending:
            try:
                await channel.close()
            except Exception as exc:
                errors.append(exc)
            else:
                self._channel_registry.close_succeeded(channel)
        if errors:
            raise ExceptionGroup("One or more async event channels failed to close", errors)

    close = stop

    async def __aenter__(self) -> AsyncEventEngine:
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.stop()
