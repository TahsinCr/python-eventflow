"""Synchronous and asyncio-native in-process event delivery."""

__version__ = "1.0"

from .actions import (
    AsyncEventBusActions,
    EventBusActions,
    EventErrorContext,
    EventTimeoutContext,
)
from .async_event_bus import AsyncEventBus
from .base import BaseEventBus
from .callback import CallbackContext, CallbackSubscription
from .contracts import DeliveryMode, ErrorPolicy, EventBusStats, PublishResult
from .engine import AsyncEventEngine, EventEngine
from .errors import (
    CallbackTimeoutError,
    EventBusClosedError,
    EventBusError,
    EventShutdownTimeoutError,
    InvalidEventHandlerError,
)
from .event_bus import EventBus
from .filtering import EventFilter
from .history import EventHistory, MemoryEventHistory
from .subscription import AsyncSubscription, Subscription

__all__ = [
    "__version__",
    "AsyncEventBus",
    "AsyncEventBusActions",
    "AsyncEventEngine",
    "AsyncSubscription",
    "BaseEventBus",
    "CallbackContext",
    "CallbackSubscription",
    "CallbackTimeoutError",
    "DeliveryMode",
    "ErrorPolicy",
    "EventBus",
    "EventBusActions",
    "EventBusClosedError",
    "EventBusError",
    "EventBusStats",
    "EventEngine",
    "EventFilter",
    "EventErrorContext",
    "EventHistory",
    "EventShutdownTimeoutError",
    "EventTimeoutContext",
    "InvalidEventHandlerError",
    "MemoryEventHistory",
    "PublishResult",
    "Subscription",
]
