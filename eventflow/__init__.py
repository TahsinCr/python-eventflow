"""Synchronous and asyncio-native in-process event delivery."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

__version__ = "1.1"

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

if TYPE_CHECKING:
    from .context import EventEnvelope, correlation_context, current_correlation_id
    from .observability import (
        ExecutionMetrics,
        PipelineObservation,
        PipelineObserver,
        PipelineStats,
        PipelineStatsSnapshot,
    )
    from .pipeline import AsyncPipeline, Pipeline, PipelineExecutionError
    from .scopes import AsyncCancelHandle, AsyncSubscriptionScope, CancelHandle, SubscriptionScope
    from .stream import AsyncStream

# Optional execution components load only when requested, keeping legacy bus
# imports independent of pipeline, stream, context and observability modules.
_OPTIONAL_EXPORTS = {
    **dict.fromkeys(("EventEnvelope", "correlation_context", "current_correlation_id"), ".context"),
    **dict.fromkeys(
        (
            "ExecutionMetrics",
            "PipelineObservation",
            "PipelineObserver",
            "PipelineStats",
            "PipelineStatsSnapshot",
        ),
        ".observability",
    ),
    **dict.fromkeys(("AsyncPipeline", "Pipeline", "PipelineExecutionError"), ".pipeline"),
    **dict.fromkeys(
        ("AsyncCancelHandle", "AsyncSubscriptionScope", "CancelHandle", "SubscriptionScope"),
        ".scopes",
    ),
    "AsyncStream": ".stream",
}


def __getattr__(name: str) -> Any:
    module = _OPTIONAL_EXPORTS.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module, __name__), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))


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
    "AsyncCancelHandle",
    "AsyncPipeline",
    "AsyncStream",
    "AsyncSubscriptionScope",
    "CancelHandle",
    "EventEnvelope",
    "ExecutionMetrics",
    "Pipeline",
    "PipelineExecutionError",
    "PipelineObservation",
    "PipelineObserver",
    "PipelineStats",
    "PipelineStatsSnapshot",
    "SubscriptionScope",
    "correlation_context",
    "current_correlation_id",
]
