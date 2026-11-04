"""Synchronous and asyncio-native in-process event delivery."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

__version__ = "1.3.1"

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
    CircuitOpenError,
    EventBusClosedError,
    EventBusError,
    EventQueueClosedError,
    EventShutdownTimeoutError,
    InvalidEventHandlerError,
    LoopPortalClosedError,
    QueueOverflowError,
)
from .event_bus import EventBus
from .filtering import EventFilter
from .history import EventHistory, MemoryEventHistory
from .subscription import AsyncSubscription, Subscription

if TYPE_CHECKING:
    from .context import EventEnvelope, correlation_context, current_correlation_id
    from .observability import (
        ExecutionMetrics,
        OperationMetrics,
        OperationStats,
        OperationStatsSnapshot,
        PipelineObservation,
        PipelineObserver,
        PipelineStats,
        PipelineStatsSnapshot,
    )
    from .pipeline import AsyncPipeline, Pipeline, PipelineExecutionError
    from .scopes import (
        AsyncCancelHandle,
        AsyncResourceScope,
        AsyncSubscriptionScope,
        CancelHandle,
        ResourceScope,
        SubscriptionScope,
    )
    from .stream import AsyncStream
    from .bridges import AsyncEventQueue, AsyncEventQueueStats, LoopPortal
    from .cache import CacheStats, VersionedCache
    from .concurrency import (
        KeyedConcurrencyLimiter,
        KeyedConcurrencyKeyStats,
        KeyedConcurrencyStats,
        KeyedRateLimiter,
        KeyedRateStats,
        SingleFlight,
        SingleFlightStats,
    )
    from .executors import BoundedWorkPool, WorkPoolStats
    from .policy import PolicyDecision, PolicyEvaluator
    from .registry import RegistryChange, ServiceRegistry
    from .resilience import (
        CircuitBreaker,
        CircuitBreakerStats,
        CooldownRegistry,
        CooldownStats,
        RetryObservation,
        RetryPolicy,
    )
    from .scheduling import (
        AsyncScheduler,
        CoalescerStats,
        KeyedCoalescer,
        ScheduledHandle,
        SchedulerStats,
    )
    from .state import (
        StateMachine,
        StateSnapshot,
        StateTransitionError,
        VersionedSnapshot,
        VersionedValue,
    )
    from .tasks import (
        AsyncTaskScope,
        LatestTaskCoordinator,
        LatestTaskSnapshot,
        TaskHandle,
        TaskScopeSnapshot,
    )
    from .timing import Deadline

# Optional execution components load only when requested, keeping core bus
# imports independent of pipeline, stream, context and observability modules.
_OPTIONAL_EXPORTS = {
    **dict.fromkeys(("EventEnvelope", "correlation_context", "current_correlation_id"), ".context"),
    **dict.fromkeys(
        (
            "ExecutionMetrics",
            "OperationMetrics",
            "OperationStats",
            "OperationStatsSnapshot",
            "PipelineObservation",
            "PipelineObserver",
            "PipelineStats",
            "PipelineStatsSnapshot",
        ),
        ".observability",
    ),
    **dict.fromkeys(("AsyncPipeline", "Pipeline", "PipelineExecutionError"), ".pipeline"),
    **dict.fromkeys(
        (
            "AsyncCancelHandle",
            "AsyncResourceScope",
            "AsyncSubscriptionScope",
            "CancelHandle",
            "ResourceScope",
            "SubscriptionScope",
        ),
        ".scopes",
    ),
    "AsyncStream": ".stream",
    **dict.fromkeys(("AsyncEventQueue", "AsyncEventQueueStats", "LoopPortal"), ".bridges"),
    **dict.fromkeys(("CacheStats", "VersionedCache"), ".cache"),
    **dict.fromkeys(
        (
            "KeyedConcurrencyLimiter",
            "KeyedConcurrencyKeyStats",
            "KeyedConcurrencyStats",
            "KeyedRateLimiter",
            "KeyedRateStats",
            "SingleFlight",
            "SingleFlightStats",
        ),
        ".concurrency",
    ),
    **dict.fromkeys(("BoundedWorkPool", "WorkPoolStats"), ".executors"),
    **dict.fromkeys(("PolicyDecision", "PolicyEvaluator"), ".policy"),
    **dict.fromkeys(("RegistryChange", "ServiceRegistry"), ".registry"),
    **dict.fromkeys(
        (
            "CircuitBreaker",
            "CircuitBreakerStats",
            "CooldownRegistry",
            "CooldownStats",
            "RetryObservation",
            "RetryPolicy",
        ),
        ".resilience",
    ),
    **dict.fromkeys(
        ("AsyncScheduler", "CoalescerStats", "KeyedCoalescer", "ScheduledHandle", "SchedulerStats"),
        ".scheduling",
    ),
    **dict.fromkeys(
        (
            "StateMachine",
            "StateSnapshot",
            "StateTransitionError",
            "VersionedSnapshot",
            "VersionedValue",
        ),
        ".state",
    ),
    **dict.fromkeys(
        (
            "AsyncTaskScope",
            "LatestTaskCoordinator",
            "LatestTaskSnapshot",
            "TaskHandle",
            "TaskScopeSnapshot",
        ),
        ".tasks",
    ),
    "Deadline": ".timing",
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
    "CircuitOpenError",
    "DeliveryMode",
    "ErrorPolicy",
    "EventBus",
    "EventBusActions",
    "EventBusClosedError",
    "EventBusError",
    "EventBusStats",
    "EventEngine",
    "EventQueueClosedError",
    "EventFilter",
    "EventErrorContext",
    "EventHistory",
    "EventShutdownTimeoutError",
    "EventTimeoutContext",
    "InvalidEventHandlerError",
    "LoopPortalClosedError",
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
    "QueueOverflowError",
    "correlation_context",
    "current_correlation_id",
    "AsyncEventQueue",
    "AsyncEventQueueStats",
    "AsyncResourceScope",
    "AsyncScheduler",
    "AsyncTaskScope",
    "BoundedWorkPool",
    "CacheStats",
    "CircuitBreaker",
    "CircuitBreakerStats",
    "CoalescerStats",
    "CooldownRegistry",
    "CooldownStats",
    "Deadline",
    "KeyedCoalescer",
    "KeyedConcurrencyLimiter",
    "KeyedConcurrencyKeyStats",
    "KeyedConcurrencyStats",
    "KeyedRateLimiter",
    "KeyedRateStats",
    "LatestTaskCoordinator",
    "LatestTaskSnapshot",
    "LoopPortal",
    "OperationMetrics",
    "OperationStats",
    "OperationStatsSnapshot",
    "PolicyDecision",
    "PolicyEvaluator",
    "RegistryChange",
    "ResourceScope",
    "RetryObservation",
    "RetryPolicy",
    "ScheduledHandle",
    "SchedulerStats",
    "ServiceRegistry",
    "SingleFlight",
    "SingleFlightStats",
    "StateMachine",
    "StateSnapshot",
    "StateTransitionError",
    "TaskHandle",
    "TaskScopeSnapshot",
    "VersionedCache",
    "VersionedSnapshot",
    "VersionedValue",
    "WorkPoolStats",
]
