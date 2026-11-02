"""Errors raised by the in-process event buses."""


class EventBusError(RuntimeError):
    """Base error for event bus operations."""


class EventBusClosedError(EventBusError):
    """Raised when an operation requires an open event bus."""


class InvalidEventHandlerError(EventBusError, TypeError):
    """Raised when a handler is not compatible with the selected bus."""


class CallbackTimeoutError(EventBusError, TimeoutError):
    """Raised when a callback exceeds its configured execution budget."""


class EventShutdownTimeoutError(EventBusError, TimeoutError):
    """Raised when a synchronous schedule cannot stop within its budget."""


class EventQueueClosedError(EventBusError):
    """Raised when receiving from a closed and drained event queue."""


class QueueOverflowError(EventBusError):
    """Raised when an event queue configured with overflow='error' is full."""


class LoopPortalClosedError(RuntimeError):
    """Raised when submitting work to a stopped LoopPortal."""


class CircuitOpenError(RuntimeError):
    """Raised when a circuit breaker rejects a call while open or saturated."""
