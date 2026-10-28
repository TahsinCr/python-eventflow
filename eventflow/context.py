"""Optional immutable event envelopes and task-local correlation identifiers."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Generic, TypeVar
from uuid import uuid4

T = TypeVar("T")
_correlation: ContextVar[str | None] = ContextVar("eventflow_correlation", default=None)
__all__ = ["EventEnvelope", "correlation_context", "current_correlation_id"]


def current_correlation_id() -> str | None:
    """Return the identifier in the current context, if one was bound."""
    return _correlation.get()


@contextmanager
def correlation_context(correlation_id: str) -> Iterator[str]:
    """Bind an identifier until exit; nested and async task contexts are isolated."""
    if not isinstance(correlation_id, str) or not correlation_id.strip():
        raise ValueError("Correlation ID must be a non-empty string")
    token = _correlation.set(correlation_id)
    try:
        yield correlation_id
    finally:
        _correlation.reset(token)


@dataclass(frozen=True)
class EventEnvelope(Generic[T]):
    """Opt-in event metadata; metadata is copied and shallowly read-only.

    The payload and nested metadata values retain their original mutability.
    A missing correlation ID inherits the current context or the envelope ID.
    """

    event: T
    id: str = field(default_factory=lambda: uuid4().hex)
    correlation_id: str | None = None
    source: str | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.id, str) or not self.id.strip():
            raise ValueError("Envelope ID must be a non-empty string")
        correlation = self.correlation_id
        if correlation is None:
            correlation = current_correlation_id() or self.id
        if not isinstance(correlation, str) or not correlation.strip():
            raise ValueError("Correlation ID must be a non-empty string")
        if not isinstance(self.timestamp, datetime) or self.timestamp.utcoffset() is None:
            raise ValueError("Envelope timestamp must be timezone-aware")
        object.__setattr__(self, "correlation_id", correlation)
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))
