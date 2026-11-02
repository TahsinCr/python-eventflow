"""Monotonic time budgets for composing operation timeouts."""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Callable

from .base import validate_finite_duration

__all__ = ["Deadline"]


@dataclass(frozen=True, slots=True)
class Deadline:
    """An absolute monotonic deadline that can be safely shared by nested calls.

    ``None`` represents an unlimited budget. Inject ``clock`` to make boundary
    behavior deterministic in tests; the default is ``time.monotonic``.
    """

    _expires_at: float | None
    _clock: Callable[[], float] = field(default=time.monotonic, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._expires_at is not None and not math.isfinite(self._expires_at):
            raise ValueError("deadline timestamp must be finite or None")
        if not callable(self._clock):
            raise TypeError("clock must be callable")

    @classmethod
    def after(
        cls, timeout: float | None, *, clock: Callable[[], float] = time.monotonic
    ) -> Deadline:
        """Create a deadline after ``timeout`` seconds, or an unlimited one for None."""
        if not callable(clock):
            raise TypeError("clock must be callable")
        if timeout is None:
            return cls(None, clock)
        duration = validate_finite_duration(timeout, name="timeout", allow_zero=True)
        now = clock()
        if not math.isfinite(now):
            raise ValueError("clock must return a finite value")
        return cls(now + duration, clock)

    @classmethod
    def at(
        cls, timestamp: float | None, *, clock: Callable[[], float] = time.monotonic
    ) -> Deadline:
        """Create a deadline at an absolute monotonic timestamp."""
        if not callable(clock):
            raise TypeError("clock must be callable")
        if timestamp is None:
            return cls(None, clock)
        if isinstance(timestamp, bool) or not isinstance(timestamp, (int, float)):
            raise TypeError("timestamp must be finite or None")
        value = float(timestamp)
        if not math.isfinite(value):
            raise ValueError("timestamp must be finite")
        return cls(value, clock)

    @property
    def remaining(self) -> float | None:
        """Seconds left, clamped at zero; None means unlimited."""
        if self._expires_at is None:
            return None
        now = self._clock()
        if not math.isfinite(now):
            raise ValueError("clock must return a finite value")
        return max(0.0, self._expires_at - now)

    @property
    def expired(self) -> bool:
        """Whether the finite deadline has elapsed."""
        remaining = self.remaining
        return remaining is not None and remaining <= 0.0

    def child(self, timeout: float | None) -> Deadline:
        """Return a child budget that cannot outlive this deadline."""
        if timeout is None:
            return self
        duration = validate_finite_duration(timeout, name="timeout", allow_zero=True)
        now = self._clock()
        if not math.isfinite(now):
            raise ValueError("clock must return a finite value")
        candidate = now + duration
        if self._expires_at is not None:
            candidate = min(candidate, self._expires_at)
        return Deadline(candidate, self._clock)
