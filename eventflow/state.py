"""Thread-safe state transitions and versioned values."""

from __future__ import annotations

import inspect
import threading
from collections.abc import Callable, Hashable, Mapping, Set
from dataclasses import dataclass
from types import MappingProxyType
from typing import Generic, TypeVar

from ._callbacks import close_awaitable

S = TypeVar("S", bound=Hashable)
T = TypeVar("T")
__all__ = [
    "StateMachine",
    "StateSnapshot",
    "StateTransitionError",
    "VersionedValue",
    "VersionedSnapshot",
]


class StateTransitionError(ValueError):
    """Raised when a requested state transition is not allowed."""


@dataclass(frozen=True, slots=True)
class StateSnapshot(Generic[S]):
    state: S
    version: int


@dataclass(frozen=True, slots=True)
class VersionedSnapshot(Generic[T]):
    """A point-in-time value reference and its version; T itself is not copied."""

    value: T
    version: int


class StateMachine(Generic[S]):
    """Validate transitions atomically; observer callbacks run after unlocking."""

    def __init__(
        self,
        initial: S,
        transitions: Mapping[S, Set[S]],
        *,
        terminal: Set[S] = frozenset(),
        guard: Callable[[S, S], bool] | None = None,
        observer: Callable[[StateSnapshot[S], StateSnapshot[S]], None] | None = None,
    ) -> None:
        if not isinstance(transitions, Mapping):
            raise TypeError("transitions must be a mapping")
        if guard is not None and not callable(guard):
            raise TypeError("guard must be callable")
        if observer is not None and not callable(observer):
            raise TypeError("observer must be callable")
        normalized: dict[S, frozenset[S]] = {}
        for state, targets in transitions.items():
            normalized[state] = frozenset(targets)
        states = set(normalized)
        states.update(target for targets in normalized.values() for target in targets)
        if initial not in states:
            raise ValueError("initial state must appear in transitions")
        if not set(terminal).issubset(states):
            raise ValueError("terminal states must appear in transitions")
        self._transitions = MappingProxyType(normalized)
        self._terminal = frozenset(terminal)
        self._guard = guard
        self._observer = observer
        self._state = initial
        self._version = 0
        self._lock = threading.Lock()

    @property
    def state(self) -> S:
        with self._lock:
            return self._state

    @property
    def version(self) -> int:
        with self._lock:
            return self._version

    def snapshot(self) -> StateSnapshot[S]:
        with self._lock:
            return StateSnapshot(self._state, self._version)

    def can_transition(self, target: S) -> bool:
        with self._lock:
            current = self._state
        return self._allows(current, target)

    def _allows(self, current: S, target: S) -> bool:
        if current in self._terminal or target not in self._transitions.get(current, ()):
            return False
        if self._guard is None:
            return True
        result = self._guard(current, target)
        if inspect.isawaitable(result):
            close_awaitable(result)
            raise TypeError("StateMachine guard must be synchronous")
        if type(result) is not bool:
            raise TypeError("StateMachine guard must return bool")
        return result

    def transition(self, target: S) -> StateSnapshot[S]:
        result = self._transition(target, expected_version=None)
        assert result is not None
        return result

    def compare_and_transition(self, expected_version: int, target: S) -> StateSnapshot[S] | None:
        """Transition only if the observed version is still current; stale writes return None."""
        if isinstance(expected_version, bool) or not isinstance(expected_version, int):
            raise TypeError("expected_version must be a non-negative integer")
        if expected_version < 0:
            raise ValueError("expected_version must be non-negative")
        return self._transition(target, expected_version=expected_version)

    def _transition(self, target: S, expected_version: int | None) -> StateSnapshot[S] | None:
        while True:
            with self._lock:
                if expected_version is not None and expected_version != self._version:
                    return None
                previous_state = self._state
                observed_version = self._version

            allowed = self._allows(previous_state, target)

            with self._lock:
                if self._version != observed_version:
                    if expected_version is not None:
                        return None
                    continue
                if not allowed:
                    raise StateTransitionError(
                        f"Transition from {previous_state!r} to {target!r} is not allowed"
                    )
                previous = StateSnapshot(previous_state, observed_version)
                self._state = target
                self._version += 1
                current = StateSnapshot(self._state, self._version)
                break
        if self._observer is not None:
            result = self._observer(previous, current)
            if inspect.isawaitable(result):
                close_awaitable(result)
                raise TypeError("StateMachine observer must be synchronous")
        return current


class VersionedValue(Generic[T]):
    """Atomically replace a value and increment its version under a small lock."""

    def __init__(self, initial: T) -> None:
        self._value = initial
        self._version = 0
        self._lock = threading.Lock()

    @property
    def version(self) -> int:
        with self._lock:
            return self._version

    def snapshot(self) -> VersionedSnapshot[T]:
        with self._lock:
            return VersionedSnapshot(self._value, self._version)

    def set(self, value: T) -> VersionedSnapshot[T]:
        with self._lock:
            self._value = value
            self._version += 1
            return VersionedSnapshot(self._value, self._version)

    def compare_and_set(self, expected_version: int, new_value: T) -> bool:
        if isinstance(expected_version, bool) or not isinstance(expected_version, int):
            raise TypeError("expected_version must be a non-negative integer")
        if expected_version < 0:
            raise ValueError("expected_version must be non-negative")
        with self._lock:
            if self._version != expected_version:
                return False
            self._value = new_value
            self._version += 1
            return True
