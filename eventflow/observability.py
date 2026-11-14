"""Opt-in pipeline measurements; no logging setup or global collectors."""

from __future__ import annotations

import math
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Literal, Protocol

from ._native import CORE

ObservationKind = Literal["run", "stage", "middleware"]
ExecutionStatus = Literal["success", "error", "cancelled"]
__all__ = [
    "ExecutionMetrics",
    "PipelineObservation",
    "PipelineObserver",
    "PipelineStats",
    "PipelineStatsSnapshot",
    "ExecutionStatus",
    "ObservationKind",
    "OperationMetrics",
    "OperationStats",
    "OperationStatsSnapshot",
]


@dataclass(frozen=True, slots=True)
class PipelineObservation:
    """One completed execution; elapsed time is measured in seconds."""

    kind: ObservationKind
    name: str | None
    elapsed: float
    status: ExecutionStatus
    correlation_id: str | None = None
    error: BaseException | None = None


class PipelineObserver(Protocol):
    """Synchronous, fast notification callable, also used by AsyncPipeline."""

    def __call__(self, observation: PipelineObservation) -> None: ...


@dataclass(frozen=True, slots=True)
class ExecutionMetrics:
    """Aggregate counts and durations, without retained payloads or errors."""

    calls: int = 0
    successful: int = 0
    failed: int = 0
    cancelled: int = 0
    total_seconds: float = 0.0
    max_seconds: float = 0.0


@dataclass(frozen=True, slots=True)
class PipelineStatsSnapshot:
    """Immutable aggregate view; disabled stages have no measurements."""

    runs: ExecutionMetrics
    stages: Mapping[str, ExecutionMetrics]
    middleware: Mapping[str, ExecutionMetrics]


@dataclass(slots=True)
class _MetricTotals:
    calls: int = 0
    successful: int = 0
    failed: int = 0
    cancelled: int = 0
    total_seconds: float = 0.0
    max_seconds: float = 0.0

    def record(self, observation: PipelineObservation) -> None:
        self.calls += 1
        status = observation.status
        if status == "success":
            self.successful += 1
        elif status == "error":
            self.failed += 1
        else:
            self.cancelled += 1
        elapsed = observation.elapsed
        self.total_seconds += elapsed
        if elapsed > self.max_seconds:
            self.max_seconds = elapsed

    def freeze(self) -> ExecutionMetrics:
        return ExecutionMetrics(
            self.calls,
            self.successful,
            self.failed,
            self.cancelled,
            self.total_seconds,
            self.max_seconds,
        )


class PipelineStats:
    """Thread-safe observer collecting per-run, stage and middleware aggregates."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._metrics: dict[tuple[ObservationKind, str | None], _MetricTotals] = {}

    def __call__(self, observation: PipelineObservation) -> None:
        key = (observation.kind, observation.name)
        with self._lock:
            totals = self._metrics.get(key)
            if totals is None:
                totals = _MetricTotals()
                self._metrics[key] = totals
            totals.record(observation)

    @property
    def snapshot(self) -> PipelineStatsSnapshot:
        """Copy aggregate counters; the returned mappings cannot be changed."""
        with self._lock:
            runs = self._metrics.get(("run", None))
            return PipelineStatsSnapshot(
                ExecutionMetrics() if runs is None else runs.freeze(),
                MappingProxyType(
                    {
                        name: totals.freeze()
                        for (kind, name), totals in self._metrics.items()
                        if kind == "stage" and name is not None
                    }
                ),
                MappingProxyType(
                    {
                        name: totals.freeze()
                        for (kind, name), totals in self._metrics.items()
                        if kind == "middleware" and name is not None
                    }
                ),
            )

    def reset(self) -> None:
        """Reset counters; in-flight operations contribute when they finish."""
        with self._lock:
            self._metrics.clear()


@dataclass(frozen=True, slots=True)
class OperationMetrics:
    """Payload-free aggregate counters for one bounded operation label."""

    calls: int = 0
    successful: int = 0
    failed: int = 0
    cancelled: int = 0
    total_seconds: float = 0.0
    max_seconds: float = 0.0
    queue_depth: int = 0
    max_queue_depth: int = 0
    dropped: int = 0
    retries: int = 0
    rejected: int = 0
    timeouts: int = 0
    shutdown_seconds: float = 0.0


@dataclass(frozen=True, slots=True)
class OperationStatsSnapshot:
    """Immutable snapshot of execution totals keyed by bounded labels."""

    operations: Mapping[str, OperationMetrics]


@dataclass(slots=True)
class _OperationTotals:
    calls: int = 0
    successful: int = 0
    failed: int = 0
    cancelled: int = 0
    total_seconds: float = 0.0
    max_seconds: float = 0.0
    queue_depth: int = 0
    max_queue_depth: int = 0
    dropped: int = 0
    retries: int = 0
    rejected: int = 0
    timeouts: int = 0
    shutdown_seconds: float = 0.0

    def freeze(self) -> OperationMetrics:
        return OperationMetrics(
            self.calls,
            self.successful,
            self.failed,
            self.cancelled,
            self.total_seconds,
            self.max_seconds,
            self.queue_depth,
            self.max_queue_depth,
            self.dropped,
            self.retries,
            self.rejected,
            self.timeouts,
            self.shutdown_seconds,
        )


_INF = math.inf


def _validate_duration_slow(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a finite non-negative number")
    number = float(value)
    if not (0.0 <= number < _INF):
        raise ValueError(f"{name} must be a finite non-negative number")
    return number


def _validate_non_negative_counts(*values: object) -> None:
    for value in values:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(
                "drop, retry, rejection and timeout counts must be non-negative integers"
            )


if CORE is not None:
    CORE.configure_observability(OperationMetrics, OperationStatsSnapshot, MappingProxyType)


class OperationStats:
    """Opt-in, thread-safe operation metrics with bounded label cardinality.

    Once ``max_operations`` distinct names have been recorded, further unseen
    names are folded into ``"other"`` rather than growing memory without bound.
    """

    _STATUSES = frozenset(("success", "error", "cancelled"))

    def __init__(self, *, max_operations: int = 128) -> None:
        if isinstance(max_operations, bool) or not isinstance(max_operations, int):
            raise TypeError("max_operations must be an integer")
        if max_operations < 1:
            raise ValueError("max_operations must be positive")
        self._max_operations = max_operations
        if CORE is not None:
            self._core: Any = CORE.OperationStatsCore(max_operations)
            self._lock: Any = None
            self._metrics: dict[str, _OperationTotals] = {}
        else:
            self._core = None
            self._lock = threading.Lock()
            self._metrics = {}

    def record(
        self,
        name: str,
        *,
        status: str = "success",
        elapsed: float = 0.0,
        queue_depth: int | None = None,
        dropped: int = 0,
        retries: int = 0,
        rejected: int = 0,
        timeouts: int = 0,
        shutdown_seconds: float = 0.0,
    ) -> None:
        """Record one completed operation without retaining user payloads."""
        if self._core is not None:
            self._core.record(
                name,
                status,
                elapsed,
                queue_depth,
                dropped,
                retries,
                rejected,
                timeouts,
                shutdown_seconds,
            )
            return
        if type(name) is not str:
            if not isinstance(name, str):
                raise ValueError("operation name must be a non-empty string")
        if not name:
            raise ValueError("operation name must be a non-empty string")
        if status not in self._STATUSES:
            raise ValueError("status must be success, error, or cancelled")
        if type(elapsed) is float and 0.0 <= elapsed < _INF:
            elapsed_value = elapsed
        else:
            elapsed_value = _validate_duration_slow(elapsed, "elapsed")
        if type(shutdown_seconds) is float and 0.0 <= shutdown_seconds < _INF:
            shutdown_value = shutdown_seconds
        else:
            shutdown_value = _validate_duration_slow(shutdown_seconds, "shutdown_seconds")
        if (
            type(dropped) is not int
            or dropped < 0
            or type(retries) is not int
            or retries < 0
            or type(rejected) is not int
            or rejected < 0
            or type(timeouts) is not int
            or timeouts < 0
        ):
            _validate_non_negative_counts(dropped, retries, rejected, timeouts)
        if queue_depth is not None and (type(queue_depth) is not int or queue_depth < 0):
            if isinstance(queue_depth, bool) or not isinstance(queue_depth, int) or queue_depth < 0:
                raise ValueError("queue_depth must be a non-negative integer or None")
        with self._lock:
            if len(name) > 128:
                name = "other"
            totals = self._metrics.get(name)
            if totals is None:
                if self._max_operations == 1 or len(self._metrics) >= self._max_operations - 1:
                    name = "other"
                    totals = self._metrics.get(name)
                if totals is None:
                    totals = _OperationTotals()
                    self._metrics[name] = totals
            totals.calls += 1
            if status == "success":
                totals.successful += 1
            elif status == "error":
                totals.failed += 1
            else:
                totals.cancelled += 1
            totals.total_seconds += elapsed_value
            if elapsed_value > totals.max_seconds:
                totals.max_seconds = elapsed_value
            if queue_depth is not None:
                totals.queue_depth = queue_depth
                if queue_depth > totals.max_queue_depth:
                    totals.max_queue_depth = queue_depth
            if dropped:
                totals.dropped += dropped
            if retries:
                totals.retries += retries
            if rejected:
                totals.rejected += rejected
            if timeouts:
                totals.timeouts += timeouts
            if shutdown_value:
                totals.shutdown_seconds += shutdown_value

    def snapshot(self) -> OperationStatsSnapshot:
        if self._core is not None:
            return self._core.snapshot()
        with self._lock:
            return OperationStatsSnapshot(
                MappingProxyType({name: totals.freeze() for name, totals in self._metrics.items()})
            )

    def reset(self) -> None:
        if self._core is not None:
            self._core.reset()
            return
        with self._lock:
            self._metrics.clear()


def _require_operation_stats(observer: OperationStats | None) -> None:
    if observer is not None and not isinstance(observer, OperationStats):
        raise TypeError("observer must be an OperationStats instance")
