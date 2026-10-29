"""Opt-in pipeline measurements; no logging setup or global collectors."""

from __future__ import annotations

import threading
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal, Protocol

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
        self.successful += observation.status == "success"
        self.failed += observation.status == "error"
        self.cancelled += observation.status == "cancelled"
        self.total_seconds += observation.elapsed
        self.max_seconds = max(self.max_seconds, observation.elapsed)

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
