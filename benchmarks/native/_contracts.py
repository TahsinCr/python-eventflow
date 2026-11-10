"""Static contracts for optional benchmark-only native kernels."""

from __future__ import annotations

from typing import Protocol


class FilterKernel(Protocol):
    def __call__(
        self,
        events: tuple[object, ...],
        event_type: type[object],
    ) -> tuple[object, ...]: ...


class MergeKernel(Protocol):
    def __call__(
        self,
        unkeyed: dict[int, object],
        keyed: dict[int, object],
    ) -> tuple[object, ...]: ...
