"""Small shared test utilities."""

from collections.abc import Callable
from typing import NoReturn

from eventflow import EventFilter, MemoryEventHistory


def _reject_task_creation(rejected: list[object]) -> Callable[..., NoReturn]:
    def reject_task(coroutine: object, **_: object) -> NoReturn:
        rejected.append(coroutine)
        raise RuntimeError("task creation rejected")

    return reject_task


class ManualClock:
    """A controllable monotonic clock for deterministic timing tests."""

    def __init__(self, value: float = 0.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value

    def set(self, value: float) -> None:
        self.value = value

    def advance(self, seconds: float) -> None:
        self.value += seconds


class _AsyncValueMixin:
    @staticmethod
    async def _value(value):
        return value


class _FailingQueryHistory(MemoryEventHistory[int]):
    def query(
        self,
        event_filter: EventFilter[int] | None = None,
        *,
        limit: int | None = None,
    ) -> tuple[int, ...]:
        raise RuntimeError("history query failed")


class _FailingAppendHistory(MemoryEventHistory[int]):
    def append(self, event: int) -> None:
        raise RuntimeError("history append failed")


class _RecordingQueryHistory(MemoryEventHistory[int]):
    def __init__(self) -> None:
        super().__init__(capacity=10)
        self.limits: list[int | None] = []

    def query(
        self,
        event_filter: EventFilter[int] | None = None,
        *,
        limit: int | None = None,
    ) -> tuple[int, ...]:
        self.limits.append(limit)
        return super().query(event_filter, limit=limit)
