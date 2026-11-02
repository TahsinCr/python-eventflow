"""Compose a synchronous bus with async queue, task and keyed limits."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from eventflow import (
    AsyncEventQueue,
    AsyncTaskScope,
    Deadline,
    EventBus,
    KeyedConcurrencyLimiter,
    OperationStats,
    RetryPolicy,
)


@dataclass(frozen=True)
class WorkItem:
    key: str
    value: int


class TemporaryFailure(Exception):
    """A transient error used to demonstrate an explicit retry predicate."""


async def main() -> None:
    bus = EventBus[WorkItem]()
    metrics = OperationStats()
    queue = AsyncEventQueue(bus, event_type=WorkItem, capacity=16, observer=metrics)
    limiter = KeyedConcurrencyLimiter[str](2, observer=metrics)
    tasks = AsyncTaskScope(observer=metrics)
    retry = RetryPolicy(max_attempts=2, initial_delay=0)
    attempts = 0

    async def transform(value: int) -> int:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise TemporaryFailure("retry once")
        return value * 2

    async def consume_one() -> int:
        item = await queue.receive()
        async with limiter.acquire(item.key):
            return await retry.run_async(
                lambda: transform(item.value),
                should_retry=lambda error: isinstance(error, TemporaryFailure),
                deadline=Deadline.after(2),
            )

    try:
        async with queue:
            async with tasks:
                worker = tasks.start(consume_one, name="one-item-worker", shutdown="drain")
                bus.publish(WorkItem("account-42", 21))
                result = await worker.join(timeout=2)
    finally:
        await limiter.aclose()
        bus.close()

    print(f"result={result}; attempts={attempts}")
    print("operations=" + ", ".join(sorted(metrics.snapshot().operations)))


if __name__ == "__main__":
    asyncio.run(main())
