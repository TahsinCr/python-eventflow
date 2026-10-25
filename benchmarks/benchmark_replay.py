"""Compare full history reads with bounded replay setup costs."""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Awaitable, Callable
from statistics import median
from time import perf_counter

from eventflow import AsyncEventBus, EventBus, MemoryEventHistory


def _positive_integer(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def _measure_sync(
    label: str,
    operation: Callable[[], object],
    *,
    iterations: int,
    samples: int,
) -> None:
    durations = []
    for _ in range(samples):
        started = perf_counter()
        for _ in range(iterations):
            operation()
        durations.append((perf_counter() - started) * 1_000 / iterations)
    print(f"{label}: median {median(durations):.4f} ms/op")


async def _measure_async(
    label: str,
    operation: Callable[[], Awaitable[object]],
    *,
    iterations: int,
    samples: int,
) -> None:
    durations = []
    for _ in range(samples):
        started = perf_counter()
        for _ in range(iterations):
            await operation()
        durations.append((perf_counter() - started) * 1_000 / iterations)
    print(f"{label}: median {median(durations):.4f} ms/op")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=_positive_integer, default=100_000)
    parser.add_argument("--replay", type=_positive_integer, default=1)
    parser.add_argument("--iterations", type=_positive_integer, default=25)
    parser.add_argument("--samples", type=_positive_integer, default=5)
    args = parser.parse_args()

    history = MemoryEventHistory[int](capacity=args.events)
    for event in range(args.events):
        history.append(event)

    sync_bus = EventBus[int](history=history)
    async_bus = AsyncEventBus[int](history=history)

    def receive_sync(_event: int) -> None:
        pass

    async def receive_async(_event: int) -> None:
        pass

    def query_all() -> object:
        return history.query()

    def query_replay_window() -> object:
        return history.query(limit=args.replay)

    def latest_history() -> object:
        return history.latest()

    def latest_sync_bus() -> object:
        return sync_bus.latest()

    def query_sync_bus() -> object:
        return sync_bus.query(limit=args.replay)

    def latest_async_bus() -> object:
        return async_bus.latest()

    def query_async_bus() -> object:
        return async_bus.query(limit=args.replay)

    def subscribe_sync() -> None:
        subscription = sync_bus.subscribe(receive_sync, replay=args.replay)
        subscription.cancel()

    async def subscribe_async() -> None:
        subscription = await async_bus.subscribe(receive_async, replay=args.replay)
        await subscription.cancel()

    print(
        f"events={args.events}, replay={args.replay}, "
        f"iterations={args.iterations}, samples={args.samples}"
    )
    _measure_sync(
        "Full history query",
        query_all,
        iterations=args.iterations,
        samples=args.samples,
    )
    _measure_sync(
        "Bounded history query",
        query_replay_window,
        iterations=args.iterations,
        samples=args.samples,
    )
    _measure_sync(
        "MemoryEventHistory.latest",
        latest_history,
        iterations=args.iterations,
        samples=args.samples,
    )
    _measure_sync(
        "EventBus.latest",
        latest_sync_bus,
        iterations=args.iterations,
        samples=args.samples,
    )
    _measure_sync(
        "EventBus.query(limit=replay)",
        query_sync_bus,
        iterations=args.iterations,
        samples=args.samples,
    )
    _measure_sync(
        "AsyncEventBus.latest",
        latest_async_bus,
        iterations=args.iterations,
        samples=args.samples,
    )
    _measure_sync(
        "AsyncEventBus.query(limit=replay)",
        query_async_bus,
        iterations=args.iterations,
        samples=args.samples,
    )
    _measure_sync(
        "EventBus replay subscription",
        subscribe_sync,
        iterations=args.iterations,
        samples=args.samples,
    )
    async def run_async_benchmarks() -> None:
        await _measure_async(
            "AsyncEventBus replay subscription",
            subscribe_async,
            iterations=args.iterations,
            samples=args.samples,
        )
        await async_bus.close()

    asyncio.run(run_async_benchmarks())
    sync_bus.close()


if __name__ == "__main__":
    main()
