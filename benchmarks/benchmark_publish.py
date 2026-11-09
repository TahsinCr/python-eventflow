"""Benchmark synchronous and asynchronous event publication with pyperf."""

# pyright: strict

from __future__ import annotations

import asyncio
from time import perf_counter

from eventflow import AsyncEventBus, DeliveryMode, ErrorPolicy, EventBus
from eventflow.engine import AsyncEventEngine, EventEngine

from ._runner import bench_sync, bench_time_func, create_runner


async def _receive_async(_event: int) -> None:
    pass


async def _yield_async(_event: int) -> None:
    await asyncio.sleep(0)


async def _fail_async(_event: int) -> None:
    raise ValueError("benchmark callback failure")


def _time_async_publish(
    loops: int,
    mode: DeliveryMode,
    subscribers: int,
    failure_ratio: float | None,
    yield_once: bool = False,
) -> float:
    failed_callbacks = round(10 * failure_ratio) if failure_ratio is not None else 0

    async def measure() -> float:
        error_policy = ErrorPolicy.ISOLATE if failure_ratio is not None else ErrorPolicy.RAISE
        async with AsyncEventBus[int](delivery_mode=mode, error_policy=error_policy) as bus:
            for index in range(subscribers):
                callback = (
                    _fail_async
                    if index < failed_callbacks
                    else (_yield_async if yield_once else _receive_async)
                )
                await bus.subscribe(callback)

            started = perf_counter()
            for _ in range(loops):
                await bus.publish(1)
            return perf_counter() - started

    return asyncio.run(measure())


def _time_sync_engine_publish(loops: int, subscribers: int) -> float:
    engine = EventEngine()
    bus = engine.add("events", EventBus[int]())
    for _ in range(subscribers):
        bus.subscribe(lambda _event: None)
    try:
        started = perf_counter()
        for _ in range(loops):
            engine.publish("events", 1)
        return perf_counter() - started
    finally:
        engine.stop()


def _time_async_engine_publish(loops: int, subscribers: int) -> float:
    async def measure() -> float:
        engine = AsyncEventEngine()
        bus = engine.add("events", AsyncEventBus[int]())
        for _ in range(subscribers):
            await bus.subscribe(_receive_async)
        try:
            started = perf_counter()
            for _ in range(loops):
                await engine.publish("events", 1)
            return perf_counter() - started
        finally:
            await engine.stop()

    return asyncio.run(measure())


def main() -> None:
    runner = create_runner(__doc__ or "Event publication benchmarks")
    runner.parse_args()

    def receive(_event: int) -> None:
        pass

    def fail_sync(_event: int) -> None:
        raise ValueError("benchmark callback failure")

    for subscribers in (0, 1, 10, 100):
        with EventBus[int]() as bus:
            for _ in range(subscribers):
                bus.subscribe(receive)
            bench_sync(
                runner,
                f"sync/{subscribers}",
                lambda bus=bus: bus.publish(1),
                metadata={"subscribers": subscribers},
            )

    for kind in ("type", "predicate"):
        with EventBus[int]() as bus:
            for _ in range(10):
                if kind == "type":
                    bus.subscribe(receive, event_type=int)
                else:
                    bus.subscribe(receive, predicate=lambda event: event > 0)
            bench_sync(
                runner,
                f"sync/filter-{kind}",
                lambda bus=bus: bus.publish(1),
                metadata={"filter": kind, "subscribers": 10},
            )

    for subscribers in (10, 100, 1_000):
        with EventBus[int](routing_key=lambda event: event) as bus:
            for key in range(subscribers):
                bus.subscribe(receive, routing_key=key)
            bench_sync(
                runner,
                f"sync/routing-key/{subscribers}",
                lambda bus=bus: bus.publish(0),
                metadata={"routing_key_subscribers": subscribers, "matched_subscribers": 1},
            )

    for failure_ratio in (0.0, 0.1, 1.0):
        failed_callbacks = round(10 * failure_ratio)
        with EventBus[int](error_policy=ErrorPolicy.ISOLATE) as bus:
            for index in range(10):
                bus.subscribe(fail_sync if index < failed_callbacks else receive)
            bench_sync(
                runner,
                f"sync/callback-error-rate-{failure_ratio:.0%}",
                lambda bus=bus: bus.publish(1),
                metadata={"error_rate": failure_ratio, "subscribers": 10},
            )

    for subscribers in (0, 10):
        bench_time_func(
            runner,
            f"sync-engine/{subscribers}",
            _time_sync_engine_publish,
            subscribers,
            metadata={"subscribers": subscribers, "dispatch": "named-channel"},
        )

    for mode in DeliveryMode:
        for subscribers in (0, 1, 10, 100):
            bench_time_func(
                runner,
                f"async-{mode.value}/{subscribers}",
                _time_async_publish,
                mode,
                subscribers,
                None,
                metadata={"delivery_mode": mode.value, "subscribers": subscribers},
            )

    for mode in DeliveryMode:
        for subscribers in (10, 100):
            bench_time_func(
                runner,
                f"async-{mode.value}/yield-once/{subscribers}",
                _time_async_publish,
                mode,
                subscribers,
                None,
                True,
                metadata={
                    "delivery_mode": mode.value,
                    "subscribers": subscribers,
                    "callback_behavior": "yield-once",
                },
            )

    for mode in DeliveryMode:
        for failure_ratio in (0.0, 0.1, 1.0):
            bench_time_func(
                runner,
                f"async-{mode.value}/callback-error-rate-{failure_ratio:.0%}",
                _time_async_publish,
                mode,
                10,
                failure_ratio,
                metadata={"delivery_mode": mode.value, "error_rate": failure_ratio},
            )

    for subscribers in (0, 10):
        bench_time_func(
            runner,
            f"async-engine/{subscribers}",
            _time_async_engine_publish,
            subscribers,
            metadata={"subscribers": subscribers, "dispatch": "named-channel"},
        )


if __name__ == "__main__":
    main()
