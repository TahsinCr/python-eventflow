"""Benchmark publisher contention, schedule registration and retained memory."""

# pyright: strict

from __future__ import annotations

import argparse
import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from time import perf_counter

from eventflow import AsyncEventBus, DeliveryMode, EventBus

from ._runner import bench_time_func, create_runner, forward_cli_arguments
from ._support import ignore_event, positive_int, yield_once_async

_SUBSCRIBERS = 4
_SCHEDULE_INTERVAL = 3_600.0
_PAYLOAD_BYTES = 4_096


def _counts(value: str) -> tuple[int, ...]:
    try:
        values = tuple(int(item) for item in value.split(","))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "counts must be comma-separated positive integers"
        ) from exc
    if not values or any(item <= 0 for item in values):
        raise argparse.ArgumentTypeError("counts must be comma-separated positive integers")
    return values


def _time_sync_contention(loops: int, publishers: int, batch_size: int) -> float:
    barrier = threading.Barrier(publishers)
    with EventBus[int]() as bus:
        bus.subscribe(ignore_event)

        def publish_many(_publisher: int) -> None:
            barrier.wait(timeout=10)
            for _ in range(batch_size):
                bus.publish(1)

        with ThreadPoolExecutor(max_workers=publishers) as executor:
            started = perf_counter()
            for _ in range(loops):
                list(executor.map(publish_many, range(publishers)))
            return perf_counter() - started


def _time_async_contention(
    loops: int,
    publishers: int,
    batch_size: int,
    limit: int | None,
) -> float:
    async def measure() -> float:
        async with AsyncEventBus[int](
            delivery_mode=DeliveryMode.CONCURRENT,
            max_concurrency=limit,
        ) as bus:
            for _ in range(_SUBSCRIBERS):
                await bus.subscribe(yield_once_async)

            async def publish_many() -> None:
                for _ in range(batch_size):
                    await bus.publish(1)

            started = perf_counter()
            for _ in range(loops):
                await asyncio.gather(*(publish_many() for _ in range(publishers)))
            return perf_counter() - started

    return asyncio.run(measure())


def _time_sync_memory(loops: int, operations: int, history_capacity: int | None) -> float:
    with EventBus[bytes](history=history_capacity) as bus:
        bus.subscribe(ignore_event)
        started = perf_counter()
        for index in range(loops * operations):
            bus.publish(f"{index}:".encode() + b"x" * _PAYLOAD_BYTES)
        return perf_counter() - started


def _time_async_memory(loops: int, operations: int, history_capacity: int | None) -> float:
    async def measure() -> float:
        async with AsyncEventBus[bytes](history=history_capacity) as bus:
            await bus.subscribe(yield_once_async)
            started = perf_counter()
            for index in range(loops * operations):
                await bus.publish(f"{index}:".encode() + b"x" * _PAYLOAD_BYTES)
            return perf_counter() - started

    return asyncio.run(measure())


def _time_sync_schedules(loops: int, count: int) -> float:
    elapsed = 0.0
    for _ in range(loops):
        with EventBus[int]() as bus:
            started = perf_counter()
            handles = [
                bus.publish_every(index, _SCHEDULE_INTERVAL, immediately=False)
                for index in range(count)
            ]
            elapsed += perf_counter() - started
            for handle in handles:
                handle.cancel()
    return elapsed


def _time_async_schedules(loops: int, count: int) -> float:
    async def measure() -> float:
        async with AsyncEventBus[int]() as bus:
            elapsed = 0.0
            for _ in range(loops):
                started = perf_counter()
                handles = [
                    await bus.publish_every(index, _SCHEDULE_INTERVAL, immediately=False)
                    for index in range(count)
                ]
                elapsed += perf_counter() - started
                await asyncio.gather(*(handle.cancel() for handle in handles))
            return elapsed

    return asyncio.run(measure())


def main() -> None:
    runner = create_runner(
        __doc__ or "Runtime workload benchmarks",
        add_cmdline_args=forward_cli_arguments(
            "batch_size", "soak_operations", "publisher_counts", "schedule_counts"
        ),
    )
    runner.argparser.add_argument("--batch-size", type=positive_int, default=100)
    runner.argparser.add_argument("--soak-operations", type=positive_int, default=10_000)
    runner.argparser.add_argument("--publisher-counts", type=_counts, default=(1, 4, 16))
    runner.argparser.add_argument("--schedule-counts", type=_counts, default=(1, 10, 50))
    args = runner.parse_args()

    for publishers in args.publisher_counts:
        bench_time_func(
            runner,
            f"sync-contention/{publishers}",
            _time_sync_contention,
            publishers,
            args.batch_size,
            inner_loops=publishers * args.batch_size,
            metadata={"publishers": publishers, "events_per_publisher_per_batch": args.batch_size},
        )
        for limit in (1, 4, None):
            benchmark_limit: int | str = limit if limit is not None else "unbounded"
            bench_time_func(
                runner,
                f"async-contention/{publishers}/limit-{benchmark_limit}",
                _time_async_contention,
                publishers,
                args.batch_size,
                limit,
                inner_loops=publishers * args.batch_size,
                metadata={"publishers": publishers, "max_concurrency": benchmark_limit},
            )

    for capacity in (None, 128):
        capacity_name = capacity if capacity is not None else "unbounded"
        bench_time_func(
            runner,
            f"sync-memory-soak/{capacity_name}",
            _time_sync_memory,
            args.soak_operations,
            capacity,
            inner_loops=args.soak_operations,
            metadata={"history_capacity": capacity_name, "events": args.soak_operations},
        )
        bench_time_func(
            runner,
            f"async-memory-soak/{capacity_name}",
            _time_async_memory,
            args.soak_operations,
            capacity,
            inner_loops=args.soak_operations,
            metadata={"history_capacity": capacity_name, "events": args.soak_operations},
        )

    for count in args.schedule_counts:
        bench_time_func(
            runner,
            f"sync-periodic-schedules/{count}",
            _time_sync_schedules,
            count,
            inner_loops=count,
            metadata={"schedules": count},
        )
        bench_time_func(
            runner,
            f"async-periodic-schedules/{count}",
            _time_async_schedules,
            count,
            inner_loops=count,
            metadata={"schedules": count},
        )


if __name__ == "__main__":
    main()
