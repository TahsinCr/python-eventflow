"""Measure concurrent publishing, retained memory and periodic schedule resources.

Run from the repository root: ``python -m benchmarks.benchmark_runtime``.
Results are local workload measurements, not deployment guarantees.
"""

# pyright: strict

from __future__ import annotations

import argparse
import asyncio
import gc
import threading
import tracemalloc
from concurrent.futures import ThreadPoolExecutor
from statistics import median
from time import perf_counter

from eventflow import AsyncEventBus, DeliveryMode, EventBus

from ._measure import report

_SUBSCRIBERS = 4
_SCHEDULE_INTERVAL = 3600.0
_PAYLOAD_BYTES = 4096


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


def _positive(value: str) -> int:
    try:
        result = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("value must be a positive integer") from exc
    if result <= 0:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return result


def _sync_contention(publishers: int, iterations: int, samples: int) -> dict[str, object]:
    barrier = threading.Barrier(publishers)
    active_threads: list[int] = []

    def receive(_event: int) -> None:
        return None

    with EventBus[int]() as bus:
        bus.subscribe(receive)

        def publish_many(_publisher: int) -> None:
            barrier.wait()
            active_threads.append(threading.active_count())
            for _ in range(iterations):
                bus.publish(1)

        with ThreadPoolExecutor(max_workers=publishers) as executor:
            timings: list[float] = []
            for _ in range(samples):
                started = perf_counter()
                list(executor.map(publish_many, range(publishers)))
                timings.append(perf_counter() - started)

    operations = publishers * iterations
    elapsed = median(timings)
    return {
        "case": f"sync-publisher-contention/{publishers}",
        "publishers": publishers,
        "operations": operations,
        "samples": samples,
        "sample_seconds": timings,
        "elapsed_seconds": elapsed,
        "operations_per_second": operations / elapsed,
        "peak_active_threads": max(active_threads, default=1),
    }


async def _async_contention(
    publishers: int, iterations: int, limit: int | None, samples: int
) -> dict[str, object]:
    async def receive(_event: int) -> None:
        await asyncio.sleep(0)

    async with AsyncEventBus[int](
        delivery_mode=DeliveryMode.CONCURRENT,
        max_concurrency=limit,
    ) as bus:
        for _ in range(_SUBSCRIBERS):
            await bus.subscribe(receive)

        async def publish_many() -> None:
            for _ in range(iterations):
                await bus.publish(1)

        timings: list[float] = []
        for _ in range(samples):
            started = perf_counter()
            await asyncio.gather(*(publish_many() for _ in range(publishers)))
            timings.append(perf_counter() - started)

    operations = publishers * iterations
    elapsed = median(timings)
    return {
        "case": f"async-publisher-contention/{publishers}/max-concurrency={limit}",
        "publishers": publishers,
        "max_concurrency": limit,
        "subscribers_per_publish": _SUBSCRIBERS,
        "operations": operations,
        "samples": samples,
        "sample_seconds": timings,
        "elapsed_seconds": elapsed,
        "operations_per_second": operations / elapsed,
    }


async def _async_resource_sample(publishers: int, limit: int | None) -> dict[str, object]:
    started_callbacks = 0
    all_started = asyncio.Event()
    release = asyncio.Event()
    expected = publishers * min(_SUBSCRIBERS, limit or _SUBSCRIBERS)

    async def wait_for_release(_event: int) -> None:
        nonlocal started_callbacks
        started_callbacks += 1
        if started_callbacks == expected:
            all_started.set()
        await release.wait()

    async with AsyncEventBus[int](
        delivery_mode=DeliveryMode.CONCURRENT,
        max_concurrency=limit,
    ) as bus:
        for _ in range(_SUBSCRIBERS):
            await bus.subscribe(wait_for_release)

        tracemalloc.start()
        tasks = [asyncio.create_task(bus.publish(1)) for _ in range(publishers)]
        try:
            await asyncio.wait_for(all_started.wait(), timeout=10)
            peak_tasks = len(asyncio.all_tasks())
            peak_traced_bytes = tracemalloc.get_traced_memory()[1]
        finally:
            release.set()
            if any(not task.done() for task in tasks):
                await asyncio.gather(*tasks, return_exceptions=True)
            tracemalloc.stop()

    return {
        "case": f"async-live-resources/{publishers}/max-concurrency={limit}",
        "publishers": publishers,
        "max_concurrency": limit,
        "subscribers_per_publish": _SUBSCRIBERS,
        "peak_live_tasks_including_caller": peak_tasks,
        "peak_traced_bytes": peak_traced_bytes,
    }


def _memory_sync(operations: int, history_capacity: int | None) -> dict[str, object]:
    midpoint = max(1, operations // 2)
    bus: EventBus[bytes] = EventBus(history=history_capacity)

    def receive(_event: bytes) -> None:
        pass

    bus.subscribe(receive)
    tracemalloc.start()
    after_half = tracemalloc.get_traced_memory()[0]
    started = perf_counter()
    try:
        for index in range(operations):
            bus.publish(f"{index}:".encode() + b"x" * _PAYLOAD_BYTES)
            if index + 1 == midpoint:
                gc.collect()
                after_half = tracemalloc.get_traced_memory()[0]
        gc.collect()
        end_bytes, peak_bytes = tracemalloc.get_traced_memory()
    finally:
        bus.close()
        tracemalloc.stop()
    return {
        "case": f"sync-memory-soak/history={history_capacity or 0}",
        "operations": operations,
        "history_capacity": history_capacity,
        "traced_bytes_after_half": after_half,
        "traced_bytes_at_end": end_bytes,
        "late_growth_bytes": end_bytes - after_half,
        "peak_traced_bytes": peak_bytes,
        "elapsed_seconds": perf_counter() - started,
    }


async def _memory_async(operations: int, history_capacity: int | None) -> dict[str, object]:
    midpoint = max(1, operations // 2)
    bus: AsyncEventBus[bytes] = AsyncEventBus(history=history_capacity)

    async def receive(_event: bytes) -> None:
        pass

    await bus.subscribe(receive)
    tracemalloc.start()
    after_half = tracemalloc.get_traced_memory()[0]
    started = perf_counter()
    try:
        for index in range(operations):
            await bus.publish(f"{index}:".encode() + b"x" * _PAYLOAD_BYTES)
            if index + 1 == midpoint:
                gc.collect()
                after_half = tracemalloc.get_traced_memory()[0]
        gc.collect()
        end_bytes, peak_bytes = tracemalloc.get_traced_memory()
    finally:
        await bus.close()
        tracemalloc.stop()
    return {
        "case": f"async-memory-soak/history={history_capacity or 0}",
        "operations": operations,
        "history_capacity": history_capacity,
        "traced_bytes_after_half": after_half,
        "traced_bytes_at_end": end_bytes,
        "late_growth_bytes": end_bytes - after_half,
        "peak_traced_bytes": peak_bytes,
        "elapsed_seconds": perf_counter() - started,
    }


def _sync_schedules(count: int, samples: int) -> dict[str, object]:
    timings: list[float] = []
    live_thread_counts: list[int] = []
    remaining_thread_counts: list[int] = []
    for _ in range(samples):
        baseline_threads = threading.active_count()
        with EventBus[int]() as bus:
            started = perf_counter()
            handles = [
                bus.publish_every(index, _SCHEDULE_INTERVAL, immediately=False)
                for index in range(count)
            ]
            timings.append(perf_counter() - started)
            live_thread_counts.append(threading.active_count() - baseline_threads)
            for handle in handles:
                handle.cancel()
        remaining_thread_counts.append(threading.active_count() - baseline_threads)
    return {
        "case": f"sync-periodic-schedules/{count}",
        "schedules": count,
        "samples": samples,
        "registration_seconds_median": median(timings),
        "live_schedule_threads_peak": max(live_thread_counts, default=0),
        "remaining_threads_after_close_peak": max(0, max(remaining_thread_counts, default=0)),
    }


async def _async_schedules(count: int, samples: int) -> dict[str, object]:
    timings: list[float] = []
    live_task_counts: list[int] = []
    remaining_task_counts: list[int] = []
    for _ in range(samples):
        async with AsyncEventBus[int]() as bus:
            baseline_tasks = len(asyncio.all_tasks())
            started = perf_counter()
            handles = [
                await bus.publish_every(index, _SCHEDULE_INTERVAL, immediately=False)
                for index in range(count)
            ]
            timings.append(perf_counter() - started)
            await asyncio.sleep(0)
            live_task_counts.append(len(asyncio.all_tasks()) - baseline_tasks)
            await asyncio.gather(*(handle.cancel() for handle in handles))
            await asyncio.sleep(0)
            remaining_task_counts.append(len(asyncio.all_tasks()) - baseline_tasks)
    return {
        "case": f"async-periodic-schedules/{count}",
        "schedules": count,
        "samples": samples,
        "registration_seconds_median": median(timings),
        "live_schedule_tasks_peak": max(live_task_counts, default=0),
        "remaining_tasks_after_cancel_peak": max(0, max(remaining_task_counts, default=0)),
    }


async def _run_async(
    results: list[dict[str, object]],
    iterations: int,
    samples: int,
    soak_operations: int,
    publisher_counts: tuple[int, ...],
    schedule_counts: tuple[int, ...],
) -> None:
    for publishers in publisher_counts:
        for limit in (1, 4, None):
            results.append(await _async_contention(publishers, iterations, limit, samples))
            results.append(await _async_resource_sample(publishers, limit))
    for capacity in (None, 128):
        results.append(await _memory_async(soak_operations, capacity))
    for count in schedule_counts:
        results.append(await _async_schedules(count, samples))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--iterations", type=_positive, default=500)
    parser.add_argument("--samples", type=_positive, default=5)
    parser.add_argument("--soak-operations", type=_positive, default=100000)
    parser.add_argument("--publisher-counts", type=_counts, default=(1, 4, 16))
    parser.add_argument("--schedule-counts", type=_counts, default=(1, 10, 50))
    parser.add_argument("--output")
    args = parser.parse_args()

    results: list[dict[str, object]] = [
        _sync_contention(publishers, args.iterations, args.samples)
        for publishers in args.publisher_counts
    ]
    for capacity in (None, 128):
        results.append(_memory_sync(args.soak_operations, capacity))
    for count in args.schedule_counts:
        results.append(_sync_schedules(count, args.samples))

    asyncio.run(
        _run_async(
            results,
            args.iterations,
            args.samples,
            args.soak_operations,
            args.publisher_counts,
            args.schedule_counts,
        )
    )
    report(results, args.output)


if __name__ == "__main__":
    main()
