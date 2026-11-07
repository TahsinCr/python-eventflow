"""Measure bridge round-trip throughput with multiple synchronous publishers.

Run with ``python -m benchmarks.benchmark_bridge_publishers``. Each publisher
keeps at most one event in flight, so the bounded async queue measures handoff
contention without dropping events.
"""

from __future__ import annotations

import argparse
import asyncio
import math
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from statistics import median
from time import perf_counter_ns, process_time_ns
from typing import NamedTuple, TypedDict

from eventflow import EventBus
from eventflow.bridges import AsyncEventQueue
from eventflow.errors import EventQueueClosedError

from ._measure import report


class _BridgeEvent(NamedTuple):
    publisher: int
    sequence: int
    acknowledgement: Future[None]


class _RoundtripLatency(TypedDict):
    p50: float
    p95: float
    p99: float
    max: float


class _Sample(TypedDict):
    elapsed_seconds: float
    cpu_seconds: float
    cpu_utilization_percent: float
    delivered_events: int
    throughput_ops_per_second: float
    roundtrip_latency_us: _RoundtripLatency


def _positive_int(value: str) -> int:
    try:
        result = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("value must be a positive integer") from error
    if result < 1:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return result


def _percentile(values: list[int], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)] / 1_000


async def _sample(publishers: int, iterations: int) -> _Sample:
    latencies: list[list[int]] = [[] for _ in range(publishers)]
    consumer_ready = asyncio.Event()
    start_barrier = threading.Barrier(publishers + 1)
    start_publishers = threading.Event()
    delivered = 0

    with EventBus[_BridgeEvent]() as bus:
        queue = AsyncEventQueue(bus, capacity=publishers)
        consumer: asyncio.Task[None] | None = None

        async def consume() -> None:
            nonlocal delivered
            consumer_ready.set()
            try:
                while True:
                    event = await queue.receive()
                    delivered += 1
                    if not event.acknowledgement.done():
                        event.acknowledgement.set_result(None)
            except EventQueueClosedError:
                return

        def publish(publisher: int) -> None:
            start_barrier.wait(timeout=10)
            if not start_publishers.wait(timeout=10):
                raise TimeoutError("bridge benchmark publishers were not released")
            for sequence in range(iterations):
                acknowledgement: Future[None] = Future()
                started = perf_counter_ns()
                result = bus.publish(_BridgeEvent(publisher, sequence, acknowledgement))
                if result.failed:
                    error = (
                        result.errors[0] if result.errors else RuntimeError("queue delivery failed")
                    )
                    raise RuntimeError("bridge benchmark event was not accepted") from error
                acknowledgement.result(timeout=20)
                latencies[publisher].append(perf_counter_ns() - started)

        try:
            async with queue:
                consumer = asyncio.create_task(consume())
                await consumer_ready.wait()
                with ThreadPoolExecutor(
                    max_workers=publishers,
                    thread_name_prefix="EventFlowPublisherBenchmark",
                ) as executor:
                    workers = [
                        executor.submit(publish, publisher) for publisher in range(publishers)
                    ]
                    start_barrier.wait(timeout=10)
                    started = perf_counter_ns()
                    cpu_started = process_time_ns()
                    start_publishers.set()
                    outcomes = await asyncio.gather(
                        *(asyncio.wrap_future(worker) for worker in workers),
                        return_exceptions=True,
                    )
                    elapsed_seconds = (perf_counter_ns() - started) / 1_000_000_000
                    cpu_seconds = (process_time_ns() - cpu_started) / 1_000_000_000
                    for outcome in outcomes:
                        if isinstance(outcome, BaseException):
                            raise RuntimeError("bridge benchmark publisher failed") from outcome
        finally:
            if consumer is not None:
                await consumer

    operation_count = publishers * iterations
    all_latencies = [latency for worker_latencies in latencies for latency in worker_latencies]
    if delivered != operation_count or len(all_latencies) != operation_count:
        raise RuntimeError("bridge benchmark did not deliver every published event")

    return {
        "elapsed_seconds": elapsed_seconds,
        "cpu_seconds": cpu_seconds,
        "cpu_utilization_percent": cpu_seconds / elapsed_seconds * 100,
        "delivered_events": delivered,
        "throughput_ops_per_second": operation_count / elapsed_seconds,
        "roundtrip_latency_us": {
            "p50": _percentile(all_latencies, 0.50),
            "p95": _percentile(all_latencies, 0.95),
            "p99": _percentile(all_latencies, 0.99),
            "max": max(all_latencies) / 1_000,
        },
    }


async def _run(
    publisher_counts: list[int], iterations: int, repeats: int, output: str | None
) -> None:
    results: list[dict[str, object]] = []
    for publishers in publisher_counts:
        samples = [await _sample(publishers, iterations) for _ in range(repeats)]
        results.append(
            {
                "publishers": publishers,
                "iterations_per_publisher": iterations,
                "operations_per_sample": publishers * iterations,
                "repeats": repeats,
                "median_cpu_utilization_percent": median(
                    sample["cpu_utilization_percent"] for sample in samples
                ),
                "median_throughput_ops_per_second": median(
                    sample["throughput_ops_per_second"] for sample in samples
                ),
                "samples": samples,
            }
        )
    report(results, output)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--publishers", type=_positive_int, nargs="+", default=[2, 4, 8, 16])
    parser.add_argument("--iterations", type=_positive_int, default=1_000)
    parser.add_argument("--repeats", type=_positive_int, default=5)
    parser.add_argument("--output")
    args = parser.parse_args()
    if len(set(args.publishers)) != len(args.publishers):
        parser.error("publisher counts must be unique")
    asyncio.run(_run(args.publishers, args.iterations, args.repeats, args.output))


if __name__ == "__main__":
    main()
