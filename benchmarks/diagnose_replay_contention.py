"""Collect raw replay, history and concurrent publication latency samples."""

import argparse
import csv
import json
import os
import platform
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from itertools import count, product
from pathlib import Path
from sys import getsizeof
from threading import Barrier
from time import perf_counter_ns

from eventflow import EventBus, EventFilter, MemoryEventHistory

from ._runner import benchmark_metadata
from ._support import ignore_event, positive_int


class MeasuredHistory(MemoryEventHistory[int]):
    query_ns = 0
    snapshot_bytes = 0

    def query(
        self, event_filter: EventFilter[int] | None = None, *, limit: int | None = None
    ) -> tuple[int, ...]:
        start = perf_counter_ns()
        result = super().query(event_filter=event_filter, limit=limit)
        self.query_ns = perf_counter_ns() - start
        self.snapshot_bytes = getsizeof(result)
        return result


def collect(capacity: int, rounds: int, publishers: int) -> Iterator[dict[str, int]]:
    history = MeasuredHistory(capacity=capacity)
    for event in range(capacity):
        history.append(event)
    events = count(capacity)
    barrier = Barrier(publishers + 1)
    with EventBus[int](history=history) as bus, ThreadPoolExecutor(publishers) as pool:

        def publish(event: int) -> tuple[int, int]:
            barrier.wait(timeout=10)
            started = perf_counter_ns()
            with bus._lock:
                lock_wait = perf_counter_ns() - started
            started = perf_counter_ns()
            bus.publish(event)
            return lock_wait, perf_counter_ns() - started

        for per_thousand, replay, cold_every in product(
            (0, 1, 10, 100, 1000), (1, 10, 100, 1000), (0, 10, 1)
        ):

            def matches(event: int, rate: int = per_thousand) -> bool:
                return event % 1000 < rate

            for index in range(rounds):
                if not cold_every or index % cold_every:
                    history.query()
                tasks = [pool.submit(publish, next(events)) for _ in range(publishers)]
                barrier.wait(timeout=10)
                start = perf_counter_ns()
                subscription = bus.subscribe(ignore_event, replay=replay, predicate=matches)
                replay_ns = perf_counter_ns() - start
                query_ns, snapshot_bytes = history.query_ns, history.snapshot_bytes
                subscription.cancel()
                for publisher, task in enumerate(tasks):
                    lock_wait_ns, publish_ns = task.result(timeout=10)
                    yield {
                        "capacity": capacity,
                        "matches_per_thousand": per_thousand,
                        "replay": replay,
                        "cold_every": cold_every,
                        "round": index,
                        "publisher": publisher,
                        "replay_ns": replay_ns,
                        "snapshot_ns": query_ns,
                        "snapshot_bytes": snapshot_bytes,
                        "probe_wait_ns": lock_wait_ns,
                        "publish_ns": publish_ns,
                    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--capacity", nargs="+", type=positive_int, default=[1000, 10000, 100000, 1000000]
    )
    parser.add_argument("--rounds", type=positive_int, default=20)
    parser.add_argument("--publishers", type=positive_int, default=2)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.suffix != ".csv":
        parser.error("--output must end in .csv")
    metadata = benchmark_metadata()
    cpu_affinity = getattr(os, "sched_getaffinity", None)
    type_query_ns = {}
    int_filter = EventFilter[int](event_type=int)
    for capacity in args.capacity:
        history = MemoryEventHistory[int](capacity=capacity)
        for event in range(capacity):
            history.append(event)
        history.query()
        samples = []
        for _ in range(args.rounds):
            start = perf_counter_ns()
            history.query(int_filter)
            samples.append(perf_counter_ns() - start)
        type_query_ns[capacity] = samples
    with args.output.open("w", newline="", encoding="utf-8") as output:
        rows = (
            row
            for capacity in args.capacity
            for row in collect(capacity, args.rounds, args.publishers)
        )
        first = next(rows)
        writer = csv.DictWriter(output, fieldnames=list(first))
        writer.writeheader()
        writer.writerow(first)
        writer.writerows(rows)
    args.output.with_suffix(".json").write_text(
        json.dumps(
            {
                "metadata": metadata,
                "python": platform.python_version(),
                "platform": platform.platform(),
                "cpu_affinity": sorted(cpu_affinity(0)) if cpu_affinity else None,
                "publishers": args.publishers,
                "type_query_ns": type_query_ns,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
