"""Benchmark keyed coalescer updates, merges, flushes and worker teardown."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from time import perf_counter

from eventflow import KeyedCoalescer

from ._runner import bench_time_func, create_runner, forward_cli_arguments
from ._support import positive_int


def _sample(
    key_count: int,
    *,
    producers: int,
    updates_per_key: int,
    flush_callers: int,
) -> float:
    delivered = 0

    def callback(_value: tuple[int, int]) -> None:
        nonlocal delivered
        delivered += 1

    coalescer = KeyedCoalescer(
        interval=3600,
        key=lambda item: item[0],
        merge=lambda first, second: (first[0], first[1] + second[1]),
        callback=callback,
        max_pending_keys=key_count,
    )
    started = perf_counter()
    try:
        if producers == 1:
            for key in range(key_count):
                for _ in range(updates_per_key):
                    coalescer.push((key, 1))
        else:
            barrier = threading.Barrier(producers + 1)

            def push_partition(producer: int) -> None:
                barrier.wait(timeout=10)
                for key in range(producer, key_count, producers):
                    for _ in range(updates_per_key):
                        coalescer.push((key, 1))

            with ThreadPoolExecutor(max_workers=producers) as executor:
                futures = [executor.submit(push_partition, index) for index in range(producers)]
                barrier.wait(timeout=10)
                for future in futures:
                    future.result(timeout=60)

        if flush_callers == 1:
            coalescer.flush(timeout=60)
        else:
            barrier = threading.Barrier(flush_callers + 1)

            def flush_pending() -> None:
                barrier.wait(timeout=10)
                coalescer.flush(timeout=60)

            with ThreadPoolExecutor(max_workers=flush_callers) as executor:
                futures = [executor.submit(flush_pending) for _ in range(flush_callers)]
                barrier.wait(timeout=10)
                for future in futures:
                    future.result(timeout=60)

        stats = coalescer.stats()
        expected_merges = key_count * (updates_per_key - 1)
        if delivered != key_count or stats.merged != expected_merges:
            raise RuntimeError("coalescer benchmark lost or duplicated work")
    finally:
        coalescer.close(flush=False, timeout=60)
    return perf_counter() - started


def _time_coalescer(
    loops: int,
    key_count: int,
    producers: int,
    updates_per_key: int,
    flush_callers: int,
) -> float:
    elapsed = 0.0
    for _ in range(loops):
        elapsed += _sample(
            key_count,
            producers=producers,
            updates_per_key=updates_per_key,
            flush_callers=flush_callers,
        )
    return elapsed


def main() -> None:
    runner = create_runner(
        __doc__ or "Keyed coalescer benchmarks",
        add_cmdline_args=forward_cli_arguments("key_counts", "producers", "merge_updates"),
    )
    runner.argparser.add_argument(
        "--key-counts", type=positive_int, nargs="+", default=[128, 1024, 4096, 16384]
    )
    runner.argparser.add_argument("--producers", type=positive_int, default=4)
    runner.argparser.add_argument("--merge-updates", type=positive_int, default=8)
    args = runner.parse_args()
    if len(set(args.key_counts)) != len(args.key_counts):
        runner.argparser.error("key counts must be unique")

    for key_count in args.key_counts:
        cases = (
            ("single-producer", 1, 1, 1),
            ("multi-producer", min(args.producers, key_count), 1, 1),
            ("merge-heavy", 1, args.merge_updates, 1),
            ("concurrent-flush", 1, 1, 2),
        )
        for name, producers, updates, flush_callers in cases:
            bench_time_func(
                runner,
                f"{name}/{key_count}",
                _time_coalescer,
                key_count,
                producers,
                updates,
                flush_callers,
                inner_loops=key_count * updates,
                metadata={
                    "keys": key_count,
                    "producers": producers,
                    "updates_per_key": updates,
                    "flush_callers": flush_callers,
                },
            )


if __name__ == "__main__":
    main()
