"""Measure publish lock acquisition and async loop delay during replay."""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from time import perf_counter_ns
from types import SimpleNamespace
from unittest.mock import patch

import eventflow.event_bus as bus_module
from eventflow import AsyncEventBus, EventBus, MemoryEventHistory

from ._runner import benchmark_metadata
from ._support import ignore_event, ignore_event_async, positive_int


class TimedLock:
    """Record only the first acquisition inside a sampled publish call."""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.sample = threading.local()

    def acquire(self, *args, **kwargs):
        if getattr(self.sample, "first_ns", None) != -1:
            return self.lock.acquire(*args, **kwargs)
        started = perf_counter_ns()
        acquired = self.lock.acquire(*args, **kwargs)
        self.sample.first_ns = perf_counter_ns() - started
        return acquired

    def __enter__(self):
        return self.acquire()

    def __exit__(self, *_):
        self.lock.release()

    def __getattr__(self, name: str):
        return getattr(self.lock, name)


def history_for(capacity: int) -> MemoryEventHistory[int]:
    history = MemoryEventHistory[int](capacity=capacity)
    for event in range(capacity):
        history.append(event)
    return history


def threaded_rounds(capacity: int, rounds: int, replayers: int, instrument: bool) -> list[dict]:
    history = history_for(capacity)
    timing = TimedLock() if instrument else None
    if timing is None:
        bus = EventBus[int](history=history)
    else:
        factory = SimpleNamespace(RLock=lambda: timing, Condition=threading.Condition)
        # Install before construction, so both native and Python dispatch use this lock.
        with patch.object(bus_module, "threading", factory):
            bus = EventBus[int](history=history)

    def publish(barrier: threading.Barrier) -> tuple[int, int | None]:
        barrier.wait(timeout=30)
        if timing is not None:
            timing.sample.first_ns = -1
        start = perf_counter_ns()
        bus.publish(capacity)
        elapsed = perf_counter_ns() - start
        wait = timing.sample.first_ns if timing is not None else None
        if wait is not None and wait < 0:
            raise AssertionError("publish did not use the instrumented lock")
        return elapsed, wait

    def replay(barrier: threading.Barrier) -> int:
        barrier.wait(timeout=30)
        start = perf_counter_ns()
        handle = bus.subscribe(ignore_event, predicate=lambda event: event >= 0, replay=10)
        elapsed = perf_counter_ns() - start
        handle.cancel()
        return elapsed

    rows = []
    with bus, ThreadPoolExecutor(replayers + 2) as pool:
        for _ in range(rounds):
            history.append(capacity)
            barrier = threading.Barrier(replayers + 2)
            publishers = [pool.submit(publish, barrier) for _ in range(2)]
            subscribers = [pool.submit(replay, barrier) for _ in range(replayers)]
            rows.append(
                {
                    "publisher": [task.result(timeout=30) for task in publishers],
                    "replay_ns": [task.result(timeout=30) for task in subscribers],
                }
            )
    return rows


async def async_rounds(capacity: int, rounds: int, cold: bool) -> list[int]:
    history = history_for(capacity)
    loop = asyncio.get_running_loop()
    delays: list[int] = []
    async with AsyncEventBus[int](history=history) as bus:
        for _ in range(rounds):
            if cold:
                await bus.publish(capacity)
            else:
                history.query()
            start = perf_counter_ns()
            loop.call_soon(lambda start=start: delays.append(perf_counter_ns() - start))
            handle = await bus.subscribe(
                ignore_event_async, predicate=lambda event: event >= 0, replay=10
            )
            await asyncio.sleep(0)
            await handle.cancel()
    return delays


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--capacity", nargs="+", type=positive_int, default=[1000, 100_000, 1_000_000]
    )
    parser.add_argument("--rounds", type=positive_int, default=24)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = {"metadata": benchmark_metadata(), "python": platform.python_version(), "profiles": []}
    for capacity in args.capacity:
        for replayers in (0, 1, 2, 4, 8):
            for instrument in (False, True):
                result["profiles"].append(
                    {
                        "capacity": capacity,
                        "replayers": replayers,
                        "instrumented": instrument,
                        "rounds": threaded_rounds(capacity, args.rounds, replayers, instrument),
                    }
                )
        for cold in (False, True):
            result["profiles"].append(
                {
                    "capacity": capacity,
                    "cold": cold,
                    "loop_delay_ns": asyncio.run(async_rounds(capacity, args.rounds, cold)),
                }
            )
    args.output.write_text(json.dumps(result, separators=(",", ":")) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
