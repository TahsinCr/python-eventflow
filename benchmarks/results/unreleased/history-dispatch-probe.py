"""Reproduce the paired async dispatch and history timing checks."""

import asyncio
import json
import tracemalloc
from time import perf_counter_ns

from eventflow import AsyncEventBus, EventFilter, MemoryEventHistory


async def noop(_):
    pass


async def dispatch(size, mode):
    async with AsyncEventBus() as bus:
        for n in range(size):
            target = (
                None if mode == "unfiltered" else int if mode == "mixed" and n % 64 == 0 else str
            )
            await bus.subscribe(noop, event_type=target)
        for _ in range(4):
            await bus.publish(1)
        values = []
        for _ in range(11):
            n = 500 if size >= 512 else 1500
            start = perf_counter_ns()
            for _ in range(n):
                if mode == "churn":
                    handle = await bus.subscribe(noop, event_type=str)
                await bus.publish(1)
                if mode == "churn":
                    await handle.cancel()
            values.append((perf_counter_ns() - start) / n)
        return values


async def main():
    results = {}
    for mode in ("miss", "mixed", "unfiltered", "churn"):
        for size in (8, 64, 512, 2048):
            results[f"{mode}/{size}"] = await dispatch(size, mode)
    for blocks in (None, 256):
        history = MemoryEventHistory(100_000) if blocks is None else None
        if blocks:
            try:
                history = MemoryEventHistory(100_000, block_size=blocks)
            except TypeError:
                continue
        assert history is not None
        for i in range(100_000):
            history.append(i)
        values = []
        for _ in range(11):
            start = perf_counter_ns()
            for i in range(100_000):
                history.append(i)
            values.append((perf_counter_ns() - start) / 100_000)
        results[f"append/{blocks}"] = values
    history = MemoryEventHistory(100_000)
    for event in range(100_000):
        history.append(event)
    history.query()
    for target in (int, str):
        event_filter = EventFilter(event_type=target)
        values = []
        for _ in range(11):
            start = perf_counter_ns()
            for _ in range(15):
                history.query(event_filter)
            values.append((perf_counter_ns() - start) / 15)
        tracemalloc.start()
        history.query(event_filter)
        _, peak_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        results[f"type-query/{target.__name__}"] = {"ns": values, "peak_bytes": peak_bytes}
    print(json.dumps(results))


asyncio.run(main())
