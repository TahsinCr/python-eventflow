"""Compare replay responsiveness, channel partitioning and callback overhead."""

import asyncio
import inspect
import json
from time import perf_counter_ns

from eventflow import AsyncEventBus, AsyncEventEngine, EventBus, MemoryEventHistory


def noop(_):
    pass


async def noop_async(_):
    pass


async def replay_samples(*, partitioned=False, batch_size=None):
    capacity = 100_000
    loop = asyncio.get_running_loop()
    history = MemoryEventHistory(capacity, block_size=256)
    history.append(0)
    options = {} if batch_size is None else {"replay_scan_batch_size": batch_size}
    async with AsyncEventEngine() as engine:
        bus = AsyncEventBus(history=history, **options)
        engine.add("target", bus)
        other = AsyncEventBus(history=MemoryEventHistory(capacity, block_size=256))
        engine.add("other", other)
        destination = engine.channel("other" if partitioned else "target")
        for event in range(1, capacity):
            await destination.publish(event)
        rows = []
        received = []

        async def receive(event):
            received.append(event)

        for _ in range(20):
            received.clear()
            tick = loop.create_future()
            start = perf_counter_ns()
            loop.call_soon(
                lambda tick=tick, start=start: tick.set_result(perf_counter_ns() - start)
            )
            handle = await engine.channel("target").subscribe(
                receive, replay=1, predicate=lambda event: event == 0
            )
            elapsed = perf_counter_ns() - start
            assert received == [0]
            rows.append({"replay_ns": elapsed, "loop_delay_ns": await tick})
            await handle.cancel()
        return rows


async def fanout_samples():
    subscribers = 2048
    rounds = 100
    rows = []
    with EventBus() as sync_bus:
        async with AsyncEventBus() as async_bus:
            for _ in range(subscribers):
                sync_bus.subscribe(noop)
                await async_bus.subscribe(noop_async)
            await async_bus.publish(0)
            sync_bus.publish(0)
            for _ in range(9):
                start = perf_counter_ns()
                for _ in range(rounds):
                    for _ in range(subscribers):
                        await noop_async(0)
                direct_async = (perf_counter_ns() - start) / rounds
                start = perf_counter_ns()
                for _ in range(rounds):
                    await async_bus.publish(0)
                async_publish = (perf_counter_ns() - start) / rounds
                start = perf_counter_ns()
                for _ in range(rounds):
                    for _ in range(subscribers):
                        noop(0)
                direct_sync = (perf_counter_ns() - start) / rounds
                start = perf_counter_ns()
                for _ in range(rounds):
                    sync_bus.publish(0)
                rows.append(
                    {
                        "direct_async_ns": direct_async,
                        "async_publish_ns": async_publish,
                        "direct_sync_ns": direct_sync,
                        "sync_publish_ns": (perf_counter_ns() - start) / rounds,
                    }
                )
    return rows


async def main():
    results = {
        "shared": await replay_samples(),
        "partitioned": await replay_samples(partitioned=True),
        "fanout": await fanout_samples(),
    }
    if "replay_scan_batch_size" in inspect.signature(AsyncEventBus).parameters:
        results["cooperative"] = await replay_samples(batch_size=512)
    print(json.dumps(results))


asyncio.run(main())
