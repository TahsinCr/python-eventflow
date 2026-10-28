"""A bounded application-owned queue connects an async bus to a stream."""

import asyncio
from collections.abc import AsyncIterator
from typing import cast

from eventflow import AsyncEventBus, AsyncStream, AsyncSubscriptionScope, Pipeline


async def run_example() -> list[int]:
    """Forward bus publications through a stream without per-event tasks."""
    queue: asyncio.Queue[object] = asyncio.Queue(maxsize=2)
    end = object()
    pipeline = Pipeline[int]()
    pipeline.add("double", lambda value: value * 2)

    async def source() -> AsyncIterator[int]:
        while True:
            value = await queue.get()
            if value is end:
                return
            yield cast(int, value)

    output: list[int] = []
    async with AsyncEventBus[object]() as bus, AsyncSubscriptionScope() as scope:
        scope.add(await bus.subscribe(queue.put))

        async def publish() -> None:
            for value in range(4):
                await bus.publish(value)
            await bus.publish(end)

        publisher = asyncio.create_task(publish(), name="ExamplePublisher")
        try:
            async with AsyncStream(source()).map(pipeline.run).buffer(2) as stream:
                async for value in stream:
                    output.append(value)
            await publisher
        finally:
            publisher.cancel()
            await asyncio.gather(publisher, return_exceptions=True)
    return output


if __name__ == "__main__":
    print(asyncio.run(run_example()))
