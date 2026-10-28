"""Run with python -m examples.integrated from the repository root."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import replace

from eventflow import (
    AsyncEventBus,
    AsyncPipeline,
    AsyncStream,
    AsyncSubscriptionScope,
    CallbackContext,
    CallbackSubscription,
    EventEnvelope,
    Pipeline,
    PipelineStats,
    correlation_context,
)


async def run_example() -> list[int]:
    """Compose both pipeline modes, callback hooks, a bounded stream and a bus."""
    stats = PipelineStats()
    prepare = Pipeline[EventEnvelope[int]](observer=stats)

    @prepare.stage("validate")
    def validate(envelope: EventEnvelope[int]) -> EventEnvelope[int]:
        if envelope.event < 0:
            raise ValueError("Reading must be non-negative")
        return envelope

    policy = CallbackSubscription(
        1,
        lambda: None,
        lambda envelope: replace(envelope, event=envelope.event + 1),
    )

    @policy.on_before
    def before(context: CallbackContext) -> None:
        assert context.event is not None

    prepare.add("increment", policy.invoke, after="validate")

    process = AsyncPipeline[EventEnvelope[int]](observer=stats)

    @process.stage("double")
    async def double(envelope: EventEnvelope[int]) -> EventEnvelope[int]:
        return replace(envelope, event=envelope.event * 2)

    @process.middleware("correlate")
    async def correlate(
        envelope: EventEnvelope[int],
        next_: Callable[[EventEnvelope[int]], Awaitable[EventEnvelope[int]]],
    ) -> EventEnvelope[int]:
        with correlation_context(envelope.correlation_id or envelope.id):
            return await next_(envelope)

    async def source() -> AsyncIterator[EventEnvelope[int]]:
        for value in range(6):
            yield EventEnvelope(value, correlation_id=f"request-{value}")

    output: list[int] = []
    async with AsyncEventBus[EventEnvelope[int]]() as bus, AsyncSubscriptionScope() as scope:

        async def collect(envelope: EventEnvelope[int]) -> None:
            output.append(envelope.event)

        scope.add(await bus.subscribe(collect))
        stream = (
            AsyncStream(source())
            .map(prepare.run)
            .map(process.run)
            .filter(lambda envelope: envelope.event % 4 == 0)
            .buffer(2)
            .batch(2)
        )
        async with stream:
            async for batch in stream:
                for envelope in batch:
                    await bus.publish(envelope)
    assert stats.snapshot.runs.successful == 12
    return output


if __name__ == "__main__":
    print(asyncio.run(run_example()))
