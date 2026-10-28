import asyncio
import subprocess
import sys
import unittest
from dataclasses import replace
from pathlib import Path

from eventflow import (
    AsyncEventBus,
    AsyncPipeline,
    AsyncStream,
    AsyncSubscriptionScope,
    CallbackSubscription,
    EventBus,
    EventEnvelope,
    Pipeline,
    PipelineStats,
    SubscriptionScope,
    correlation_context,
)


class IntegrationTests(unittest.TestCase):
    def test_legacy_import_does_not_load_optional_execution_components(self):
        source = (
            "import sys, eventflow; "
            "assert 'eventflow.pipeline' not in sys.modules; "
            "assert 'eventflow.stream' not in sys.modules; "
            "assert 'eventflow.context' not in sys.modules; "
            "assert 'Pipeline' in dir(eventflow); "
            "assert eventflow.Pipeline is eventflow.Pipeline; "
            "assert 'eventflow.pipeline' in sys.modules"
        )
        subprocess.run(
            [sys.executable, "-c", source], check=True, cwd=Path(__file__).resolve().parents[1]
        )
        import eventflow

        with self.assertRaises(AttributeError):
            _ = eventflow.missing_export

    def test_bus_pipeline_envelope_scope_callback_policy_and_metrics(self):
        stats = PipelineStats()
        pipeline = Pipeline[EventEnvelope[int]](observer=stats)
        callback = CallbackSubscription(
            1, lambda: None, lambda envelope: replace(envelope, event=envelope.event + 1)
        )
        hooks = []

        @callback.on_before
        def before(context):
            hooks.append(context.event.id)

        pipeline.stage("policy")(callback.invoke)

        @pipeline.middleware("correlation")
        def correlate(envelope, next_):
            with correlation_context(envelope.correlation_id):
                return next_(envelope)

        output = []
        with EventBus[EventEnvelope[int]]() as bus, SubscriptionScope() as scope:
            scope.add(bus.subscribe(lambda event: output.append(pipeline.run(event))))
            event = EventEnvelope(1, correlation_id="request")
            bus.publish(event)
        self.assertEqual(output[0].event, 2)
        self.assertEqual(output[0].id, event.id)
        self.assertEqual(hooks, [event.id])
        self.assertEqual(stats.snapshot.runs.successful, 1)


class AsyncIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_documented_complete_example(self):
        from examples.integrated import run_example

        self.assertEqual(await run_example(), [4, 8, 12])
        from examples.bus_stream import run_example as from_bus

        self.assertEqual(await from_bus(), [0, 2, 4, 6])

    async def test_stream_pipeline_bus_scope_and_envelope_work_together(self):
        stats = PipelineStats()
        pipeline = AsyncPipeline[EventEnvelope[int]](observer=stats)

        @pipeline.stage("double")
        async def double(envelope):
            return replace(envelope, event=envelope.event * 2)

        @pipeline.middleware("correlation")
        async def correlate(envelope, next_):
            with correlation_context(envelope.correlation_id):
                return await next_(envelope)

        async def source():
            for value in range(6):
                yield EventEnvelope(value, correlation_id=f"request-{value}")

        output = []
        async with AsyncEventBus[EventEnvelope[int]]() as bus, AsyncSubscriptionScope() as scope:

            async def receive(envelope):
                output.append(envelope.event)

            handle = scope.add(await bus.subscribe(receive))
            stream = (
                AsyncStream(source())
                .map(pipeline.run)
                .filter(lambda envelope: envelope.event % 4 == 0)
                .buffer(2)
                .batch(2)
            )
            async with stream:
                async for batch in stream:
                    for envelope in batch:
                        await bus.publish(envelope)
        self.assertEqual(output, [0, 4, 8])
        self.assertFalse(handle.active)
        self.assertEqual(stats.snapshot.runs.calls, 6)
        self.assertFalse(
            [
                task
                for task in asyncio.all_tasks()
                if task.get_name().startswith("EventFlow") and not task.done()
            ]
        )
