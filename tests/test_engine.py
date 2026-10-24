from __future__ import annotations

import asyncio
import unittest

from eventflow import (
    AsyncEventBus,
    AsyncEventEngine,
    EventBus,
    EventBusClosedError,
    EventEngine,
)


class EventEngineTests(unittest.TestCase):
    def test_named_channels_are_lazy_normalized_and_reusable(self) -> None:
        engine = EventEngine(history=2)
        received: list[int] = []

        engine.subscribe(" Telemetry ", received.append)
        result = engine.publish("telemetry", 4)
        engine.publish("telemetry", 5)

        self.assertEqual(received, [4, 5])
        self.assertEqual(result.delivered, 1)
        self.assertEqual(engine.channel_names, ("telemetry",))
        self.assertEqual(engine.channel("telemetry").query(), (4, 5))

    def test_custom_channel_remove_and_engine_stop_own_lifecycles(self) -> None:
        engine = EventEngine()
        custom = EventBus[str](history=1)
        engine.add("commands", custom)

        self.assertIs(engine.channel("commands"), custom)
        self.assertTrue(engine.remove("commands"))
        self.assertTrue(custom.closed)
        self.assertFalse(engine.remove("commands"))

        recreated = engine.channel("commands")
        engine.stop()
        self.assertTrue(recreated.closed)
        self.assertEqual(engine.channel_names, ())

    def test_validation_context_manager_and_direct_actions(self) -> None:
        before: list[int] = []
        with EventEngine(on_before=before.append) as engine:
            state = engine.channel("state")
            engine.publish("state", 3)
            self.assertEqual(before, [3])
            with self.assertRaises(ValueError):
                engine.add("state", EventBus())
            with self.assertRaises(ValueError):
                engine.channel("   ")
            with self.assertRaises(TypeError):
                engine.add("invalid", object())  # type: ignore[arg-type]
        self.assertFalse(engine.running)
        with self.assertRaises(EventBusClosedError):
            state.publish(4)


class AsyncEventEngineTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_named_channel_publish_once_and_stop(self) -> None:
        engine = AsyncEventEngine(history=2)
        received: list[int] = []

        async def receive(event: int) -> None:
            received.append(event)

        await engine.once("telemetry", receive)
        await engine.publish("telemetry", 1)
        await engine.publish("telemetry", 2)

        self.assertEqual(received, [1])
        self.assertEqual(engine.channel("telemetry").query(), (1, 2))
        await engine.stop()
        self.assertEqual(engine.channel_names, ())

    async def test_async_periodic_channel_is_owned_by_engine(self) -> None:
        engine = AsyncEventEngine()
        completed = asyncio.Event()
        received: list[str] = []

        async def receive(event: str) -> None:
            received.append(event)
            if len(received) == 2:
                completed.set()

        await engine.subscribe("heartbeat", receive)
        schedule = await engine.publish_every(
            "heartbeat",
            "tick",
            0.001,
            times=2,
        )
        await asyncio.wait_for(completed.wait(), timeout=1.0)
        await engine.stop()

        self.assertEqual(received, ["tick", "tick"])
        self.assertFalse(schedule.active)

    async def test_async_engine_validation_remove_and_context_manager(self) -> None:
        async with AsyncEventEngine() as engine:
            custom = engine.channel("custom")
            self.assertTrue(await engine.remove("custom"))
            self.assertFalse(await engine.remove("custom"))
            self.assertTrue(custom.closed)
            with self.assertRaises(ValueError):
                engine.add(" ", AsyncEventBus())
            with self.assertRaises(TypeError):
                engine.add("invalid", EventBus())  # type: ignore[arg-type]
        self.assertFalse(engine.running)
