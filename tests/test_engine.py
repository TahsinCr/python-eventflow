from __future__ import annotations

import asyncio
import unittest
from unittest.mock import patch

from eventflow import (
    AsyncEventBus,
    AsyncEventEngine,
    EventBus,
    EventBusClosedError,
    EventEngine,
)
from eventflow.compatibility import ExceptionGroup


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

    def test_engine_can_create_new_channels_after_stop(self) -> None:
        engine = EventEngine()
        previous = engine.channel("events")
        engine.stop()

        reopened = engine.channel("events")
        self.assertIsNot(previous, reopened)
        self.assertFalse(reopened.closed)
        self.assertTrue(engine.running)
        engine.stop()

    def test_engine_forwards_typed_subscription_options(self) -> None:
        engine = EventEngine(history=3)
        received: list[int] = []
        replayed_once: list[int] = []
        engine.publish("values", 1)

        subscription = engine.subscribe(
            "values",
            received.append,
            event_type=int,
            times=2,
            replay=1,
        )
        engine.publish("values", 2)
        engine.publish("values", 3)
        engine.once("values", replayed_once.append, event_type=int, replay=1)

        self.assertEqual(received, [1, 2])
        self.assertFalse(subscription.active)
        self.assertEqual(replayed_once, [3])
        engine.stop()

    def test_stop_closes_every_channel_and_retains_failed_close_for_retry(self) -> None:
        engine = EventEngine()
        failing = EventBus[int]()
        healthy = EventBus[int]()
        engine.add("failing", failing)
        engine.add("healthy", healthy)

        with patch.object(failing, "close", side_effect=RuntimeError("close failed")):
            with patch.object(healthy, "close", wraps=healthy.close) as healthy_close:
                with self.assertRaises(ExceptionGroup) as raised:
                    engine.stop()
                self.assertEqual(healthy_close.call_count, 1)
                self.assertEqual(len(raised.exception.exceptions), 1)

        self.assertEqual(engine.channel_names, ())
        engine.stop()
        self.assertTrue(failing.closed)

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
        reopened = engine.channel("telemetry")
        self.assertFalse(reopened.closed)
        self.assertTrue(engine.running)
        await engine.stop()

    async def test_async_engine_forwards_typed_subscription_options(self) -> None:
        engine = AsyncEventEngine(history=3)
        received: list[int] = []
        replayed_once: list[int] = []
        await engine.publish("values", 1)

        async def receive(event: int) -> None:
            received.append(event)

        async def receive_once(event: int) -> None:
            replayed_once.append(event)

        subscription = await engine.subscribe(
            "values",
            receive,
            event_type=int,
            times=2,
            replay=1,
        )
        await engine.publish("values", 2)
        await engine.publish("values", 3)
        await engine.once("values", receive_once, event_type=int, replay=1)

        self.assertEqual(received, [1, 2])
        self.assertFalse(subscription.active)
        self.assertEqual(replayed_once, [3])
        await engine.stop()

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

    async def test_async_stop_closes_every_channel_and_retains_failure(self) -> None:
        engine = AsyncEventEngine()
        failing = AsyncEventBus[int]()
        healthy = AsyncEventBus[int]()
        engine.add("failing", failing)
        engine.add("healthy", healthy)

        with patch.object(failing, "close", side_effect=RuntimeError("close failed")):
            with patch.object(healthy, "close", wraps=healthy.close) as healthy_close:
                with self.assertRaises(ExceptionGroup) as raised:
                    await engine.stop()
                self.assertEqual(healthy_close.await_count, 1)
                self.assertEqual(len(raised.exception.exceptions), 1)

        self.assertEqual(engine.channel_names, ())
        await engine.stop()
        self.assertTrue(failing.closed)

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
