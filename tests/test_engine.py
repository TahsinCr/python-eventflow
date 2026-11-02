from __future__ import annotations

import asyncio
import gc
import threading
import unittest
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from unittest.mock import patch
from weakref import ref

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

    def test_custom_bus_cannot_be_registered_under_multiple_channels(self) -> None:
        engine = EventEngine()
        custom = EventBus[str]()
        engine.add("first", custom)

        with self.assertRaisesRegex(ValueError, "only one channel"):
            engine.add("second", custom)

        close_started = threading.Event()
        close_release = threading.Event()
        original_close = custom.close

        def blocked_close() -> None:
            close_started.set()
            if not close_release.wait(1.0):
                raise TimeoutError("test close was not released")
            original_close()

        with patch.object(custom, "close", side_effect=blocked_close):
            with ThreadPoolExecutor(max_workers=1) as executor:
                removing = executor.submit(engine.remove, "first")
                self.assertTrue(close_started.wait(1.0))
                with self.assertRaisesRegex(ValueError, "only one channel"):
                    engine.add("second", custom)
                close_release.set()
                self.assertTrue(removing.result(timeout=1.0))

    def test_bus_has_one_owner_across_concurrent_engine_registration(self) -> None:
        bus = EventBus[int]()
        engines = (EventEngine(), EventEngine())
        barrier = threading.Barrier(len(engines))
        results: list[bool] = []
        results_lock = threading.Lock()

        def register(engine: EventEngine) -> None:
            barrier.wait(timeout=1.0)
            try:
                engine.add("shared", bus)
            except ValueError:
                accepted = False
            else:
                accepted = True
            with results_lock:
                results.append(accepted)

        threads = [threading.Thread(target=register, args=(engine,)) for engine in engines]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=2.0)

        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(results.count(True), 1)
        self.assertEqual(results.count(False), 1)
        self.assertEqual(sum(bool(engine.channel_names) for engine in engines), 1)
        for engine in engines:
            engine.stop()
        self.assertTrue(bus.closed)

    def test_bus_engine_ownership_does_not_keep_engine_alive(self) -> None:
        bus = EventBus[int]()
        engine = EventEngine()
        engine.add("events", bus)
        engine_ref = ref(engine)

        del engine
        gc.collect()

        self.assertIsNone(engine_ref())
        replacement = EventEngine()
        replacement.add("events", bus)
        replacement.stop()

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

    def test_engine_validates_history_and_thread_executor_at_construction(self) -> None:
        for invalid in (True, 1.5, "10"):
            with self.subTest(history=invalid), self.assertRaises(TypeError):
                EventEngine(history=invalid)  # type: ignore[arg-type]
            with self.subTest(async_history=invalid), self.assertRaises(TypeError):
                AsyncEventEngine(history=invalid)  # type: ignore[arg-type]
        for invalid in (0, -1):
            with self.subTest(history=invalid), self.assertRaises(ValueError):
                EventEngine(history=invalid)
            with self.subTest(async_history=invalid), self.assertRaises(ValueError):
                AsyncEventEngine(history=invalid)

        with ProcessPoolExecutor(max_workers=1) as executor:
            with self.assertRaisesRegex(TypeError, "ThreadPoolExecutor"):
                EventEngine(executor=executor)  # type: ignore[arg-type]

    def test_engine_rejects_closed_bus(self) -> None:
        engine = EventEngine()
        bus = EventBus[int]()
        bus.close()
        with self.assertRaises(EventBusClosedError):
            engine.add("closed", bus)

    def test_shared_executor_reentrancy_across_engine_channels_does_not_deadlock(self) -> None:
        executor = ThreadPoolExecutor(max_workers=1)
        engine = EventEngine(executor=executor)
        received: list[int] = []
        engine.subscribe("second", received.append)
        engine.subscribe("first", lambda value: engine.publish("second", value))
        results = []
        publisher = threading.Thread(
            target=lambda: results.append(engine.publish("first", 42)),
            daemon=True,
        )

        publisher.start()
        publisher.join(timeout=1.0)
        if publisher.is_alive():
            executor.shutdown(wait=False, cancel_futures=True)
            publisher.join(timeout=1.0)
        self.assertFalse(publisher.is_alive(), "cross-channel publish deadlocked")
        executor.shutdown(wait=True)
        engine.stop()

        self.assertEqual(received, [42])
        self.assertTrue(results[0].successful)


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

    async def test_async_engine_rejects_closed_bus(self) -> None:
        engine = AsyncEventEngine()
        bus = AsyncEventBus[int]()
        await bus.close()
        with self.assertRaises(EventBusClosedError):
            engine.add("closed", bus)

    async def test_async_bus_cannot_be_registered_under_multiple_channels(self) -> None:
        engine = AsyncEventEngine()
        custom = AsyncEventBus[int]()
        engine.add("first", custom)

        with self.assertRaisesRegex(ValueError, "only one channel"):
            engine.add("second", custom)

        close_started = asyncio.Event()
        close_release = asyncio.Event()
        original_close = custom.close

        async def blocked_close() -> None:
            close_started.set()
            await close_release.wait()
            await original_close()

        with patch.object(custom, "close", new=blocked_close):
            removing = asyncio.create_task(engine.remove("first"))
            await asyncio.wait_for(close_started.wait(), 1.0)
            with self.assertRaisesRegex(ValueError, "only one channel"):
                engine.add("second", custom)
            close_release.set()
            self.assertTrue(await asyncio.wait_for(removing, 1.0))

    async def test_async_bus_has_one_owner_across_engine_instances(self) -> None:
        bus = AsyncEventBus[int]()
        engines = (AsyncEventEngine(), AsyncEventEngine())
        barrier = threading.Barrier(len(engines))
        results: list[bool] = []
        results_lock = threading.Lock()

        def register(engine: AsyncEventEngine) -> None:
            barrier.wait(timeout=1.0)
            try:
                engine.add("shared", bus)
            except ValueError:
                accepted = False
            else:
                accepted = True
            with results_lock:
                results.append(accepted)

        threads = [threading.Thread(target=register, args=(engine,)) for engine in engines]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=2.0)

        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(results.count(True), 1)
        self.assertEqual(results.count(False), 1)
        self.assertEqual(sum(bool(engine.channel_names) for engine in engines), 1)
        for engine in engines:
            await engine.stop()
        self.assertTrue(bus.closed)
