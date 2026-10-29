from __future__ import annotations

import asyncio
import unittest

from eventflow import AsyncEventBus, AsyncEventEngine, DeliveryMode, ErrorPolicy
from eventflow.compatibility import ExceptionGroup


class AsyncDeliveryLimitTests(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_bus_delivery_respects_configured_limit(self) -> None:
        await self._assert_delivery_limit(
            AsyncEventBus[int](
                delivery_mode=DeliveryMode.CONCURRENT,
                max_concurrency=4,
            ),
            subscribe=lambda bus, callback: bus.subscribe(callback),
            publish=lambda bus, event: bus.publish(event),
            close=lambda bus: bus.close(),
            limit=4,
            publish_count=2,
        )

    async def test_engine_applies_limit_to_created_channels(self) -> None:
        engine = AsyncEventEngine(
            delivery_mode=DeliveryMode.CONCURRENT,
            max_concurrency=2,
        )
        await self._assert_delivery_limit(
            engine.channel("events"),
            subscribe=lambda bus, callback: bus.subscribe(callback),
            publish=lambda bus, event: bus.publish(event),
            close=lambda _bus: engine.stop(),
            limit=2,
        )

    async def test_concurrency_limit_requires_a_positive_integer(self) -> None:
        for value in (True, 0, -1, 1.5):
            with self.subTest(value=value), self.assertRaises(ValueError):
                AsyncEventBus[int](
                    delivery_mode=DeliveryMode.CONCURRENT,
                    max_concurrency=value,
                )

    async def test_concurrency_limit_requires_concurrent_delivery_mode(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires DeliveryMode.CONCURRENT"):
            AsyncEventBus[int](max_concurrency=2)
        with self.assertRaisesRegex(ValueError, "requires DeliveryMode.CONCURRENT"):
            AsyncEventEngine(max_concurrency=2)

    async def test_bounded_publish_allows_recursive_publication(self) -> None:
        async with AsyncEventBus[int](
            delivery_mode=DeliveryMode.CONCURRENT,
            max_concurrency=1,
        ) as bus:
            delivered: list[int] = []

            async def callback(event: int) -> None:
                if event == 1:
                    await bus.publish(2)
                else:
                    delivered.append(event)

            await bus.subscribe(callback)
            result = await asyncio.wait_for(bus.publish(1), timeout=1)

        self.assertEqual(result.delivered, 1)
        self.assertEqual(delivered, [2])

    async def test_cancelling_bounded_publish_finishes_active_callbacks(self) -> None:
        bus = AsyncEventBus[int](
            delivery_mode=DeliveryMode.CONCURRENT,
            max_concurrency=1,
        )
        entered = asyncio.Event()
        finished = asyncio.Event()
        active = 0
        calls: list[int] = []

        async def callback(event: int) -> None:
            nonlocal active
            calls.append(event)
            active += 1
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                active -= 1
                finished.set()

        for _ in range(3):
            await bus.subscribe(callback)

        tasks_before = asyncio.all_tasks()
        publishing = asyncio.create_task(bus.publish(1))
        await asyncio.wait_for(entered.wait(), timeout=1)
        publishing.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await publishing
        await bus.close()
        await asyncio.sleep(0)

        self.assertTrue(finished.is_set())
        self.assertEqual(active, 0)
        self.assertEqual(calls, [1])
        self.assertFalse(
            [task for task in asyncio.all_tasks() if task not in tasks_before and not task.done()]
        )

    async def test_bounded_delivery_reports_errors_after_all_batches(self) -> None:
        reported = []
        after = []

        async def on_error(context) -> None:
            reported.append(context.error)

        async def on_after(_event, result) -> None:
            after.append(result)

        bus = AsyncEventBus[int](
            error_policy=ErrorPolicy.RAISE,
            delivery_mode=DeliveryMode.CONCURRENT,
            max_concurrency=1,
            on_error=on_error,
            on_after=on_after,
        )
        async with bus:
            calls = []

            async def fail(_event: int) -> None:
                calls.append("fail")
                raise ValueError("callback failed")

            async def receive(_event: int) -> None:
                calls.append("receive")

            await bus.subscribe(fail)
            await bus.subscribe(receive)
            await bus.subscribe(receive)
            with self.assertRaises(ExceptionGroup):
                await bus.publish(1)

            self.assertEqual(calls, ["fail", "receive", "receive"])
            self.assertEqual(len(reported), 1)
            self.assertIsInstance(reported[0], ValueError)
            self.assertEqual(len(after), 1)
            self.assertEqual((after[0].matched, after[0].delivered, after[0].failed), (3, 2, 1))
            self.assertEqual((bus.stats.delivered, bus.stats.failed), (2, 1))

    async def _assert_delivery_limit(
        self, bus, *, subscribe, publish, close, limit, publish_count=1
    ):
        active: dict[int, int] = {}
        peak: dict[int, int] = {}
        active_total = peak_total = 0
        delivered = batches_started = 0
        first_batches = asyncio.Event()
        release = asyncio.Event()

        async def callback(event: int) -> None:
            nonlocal active_total, peak_total, delivered, batches_started
            active[event] = active.get(event, 0) + 1
            peak[event] = max(peak.get(event, 0), active[event])
            active_total += 1
            peak_total = max(peak_total, active_total)
            if active[event] == limit:
                batches_started += 1
                if batches_started == publish_count:
                    first_batches.set()
            await release.wait()
            active[event] -= 1
            active_total -= 1
            delivered += 1

        for _ in range(limit * 3 + 1):
            await subscribe(bus, callback)
        publishing = [asyncio.create_task(publish(bus, event)) for event in range(publish_count)]
        await asyncio.wait_for(first_batches.wait(), timeout=2)
        self.assertEqual(peak, {event: limit for event in range(publish_count)})
        release.set()
        results = await asyncio.gather(*publishing)

        self.assertEqual(delivered, publish_count * (limit * 3 + 1))
        self.assertEqual(peak, {event: limit for event in range(publish_count)})
        if publish_count > 1:
            self.assertEqual(peak_total, limit * publish_count)
        self.assertEqual(sum(result.delivered for result in results), delivered)
        await close(bus)
