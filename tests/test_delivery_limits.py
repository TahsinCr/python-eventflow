from __future__ import annotations

import asyncio
import unittest

from eventflow import AsyncEventBus, AsyncEventEngine, DeliveryMode


class AsyncDeliveryLimitTests(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_bus_delivery_respects_configured_limit(self) -> None:
        await self._assert_delivery_limit(
            AsyncEventBus[int](
                delivery_mode=DeliveryMode.CONCURRENT,
                max_concurrency=3,
            ),
            subscribe=lambda bus, callback: bus.subscribe(callback),
            publish=lambda bus, event: bus.publish(event),
            close=lambda bus: bus.close(),
            limit=3,
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

    async def _assert_delivery_limit(
        self, bus, *, subscribe, publish, close, limit, publish_count=1
    ):
        active: dict[int, int] = {}
        peak: dict[int, int] = {}
        delivered = batches_started = 0
        first_batches = asyncio.Event()
        release = asyncio.Event()

        async def callback(event: int) -> None:
            nonlocal delivered, batches_started
            active[event] = active.get(event, 0) + 1
            peak[event] = max(peak.get(event, 0), active[event])
            if active[event] == limit:
                batches_started += 1
                if batches_started == publish_count:
                    first_batches.set()
            await release.wait()
            active[event] -= 1
            delivered += 1

        for _ in range(limit * 3 + 1):
            await subscribe(bus, callback)
        publishing = [
            asyncio.create_task(publish(bus, event))
            for event in range(publish_count)
        ]
        await asyncio.wait_for(first_batches.wait(), timeout=2)
        self.assertEqual(peak, {event: limit for event in range(publish_count)})
        release.set()
        results = await asyncio.gather(*publishing)

        self.assertEqual(delivered, publish_count * (limit * 3 + 1))
        self.assertEqual(peak, {event: limit for event in range(publish_count)})
        self.assertEqual(sum(result.delivered for result in results), delivered)
        await close(bus)
