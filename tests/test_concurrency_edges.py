"""Edge cases for keyed concurrency and single-flight behavior."""

from __future__ import annotations

import asyncio
import unittest
from unittest.mock import patch

from _support import _AsyncValueMixin

from eventflow.concurrency import KeyedConcurrencyLimiter, KeyedRateLimiter, SingleFlight


class AsyncCoordinationEdgeTests(unittest.IsolatedAsyncioTestCase, _AsyncValueMixin):
    async def test_limiter_nowait_per_key_and_rate_key_capacity_pruning(self):
        limiter = KeyedConcurrencyLimiter[str](1)
        self.assertTrue(limiter.acquire_nowait("a"))
        self.assertTrue(limiter.acquire_nowait("b"))
        self.assertFalse(limiter.acquire_nowait("a"))
        self.assertEqual(limiter.key_stats("a").active, 1)
        await limiter.release("a")
        await limiter.release("b")
        await limiter.aclose()
        self.assertFalse(limiter.acquire_nowait("closed"))

        rate = KeyedRateLimiter[str](rate=100, period=0.1, burst=1, max_keys=1)
        loop = asyncio.get_running_loop()
        with patch.object(loop, "time", return_value=loop.time()):
            self.assertTrue(rate.try_acquire("a"))
            with self.assertRaises(RuntimeError):
                rate.try_acquire("b")
        await asyncio.sleep(0.02)
        self.assertTrue(rate.try_acquire("b"))
        self.assertGreaterEqual(rate.available("b"), 0)
        await asyncio.sleep(0.02)
        self.assertEqual(rate.prune(), 1)
        await rate.aclose()
        self.assertFalse(rate.try_acquire("closed"))

    async def test_single_flight_failure_is_shared_then_retried(self):
        flight = SingleFlight[str]()
        started = asyncio.Event()
        release = asyncio.Event()
        calls = 0

        async def fail_once():
            nonlocal calls
            calls += 1
            started.set()
            await release.wait()
            raise OSError("shared")

        first = asyncio.create_task(flight.run("key", fail_once))
        await started.wait()
        second = asyncio.create_task(flight.run("key", fail_once))
        await asyncio.sleep(0)
        release.set()
        for task in (first, second):
            with self.assertRaisesRegex(OSError, "shared"):
                await task
        self.assertEqual(calls, 1)
        self.assertEqual(flight.stats().active, 0)
        self.assertEqual(await flight.run("key", lambda: self._value(7)), 7)

    async def test_single_flight_closes_coroutine_when_task_creation_fails(self):
        flight = SingleFlight[str]()
        loop = asyncio.get_running_loop()
        rejected = []

        def reject_task(coroutine, *, name=None, context=None):
            rejected.append(coroutine)
            raise RuntimeError("task creation rejected")

        async def factory():
            return "ready"

        with patch.object(loop, "create_task", side_effect=reject_task):
            with self.assertRaisesRegex(RuntimeError, "creation rejected"):
                await flight.run("key", factory)

        self.assertEqual(len(rejected), 1)
        self.assertIsNone(rejected[0].cr_frame)
        self.assertEqual(flight.stats().active, 0)
        self.assertEqual(await flight.run("key", factory), "ready")
        await flight.aclose()
