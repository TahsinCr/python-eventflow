from __future__ import annotations

import asyncio
import threading
import unittest

from eventflow.subscription import AsyncSubscription, Subscription


class SubscriptionTests(unittest.TestCase):
    def test_cancel_can_retry_after_removal_failure(self) -> None:
        attempts = 0

        def remove() -> None:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("removal failed")

        subscription = Subscription(1, remove)
        with self.assertRaisesRegex(RuntimeError, "removal failed"):
            subscription.cancel()

        self.assertTrue(subscription.active)
        subscription.cancel()
        subscription.cancel()
        self.assertFalse(subscription.active)
        self.assertEqual(attempts, 2)

    def test_concurrent_and_reentrant_cancel_do_not_duplicate_removal(self) -> None:
        entered, release = threading.Event(), threading.Event()
        attempts = 0
        errors: list[BaseException] = []
        holder: dict[str, Subscription] = {}

        def remove() -> None:
            nonlocal attempts
            attempts += 1
            holder["subscription"].cancel()
            entered.set()
            release.wait(timeout=2)
            raise RuntimeError("removal failed")

        subscription = Subscription(1, remove)
        holder["subscription"] = subscription

        def cancel() -> None:
            try:
                subscription.cancel()
            except BaseException as error:
                errors.append(error)

        worker = threading.Thread(target=cancel)
        worker.start()
        self.assertTrue(entered.wait(timeout=2))
        subscription.cancel()
        release.set()
        worker.join(timeout=2)

        self.assertFalse(worker.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertEqual(attempts, 1)
        self.assertTrue(subscription.active)


class AsyncSubscriptionTests(unittest.IsolatedAsyncioTestCase):
    async def test_cancel_can_retry_after_removal_failure(self) -> None:
        attempts = 0

        async def remove() -> None:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("removal failed")

        subscription = AsyncSubscription(1, remove)
        with self.assertRaisesRegex(RuntimeError, "removal failed"):
            await subscription.cancel()

        self.assertTrue(subscription.active)
        await subscription.cancel()
        await subscription.cancel()
        self.assertFalse(subscription.active)
        self.assertEqual(attempts, 2)

    async def test_concurrent_cancel_does_not_duplicate_inflight_removal(self) -> None:
        entered = asyncio.Event()
        release = asyncio.Event()
        attempts = 0

        async def remove() -> None:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                entered.set()
                await release.wait()
                raise RuntimeError("removal failed")

        subscription = AsyncSubscription(1, remove)
        first = asyncio.create_task(subscription.cancel())
        await entered.wait()
        second = asyncio.create_task(subscription.cancel())

        await second
        release.set()
        with self.assertRaisesRegex(RuntimeError, "removal failed"):
            await first

        self.assertTrue(subscription.active)
        self.assertEqual(attempts, 1)
        await subscription.cancel()
        self.assertFalse(subscription.active)
        self.assertEqual(attempts, 2)
