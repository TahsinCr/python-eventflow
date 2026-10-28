import asyncio
import unittest

from eventflow import AsyncEventBus, EventBus
from eventflow.scopes import AsyncSubscriptionScope, SubscriptionScope
from eventflow.compatibility import ExceptionGroup


class Handle:
    def __init__(self, calls, name, fail=False):
        self.calls, self.name, self.fail = calls, name, fail

    def cancel(self):
        self.calls.append(self.name)
        if self.fail:
            raise ValueError(self.name)


class AsyncHandle(Handle):
    async def cancel(self):
        super().cancel()


class ScopeTests(unittest.TestCase):
    def test_old_handles_nested_scopes_and_idempotence(self):
        seen = []
        with EventBus[int]() as bus, SubscriptionScope() as outer:
            handle = bus.subscribe(seen.append)
            self.assertIs(outer.add(handle), handle)
            outer.add(handle)
            with SubscriptionScope() as inner:
                inner.add(bus.subscribe(seen.append))
                bus.publish(1)
            bus.publish(2)
        self.assertEqual(seen, [1, 1, 2])
        outer.close()
        self.assertFalse(handle.active)
        with self.assertRaises(RuntimeError):
            outer.add(handle)

    def test_failure_does_not_skip_other_handles_and_can_retry(self):
        calls = []
        scope = SubscriptionScope()
        a, b = Handle(calls, "a"), Handle(calls, "b", fail=True)
        scope.add(a)
        scope.add(b)
        with self.assertRaises(ExceptionGroup):
            scope.close()
        self.assertEqual(calls, ["b", "a"])
        b.fail = False
        scope.close()
        scope.close()
        self.assertEqual(calls, ["b", "a", "b"])
        with self.assertRaises(TypeError):
            SubscriptionScope().add(object())
        with self.assertRaises(TypeError):
            SubscriptionScope().add(AsyncHandle([], "async"))

    def test_runtime_awaitable_from_sync_handle_is_closed(self):
        created = []

        class Wrong:
            def cancel(self):
                async def operation():
                    pass

                result = operation()
                created.append(result)
                return result

        scope = SubscriptionScope()
        scope.add(Wrong())
        with self.assertRaises(ExceptionGroup):
            scope.close()
        self.assertIsNone(created[0].cr_frame)


class AsyncScopeTests(unittest.IsolatedAsyncioTestCase):
    async def test_old_async_handles_and_failures(self):
        calls = []
        async with AsyncEventBus[int]() as bus, AsyncSubscriptionScope() as scope:

            async def receive(value):
                calls.append(value)

            handle = await bus.subscribe(receive)
            self.assertIs(scope.add(handle), handle)
            scope.add(handle)
            await bus.publish(1)
        await scope.aclose()
        self.assertFalse(handle.active)
        self.assertEqual(calls, [1])
        with self.assertRaises(RuntimeError):
            scope.add(handle)
        with self.assertRaises(TypeError):
            AsyncSubscriptionScope().add(object())
        failed = AsyncSubscriptionScope()
        bad, good = AsyncHandle(calls, "bad", True), AsyncHandle(calls, "good")
        failed.add(good)
        failed.add(bad)
        with self.assertRaises(ExceptionGroup):
            await failed.aclose()
        self.assertEqual(calls[-2:], ["bad", "good"])
        bad.fail = False
        await failed.aclose()

    async def test_repeated_caller_cancellation_finishes_cleanup(self):
        entered, release, completed = asyncio.Event(), asyncio.Event(), asyncio.Event()
        calls = []

        class Blocking:
            async def cancel(self):
                entered.set()
                await release.wait()
                completed.set()

        scope = AsyncSubscriptionScope()
        scope.add(AsyncHandle(calls, "last"))
        scope.add(Blocking())
        closing = asyncio.create_task(scope.aclose())
        await entered.wait()
        closing.cancel()
        await asyncio.sleep(0)
        closing.cancel()
        other = asyncio.create_task(scope.aclose())
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await closing
        await other
        self.assertTrue(completed.is_set())
        self.assertEqual(calls, ["last"])

    async def test_handle_owned_cancellation_and_reentrant_close_do_not_skip_cleanup(self):
        calls = []
        scope = AsyncSubscriptionScope()
        scope.add(AsyncHandle(calls, "good"))

        class Cancelled:
            async def cancel(self):
                raise asyncio.CancelledError

        scope.add(Cancelled())
        with self.assertRaises(asyncio.CancelledError):
            await scope.aclose()
        self.assertEqual(calls, ["good"])
        recursive = AsyncSubscriptionScope()

        class Reentrant:
            async def cancel(self):
                await recursive.aclose()

        recursive.add(Reentrant())
        with self.assertRaises(ExceptionGroup):
            await asyncio.wait_for(recursive.aclose(), 1)
