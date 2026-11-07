import asyncio
import threading
import unittest
from unittest.mock import patch

from eventflow import AsyncEventBus, EventBus
from eventflow.scopes import AsyncSubscriptionScope, ResourceScope, SubscriptionScope
from eventflow.compatibility import ExceptionGroup
from eventflow.subscription import AsyncSubscription


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
    def test_concurrent_scope_close_retries_failed_subscription_removal(self):
        bus = EventBus[int]()
        subscription = bus.subscribe(lambda _event: None)
        scope = SubscriptionScope()
        scope.add(subscription)
        removal_started = threading.Event()
        release_removal = threading.Event()
        second_cancel_started = threading.Event()
        attempts = 0
        first_errors = []
        original_cancel = subscription._cancel
        original_handle_cancel = subscription.cancel

        def remove():
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                removal_started.set()
                if not release_removal.wait(1.0):
                    raise TimeoutError("test removal was not released")
                raise RuntimeError("first removal failed")
            original_cancel()

        def observed_cancel():
            second_cancel_started.set()
            original_handle_cancel()

        def cancel_first_time():
            try:
                subscription.cancel()
            except RuntimeError as exc:
                first_errors.append(exc)

        with (
            patch.object(subscription, "_cancel", side_effect=remove),
            patch.object(subscription, "cancel", side_effect=observed_cancel),
        ):
            first = threading.Thread(target=cancel_first_time)
            first.start()
            self.assertTrue(removal_started.wait(1.0))
            closing = threading.Thread(target=scope.close)
            closing.start()
            self.assertTrue(second_cancel_started.wait(1.0))
            closing.join(timeout=0.05)
            self.assertTrue(closing.is_alive())
            release_removal.set()
            first.join(timeout=1.0)
            closing.join(timeout=1.0)

        self.assertFalse(first.is_alive())
        self.assertFalse(closing.is_alive())
        self.assertEqual(len(first_errors), 1)
        self.assertEqual(attempts, 2)
        self.assertFalse(subscription.active)
        self.assertEqual(bus.subscriber_count, 0)
        bus.close()

    def test_reentrant_subscription_cancel_does_not_deadlock(self):
        bus = EventBus[int]()
        subscription = bus.subscribe(lambda _event: None)
        original_cancel = subscription._cancel
        nested_cancel_completed = threading.Event()

        def reentrant_remove():
            subscription.cancel()
            nested_cancel_completed.set()
            original_cancel()

        completed = threading.Event()
        with patch.object(subscription, "_cancel", side_effect=reentrant_remove):
            worker = threading.Thread(
                target=lambda: (subscription.cancel(), completed.set()),
                daemon=True,
            )
            worker.start()
            self.assertTrue(completed.wait(1.0))
            worker.join(timeout=1.0)

        self.assertFalse(worker.is_alive())
        self.assertTrue(nested_cancel_completed.is_set())
        self.assertEqual(bus.subscriber_count, 0)
        bus.close()

    def test_base_exception_still_attempts_remaining_handles_and_keeps_failures(self):
        class Abort(BaseException):
            pass

        calls = []

        class Interrupt:
            def cancel(self):
                calls.append("interrupt")
                raise Abort("interrupt")

        scope = SubscriptionScope()
        scope.add(Handle(calls, "good"))
        scope.add(Handle(calls, "bad", True))
        scope.add(Interrupt())
        with self.assertRaises(Abort) as caught:
            scope.close()
        self.assertEqual(calls, ["interrupt", "bad", "good"])
        self.assertIsInstance(caught.exception.__cause__, ExceptionGroup)

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
    async def test_handle_cancellation_retains_other_cleanup_errors_as_cause(self):
        calls = []
        scope = AsyncSubscriptionScope()
        scope.add(AsyncHandle(calls, "good"))
        scope.add(AsyncHandle(calls, "bad", True))

        class Cancelled:
            async def cancel(self):
                calls.append("cancelled")
                raise asyncio.CancelledError

        scope.add(Cancelled())
        with self.assertRaises(asyncio.CancelledError) as caught:
            await scope.aclose()
        self.assertEqual(calls, ["cancelled", "bad", "good"])
        self.assertIsInstance(caught.exception.__cause__, ExceptionGroup)

    async def test_async_handles_and_failures(self):
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

    async def test_sync_cancel_method_returning_awaitable_is_supported(self):
        calls = []

        class FactoryHandle:
            def cancel(self):
                async def finish():
                    calls.append("cancelled")

                return finish()

        scope = AsyncSubscriptionScope()
        handle = FactoryHandle()
        self.assertIs(scope.add(handle), handle)
        await scope.aclose()
        self.assertEqual(calls, ["cancelled"])

    async def test_nonawaitable_async_cancel_result_has_clear_error(self):
        class InvalidHandle:
            def cancel(self):
                return None

        scope = AsyncSubscriptionScope()
        scope.add(InvalidHandle())
        with self.assertRaises(ExceptionGroup) as caught:
            await scope.aclose()
        self.assertRegex(str(caught.exception.exceptions[0]), "must return an awaitable")

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

    async def test_scope_waits_for_external_subscription_cancellation_and_retains_failure(self):
        entered = asyncio.Event()
        attempts = 0

        async def remove():
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                entered.set()
                await asyncio.Event().wait()

        handle = AsyncSubscription(1, remove)
        scope = AsyncSubscriptionScope()
        scope.add(handle)
        external = asyncio.create_task(handle.cancel())
        await entered.wait()
        closing = asyncio.create_task(scope.aclose())
        await asyncio.sleep(0)
        self.assertFalse(closing.done())

        external.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await external
        with self.assertRaises(asyncio.CancelledError):
            await closing
        self.assertTrue(handle.active)

        await scope.aclose()
        self.assertFalse(handle.active)
        self.assertEqual(attempts, 2)


class ResourceScopeEdgeTests(unittest.TestCase):
    def test_sync_scope_duplicate_borrowed_and_missing_detach(self):
        calls = []

        class Resource:
            def close(self):
                calls.append("closed")

        resource = Resource()
        scope = ResourceScope()
        self.assertIs(scope.add(resource), resource)
        scope.add(resource)
        self.assertIs(scope.detach(resource), resource)
        scope.add(resource)
        scope.add(resource)
        with self.assertRaises(KeyError):
            scope.detach(object())
        scope.close()
        scope.close()
        self.assertEqual(calls, ["closed"])
        with self.assertRaises(RuntimeError):
            scope.add(Resource())
        with self.assertRaises(TypeError):
            ResourceScope().add(Resource(), close="missing")
        with self.assertRaises(TypeError):
            ResourceScope().defer(object())
