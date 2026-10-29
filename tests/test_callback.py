import asyncio
import unittest
import time
import warnings
from unittest.mock import patch

from eventflow import CallbackSubscription, CallbackTimeoutError


class CallbackTests(unittest.TestCase):
    def test_hook_failures_do_not_skip_hooks_or_hide_callback_failure(self):
        from eventflow.compatibility import ExceptionGroup

        calls = []

        def fail(event):
            raise ValueError("callback")

        def after(context):
            raise RuntimeError("cleanup")

        sub = CallbackSubscription(1, lambda: None, fail)
        sub.on_after(after)
        sub.on_after(lambda context: calls.append("second after"))
        sub.on_error(lambda context: calls.append(str(context.error)))
        with self.assertRaises(ExceptionGroup) as caught:
            sub.invoke(1)
        self.assertEqual(
            [str(error) for error in caught.exception.exceptions], ["callback", "cleanup"]
        )
        self.assertEqual(calls, ["callback", "second after", "cleanup"])

    def test_success_hook_error_reaches_local_error_hook(self):
        calls = []

        def fail(context):
            raise ValueError("success")

        sub = CallbackSubscription(1, lambda: None, lambda event: 42, on_success=fail)
        sub.on_success(lambda context: calls.append("second success"))
        sub.on_error(lambda context: calls.append(str(context.error)))
        with self.assertRaises(ValueError):
            sub.invoke(1)
        self.assertEqual(calls, ["second success", "success"])

    def test_filter_rate_enable_count_and_hooks(self):
        calls, hooks = [], []
        subscription = CallbackSubscription(
            1,
            lambda: None,
            calls.append,
            max_calls=2,
            frequency_hz=2,
            enabled=False,
            predicate=lambda item: item > 0,
            on_before=lambda ctx: hooks.append("before"),
            on_success=lambda ctx: hooks.append("success"),
            on_after=lambda ctx: hooks.append("after"),
        )
        with patch.object(time, "monotonic", return_value=10):
            subscription.invoke(1)
            subscription.enable()
            subscription.invoke(-1)
            subscription.invoke(1)
            subscription.invoke(2)
        with patch.object(time, "monotonic", return_value=11):
            subscription.invoke(3)
            subscription.invoke(4)
        self.assertEqual(calls, [1, 3])
        self.assertFalse(subscription.active)
        self.assertEqual(hooks, ["before", "success", "after"] * 2)

    def test_error_and_observational_sync_timeout(self):
        hooks = []
        sub = CallbackSubscription(1, lambda: None, lambda item: 1 / 0)

        @sub.on_error
        def error(context):
            hooks.append(type(context.error))

        sub.on_after(lambda ctx: hooks.append("after"))
        with self.assertRaises(ZeroDivisionError):
            sub.invoke(1)
        self.assertEqual(hooks, [ZeroDivisionError, "after"])
        timeout = CallbackSubscription(
            2,
            lambda: None,
            lambda item: None,
            timeout=1,
            on_timeout=lambda ctx: hooks.append("timeout"),
        )
        with patch.object(time, "monotonic", side_effect=[0, 0, 2, 2]):
            with self.assertRaises(TimeoutError):
                timeout.invoke(1)
        self.assertEqual(hooks[-1], "timeout")

    def test_invalid_options(self):
        for options in (
            {"once": True, "max_calls": 1},
            {"frequency_hz": 0},
            {"timeout": float("inf")},
            {"max_calls": True},
        ):
            with self.assertRaises(ValueError):
                CallbackSubscription(1, lambda: None, lambda event: None, **options)


class AsyncCallbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_success_and_after_errors_are_preserved(self):
        from eventflow.compatibility import ExceptionGroup

        calls = []

        async def callback(event):
            return 1

        async def success(context):
            raise ValueError("success")

        async def after(context):
            raise RuntimeError("after")

        async def error(context):
            calls.append(str(context.error))

        sub = CallbackSubscription(
            1,
            lambda: None,
            callback,
            asynchronous=True,
            on_success=success,
            on_after=after,
            on_error=error,
        )
        with self.assertRaises(ExceptionGroup) as caught:
            await sub.invoke_async(1)
        self.assertEqual(len(caught.exception.exceptions), 2)
        self.assertEqual(calls, ["success", "after"])

    async def test_async_timeout_and_after(self):
        hooks = []

        async def callback(event):
            await asyncio.Event().wait()

        sub = CallbackSubscription(1, lambda: None, callback, asynchronous=True, timeout=0.01)

        @sub.on_timeout
        async def timeout(context):
            hooks.append("timeout")

        @sub.on_after
        async def after(context):
            hooks.append("after")

        with self.assertRaises(CallbackTimeoutError):
            await sub.invoke_async(1)
        self.assertEqual(hooks, ["timeout", "after"])

    async def test_user_timeout_error_uses_error_hook(self):
        hooks = []

        async def callback(_event):
            raise TimeoutError("domain timeout")

        sub = CallbackSubscription(1, lambda: None, callback, asynchronous=True, timeout=1)

        @sub.on_error
        async def error(_context):
            hooks.append("error")

        with self.assertRaisesRegex(TimeoutError, "domain timeout"):
            await sub.invoke_async(1)
        self.assertEqual(hooks, ["error"])

    async def test_cancelling_timed_invocation_finishes_child_cleanup(self):
        started, cleaning, release, cleaned = (asyncio.Event() for _ in range(4))
        callback_tasks = []

        async def callback(_event):
            callback_tasks.append(asyncio.current_task())
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleaning.set()
                await release.wait()
                cleaned.set()

        subscription = CallbackSubscription(
            1, lambda: None, callback, asynchronous=True, timeout=60.0
        )
        running = asyncio.create_task(subscription.invoke_async("event"))
        await asyncio.wait_for(started.wait(), 1.0)

        try:
            running.cancel()
            await asyncio.wait_for(cleaning.wait(), 1.0)
            self.assertFalse(running.done())
            running.cancel()
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await running
            self.assertTrue(cleaned.is_set())
            self.assertTrue(callback_tasks[0].done())
        finally:
            release.set()
            if not running.done():
                running.cancel()
            for task in callback_tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(running, *callback_tasks, return_exceptions=True)

    def test_sync_hooks_reject_and_close_coroutines(self):
        subscription = CallbackSubscription(1, lambda: None, lambda event: event)
        subscription.on_before(lambda _context: asyncio.sleep(0))

        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always", RuntimeWarning)
            with self.assertRaisesRegex(TypeError, "awaitable"):
                subscription.invoke("event")
        self.assertEqual(captured, [])
