"""Edge cases for task and resource ownership."""

from __future__ import annotations

import asyncio
import threading
import unittest
from unittest.mock import patch

from eventflow.compatibility import ExceptionGroup
from eventflow.observability import OperationStats
from eventflow.scopes import AsyncResourceScope
from eventflow.tasks import AsyncTaskScope, LatestTaskCoordinator
from _support import _AsyncValueMixin


class AsyncCoordinationEdgeTests(unittest.IsolatedAsyncioTestCase, _AsyncValueMixin):
    async def test_task_scope_handles_failure_cancellation_and_join_timeout(self):
        reports = []
        observer = OperationStats()
        scope = AsyncTaskScope(
            on_error=lambda handle, error: reports.append((handle.name, error)), observer=observer
        )

        async def fail():
            raise ValueError("background failure")

        failed = scope.start(fail, name="failed")
        with self.assertRaisesRegex(ValueError, "background failure"):
            await failed.join(1)
        await asyncio.sleep(0)
        self.assertEqual(reports[0][0], "failed")

        started = asyncio.Event()

        async def blocked():
            started.set()
            await asyncio.Event().wait()

        handle = scope.start(blocked, name="blocked")
        await started.wait()
        with self.assertRaises(TimeoutError):
            await scope.join(0)
        self.assertFalse(handle.done)
        self.assertTrue(scope.cancel(handle))
        with self.assertRaises(asyncio.CancelledError):
            await handle.join(1)
        await asyncio.sleep(0)
        self.assertFalse(scope.cancel(handle))
        await scope.aclose()
        snapshot = scope.snapshot()
        self.assertEqual(snapshot.failed, 1)
        self.assertEqual(snapshot.cancelled, 1)
        self.assertTrue(scope.closed)
        self.assertIn("task_scope.task", observer.snapshot().operations)

    async def test_task_handle_cancels_from_a_foreign_thread(self):
        scope = AsyncTaskScope()
        started = asyncio.Event()

        async def blocked():
            started.set()
            await asyncio.Event().wait()

        handle = scope.start(blocked)
        await started.wait()
        results = []
        thread = threading.Thread(target=lambda: results.append(handle.cancel()))
        thread.start()
        thread.join(1)
        self.assertFalse(thread.is_alive())
        self.assertEqual(results, [True])
        with self.assertRaises(asyncio.CancelledError):
            await handle.join(1)
        await scope.aclose()

    async def test_task_scope_drain_waits_and_rejects_new_coroutines_after_close(self):
        scope = AsyncTaskScope()
        started = asyncio.Event()
        release = asyncio.Event()

        async def drained():
            started.set()
            await release.wait()

        handle = scope.start(drained, shutdown="drain")
        await started.wait()
        closing = asyncio.create_task(scope.aclose(timeout=1))
        await asyncio.sleep(0)
        self.assertTrue(scope.closed)
        self.assertFalse(closing.done())
        rejected = asyncio.sleep(0)
        with self.assertRaises(RuntimeError):
            scope.start(rejected)
        self.assertIsNone(rejected.cr_frame)
        release.set()
        await closing
        await handle.join(1)

    async def test_async_resource_cleanup_finishes_before_caller_cancellation(self):
        scope = AsyncResourceScope()
        started = asyncio.Event()
        release = asyncio.Event()
        calls = []

        async def cleanup():
            started.set()
            await release.wait()
            calls.append("finished")

        scope.defer(cleanup)
        closing = asyncio.create_task(scope.aclose())
        await started.wait()
        closing.cancel()
        await asyncio.sleep(0)
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await closing
        self.assertEqual(calls, ["finished"])
        self.assertEqual(scope.pending_cleanup(), 0)

    async def test_async_resource_scope_retries_only_failed_reverse_cleanup(self):
        calls = []

        class Resource:
            def __init__(self, name, *, fail=False):
                self.name = name
                self.fail = fail

            async def aclose(self):
                calls.append(self.name)
                if self.fail:
                    raise OSError(self.name)

        first = Resource("first", fail=True)
        second = Resource("second")
        borrowed = Resource("borrowed")
        scope = AsyncResourceScope()
        scope.add(first)
        scope.add(second)
        scope.add(borrowed, owned=False)
        scope.add(borrowed, owned=False)
        self.assertIs(scope.detach(borrowed), borrowed)
        scope.add(borrowed, owned=False)
        scope.add(borrowed, owned=False)
        self.assertIs(scope.detach(borrowed), borrowed)
        with self.assertRaises(KeyError):
            scope.detach(borrowed)
        scope.defer(lambda: calls.append("deferred"))
        with self.assertRaises(ExceptionGroup) as raised:
            await scope.aclose()
        self.assertIn("Resource scope cleanup failed", str(raised.exception))
        self.assertEqual(calls, ["deferred", "second", "first"])
        self.assertEqual(scope.pending_cleanup(), 1)
        first.fail = False
        await scope.aclose()
        self.assertEqual(calls[-1], "first")
        self.assertEqual(scope.pending_cleanup(), 0)

    async def test_latest_task_suppresses_stale_result_without_cancelling_work(self):
        coordinator = LatestTaskCoordinator[str]()
        started = asyncio.Event()
        release = asyncio.Event()
        results = []

        async def old_factory():
            started.set()
            await release.wait()
            return "old"

        old = coordinator.submit("account", old_factory, results.append, cancel_previous=False)
        await started.wait()
        new = coordinator.submit(
            "account", lambda: self._value("new"), results.append, cancel_previous=False
        )
        await new
        release.set()
        await old
        self.assertEqual(results, ["new"])
        self.assertEqual(coordinator.snapshot().discarded_results, 1)
        await coordinator.aclose()

    async def test_latest_task_cancel_removes_only_the_current_entry(self):
        coordinator = LatestTaskCoordinator[str]()
        started = asyncio.Event()
        blocker = asyncio.Event()

        async def work():
            started.set()
            await blocker.wait()

        task = coordinator.submit("key", work, lambda _value: None)
        await started.wait()
        self.assertTrue(coordinator.cancel("key"))
        with self.assertRaises(asyncio.CancelledError):
            await task
        await asyncio.sleep(0)
        self.assertFalse(coordinator.cancel("key"))
        self.assertEqual(coordinator.snapshot().active, {})
        await coordinator.aclose()

    async def test_latest_task_close_timeout_keeps_cancellation_resistant_work_owned(self):
        coordinator = LatestTaskCoordinator[str]()
        started = asyncio.Event()
        release = asyncio.Event()

        async def cancellation_resistant():
            started.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                await release.wait()
            return "late"

        task = coordinator.submit("key", cancellation_resistant, lambda _value: None)
        await started.wait()
        with self.assertRaises(TimeoutError):
            await coordinator.aclose(0)
        self.assertFalse(task.done())
        release.set()
        await task
        await coordinator.aclose(1)
        self.assertEqual(coordinator.snapshot().discarded_results, 1)
        self.assertEqual(coordinator.snapshot().active, {})

    async def test_resource_scope_and_task_scope_retry_after_failure_and_timeout(self):
        cleanup_calls = []
        resources = AsyncResourceScope()

        async def flaky_cleanup():
            cleanup_calls.append(1)
            if len(cleanup_calls) == 1:
                raise OSError("cleanup")

        resources.defer(flaky_cleanup)
        with self.assertRaises(ExceptionGroup):
            await resources.aclose()
        self.assertEqual(resources.pending_cleanup(), 1)
        await resources.aclose()
        self.assertEqual(resources.pending_cleanup(), 0)

        scope = AsyncTaskScope()
        started = asyncio.Event()
        finish = asyncio.Event()

        async def work():
            started.set()
            try:
                await finish.wait()
            except asyncio.CancelledError:
                await finish.wait()

        handle = scope.start(work)
        await started.wait()
        with self.assertRaises(TimeoutError):
            await handle.join(0)
        with self.assertRaises(TimeoutError):
            await scope.aclose(0)
        finish.set()
        await scope.aclose(1)
        self.assertTrue(handle.done)

    async def test_task_scope_closes_both_coroutines_when_task_creation_fails(self):
        scope = AsyncTaskScope()

        async def operation():
            return 1

        supplied = operation()
        loop = asyncio.get_running_loop()
        rejected = []

        def reject_task(coroutine, *, name=None, context=None):
            rejected.append(coroutine)
            raise RuntimeError("task creation rejected")

        with patch.object(loop, "create_task", side_effect=reject_task):
            with self.assertRaisesRegex(RuntimeError, "creation rejected"):
                scope.start(supplied)

        self.assertEqual(len(rejected), 1)
        self.assertIsNone(rejected[0].cr_frame)
        self.assertIsNone(supplied.cr_frame)
        await scope.aclose()

    async def test_task_scope_rejects_async_error_observer_and_reports_both_errors(self):
        loop = asyncio.get_running_loop()
        previous_handler = loop.get_exception_handler()
        contexts = []
        loop.set_exception_handler(lambda _loop, context: contexts.append(context))
        observer_coroutines = []

        async def async_observer(_handle, _error):
            return None

        def observer(handle, error):
            coroutine = async_observer(handle, error)
            observer_coroutines.append(coroutine)
            return coroutine

        scope = AsyncTaskScope(on_error=observer)

        async def fail():
            raise OSError("operation failed")

        try:
            task = scope.start(fail)
            with self.assertRaisesRegex(OSError, "operation failed"):
                await task.join(1)
            await asyncio.sleep(0)
            self.assertEqual(len(observer_coroutines), 1)
            self.assertIsNone(observer_coroutines[0].cr_frame)
            self.assertEqual(len(contexts), 1)
            grouped = contexts[0]["exception"]
            self.assertEqual(len(grouped.exceptions), 2)
            self.assertIsInstance(grouped.exceptions[0], OSError)
            self.assertIsInstance(grouped.exceptions[1], TypeError)
        finally:
            loop.set_exception_handler(previous_handler)
            await scope.aclose()

    async def test_latest_task_reports_factory_failures_and_closes_cleanly(self):
        coordinator = LatestTaskCoordinator[str]()
        errors = []
        applied = []

        async def fail():
            raise OSError("latest failed")

        async def on_error(error):
            errors.append(error)
            await asyncio.sleep(0)

        async def on_result(value):
            applied.append(value)
            await asyncio.sleep(0)

        task = coordinator.submit("key", fail, lambda _value: None, on_error=on_error)
        await task
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], OSError)
        await coordinator.submit("success", lambda: self._value("ok"), on_result)
        self.assertEqual(applied, ["ok"])
        await coordinator.aclose()
        with self.assertRaises(RuntimeError):
            coordinator.submit("key", fail, lambda _value: None)

    async def test_latest_task_creation_failure_preserves_previous_work(self):
        coordinator = LatestTaskCoordinator[str]()
        started = asyncio.Event()
        release = asyncio.Event()
        applied = []

        async def original():
            started.set()
            await release.wait()
            return "original"

        previous = coordinator.submit("key", original, applied.append)
        await started.wait()
        loop = asyncio.get_running_loop()
        rejected = []

        def reject_task(coroutine, *, name=None, context=None):
            rejected.append(coroutine)
            raise RuntimeError("task creation rejected")

        with patch.object(loop, "create_task", side_effect=reject_task):
            with self.assertRaisesRegex(RuntimeError, "creation rejected"):
                coordinator.submit("key", lambda: self._value("replacement"), applied.append)

        self.assertEqual(len(rejected), 1)
        self.assertIsNone(rejected[0].cr_frame)
        self.assertFalse(previous.cancelled())
        release.set()
        await previous
        self.assertEqual(applied, ["original"])
        self.assertEqual(coordinator.snapshot().active, {})

        with patch.object(loop, "create_task", side_effect=reject_task):
            with self.assertRaisesRegex(RuntimeError, "creation rejected"):
                coordinator.submit("fresh", lambda: self._value("retry"), applied.append)
        self.assertEqual(len(rejected), 2)
        self.assertIsNone(rejected[1].cr_frame)
        self.assertNotIn("fresh", coordinator._versions)
        retry = coordinator.submit("fresh", lambda: self._value("retry"), applied.append)
        await retry
        self.assertEqual(applied, ["original", "retry"])
        await coordinator.aclose()

    @unittest.skipUnless(hasattr(asyncio, "eager_task_factory"), "requires Python 3.12+")
    async def test_latest_task_eager_completion_does_not_leave_active_entry(self):
        coordinator = LatestTaskCoordinator[str]()
        loop = asyncio.get_running_loop()
        original_factory = loop.get_task_factory()
        loop.set_task_factory(asyncio.eager_task_factory)
        started = asyncio.Event()
        release = asyncio.Event()
        applied = []

        async def old_work():
            started.set()
            await release.wait()
            return "old"

        try:
            immediate = coordinator.submit(
                "immediate", lambda: self._value("immediate"), applied.append
            )
            self.assertTrue(immediate.done())
            await immediate
            self.assertEqual(coordinator.snapshot().active, {})

            old = coordinator.submit("key", old_work, applied.append, cancel_previous=False)
            await started.wait()
            current = coordinator.submit(
                "key", lambda: self._value("current"), applied.append, cancel_previous=False
            )
            self.assertTrue(current.done())
            await current
            self.assertEqual(applied, ["immediate", "current"])
            self.assertEqual(coordinator.snapshot().active, {})

            release.set()
            await old
            self.assertEqual(applied, ["immediate", "current"])
            self.assertEqual(coordinator.snapshot().active, {})
        finally:
            loop.set_task_factory(original_factory)
            await coordinator.aclose()
