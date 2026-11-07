"""Edge cases for async scheduling and deadline handling."""

from __future__ import annotations

import asyncio
import unittest

from eventflow.observability import OperationStats
from eventflow.scheduling import AsyncScheduler
from eventflow.tasks import AsyncTaskScope


class AsyncCoordinationEdgeTests(unittest.IsolatedAsyncioTestCase):
    async def test_scheduler_context_manager_drains_one_shot_work(self):
        calls = []

        async with AsyncScheduler() as scheduler:
            handle = scheduler.schedule_once(calls.append, "complete")
            await handle.join(timeout=1)
            self.assertEqual(calls, ["complete"])
            self.assertFalse(scheduler.snapshot().closed)

        self.assertTrue(scheduler.snapshot().closed)
        with self.assertRaises(RuntimeError):
            scheduler.schedule_once(calls.append, "rejected")

    async def test_periodic_scheduler_caps_parallel_callbacks(self):
        observer = OperationStats()
        scheduler = AsyncScheduler(observer=observer)
        entered = asyncio.Event()
        dropped = asyncio.Event()
        calls = 0

        record = observer.record

        def record_and_signal(*args, **kwargs):
            record(*args, **kwargs)
            if kwargs.get("dropped"):
                dropped.set()

        observer.record = record_and_signal  # type: ignore[method-assign]

        async def blocked_tick():
            nonlocal calls
            calls += 1
            entered.set()
            await asyncio.Event().wait()

        handle = scheduler.periodic(
            blocked_tick,
            interval=0.002,
            initial_delay=0,
            overlap="parallel",
            max_parallel=1,
        )
        await entered.wait()
        await asyncio.wait_for(dropped.wait(), timeout=5)
        self.assertEqual(calls, 1)
        self.assertTrue(scheduler.cancel(handle))
        with self.assertRaises(asyncio.CancelledError):
            await handle.join(1)
        await scheduler.aclose()
        self.assertGreaterEqual(scheduler.snapshot().cancelled, 1)

    async def test_scheduler_queue_overlap_and_argument_validation(self):
        scheduler = AsyncScheduler()
        for interval in (0, -1, float("inf"), True):
            with self.assertRaises((TypeError, ValueError)):
                scheduler.periodic(lambda: None, interval=interval)
        with self.assertRaises(ValueError):
            scheduler.periodic(lambda: None, interval=1, overlap="invalid")
        with self.assertRaises(ValueError):
            scheduler.periodic(lambda: None, interval=1, max_parallel=0)
        with self.assertRaises(ValueError):
            scheduler.schedule_once(lambda: None, delay=-1)

        calls = 0
        second_tick_started = asyncio.Event()

        async def slow_tick():
            nonlocal calls
            calls += 1
            if calls == 2:
                second_tick_started.set()
            await asyncio.sleep(0.008)

        handle = scheduler.periodic(slow_tick, interval=0.002, initial_delay=0, overlap="queue")
        try:
            await asyncio.wait_for(second_tick_started.wait(), timeout=5)
            self.assertGreaterEqual(calls, 2)
        finally:
            self.assertTrue(scheduler.cancel(handle))
            with self.assertRaises(asyncio.CancelledError):
                await handle.join(1)
            await scheduler.aclose()

    async def test_scheduler_reports_periodic_callback_failures(self):
        reported = []
        observer = OperationStats()
        scheduler = AsyncScheduler(on_error=reported.append, observer=observer)

        async def fail_tick():
            raise ValueError("tick failed")

        handle = scheduler.periodic(fail_tick, interval=0.001, initial_delay=0)
        with self.assertRaisesRegex(ValueError, "tick failed"):
            await handle.join(1)
        await asyncio.sleep(0)
        self.assertEqual(len(reported), 1)
        self.assertEqual(scheduler.snapshot().failed, 1)
        self.assertIn("scheduler.periodic", observer.snapshot().operations)
        await scheduler.aclose()

    async def test_task_scope_and_scheduler_retry_after_timed_out_work_finishes(self):
        scope = AsyncTaskScope()
        scope_started = asyncio.Event()
        scope_release = asyncio.Event()

        async def stubborn_task():
            scope_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await scope_release.wait()

        task_handle = scope.start(stubborn_task)
        await scope_started.wait()
        with self.assertRaises(TimeoutError):
            await scope.aclose(timeout=0)
        scope_release.set()
        await task_handle.join(1)
        await asyncio.sleep(0)
        await scope.aclose()

        scheduler = AsyncScheduler()
        job_started = asyncio.Event()
        job_release = asyncio.Event()

        async def stubborn_job():
            job_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await job_release.wait()

        job = scheduler.schedule_once(stubborn_job)
        await job_started.wait()
        with self.assertRaises(TimeoutError):
            await scheduler.aclose(timeout=0)
        job_release.set()
        await job.join(1)
        await asyncio.sleep(0)
        await scheduler.aclose()

    async def test_scheduler_reports_one_shot_failures_and_cancels_delayed_jobs(self):
        reported = []
        scheduler = AsyncScheduler(on_error=reported.append)

        async def fail():
            raise OSError("scheduled")

        failed = scheduler.schedule_once(fail)
        with self.assertRaises(OSError):
            await failed.join(1)
        await asyncio.sleep(0)
        self.assertEqual(len(reported), 1)
        delayed = scheduler.schedule_once(asyncio.sleep, 10, delay=10)
        self.assertTrue(delayed.cancel())
        with self.assertRaises(asyncio.CancelledError):
            await delayed.join(1)
        await scheduler.aclose()
        self.assertTrue(scheduler.snapshot().closed)
