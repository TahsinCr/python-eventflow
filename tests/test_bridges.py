"""Thread-to-async bridge behavior and ownership tests."""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import threading
import unittest
from unittest.mock import patch

from eventflow import ErrorPolicy, EventBus
from eventflow._compatibility import ExceptionGroup
from eventflow.bridges import AsyncEventQueue, LoopPortal
from eventflow.errors import EventQueueClosedError, LoopPortalClosedError
from eventflow.observability import OperationStats


class ThreadBridgeTests(unittest.TestCase):
    def test_loop_portal_recovers_after_thread_start_failure(self):
        portal = LoopPortal()

        with patch("threading.Thread.start", side_effect=RuntimeError("thread start failed")):
            with self.assertRaisesRegex(RuntimeError, "thread start failed"):
                portal.start()

        self.assertIs(portal.start(), portal)
        portal.close()

    def test_loop_portal_releases_thread_after_loop_factory_failure(self):
        portal = LoopPortal(name="eventflow-startup-failure")

        with patch("asyncio.new_event_loop", side_effect=RuntimeError("loop creation failed")):
            with self.assertRaises(LoopPortalClosedError) as start_error:
                portal.start()
            self.assertIsInstance(start_error.exception.__cause__, RuntimeError)

            thread = portal._thread
            assert thread is not None
            with self.assertRaises(LoopPortalClosedError) as close_error:
                portal.close()

        self.assertIsInstance(close_error.exception.__cause__, RuntimeError)
        self.assertFalse(thread.is_alive())
        self.assertIsNone(portal._thread)
        self.assertIsNone(portal._loop)
        portal.close()

    def test_loop_portal_call_context_and_owned_task_shutdown(self):
        value = contextvars.ContextVar("portal-test", default="missing")
        portal = LoopPortal()
        portal.start()
        value.set("caller-context")

        async def read_context():
            return value.get()

        self.assertEqual(portal.call(read_context, timeout=1), "caller-context")

        async def blocking_call_from_loop():
            return portal.call(asyncio.sleep(0))

        with self.assertRaises(RuntimeError):
            portal.submit(blocking_call_from_loop).result(1)
        started = threading.Event()
        cancelled = threading.Event()

        async def background():
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        portal.submit(background)
        self.assertTrue(started.wait(1))
        portal.close(timeout=1)
        self.assertTrue(cancelled.wait(1))
        self.assertFalse(portal.running)
        with self.assertRaises(RuntimeError):
            portal.start()

    def test_loop_portal_preserves_operation_timeout_errors(self):
        operation_error = TimeoutError("operation timed out")

        async def operation():
            raise operation_error

        with LoopPortal() as portal:
            for timeout in (None, 1.0):
                with self.subTest(timeout=timeout):
                    with self.assertRaises(TimeoutError) as raised:
                        portal.call(operation, timeout=timeout)
                    self.assertIs(raised.exception, operation_error)

    def test_loop_portal_rejects_work_after_stop_is_queued(self):
        portal = LoopPortal().start()
        loop = portal._loop
        self.assertIsNotNone(loop)
        assert loop is not None
        original_call_soon_threadsafe = loop.call_soon_threadsafe
        loop_blocked = threading.Event()
        release_loop = threading.Event()

        def block_loop():
            loop_blocked.set()
            release_loop.wait()

        def intercept_stop(callback, *args, **kwargs):
            if (
                getattr(callback, "__self__", None) is loop
                and getattr(callback, "__name__", None) == "stop"
            ):
                original_call_soon_threadsafe(block_loop)
            return original_call_soon_threadsafe(callback, *args, **kwargs)

        portal_thread = portal._thread
        self.assertIsNotNone(portal_thread)
        assert portal_thread is not None
        original_join = portal_thread.join

        def return_before_thread_exits(timeout=None):
            self.assertTrue(loop_blocked.wait(1))

        loop.call_soon_threadsafe = intercept_stop
        portal_thread.join = return_before_thread_exits
        try:
            with self.assertRaises(TimeoutError):
                portal.close(timeout=2)
            self.assertFalse(portal.running)
            with self.assertRaises(LoopPortalClosedError):
                portal.start()
            coroutine = asyncio.sleep(0)
            with self.assertRaises(LoopPortalClosedError):
                portal.submit(coroutine)
            self.assertIsNone(coroutine.cr_frame)
        finally:
            release_loop.set()
            loop.call_soon_threadsafe = original_call_soon_threadsafe
            portal_thread.join = original_join
            portal.close(timeout=1)

    def test_loop_portal_serializes_concurrent_start_and_close(self):
        portal = LoopPortal()
        barrier = threading.Barrier(3)
        errors = []

        def start_portal():
            barrier.wait()
            try:
                portal.start()
            except BaseException as error:
                errors.append(error)

        starters = [threading.Thread(target=start_portal) for _ in range(2)]
        for thread in starters:
            thread.start()
        barrier.wait()
        for thread in starters:
            thread.join(1)
        self.assertFalse(any(thread.is_alive() for thread in starters))
        self.assertEqual(errors, [])
        self.assertTrue(portal.running)

        closers = [threading.Thread(target=portal.close) for _ in range(2)]
        for thread in closers:
            thread.start()
        for thread in closers:
            thread.join(1)
        self.assertFalse(any(thread.is_alive() for thread in closers))
        self.assertFalse(portal.running)

    def test_loop_portal_closes_an_awaitable_cancelled_before_start(self):
        portal = LoopPortal()
        portal.start()
        loop_blocked = threading.Event()
        release_loop = threading.Event()

        async def operation():
            return 1

        async def hold_loop():
            loop_blocked.set()
            release_loop.wait()

        coroutine = operation()
        blocker = portal.submit(hold_loop())
        try:
            self.assertTrue(loop_blocked.wait(1))
            future = portal.submit(coroutine)
            self.assertTrue(future.cancel())
        finally:
            release_loop.set()
            portal.call(asyncio.sleep, 0)
            blocker.result(timeout=1)
            portal.close(timeout=1)
        self.assertIsNone(coroutine.cr_frame)

    def test_loop_portal_shutdown_waits_for_default_executor_threads(self):
        portal = LoopPortal().start()
        worker_started = threading.Event()
        release_worker = threading.Event()

        def blocking_work():
            worker_started.set()
            release_worker.wait(5)

        async def operation():
            await asyncio.to_thread(blocking_work)

        portal.submit(operation())
        try:
            self.assertTrue(worker_started.wait(1))
            with self.assertRaises(TimeoutError):
                portal.close(timeout=1)
            self.assertFalse(portal.running)
        finally:
            release_worker.set()
            portal.close(timeout=2)


class AsyncCoordinationEdgeTests(unittest.IsolatedAsyncioTestCase):
    async def test_event_queue_wakes_waiter_when_observer_raises(self):
        bus = EventBus[int](error_policy=ErrorPolicy.RAISE)
        observer = OperationStats()
        queue = AsyncEventQueue(bus, observer=observer)
        consumer = asyncio.create_task(queue.receive())
        await asyncio.sleep(0)

        try:
            with patch.object(observer, "record", side_effect=RuntimeError("observer")):
                with self.assertRaises(ExceptionGroup):
                    bus.publish(7)
            self.assertEqual(await asyncio.wait_for(consumer, timeout=1), 7)
        finally:
            await queue.aclose()
            bus.close()

    async def test_event_queue_uses_local_wakeup_for_same_thread_publish(self):
        bus = EventBus[int]()
        queue = AsyncEventQueue(bus)
        consumer = asyncio.create_task(queue.receive())
        await asyncio.sleep(0)
        loop = asyncio.get_running_loop()

        try:
            with patch.object(
                loop,
                "call_soon_threadsafe",
                side_effect=AssertionError("same-thread wake used cross-thread scheduling"),
            ):
                bus.publish(7)
            self.assertEqual(await asyncio.wait_for(consumer, timeout=1), 7)
        finally:
            await queue.aclose()
            bus.close()

    async def test_event_queue_wakes_waiter_from_publisher_thread(self):
        bus = EventBus[int]()
        queue = AsyncEventQueue(bus)
        consumer = asyncio.create_task(queue.receive())
        await asyncio.sleep(0)
        errors: list[BaseException] = []
        loop = asyncio.get_running_loop()

        def publish() -> None:
            try:
                bus.publish(11)
            except BaseException as error:
                errors.append(error)

        publisher = threading.Thread(target=publish, name="EventQueuePublisherTest")
        try:
            with patch.object(
                loop,
                "call_soon_threadsafe",
                wraps=loop.call_soon_threadsafe,
            ) as schedule_threadsafe:
                publisher.start()
                self.assertEqual(await asyncio.wait_for(consumer, timeout=1), 11)
            publisher.join(timeout=1)
            self.assertFalse(publisher.is_alive())
            self.assertEqual(errors, [])
            schedule_threadsafe.assert_called_once()
        finally:
            await queue.aclose()
            bus.close()

    async def test_event_queue_recovers_when_publish_races_with_waiter_cancellation(self):
        bus = EventBus[int]()
        queue = AsyncEventQueue(bus)
        consumer = asyncio.create_task(queue.receive())
        await asyncio.sleep(0)
        self.assertIsNotNone(queue._waiter)

        publisher = threading.Thread(target=lambda: bus.publish(13))
        try:
            publisher.start()
            publisher.join(timeout=1)
            self.assertFalse(publisher.is_alive())
            consumer.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await consumer
            self.assertEqual(await asyncio.wait_for(queue.receive(), timeout=1), 13)
        finally:
            await queue.aclose()
            bus.close()

    async def test_event_queue_rejects_second_consumer_and_other_loop(self):
        bus = EventBus[int]()
        queue = AsyncEventQueue(bus)
        bus.publish(1)
        self.assertEqual(await queue.receive(), 1)

        async def receive_again():
            return await queue.receive()

        second = asyncio.create_task(receive_again())
        with self.assertRaises(RuntimeError):
            await second

        errors = []

        def use_from_another_loop():
            try:
                asyncio.run(queue.receive())
            except RuntimeError as error:
                errors.append(error)

        thread = threading.Thread(target=use_from_another_loop)
        thread.start()
        thread.join(1)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(errors), 1)
        await queue.aclose()
        with self.assertRaises(EventQueueClosedError):
            await queue.receive()
        bus.publish(2)
        self.assertEqual(queue.stats().received, 1)
        bus.close()

    async def test_event_queue_allows_consumer_takeover_after_task_finishes(self):
        bus = EventBus[int]()
        queue = AsyncEventQueue(bus)

        async def receive_one():
            return await queue.receive()

        bus.publish(1)
        first = asyncio.create_task(receive_one())
        self.assertEqual(await first, 1)

        bus.publish(2)
        second = asyncio.create_task(receive_one())
        self.assertEqual(await second, 2)
        await queue.aclose()
        bus.close()

    async def test_event_queue_can_unsubscribe_after_owning_loop_closes(self):
        bus = EventBus[int]()
        queue = AsyncEventQueue(bus)
        errors = []

        def bind_and_close_loop():
            loop = asyncio.new_event_loop()

            async def attach_cancelled_consumer():
                consumer = asyncio.create_task(queue.receive())
                await asyncio.sleep(0)
                consumer.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await consumer

            try:
                loop.run_until_complete(attach_cancelled_consumer())
            except BaseException as error:
                errors.append(error)
            finally:
                loop.close()

        thread = threading.Thread(target=bind_and_close_loop)
        thread.start()
        thread.join(1)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])

        await queue.aclose()
        self.assertTrue(queue.closed)
        self.assertEqual(bus.subscriber_count, 0)
        bus.close()

    async def test_event_queue_recovers_a_cancelled_waiter_in_the_same_consumer(self):
        bus = EventBus[int]()
        queue = AsyncEventQueue(bus)

        async def recover():
            try:
                await queue.receive()
            except asyncio.CancelledError:
                return await queue.receive()

        consumer = asyncio.create_task(recover())
        await asyncio.sleep(0)
        consumer.cancel()
        await asyncio.sleep(0)
        bus.publish(9)
        self.assertEqual(await consumer, 9)
        await queue.aclose()
        bus.close()

    async def test_event_queue_async_context_and_portal_cancellation_api(self):
        bus = EventBus[int]()
        queue = AsyncEventQueue(bus)
        async with queue:
            bus.publish(7)
            self.assertEqual(await queue.receive(), 7)
        bus.publish(8)
        self.assertEqual(queue.stats().received, 1)
        bus.close()

        with LoopPortal() as portal:
            future = portal.submit(asyncio.sleep, 60)
            self.assertTrue(portal.cancel(future))
            with self.assertRaises(concurrent.futures.CancelledError):
                future.result(1)
            self.assertFalse(portal.cancel(future))
            with self.assertRaises(TypeError):
                portal.cancel(object())

    async def test_portal_timeout_invalid_submission_and_post_close_cleanup(self):
        portal = LoopPortal()
        rejected_coroutine = asyncio.sleep(0)
        with self.assertRaises(LoopPortalClosedError):
            portal.submit(rejected_coroutine)
        self.assertIsNone(rejected_coroutine.cr_frame)

        portal.start()
        started = threading.Event()
        cancelled = threading.Event()

        async def slow():
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise

        with self.assertRaisesRegex(TimeoutError, "LoopPortal call exceeded its timeout"):
            portal.call(slow, timeout=0.01)
        self.assertTrue(started.wait(1))
        self.assertTrue(cancelled.wait(1))
        with self.assertRaises((TypeError, ValueError)):
            portal.call(lambda: asyncio.sleep(0), timeout=True)
        with self.assertRaises(TypeError):
            portal.submit(lambda: 42).result(1)
        portal.close(timeout=1)
        closed_coroutine = asyncio.sleep(0)
        with self.assertRaises(LoopPortalClosedError):
            portal.submit(closed_coroutine)
        self.assertIsNone(closed_coroutine.cr_frame)

    async def test_portal_retains_loop_after_close_timeout_and_can_retry(self):
        portal = LoopPortal().start()
        started = threading.Event()
        cancellation_seen = threading.Event()

        async def slow_shutdown():
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancellation_seen.set()
                await asyncio.sleep(0.02)

        portal.submit(slow_shutdown)
        self.assertTrue(started.wait(1))
        with self.assertRaises(TimeoutError):
            portal.close(timeout=0)
        self.assertTrue(portal.running)
        portal.close(timeout=1)
        self.assertTrue(cancellation_seen.wait(1))
        self.assertFalse(portal.running)
