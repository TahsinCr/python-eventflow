"""Async execution primitive behavior."""

from __future__ import annotations

import asyncio
import threading
import unittest
from unittest.mock import patch

from eventflow.bridges import AsyncEventQueue
from eventflow.compatibility import ExceptionGroup
from eventflow.concurrency import KeyedConcurrencyLimiter, KeyedRateLimiter, SingleFlight
from eventflow.errors import EventQueueClosedError, QueueOverflowError
from eventflow.observability import OperationStats
from eventflow.resilience import CircuitBreaker, CooldownRegistry, RetryPolicy
from eventflow.scheduling import AsyncScheduler
from eventflow.scopes import AsyncResourceScope
from eventflow.stream import AsyncStream
from eventflow.tasks import AsyncTaskScope, LatestTaskCoordinator
from eventflow import EventBus, ErrorPolicy
from _support import ManualClock


class AsyncPrimitiveTests(unittest.IsolatedAsyncioTestCase):
    async def test_bus_queue_task_scope_limiter_and_metrics_compose(self):
        observer = OperationStats()
        bus = EventBus[int]()
        queue = AsyncEventQueue(bus, capacity=1, observer=observer)
        limiter = KeyedConcurrencyLimiter[int](1, observer=observer)
        scope = AsyncTaskScope(observer=observer)

        async def consume():
            async with limiter.acquire(42):
                return await queue.receive()

        try:
            async with queue:
                async with scope:
                    consumer = scope.start(consume, shutdown="drain")
                    bus.publish(7)
                    self.assertEqual(await consumer.join(1), 7)
            snapshot = observer.snapshot()
            self.assertEqual(
                set(snapshot.operations),
                {
                    "event_queue.publish",
                    "event_queue.receive",
                    "keyed_concurrency.acquire",
                    "task_scope.task",
                },
            )
            self.assertEqual(snapshot.operations["task_scope.task"].successful, 1)
        finally:
            await limiter.aclose()
            bus.close()

    async def test_async_resource_scope_runs_mixed_cleanup_in_reverse_order(self):
        calls = []
        scope = AsyncResourceScope()
        scope.defer(lambda: calls.append("defer"))

        class Resource:
            async def aclose(self):
                calls.append("resource")

        scope.add(Resource())
        scope.add(Resource(), close=lambda resource: resource.aclose())
        await scope.aclose()
        self.assertEqual(calls, ["resource", "resource", "defer"])
        self.assertEqual(scope.pending_cleanup(), 0)

    async def test_task_scope_cancel_drain_error_reporting_and_timeout_retry(self):
        failures = []
        scope = AsyncTaskScope(
            on_error=lambda handle, error: failures.append((handle.name, str(error)))
        )
        drained = asyncio.Event()
        scope.start(lambda: self._set_event(drained), name="drain", shutdown="drain")
        scope.start(self._fail_task, name="failure")
        await scope.join(1)
        await scope.aclose(1)
        self.assertTrue(drained.is_set())
        self.assertEqual(failures, [("failure", "task failed")])
        self.assertEqual(scope.snapshot().failed, 1)
        with self.assertRaises(RuntimeError):
            scope.start(self._fail_task)

        started = asyncio.Event()
        release = asyncio.Event()

        async def cancellation_resistant():
            started.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                await release.wait()

        retry_scope = AsyncTaskScope()
        handle = retry_scope.start(cancellation_resistant)
        await started.wait()
        with self.assertRaises(TimeoutError):
            await retry_scope.aclose(0.01)
        self.assertFalse(handle.done)
        release.set()
        await retry_scope.aclose(1)
        self.assertTrue(handle.done)

    async def test_task_scope_closes_a_coroutine_cancelled_before_first_step(self):
        scope = AsyncTaskScope()

        async def operation():
            return 1

        coroutine = operation()
        handle = scope.start(coroutine)
        self.assertTrue(handle.cancel())
        await scope.aclose(1)
        self.assertIsNone(coroutine.cr_frame)

    async def test_latest_task_never_applies_superseded_result(self):
        latest = LatestTaskCoordinator[str]()
        old_started = asyncio.Event()
        release_old = asyncio.Event()
        applied = []

        async def old_factory():
            old_started.set()
            try:
                await release_old.wait()
            except asyncio.CancelledError:
                await release_old.wait()
            return "old"

        old_task = latest.submit("resource", old_factory, applied.append)
        await old_started.wait()
        new_task = latest.submit("resource", lambda: self._value("new"), applied.append)
        await new_task
        release_old.set()
        await old_task
        self.assertEqual(applied, ["new"])
        self.assertEqual(latest.snapshot().active, {})
        await latest.aclose()

    async def test_async_event_queue_is_bounded_thread_safe_and_drains_on_close(self):
        bus = EventBus[int]()
        queue = AsyncEventQueue(bus, capacity=2, overflow="drop_oldest")
        thread = threading.Thread(target=lambda: [bus.publish(value) for value in (1, 2, 3)])
        thread.start()
        thread.join(1)
        self.assertFalse(thread.is_alive())
        self.assertEqual(queue.stats().dropped, 1)
        await queue.aclose()
        self.assertEqual([item async for item in queue], [2, 3])
        self.assertTrue(queue.closed)
        bus.close()

    async def test_async_event_queue_error_policy_and_waiter_wakeup(self):
        bus = EventBus[int](error_policy=ErrorPolicy.RAISE)
        queue = AsyncEventQueue(bus, capacity=1, overflow="error")
        bus.publish(1)
        with self.assertRaises(ExceptionGroup) as caught:
            bus.publish(2)
        self.assertTrue(
            any(isinstance(error, QueueOverflowError) for error in caught.exception.exceptions)
        )
        self.assertEqual(queue.stats().overflow_errors, 1)
        self.assertEqual(await queue.receive(), 1)
        waiting_bus = EventBus[int]()
        waiting_queue = AsyncEventQueue(waiting_bus)
        waiting = asyncio.create_task(waiting_queue.receive())
        await asyncio.sleep(0)
        await waiting_queue.aclose()
        with self.assertRaises(EventQueueClosedError):
            await waiting
        await queue.aclose()
        bus.close()
        waiting_bus.close()

    async def test_async_event_queue_drop_newest_keeps_earlier_event(self):
        bus = EventBus[int]()
        queue = AsyncEventQueue(bus, capacity=1, overflow="drop_newest")
        bus.publish(1)
        bus.publish(2)
        self.assertEqual(queue.stats().dropped, 1)
        self.assertEqual(await queue.receive(), 1)
        await queue.aclose()
        bus.close()

    async def test_async_event_queue_rejects_a_second_consumer_task(self):
        bus = EventBus[int]()
        queue = AsyncEventQueue(bus)
        first = asyncio.create_task(queue.receive())
        await asyncio.sleep(0)

        second = asyncio.create_task(queue.receive())
        with self.assertRaisesRegex(RuntimeError, "one consumer task"):
            await second

        bus.publish(7)
        self.assertEqual(await first, 7)
        await queue.aclose()
        bus.close()

    async def test_async_event_queue_retries_failed_unsubscribe_on_close(self):
        bus = EventBus[int]()
        queue = AsyncEventQueue(bus)
        subscription = queue._subscription
        self.assertIsNotNone(subscription)
        cancel = subscription.cancel
        attempts = 0

        def cancel_once_fails():
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("unsubscribe failed")
            cancel()

        with patch.object(subscription, "cancel", side_effect=cancel_once_fails):
            with self.assertRaisesRegex(RuntimeError, "unsubscribe failed"):
                await queue.aclose()
            self.assertEqual(bus.subscriber_count, 1)
            await queue.aclose()

        self.assertEqual(attempts, 2)
        self.assertEqual(bus.subscriber_count, 0)
        bus.close()

    async def test_keyed_concurrency_cancellation_and_key_cleanup(self):
        limiter = KeyedConcurrencyLimiter[str](1)
        entered = asyncio.Event()
        release = asyncio.Event()

        async def hold():
            async with limiter.acquire("same"):
                entered.set()
                await release.wait()

        owner = asyncio.create_task(hold())
        await entered.wait()
        waiter = asyncio.create_task(limiter.acquire("same").__aenter__())
        await asyncio.sleep(0)
        self.assertEqual(limiter.stats().waiters, 1)
        self.assertEqual(limiter.key_stats("same").waiters, 1)
        waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiter
        self.assertEqual(limiter.stats().waiters, 0)
        release.set()
        await owner
        self.assertEqual(limiter.stats().keys, 0)
        self.assertEqual(limiter.key_stats("same").active, 0)
        await limiter.aclose()

    async def test_cancelling_one_keyed_waiter_does_not_cancel_other_waiters(self):
        limiter = KeyedConcurrencyLimiter[str](1)
        entered = asyncio.Event()
        release = asyncio.Event()
        acquired = asyncio.Event()

        async def hold():
            async with limiter.acquire("same"):
                entered.set()
                await release.wait()

        async def wait_for_slot():
            async with limiter.acquire("same"):
                acquired.set()

        owner = asyncio.create_task(hold())
        await entered.wait()
        cancelled = asyncio.create_task(wait_for_slot())
        survivor = asyncio.create_task(wait_for_slot())
        await asyncio.sleep(0)
        cancelled.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await cancelled
        release.set()
        await owner
        await asyncio.wait_for(survivor, timeout=1)
        self.assertTrue(acquired.is_set())
        await limiter.aclose()

    async def test_keyed_concurrency_close_wakes_waiters_without_losing_active_permit(self):
        limiter = KeyedConcurrencyLimiter[str](1)
        async with limiter.acquire("key"):
            waiter = asyncio.create_task(limiter.acquire("key").__aenter__())
            await asyncio.sleep(0)
            self.assertEqual(limiter.stats().waiters, 1)
            await limiter.aclose()
            with self.assertRaises(RuntimeError):
                await waiter
            self.assertEqual(limiter.stats().active, 1)
        self.assertEqual(limiter.stats().active, 0)

    async def test_keyed_concurrency_waiters_are_fifo_and_nowait_cannot_bypass(self):
        limiter = KeyedConcurrencyLimiter[str](1)
        holder = limiter.acquire("key")
        await holder.__aenter__()
        order = []
        arrivals = 0
        all_arrived = asyncio.Event()

        async def wait_in_order(index):
            nonlocal arrivals
            arrivals += 1
            if arrivals == 5:
                all_arrived.set()
            async with limiter.acquire("key"):
                order.append(index)
                await asyncio.sleep(0)

        waiters = [asyncio.create_task(wait_in_order(index)) for index in range(5)]
        await asyncio.wait_for(all_arrived.wait(), 1)
        self.assertEqual(limiter.stats().waiters, len(waiters))
        await holder.__aexit__(None, None, None)
        barged = limiter.acquire_nowait("key")
        if barged:
            await limiter.release("key")
        await asyncio.gather(*waiters)
        self.assertFalse(barged)
        self.assertEqual(order, list(range(len(waiters))))
        await limiter.aclose()

    async def test_keyed_concurrency_observer_reports_global_queue_depth(self):
        observer = OperationStats()
        limiter = KeyedConcurrencyLimiter[str](1, observer=observer)
        holders = [limiter.acquire(key) for key in ("first", "second")]
        for holder in holders:
            await holder.__aenter__()

        arrivals = 0
        all_arrived = asyncio.Event()

        async def wait_for_key(key):
            nonlocal arrivals
            arrivals += 1
            if arrivals == len(holders):
                all_arrived.set()
            async with limiter.acquire(key):
                pass

        first = asyncio.create_task(wait_for_key("first"))
        second = asyncio.create_task(wait_for_key("second"))
        await asyncio.wait_for(all_arrived.wait(), 1)
        self.assertEqual(limiter.stats().waiters, 2)
        await holders[0].__aexit__(None, None, None)
        await first
        metrics = observer.snapshot().operations["keyed_concurrency.acquire"]
        self.assertEqual(metrics.queue_depth, 1)
        await holders[1].__aexit__(None, None, None)
        await second
        await limiter.aclose()

    async def test_keyed_concurrency_close_cancellation_does_not_cancel_waiters(self):
        limiter = KeyedConcurrencyLimiter[str](1)
        async with limiter.acquire("key"):
            arrivals = 0
            all_arrived = asyncio.Event()

            async def wait_for_slot():
                nonlocal arrivals
                arrivals += 1
                if arrivals == 2:
                    all_arrived.set()
                await limiter.acquire("key").__aenter__()

            waiters = [asyncio.create_task(wait_for_slot()) for _ in range(2)]
            await asyncio.wait_for(all_arrived.wait(), 1)
            self.assertEqual(limiter.stats().waiters, len(waiters))
            closing = asyncio.create_task(limiter.aclose())
            await asyncio.sleep(0)
            closing.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await closing
            outcomes = await asyncio.gather(*waiters, return_exceptions=True)
            self.assertTrue(all(isinstance(outcome, RuntimeError) for outcome in outcomes))
            self.assertEqual(limiter.stats().waiters, 0)
            self.assertEqual(limiter.stats().active, 1)

    async def test_single_flight_shares_work_without_coupling_waiter_cancellation(self):
        flight = SingleFlight[str]()
        started = asyncio.Event()
        release = asyncio.Event()
        calls = []

        async def factory():
            calls.append(1)
            started.set()
            await release.wait()
            return 42

        first = asyncio.create_task(flight.run("key", factory))
        await started.wait()
        second = asyncio.create_task(flight.run("key", factory))
        await asyncio.sleep(0)
        first.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await first
        release.set()
        self.assertEqual(await second, 42)
        await asyncio.sleep(0)
        self.assertEqual(len(calls), 1)
        self.assertEqual(flight.stats().active, 0)

    async def test_single_flight_aclose_owns_orphaned_work_and_survives_caller_cancel(self):
        flight = SingleFlight[str]()
        started = asyncio.Event()
        cancellation_seen = asyncio.Event()
        release = asyncio.Event()

        async def factory():
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancellation_seen.set()
                await release.wait()
                raise

        waiter = asyncio.create_task(flight.run("key", factory))
        await started.wait()
        waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiter
        self.assertEqual(flight.stats().active, 1)

        closing = asyncio.create_task(flight.aclose())
        await asyncio.wait_for(cancellation_seen.wait(), timeout=1)
        closing.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await closing
        self.assertEqual(flight.stats().active, 1)

        release.set()
        await flight.aclose()
        await asyncio.sleep(0)
        self.assertEqual(flight.stats().active, 0)
        with self.assertRaises(RuntimeError):
            await flight.run("after-close", factory)

    async def test_single_flight_operation_cannot_close_its_own_owner(self):
        flight = SingleFlight[str]()

        async def factory():
            await flight.aclose()

        with self.assertRaisesRegex(RuntimeError, "cannot close its own instance"):
            await flight.run("key", factory)

        self.assertEqual(flight.stats().active, 0)
        await flight.aclose()

    async def test_single_flight_stats_rejects_a_different_event_loop(self):
        flight = SingleFlight[str]()
        flight.stats()
        errors = []
        finished = threading.Event()

        def read_from_another_loop():
            async def read_stats():
                flight.stats()

            try:
                asyncio.run(read_stats())
            except RuntimeError as error:
                errors.append(error)
            finally:
                finished.set()

        thread = threading.Thread(target=read_from_another_loop)
        thread.start()
        self.assertTrue(finished.wait(1))
        thread.join(1)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIn("different event loop", str(errors[0]))

    async def test_keyed_rate_limiter_waits_cancellably_and_cleans_waiters(self):
        default_burst = KeyedRateLimiter[str](rate=0.1)
        self.assertTrue(default_burst.try_acquire("low-rate"))
        await default_burst.aclose()

        limiter = KeyedRateLimiter[str](rate=0.1, period=1, burst=1)
        self.assertTrue(limiter.try_acquire("x"))
        self.assertFalse(limiter.try_acquire("x"))
        waiter = asyncio.create_task(limiter.acquire("x"))
        await asyncio.sleep(0)
        self.assertEqual(limiter.stats().waiters, 1)
        waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiter
        self.assertEqual(limiter.stats().waiters, 0)
        await limiter.aclose()

    async def test_keyed_rate_limiter_try_acquire_does_not_bypass_waiters(self):
        limiter = KeyedRateLimiter[str](rate=0.1, burst=1)
        self.assertTrue(limiter.try_acquire("key"))
        waiter = asyncio.create_task(limiter.acquire("key"))
        await asyncio.sleep(0)
        entry = limiter._entries["key"]
        self.assertEqual(entry.waiters, 1)

        entry.tokens = 1.0
        entry.updated = asyncio.get_running_loop().time()
        self.assertFalse(limiter.try_acquire("key"))
        self.assertEqual(entry.tokens, 1.0)
        for signal in tuple(entry.signals or ()):
            signal.set()

        await asyncio.wait_for(waiter, timeout=0.1)
        self.assertEqual(entry.tokens, 0.0)
        await limiter.aclose()

    async def test_keyed_rate_limiter_close_wakes_waiters(self):
        limiter = KeyedRateLimiter[str](rate=0.1, period=1, burst=1)
        self.assertTrue(limiter.try_acquire("x"))
        waiter = asyncio.create_task(limiter.acquire("x"))
        await asyncio.sleep(0)
        self.assertEqual(limiter.stats().waiters, 1)
        await limiter.aclose()
        with self.assertRaises(RuntimeError):
            await waiter
        self.assertEqual(limiter.stats().waiters, 0)

    async def test_keyed_rate_limiter_close_cancellation_does_not_cancel_waiters(self):
        limiter = KeyedRateLimiter[str](rate=0.1, period=1, burst=1)
        self.assertTrue(limiter.try_acquire("key"))
        arrivals = 0
        all_arrived = asyncio.Event()

        async def wait_for_token():
            nonlocal arrivals
            arrivals += 1
            if arrivals == 2:
                all_arrived.set()
            await limiter.acquire("key")

        waiters = [asyncio.create_task(wait_for_token()) for _ in range(2)]
        await asyncio.wait_for(all_arrived.wait(), 1)
        self.assertEqual(limiter.stats().waiters, len(waiters))
        closing = asyncio.create_task(limiter.aclose())
        await asyncio.sleep(0)
        closing.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await closing
        outcomes = await asyncio.gather(*waiters, return_exceptions=True)
        self.assertTrue(all(isinstance(outcome, RuntimeError) for outcome in outcomes))
        self.assertEqual(limiter.stats().waiters, 0)

    async def test_async_retry_and_cancellation_semantics(self):
        attempts = []
        retry = RetryPolicy(max_attempts=3, initial_delay=0)

        async def transient():
            attempts.append(1)
            if len(attempts) < 2:
                raise OSError("temporary")
            return "done"

        result = await retry.run_async(
            transient, should_retry=lambda error: isinstance(error, OSError)
        )
        self.assertEqual(result, "done")
        cancelled_attempts = []

        async def cancelled():
            cancelled_attempts.append(1)
            raise asyncio.CancelledError

        with self.assertRaises(asyncio.CancelledError):
            await retry.run_async(cancelled, should_retry=lambda _error: True)
        self.assertEqual(len(cancelled_attempts), 1)

    async def test_cooldown_async_wait_wakes_when_cleared(self):
        cooldowns = CooldownRegistry()
        cooldowns.block("resource", 10)
        waiter = asyncio.create_task(cooldowns.wait_async("resource"))
        await asyncio.sleep(0)
        self.assertTrue(cooldowns.clear("resource"))
        self.assertTrue(await asyncio.wait_for(waiter, 1))

    async def test_circuit_breaker_async_call_and_cancellation_do_not_trip(self):
        breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=1)
        cancelled = asyncio.Event()

        async def operation():
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise

        task = asyncio.create_task(breaker.call_async(operation))
        await asyncio.sleep(0)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(cancelled.is_set())
        self.assertEqual(breaker.state, "closed")
        self.assertEqual(await breaker.call_async(lambda: self._value("ok")), "ok")

    async def test_circuit_breaker_ignores_stale_half_open_probe_results(self):
        clock = ManualClock()
        breaker = CircuitBreaker(
            failure_threshold=1,
            recovery_timeout=1,
            half_open_max=2,
            clock=clock,
        )
        with self.assertRaises(OSError):
            breaker.call(lambda: (_ for _ in ()).throw(OSError("initial")))
        clock.set(1.0)
        ready = asyncio.Event()
        release_success = asyncio.Event()
        release_failure = asyncio.Event()
        probes = 0

        async def successful_probe():
            nonlocal probes
            probes += 1
            if probes == 2:
                ready.set()
            await release_success.wait()
            return "ok"

        async def failed_probe():
            nonlocal probes
            probes += 1
            if probes == 2:
                ready.set()
            await release_failure.wait()
            raise OSError("probe failed")

        success = asyncio.create_task(breaker.call_async(successful_probe))
        failure = asyncio.create_task(breaker.call_async(failed_probe))
        await asyncio.wait_for(ready.wait(), 1)
        await asyncio.sleep(0)
        self.assertEqual(probes, 2)
        release_failure.set()
        with self.assertRaises(OSError):
            await failure
        release_success.set()
        self.assertEqual(await success, "ok")
        self.assertEqual(breaker.state, "open")

    async def test_circuit_breaker_ignores_stale_closed_call_results(self):
        for stale_error in (False, True):
            with self.subTest(stale_error=stale_error):
                clock = ManualClock()
                breaker = CircuitBreaker(failure_threshold=2, recovery_timeout=1, clock=clock)
                started = asyncio.Event()
                release = asyncio.Event()

                async def old_operation():
                    started.set()
                    await release.wait()
                    if stale_error:
                        raise OSError("stale failure")
                    return "stale success"

                stale_call = asyncio.create_task(breaker.call_async(old_operation))
                await asyncio.wait_for(started.wait(), 1)
                for _ in range(2):
                    with self.assertRaises(OSError):
                        breaker.call(lambda: (_ for _ in ()).throw(OSError("opens circuit")))

                clock.set(1.0)
                self.assertEqual(await breaker.call_async(lambda: self._value("probe")), "probe")
                with self.assertRaises(OSError):
                    breaker.call(lambda: (_ for _ in ()).throw(OSError("new generation")))
                self.assertEqual(breaker.stats().failures, 1)

                release.set()
                if stale_error:
                    with self.assertRaisesRegex(OSError, "stale failure"):
                        await stale_call
                else:
                    self.assertEqual(await stale_call, "stale success")
                self.assertEqual(breaker.stats().failures, 1)

    async def test_async_scheduler_runs_once_and_periodic_then_closes(self):
        observer = OperationStats()
        scheduler = AsyncScheduler(observer=observer)
        values = []
        second_tick = asyncio.Event()
        tick_count = 0

        def record_tick():
            nonlocal tick_count
            tick_count += 1
            values.append("tick")
            if tick_count == 2:
                second_tick.set()

        once = scheduler.schedule_once(lambda: values.append("once"))
        await once.join(1)
        periodic = scheduler.periodic(record_tick, interval=0.01, overlap="skip")
        await asyncio.wait_for(second_tick.wait(), 1)
        await scheduler.aclose(1)
        self.assertIn("once", values)
        self.assertGreaterEqual(values.count("tick"), 2)
        self.assertTrue(periodic.done)
        self.assertIn("scheduler.once", observer.snapshot().operations)

    async def test_scheduler_closes_coroutine_when_task_creation_fails(self):
        from unittest.mock import patch

        scheduler = AsyncScheduler()
        loop = asyncio.get_running_loop()
        created = []

        def fail_create_task(coroutine, **_kwargs):
            created.append(coroutine)
            raise RuntimeError("task factory failed")

        with patch.object(loop, "create_task", side_effect=fail_create_task):
            with self.assertRaisesRegex(RuntimeError, "task factory failed"):
                scheduler.schedule_once(lambda: None)
        self.assertEqual(len(created), 1)
        self.assertIsNone(created[0].cr_frame)
        await scheduler.aclose()

    async def test_async_stream_batch_within_flushes_by_time_and_preserves_final_partial(self):
        closed = asyncio.Event()

        async def source():
            try:
                yield 1
                await asyncio.sleep(0.03)
                yield 2
                yield 3
            finally:
                closed.set()

        stream = AsyncStream(source()).batch_within(10, 0.01)
        self.assertEqual([batch async for batch in stream], [(1,), (2, 3)])
        self.assertTrue(closed.is_set())
        for invalid in (0, -1, float("inf"), True):
            with self.assertRaises((TypeError, ValueError)):
                AsyncStream(self._source()).batch_within(2, invalid)

    async def test_timed_batch_surfaces_source_error_after_partial_batch(self):
        async def source():
            yield 4
            raise LookupError("source failed")

        batches = AsyncStream(source()).batch_within(2, 10)
        iterator = batches.__aiter__()
        self.assertEqual(await iterator.__anext__(), (4,))
        with self.assertRaisesRegex(LookupError, "source failed"):
            await iterator.__anext__()

    async def test_timed_batch_early_exit_closes_its_source(self):
        closed = asyncio.Event()

        async def source():
            try:
                for value in range(10):
                    yield value
            finally:
                closed.set()

        async with AsyncStream(source()).batch_within(2, 10) as batches:
            async for _batch in batches:
                break
        self.assertTrue(closed.is_set())

    @staticmethod
    async def _set_event(event):
        event.set()

    @staticmethod
    async def _fail_task():
        raise ValueError("task failed")

    @staticmethod
    async def _value(value):
        return value

    @staticmethod
    async def _source():
        yield 1
