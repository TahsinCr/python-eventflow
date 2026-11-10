"""Edge cases for retry, cooldown, and circuit-breaker behavior."""

from __future__ import annotations

import asyncio
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from _support import ManualClock

from eventflow._compatibility import ExceptionGroup
from eventflow.errors import CircuitOpenError
from eventflow.resilience import CircuitBreaker, CooldownRegistry, RetryPolicy
from eventflow.timing import Deadline


class AsyncCoordinationEdgeTests(unittest.IsolatedAsyncioTestCase):
    def test_circuit_breaker_reset_ignores_in_flight_results(self):
        for outcome in ("success", "failure"):
            with self.subTest(outcome=outcome):
                started = threading.Event()
                release = threading.Event()
                breaker = CircuitBreaker(failure_threshold=2)

                def stale_operation():
                    started.set()
                    if not release.wait(timeout=1):
                        raise TimeoutError("stale operation was not released")
                    if outcome == "failure":
                        raise OSError("stale failure")
                    return "stale success"

                def current_failure():
                    raise OSError("current failure")

                with ThreadPoolExecutor(max_workers=1) as workers:
                    stale_call = workers.submit(breaker.call, stale_operation)
                    try:
                        self.assertTrue(started.wait(timeout=1))
                        breaker.reset()
                        with self.assertRaisesRegex(OSError, "current failure"):
                            breaker.call(current_failure)
                        self.assertEqual(breaker.stats().failures, 1)
                    finally:
                        release.set()

                    if outcome == "failure":
                        with self.assertRaisesRegex(OSError, "stale failure"):
                            stale_call.result(timeout=1)
                    else:
                        self.assertEqual(stale_call.result(timeout=1), "stale success")

                self.assertEqual(breaker.state, "closed")
                self.assertEqual(breaker.stats().failures, 1)

    async def test_cooldown_sync_and_async_wait_timeout_and_pruning(self):
        cooldowns = CooldownRegistry()
        cooldowns.block("sync", 0.02)
        self.assertFalse(cooldowns.wait("sync", timeout=0))
        self.assertTrue(cooldowns.wait("sync", timeout=0.2))
        cooldowns.block("async", 10)
        self.assertFalse(await cooldowns.wait_async("async", timeout=0))
        waiter = asyncio.create_task(cooldowns.wait_async("async"))
        await asyncio.sleep(0)
        self.assertEqual(cooldowns.stats().waiters, 1)
        waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiter
        self.assertEqual(cooldowns.stats().waiters, 0)
        self.assertTrue(cooldowns.clear("async"))
        self.assertEqual(cooldowns.prune(), 0)

    async def test_retry_observations_deadlines_and_combined_failures(self):
        observations = []
        retry = RetryPolicy(max_attempts=3, initial_delay=0, observer=observations.append)

        def fail():
            raise OSError("temporary")

        with self.assertRaises(OSError):
            retry.run(fail, should_retry=lambda _error: False)
        self.assertEqual(observations[-1].status, "error")

        async def fail_async():
            raise OSError("temporary")

        with self.assertRaises(OSError):
            await retry.run_async(fail_async, should_retry=lambda _error: False)
        self.assertEqual(observations[-1].status, "error")

        with self.assertRaises(TimeoutError):
            await retry.run_async(fail_async, deadline=Deadline.after(0))

        def broken_observer(_observation):
            raise RuntimeError("observer failed")

        with self.assertRaises(ExceptionGroup):
            RetryPolicy(observer=broken_observer).run(fail)

        retry_observations = []
        retry_once = RetryPolicy(
            max_attempts=2,
            initial_delay=0.001,
            max_delay=0.002,
            jitter=0.1,
            observer=retry_observations.append,
        )
        attempts = 0

        def transient():
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise OSError("try again")
            return "recovered"

        self.assertEqual(retry_once.run(transient, should_retry=lambda _error: True), "recovered")
        self.assertEqual([item.status for item in retry_observations], ["retry", "success"])

    async def test_async_circuit_breaker_tracks_async_failure_and_reset(self):
        snapshots = []
        breaker = CircuitBreaker(failure_threshold=1, observer=snapshots.append)

        async def fail():
            raise OSError("failure")

        with self.assertRaises(OSError):
            await breaker.call_async(fail)
        self.assertEqual(breaker.state, "open")
        self.assertTrue(snapshots)
        breaker.reset()
        self.assertEqual(breaker.state, "closed")
        with self.assertRaises(TypeError):
            await breaker.call_async(lambda: 3)

        def broken_observer(_stats):
            raise RuntimeError("observer failed")

        observed_breaker = CircuitBreaker(failure_threshold=1, observer=broken_observer)
        with self.assertRaises(ExceptionGroup):
            observed_breaker.call(lambda: (_ for _ in ()).throw(OSError("operation failed")))
        self.assertEqual(observed_breaker.state, "open")

    async def test_cooldown_capacity_sync_and_cancellable_wait(self):
        clock = ManualClock(10.0)
        cooldowns = CooldownRegistry(max_entries=1, clock=clock)
        self.assertEqual(cooldowns.block("a", 5), 5)
        self.assertEqual(cooldowns.block("a", 2), 5)
        self.assertEqual(cooldowns.block("a", 2, extend=False), 2)
        with self.assertRaises(RuntimeError):
            cooldowns.block("b", 1)
        self.assertFalse(cooldowns.wait("a", timeout=0))
        self.assertEqual(cooldowns.remaining("a"), 2)
        clock.set(13)
        self.assertEqual(cooldowns.prune(), 1)
        self.assertTrue(cooldowns.wait("a", timeout=0))
        self.assertEqual(cooldowns.stats().keys, 0)

        cooldowns.block("wait", 30)
        waiter = asyncio.create_task(cooldowns.wait_async("wait"))
        await asyncio.sleep(0)
        self.assertEqual(cooldowns.stats().waiters, 1)
        waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiter
        self.assertEqual(cooldowns.stats().waiters, 0)
        cooldowns.clear("wait")

    async def test_cooldown_clear_wakes_all_async_waiters(self):
        cooldowns = CooldownRegistry()
        cooldowns.block("shared", 60)
        waiters = [asyncio.create_task(cooldowns.wait_async("shared")) for _ in range(4)]

        for _ in range(10):
            if cooldowns.stats().waiters == len(waiters):
                break
            await asyncio.sleep(0)

        self.assertEqual(cooldowns.stats().waiters, len(waiters))
        self.assertTrue(cooldowns.clear("shared"))
        results = await asyncio.wait_for(asyncio.gather(*waiters), timeout=1)

        self.assertEqual(results, [True] * len(waiters))
        self.assertEqual(cooldowns.stats().waiters, 0)

    async def test_retry_policy_async_deadline_and_invalid_operation(self):
        policy = RetryPolicy(max_attempts=3, initial_delay=0, backoff="fixed")
        attempts = []

        async def transient():
            attempts.append(1)
            if len(attempts) < 2:
                raise OSError("again")
            return "ok"

        self.assertEqual(await policy.run_async(transient, should_retry=lambda error: True), "ok")
        self.assertEqual(len(attempts), 2)
        with self.assertRaises(TypeError):
            await policy.run_async(lambda: 1)
        with self.assertRaises(TimeoutError):
            await policy.run_async(transient, deadline=Deadline.after(0))
        with self.assertRaises(TypeError):
            await policy.run_async(transient, should_retry=object())

    async def test_retry_deadline_does_not_interrupt_started_operations(self):
        with self.subTest(mode="sync"):
            clock = ManualClock()
            deadline = Deadline.after(1, clock=clock)

            def sync_operation():
                clock.set(2.0)
                return "completed"

            result = RetryPolicy(max_attempts=1).run(sync_operation, deadline=deadline)
            self.assertEqual(result, "completed")

        with self.subTest(mode="async"):
            clock = ManualClock()
            deadline = Deadline.after(1, clock=clock)

            async def async_operation():
                clock.set(2.0)
                return "completed"

            result = await RetryPolicy(max_attempts=1).run_async(async_operation, deadline=deadline)
            self.assertEqual(result, "completed")

    async def test_async_retry_cancellation_is_not_retried_and_is_observed(self):
        attempts = 0
        observations = []
        policy = RetryPolicy(max_attempts=4, observer=observations.append)

        async def cancel():
            nonlocal attempts
            attempts += 1
            raise asyncio.CancelledError

        with self.assertRaises(asyncio.CancelledError):
            await policy.run_async(cancel)

        self.assertEqual(attempts, 1)
        self.assertEqual([item.status for item in observations], ["error"])

    async def test_retry_deadline_caps_sync_and_async_backoff(self):
        clock = ManualClock()
        policy = RetryPolicy(max_attempts=3, initial_delay=5, backoff="fixed")

        def fail_sync():
            raise OSError("sync retry")

        with patch("eventflow.resilience.time.sleep") as sleep:
            with self.assertRaises(TimeoutError):
                policy.run(
                    fail_sync,
                    should_retry=lambda _error: True,
                    deadline=Deadline.after(1, clock=clock),
                )
        sleep.assert_called_once_with(1)

        async def fail_async():
            raise OSError("async retry")

        async_delays = []

        async def record_sleep(delay):
            async_delays.append(delay)

        with patch("eventflow.resilience.asyncio.sleep", new=record_sleep):
            with self.assertRaises(TimeoutError):
                await policy.run_async(
                    fail_async,
                    should_retry=lambda _error: True,
                    deadline=Deadline.after(1, clock=clock),
                )
        self.assertEqual(async_delays, [1])

    async def test_circuit_breaker_half_open_and_classifier_behavior(self):
        clock = ManualClock()
        breaker = CircuitBreaker(
            failure_threshold=1,
            recovery_timeout=1,
            clock=clock,
            should_count=lambda error: not isinstance(error, ValueError),
        )
        with self.assertRaises(ValueError):
            breaker.call(lambda: (_ for _ in ()).throw(ValueError("ignored")))
        self.assertEqual(breaker.state, "closed")
        with self.assertRaises(OSError):
            breaker.call(lambda: (_ for _ in ()).throw(OSError("counted")))
        self.assertEqual(breaker.state, "open")
        with self.assertRaises(CircuitOpenError):
            breaker.call(lambda: 1)
        clock.set(2)
        self.assertEqual(breaker.state, "half_open")
        self.assertEqual(breaker.call(lambda: 4), 4)
        self.assertEqual(breaker.state, "closed")
        breaker.reset()
        malformed = CircuitBreaker(should_count=lambda _error: 1)
        with self.assertRaises(ExceptionGroup):
            malformed.call(lambda: (_ for _ in ()).throw(OSError("original")))

        class FalseyClassifier:
            def __bool__(self):
                return False

            def __call__(self, _error):
                return False

        ignored = CircuitBreaker(failure_threshold=1, should_count=FalseyClassifier())
        with self.assertRaises(OSError):
            ignored.call(lambda: (_ for _ in ()).throw(OSError("ignored failure")))
        self.assertEqual(ignored.state, "closed")
