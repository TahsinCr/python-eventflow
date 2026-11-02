"""Edge-case contracts for the opt-in execution primitives."""

from __future__ import annotations

import asyncio
import concurrent.futures
import threading
import unittest
from unittest.mock import patch

from eventflow.bridges import AsyncEventQueue, LoopPortal
from eventflow.cache import VersionedCache
from eventflow.concurrency import KeyedConcurrencyLimiter, KeyedRateLimiter, SingleFlight
from eventflow.compatibility import ExceptionGroup
from eventflow.errors import CircuitOpenError, EventQueueClosedError, LoopPortalClosedError
from eventflow.event_bus import EventBus
from eventflow.executors import BoundedWorkPool
from eventflow.observability import OperationStats
from eventflow.policy import PolicyEvaluator
from eventflow.registry import ServiceRegistry
from eventflow.resilience import CircuitBreaker, CooldownRegistry, RetryPolicy
from eventflow.scheduling import AsyncScheduler, KeyedCoalescer
from eventflow.scopes import AsyncResourceScope, ResourceScope
from eventflow.state import StateMachine, StateTransitionError, VersionedValue
from eventflow.tasks import AsyncTaskScope, LatestTaskCoordinator
from eventflow.timing import Deadline


class TimingStateCacheTests(unittest.TestCase):
    def test_deadline_validation_and_unlimited_child(self):
        now = [4.0]

        def clock():
            return now[0]

        deadline = Deadline.after(0, clock=clock)
        self.assertEqual(deadline.remaining, 0)
        self.assertTrue(deadline.expired)
        self.assertIs(deadline.child(None), deadline)
        absolute = Deadline.at(8, clock=clock)
        self.assertEqual(absolute.remaining, 4)
        now[0] = 9
        self.assertEqual(absolute.remaining, 0)
        with self.assertRaises(ValueError):
            Deadline.after(10**1000)
        with self.assertRaises(TypeError):
            Deadline.at(True)
        with self.assertRaises(ValueError):
            Deadline.at(float("inf"))
        with self.assertRaises(ValueError):
            Deadline.after(1, clock=lambda: float("nan"))
        bad_clock_deadline = Deadline.at(1, clock=lambda: float("inf"))
        with self.assertRaises(ValueError):
            _ = bad_clock_deadline.remaining
        with self.assertRaises(ValueError):
            Deadline(float("nan"))

    def test_state_graph_guards_observers_and_compare_and_set_validation(self):
        guarded = StateMachine("new", {"new": {"ready"}}, guard=lambda *_: False)
        self.assertFalse(guarded.can_transition("ready"))
        self.assertFalse(guarded.can_transition("missing"))
        with self.assertRaises(StateTransitionError):
            guarded.transition("ready")

        closed = []

        async def async_guard(*_args):
            return True

        async_guard_result = async_guard("old", "new")
        machine = StateMachine("old", {"old": {"new"}}, guard=lambda *_: async_guard_result)
        with self.assertRaisesRegex(TypeError, "synchronous"):
            machine.transition("new")
        self.assertIsNone(async_guard_result.cr_frame)

        async def async_observer(*_args):
            return None

        observer_result = async_observer(None, None)

        def observe(previous, current):
            closed.append((previous, current))
            return observer_result

        machine = StateMachine(
            0,
            {0: {1}},
            observer=observe,
        )
        with self.assertRaisesRegex(TypeError, "synchronous"):
            machine.transition(1)
        self.assertEqual(machine.state, 1)
        self.assertIsNone(observer_result.cr_frame)
        self.assertEqual(len(closed), 1)

        for constructor in (
            lambda: StateMachine(0, []),
            lambda: StateMachine(0, {1: {2}}),
            lambda: StateMachine(0, {0: {1}}, terminal={3}),
            lambda: StateMachine(0, {0: {1}}, guard=object()),
            lambda: StateMachine(0, {0: {1}}, observer=object()),
        ):
            with self.assertRaises((TypeError, ValueError)):
                constructor()
        for expected in (True, -1, 1.5):
            with self.assertRaises((TypeError, ValueError)):
                machine.compare_and_transition(expected, 1)

        value = VersionedValue("start")
        self.assertTrue(value.compare_and_set(0, "done"))
        self.assertFalse(value.compare_and_set(0, "stale"))
        for expected in (True, -1, 0.5):
            with self.assertRaises((TypeError, ValueError)):
                value.compare_and_set(expected, "invalid")

    def test_cache_bounds_validation_removal_and_version_misses(self):
        with self.assertRaises(ValueError):
            VersionedCache(max_entries=0)
        with self.assertRaises(ValueError):
            VersionedCache(max_entries=1, max_bytes=0, getsizeof=lambda *_: 1)
        with self.assertRaises(ValueError):
            VersionedCache(max_entries=1, max_bytes=2)
        with self.assertRaises((TypeError, ValueError)):
            VersionedCache(max_entries=1, ttl=0)
        with self.assertRaises(TypeError):
            VersionedCache(max_entries=1, getsizeof=object())
        with self.assertRaises(TypeError):
            VersionedCache(max_entries=1, clock=object())

        cache = VersionedCache(
            max_entries=2,
            max_bytes=2,
            getsizeof=lambda _key, value: len(value),
        )
        self.assertFalse(cache.put("large", "abc", version=1))
        self.assertTrue(cache.put("item", "x", version=2))
        self.assertIsNone(cache.get("item", version=1))
        self.assertEqual(cache.get("item", version=2), "x")
        self.assertEqual(cache.remove("item"), "x")
        self.assertIsNone(cache.remove("item"))
        self.assertEqual(cache.stats().misses, 1)
        cache.put("next", "y", version=1)
        cache.clear()
        self.assertEqual(cache.stats().entries, 0)
        with self.assertRaises(ValueError):
            cache.put("bad", "x", version=True)
        with self.assertRaises(ValueError):
            cache.get("bad", version=-1)


class RegistryPolicyExecutorTests(unittest.TestCase):
    def test_registry_validation_cleanup_retry_snapshot_and_observer_failure(self):
        changes = []
        registry = ServiceRegistry(observer=changes.append)
        with self.assertRaises(TypeError):
            registry.register("async", object(), owner="owner", close="missing")
        with self.assertRaises(ValueError):
            registry.register("   ", object(), owner="owner")
        service = object()
        registry.register(" item ", service, owner="owner")
        with self.assertRaises(KeyError):
            registry.register("item", object(), owner="other")
        with self.assertRaises(PermissionError):
            registry.unregister("item", owner="other")
        with self.assertRaises(KeyError):
            registry.resolve("missing")
        with self.assertRaises(TypeError):
            registry.resolve(None)
        with self.assertRaises(TypeError):
            registry.snapshot()["new"] = object()
        self.assertIs(registry.unregister("item", owner="owner"), service)
        self.assertEqual([change.action for change in changes], ["registered", "unregistered"])
        registry.close_owner("empty")

        calls = []

        class Retryable:
            fail = True

            def close(self):
                calls.append("close")
                if self.fail:
                    raise OSError("retry")

        retryable = Retryable()
        registry.register("retry", retryable, owner="owner", close="close")
        with self.assertRaises(ExceptionGroup):
            registry.unregister("retry", owner="owner")
        self.assertEqual(len(registry), 1)
        retryable.fail = False
        registry.unregister("retry", owner="owner")
        self.assertEqual(calls, ["close", "close"])

        async def async_cleanup(_service):
            return None

        registry = ServiceRegistry()
        registry.register("async", object(), owner="owner", close=async_cleanup)
        with self.assertRaises(ExceptionGroup):
            registry.close_owner("owner")
        self.assertEqual(len(registry), 1)

        def broken_observer(_change):
            raise RuntimeError("observer")

        committed = ServiceRegistry(observer=broken_observer)
        with self.assertRaisesRegex(RuntimeError, "observer"):
            committed.register("kept", object(), owner="owner")
        self.assertEqual(len(committed), 1)

    def test_registry_groups_all_cleanup_failures_and_retains_entries_for_retry(self):
        calls = []

        def broken(name):
            def close(_service):
                calls.append(name)
                raise OSError(name)

            return close

        registry = ServiceRegistry()
        registry.register("first", object(), owner="owner", close=broken("first"))
        registry.register("second", object(), owner="owner", close=broken("second"))
        with self.assertRaises(ExceptionGroup) as caught:
            registry.close_owner("owner")
        self.assertEqual(calls, ["second", "first"])
        self.assertEqual(len(caught.exception.exceptions), 2)
        self.assertEqual(len(registry), 2)

    def test_policy_rules_are_ordered_snapshots_and_default_deny(self):
        policy = PolicyEvaluator(default="allow")
        policy.add("abstain", lambda *_: "abstain")
        policy.add("allow", lambda *_: True)
        decision = policy.evaluate(subject="worker", action="read", resource="item")
        self.assertTrue(decision.allowed)
        self.assertEqual(decision.matched, ("allow",))
        view = policy.snapshot()
        with self.assertRaises(TypeError):
            view["new"] = lambda *_: False
        policy.add("deny", lambda *_: "deny")
        decision = policy.evaluate(subject="worker", action="read", resource="item")
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.matched, ("deny",))
        self.assertNotIn("deny", view)
        self.assertIsNotNone(policy.remove("deny"))
        self.assertTrue(policy.evaluate(subject=1, action="read", resource=2).allowed)
        for name in ("", "  "):
            with self.assertRaises(ValueError):
                policy.add(name, lambda *_: None)
        with self.assertRaises(KeyError):
            policy.add("allow", lambda *_: None)
        with self.assertRaises(ValueError):
            policy.evaluate(subject=1, action=" ", resource=2)
        with self.assertRaises(TypeError):
            policy.add("bad", object())
        bad_result = PolicyEvaluator()
        bad_result.add("bad", lambda *_: 1)
        with self.assertRaises(TypeError):
            bad_result.evaluate(subject=1, action="read", resource=2)
        with self.assertRaises(ValueError):
            PolicyEvaluator(default="unknown")
        with self.assertRaises(TypeError):
            PolicyEvaluator(observer=object())

    def test_bounded_pool_rejection_cancellation_and_worker_shutdown_guard(self):
        with self.assertRaises(ValueError):
            BoundedWorkPool(max_workers=0, max_pending=0)
        with self.assertRaises(TypeError):
            BoundedWorkPool(max_workers=1, max_pending=True)
        pool = BoundedWorkPool(max_workers=1, max_pending=1)
        with self.assertRaises(TypeError):
            pool.submit(object())

        async def async_callable():
            return 1

        with self.assertRaises(TypeError):
            pool.submit(async_callable)
        blocker = threading.Event()
        started = threading.Event()

        def wait():
            started.set()
            blocker.wait(1)

        running = pool.submit(wait)
        self.assertTrue(started.wait(1))
        pending = pool.submit(lambda: 2)
        self.assertTrue(pending.cancel())
        blocker.set()
        running.result(1)
        self.assertTrue(pending.cancelled())
        pool.shutdown()

    def test_work_pool_cleans_awaitables_and_rejects_worker_self_wait(self):
        pool = BoundedWorkPool(max_workers=1, max_pending=0)

        async def result():
            return 1

        coroutine = result()
        future = pool.submit(lambda: coroutine)
        with self.assertRaises(TypeError):
            future.result(1)
        self.assertIsNone(coroutine.cr_frame)

        self_wait = pool.submit(pool.shutdown)
        with self.assertRaises(RuntimeError):
            self_wait.result(1)
        pool.shutdown()
        pool.shutdown()
        self.assertTrue(pool.stats().closed)
        with self.assertRaises(RuntimeError):
            pool.submit(lambda: 1)


class AsyncCoordinationEdgeTests(unittest.IsolatedAsyncioTestCase):
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

    async def test_periodic_scheduler_caps_parallel_callbacks(self):
        observer = OperationStats()
        scheduler = AsyncScheduler(observer=observer)
        entered = asyncio.Event()
        calls = 0

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
        await asyncio.sleep(0.015)
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

    async def test_cooldown_capacity_sync_and_cancellable_wait(self):
        now = [10.0]
        cooldowns = CooldownRegistry(max_entries=1, clock=lambda: now[0])
        self.assertEqual(cooldowns.block("a", 5), 5)
        self.assertEqual(cooldowns.block("a", 2), 5)
        self.assertEqual(cooldowns.block("a", 2, extend=False), 2)
        with self.assertRaises(RuntimeError):
            cooldowns.block("b", 1)
        self.assertFalse(cooldowns.wait("a", timeout=0))
        self.assertEqual(cooldowns.remaining("a"), 2)
        now[0] = 13
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

    async def test_retry_deadline_does_not_interrupt_started_async_operation(self):
        now = [0.0]
        deadline = Deadline.after(1, clock=lambda: now[0])

        async def operation():
            now[0] = 2.0
            return "completed"

        result = await RetryPolicy(max_attempts=1).run_async(operation, deadline=deadline)
        self.assertEqual(result, "completed")

    async def test_circuit_breaker_half_open_and_classifier_behavior(self):
        now = [0.0]
        breaker = CircuitBreaker(
            failure_threshold=1,
            recovery_timeout=1,
            clock=lambda: now[0],
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
        now[0] = 2
        self.assertEqual(breaker.state, "half_open")
        self.assertEqual(breaker.call(lambda: 4), 4)
        self.assertEqual(breaker.state, "closed")
        breaker.reset()
        malformed = CircuitBreaker(should_count=lambda _error: 1)
        with self.assertRaises(ExceptionGroup):
            malformed.call(lambda: (_ for _ in ()).throw(OSError("original")))

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

    async def test_latest_task_reports_factory_failures_and_closes_cleanly(self):
        coordinator = LatestTaskCoordinator[str]()
        errors = []

        async def fail():
            raise OSError("latest failed")

        task = coordinator.submit("key", fail, lambda _value: None, on_error=errors.append)
        await task
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], OSError)
        await coordinator.aclose()
        with self.assertRaises(RuntimeError):
            coordinator.submit("key", fail, lambda _value: None)

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

    @staticmethod
    async def _value(value):
        return value


class CoalescerEdgeTests(unittest.TestCase):
    def test_callback_failures_are_visible_and_pending_work_can_be_discarded(self):
        reported = []

        def broken(_value):
            raise ValueError("delivery")

        coalescer = KeyedCoalescer(
            interval=10,
            key=lambda value: value,
            merge=lambda _first, second: second,
            callback=broken,
            on_error=reported.append,
        )
        coalescer.push("error")
        with self.assertRaises(ExceptionGroup):
            coalescer.flush(1)
        self.assertEqual(len(reported), 1)
        coalescer.close()

        discarded = KeyedCoalescer(
            interval=10,
            key=lambda value: value,
            merge=lambda _first, second: second,
            callback=lambda _value: None,
        )
        discarded.push("pending")
        discarded.close(flush=False)
        self.assertEqual(discarded.stats().discarded, 1)


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
