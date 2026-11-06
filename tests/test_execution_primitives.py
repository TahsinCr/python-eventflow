"""Behavioral tests for EventFlow's general execution and coordination primitives."""

from __future__ import annotations

import asyncio
import contextvars
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import eventflow
from eventflow.bridges import AsyncEventQueue, LoopPortal
from eventflow.cache import VersionedCache
from eventflow.compatibility import ExceptionGroup
from eventflow.concurrency import KeyedConcurrencyLimiter, KeyedRateLimiter, SingleFlight
from eventflow.errors import EventQueueClosedError, LoopPortalClosedError, QueueOverflowError
from eventflow.executors import BoundedWorkPool
from eventflow.observability import OperationStats
from eventflow.policy import PolicyEvaluator
from eventflow.registry import ServiceRegistry
from eventflow.resilience import CircuitBreaker, CooldownRegistry, RetryPolicy
from eventflow.scheduling import AsyncScheduler, KeyedCoalescer
from eventflow.scopes import AsyncResourceScope, ResourceScope
from eventflow.state import StateMachine, StateTransitionError, VersionedValue
from eventflow.stream import AsyncStream
from eventflow.tasks import AsyncTaskScope, LatestTaskCoordinator
from eventflow.timing import Deadline
from eventflow import EventBus, ErrorPolicy
from _support import ManualClock


class PrimitiveTests(unittest.TestCase):
    def test_deadline_is_monotonic_and_child_cannot_extend_parent(self):
        clock = ManualClock(10.0)
        deadline = Deadline.after(5, clock=clock)
        child = deadline.child(2)
        clock.set(11.0)
        self.assertEqual(deadline.remaining, 4.0)
        self.assertEqual(child.remaining, 1.0)
        self.assertEqual(child.child(20)._expires_at, 12.0)
        clock.set(13.0)
        self.assertTrue(child.expired)
        self.assertIsNone(Deadline.after(None, clock=clock).remaining)
        for invalid in (-1, float("inf"), True, 10**1000):
            with self.assertRaises((TypeError, ValueError)):
                Deadline.after(invalid)

    def test_state_machine_and_versioned_value_compare_and_set(self):
        changes = []
        machine = StateMachine(
            "queued",
            {"queued": {"running", "cancelled"}, "running": {"done"}},
            terminal={"done", "cancelled"},
            observer=lambda previous, current: changes.append((previous, current)),
        )
        self.assertTrue(machine.can_transition("running"))
        first = machine.transition("running")
        self.assertEqual((first.state, first.version), ("running", 1))
        self.assertIsNone(machine.compare_and_transition(0, "done"))
        self.assertEqual(machine.compare_and_transition(1, "done").version, 2)
        with self.assertRaises(StateTransitionError):
            machine.transition("running")
        self.assertEqual(len(changes), 2)

        value = VersionedValue({"ready": False})
        snapshot = value.snapshot()
        self.assertTrue(value.compare_and_set(snapshot.version, {"ready": True}))
        self.assertFalse(value.compare_and_set(snapshot.version, {"ready": False}))
        self.assertEqual(value.snapshot().version, 1)

    def test_state_machine_runs_guards_outside_lock_and_rechecks_version(self):
        guard_started = threading.Event()
        release_guard = threading.Event()

        def guard(_current, target):
            if target == "running":
                guard_started.set()
                if not release_guard.wait(1):
                    raise TimeoutError("test guard was not released")
            return True

        machine = StateMachine(
            "queued",
            {"queued": {"running", "cancelled"}},
            guard=guard,
        )
        result = []
        worker = threading.Thread(
            target=lambda: result.append(machine.compare_and_transition(0, "running"))
        )
        worker.start()
        self.assertTrue(guard_started.wait(1))
        self.assertEqual(machine.snapshot().state, "queued")
        self.assertEqual(machine.transition("cancelled").state, "cancelled")
        release_guard.set()
        worker.join(1)
        self.assertFalse(worker.is_alive())
        self.assertEqual(result, [None])

    def test_operation_stats_bound_cardinality_and_freeze_snapshots(self):
        stats = OperationStats(max_operations=2)
        stats.record("bus.publish", elapsed=0.25, queue_depth=3, dropped=1)
        stats.record("task.run", status="error", retries=2, timeouts=1)
        stats.record("unbounded-user-key-123", status="cancelled")
        snapshot = stats.snapshot()
        self.assertEqual(set(snapshot.operations), {"bus.publish", "other"})
        self.assertEqual(snapshot.operations["bus.publish"].max_queue_depth, 3)
        self.assertEqual(snapshot.operations["other"].cancelled, 1)
        with self.assertRaises(TypeError):
            snapshot.operations["new"] = snapshot.operations["other"]

    def test_resource_scope_order_borrowing_detach_and_retry(self):
        calls = []

        class Resource:
            def __init__(self, name, fail=False):
                self.name, self.fail = name, fail

            def close(self):
                calls.append(self.name)
                if self.fail:
                    raise ValueError(self.name)

        scope = ResourceScope()
        borrowed = Resource("borrowed")
        failed = Resource("failed", fail=True)
        owned = Resource("owned")
        scope.add(borrowed, owned=False)
        scope.add(failed)
        scope.add(owned)
        detached = Resource("detached")
        scope.add(detached)
        self.assertIs(scope.detach(detached), detached)
        with self.assertRaises(ExceptionGroup) as raised:
            scope.close()
        self.assertIn("Resource scope cleanup failed", str(raised.exception))
        self.assertEqual(calls, ["owned", "failed"])
        self.assertEqual(scope.pending_cleanup(), 1)
        failed.fail = False
        scope.close()
        self.assertEqual(calls, ["owned", "failed", "failed"])
        self.assertEqual(scope.pending_cleanup(), 0)

    def test_versioned_cache_lru_byte_limit_ttl_and_stale_write(self):
        clock = ManualClock(5.0)
        cache = VersionedCache(
            max_entries=2,
            max_bytes=4,
            ttl=2,
            getsizeof=lambda _key, value: len(value),
            clock=clock,
        )
        self.assertTrue(cache.put("a", "aa", version=1))
        self.assertTrue(cache.put("b", "bb", version=3))
        self.assertEqual(cache.get("a"), "aa")
        self.assertTrue(cache.put("c", "cc", version=1))
        self.assertIsNone(cache.get("b"))
        self.assertFalse(cache.put("c", "old", version=0))
        self.assertEqual(cache.stats().stale_writes, 1)
        clock.advance(2)
        self.assertEqual(cache.prune(), 2)
        self.assertEqual(cache.stats().bytes, 0)
        with self.assertRaises(ValueError):
            VersionedCache(max_entries=1, max_bytes=2)

    def test_versioned_cache_uses_falsey_size_callback_for_byte_limit(self):
        class FalseySizer:
            def __bool__(self):
                return False

            def __call__(self, _key, value):
                return len(value)

        cache = VersionedCache(max_entries=2, max_bytes=3, getsizeof=FalseySizer())

        self.assertFalse(cache.put("large", "four", version=1))
        self.assertTrue(cache.put("small", "ok", version=1))

    def test_versioned_cache_ignores_stale_expiry_records(self):
        clock = ManualClock()
        cache = VersionedCache(max_entries=2, clock=clock)
        cache.put("key", "old", version=1, ttl=2)
        clock.set(1)
        cache.put("key", "new", version=2, ttl=5)
        clock.set(2)
        self.assertEqual(cache.get("key"), "new")
        clock.set(6)
        self.assertIsNone(cache.get("key"))

    def test_versioned_cache_forgets_versions_after_eviction(self):
        cache = VersionedCache(max_entries=1)
        self.assertTrue(cache.put("key", "new", version=2))
        self.assertTrue(cache.put("other", "resident", version=1))
        self.assertIsNone(cache.get("key"))
        self.assertTrue(cache.put("key", "older", version=1))

    def test_resource_scope_ignores_reentrant_close_from_cleanup(self):
        scope = ResourceScope()
        scope.defer(scope.close)

        scope.close()

        self.assertTrue(scope.closed)
        self.assertEqual(scope.pending_cleanup(), 0)

    def test_service_registry_closes_owner_resources_in_reverse_order(self):
        calls = []

        class Service:
            def __init__(self, name):
                self.name = name

            def close(self):
                calls.append(self.name)

        services = ServiceRegistry()
        first = services.register(" first ", Service("first"), owner="app", close="close")
        services.register("second", Service("second"), owner="app", close="close")
        self.assertIs(services.resolve(" first "), first)
        self.assertIs(services.resolve("first"), first)
        with self.assertRaises(PermissionError):
            services.unregister("first", owner="other")
        services.close_owner("app")
        self.assertEqual(calls, ["second", "first"])
        self.assertEqual(len(services), 0)

    def test_service_registry_notifies_committed_changes_outside_its_lock(self):
        changes = []
        registry = None

        def observe(change):
            changes.append((change.action, change.name, change.owner))
            self.assertEqual(len(registry), 1 if change.action == "registered" else 0)

        registry = ServiceRegistry(observer=observe)
        registry.register("cache", object(), owner="worker")
        registry.unregister("cache", owner="worker")
        self.assertEqual(
            changes,
            [("registered", "cache", "worker"), ("unregistered", "cache", "worker")],
        )

    def test_service_registry_retries_only_failed_cleanup(self):
        calls = []

        class Service:
            def __init__(self, name, fail=False):
                self.name = name
                self.fail = fail

            def close(self):
                calls.append(self.name)
                if self.fail:
                    raise OSError(self.name)

        services = ServiceRegistry()
        first = Service("first")
        retry = Service("retry", fail=True)
        services.register("first", first, owner="app", close="close")
        services.register("retry", retry, owner="app", close="close")
        with self.assertRaises(ExceptionGroup):
            services.close_owner("app")
        self.assertEqual(calls, ["retry", "first"])
        self.assertEqual(len(services), 1)
        retry.fail = False
        services.close_owner("app")
        self.assertEqual(calls, ["retry", "first", "retry"])
        self.assertEqual(len(services), 0)

    def test_policy_evaluator_defaults_to_deny_and_explicit_deny_wins(self):
        policies = PolicyEvaluator()
        self.assertFalse(
            policies.evaluate(subject="worker", action="write", resource="item").allowed
        )
        policies.add("allow-writes", lambda *_: "allow")
        policies.add(
            "blocked-resource",
            lambda _s, _a, resource, _c: "deny" if resource == "blocked" else None,
        )
        self.assertTrue(policies.evaluate(subject="w", action="write", resource="ok").allowed)
        decision = policies.evaluate(subject="w", action="write", resource="blocked")
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason, "explicit deny")

    def test_bounded_work_pool_separates_running_and_pending(self):
        started = threading.Event()
        release = threading.Event()

        def blocking():
            started.set()
            release.wait(2)
            return 1

        pool = BoundedWorkPool(max_workers=1, max_pending=1)
        first = pool.submit(blocking)
        self.assertTrue(started.wait(1))
        second = pool.submit(lambda: 2)
        self.assertEqual(pool.stats().pending, 1)
        with self.assertRaises(RuntimeError):
            pool.submit(lambda: 3)
        release.set()
        self.assertEqual(first.result(1), 1)
        self.assertEqual(second.result(1), 2)
        pool.shutdown()
        self.assertTrue(pool.stats().closed)

    def test_sync_retry_is_explicit_and_circuit_breaker_recovers(self):
        calls = []
        retry = RetryPolicy(max_attempts=3)

        def operation():
            calls.append(1)
            if len(calls) < 3:
                raise OSError("temporary")
            return "ok"

        self.assertEqual(
            retry.run(operation, should_retry=lambda error: isinstance(error, OSError)), "ok"
        )
        self.assertEqual(len(calls), 3)
        single_attempt = []
        with self.assertRaises(OSError):
            retry.run(lambda: (single_attempt.append(1), (_ for _ in ()).throw(OSError()))[1])
        self.assertEqual(len(single_attempt), 1)
        bounded = RetryPolicy(max_attempts=1_000_000, initial_delay=0.1, max_delay=1)
        self.assertEqual(bounded._delay(1_000_000), 1)

    def test_circuit_breaker_rejects_async_callable_on_sync_path(self):
        breaker = CircuitBreaker(failure_threshold=1)

        async def operation():
            return 1

        with self.assertRaisesRegex(TypeError, "synchronous operation"):
            breaker.call(operation)
        self.assertEqual(breaker.state, "closed")

    def test_sync_extension_callbacks_reject_awaitables_cleanly(self):
        async def async_result():
            return True

        retry = RetryPolicy(max_attempts=2)
        with self.assertRaises(TypeError):
            retry.run(
                lambda: (_ for _ in ()).throw(OSError("temporary")),
                should_retry=lambda _error: async_result(),
            )
        with self.assertRaises(TypeError):
            RetryPolicy(observer=lambda _observation: async_result()).run(lambda: 1)

        policies = PolicyEvaluator(observer=lambda _decision: async_result())
        with self.assertRaises(TypeError):
            policies.evaluate(subject="worker", action="read", resource="item")
        async_rule = PolicyEvaluator()

        async def rule(*_args):
            return "allow"

        async_rule.add("async", rule)
        with self.assertRaises(TypeError):
            async_rule.evaluate(subject="worker", action="read", resource="item")

        cache = VersionedCache(max_entries=1, getsizeof=lambda _key, _value: async_result())
        with self.assertRaises(TypeError):
            cache.put("key", "value", version=1)

        async def async_work():
            return 1

        pool = BoundedWorkPool(max_workers=1, max_pending=0)
        with self.assertRaises(TypeError):
            pool.submit(async_work)
        with self.assertRaises(TypeError):
            pool.submit(lambda: async_result()).result(1)
        pool.close()

        clock = ManualClock()
        breaker = CircuitBreaker(
            failure_threshold=1,
            recovery_timeout=5,
            clock=clock,
        )
        with self.assertRaises(ValueError):
            breaker.call(lambda: (_ for _ in ()).throw(ValueError("failed")))
        self.assertEqual(breaker.state, "open")
        with self.assertRaises(RuntimeError):
            breaker.call(lambda: None)
        clock.set(5.0)
        self.assertEqual(breaker.state, "half_open")
        self.assertEqual(breaker.call(lambda: "recovered"), "recovered")
        self.assertEqual(breaker.state, "closed")

    def test_cooldown_registry_uses_monotonic_deadlines_and_can_clear(self):
        clock = ManualClock(1.0)
        cooldowns = CooldownRegistry(clock=clock)
        cooldowns.block("key", 4)
        self.assertEqual(cooldowns.remaining("key"), 4)
        cooldowns.block("key", 2)
        self.assertEqual(cooldowns.remaining("key"), 4)
        cooldowns.block("key", 2, extend=False)
        self.assertEqual(cooldowns.remaining("key"), 2)
        clock.advance(2)
        self.assertFalse(cooldowns.is_blocked("key"))
        self.assertEqual(cooldowns.prune(), 0)

    def test_all_new_exports_are_lazy_and_discoverable(self):
        self.assertIn("AsyncEventQueue", eventflow.__all__)
        self.assertIn("VersionedCache", dir(eventflow))

    def test_operation_observers_are_validated_at_construction(self):
        bus = EventBus[int]()
        invalid_observers = (
            lambda: AsyncEventQueue(bus, observer=object()),
            lambda: AsyncTaskScope(observer=object()),
            lambda: KeyedConcurrencyLimiter(1, observer=object()),
            lambda: KeyedRateLimiter(1, observer=object()),
            lambda: BoundedWorkPool(max_workers=1, max_pending=0, observer=object()),
            lambda: KeyedCoalescer(
                interval=1,
                key=lambda value: value,
                merge=lambda first, _second: first,
                callback=lambda _value: None,
                observer=object(),
            ),
            lambda: AsyncScheduler(observer=object()),
        )
        try:
            for create in invalid_observers:
                with self.subTest(create=create):
                    with self.assertRaises(TypeError):
                        create()
        finally:
            bus.close()


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


class ThreadBridgeTests(unittest.TestCase):
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
            release_worker.wait(2)

        async def operation():
            await asyncio.to_thread(blocking_work)

        portal.submit(operation())
        try:
            self.assertTrue(worker_started.wait(1))
            with self.assertRaises(TimeoutError):
                portal.close(timeout=0.01)
            self.assertFalse(portal.running)
        finally:
            release_worker.set()
            portal.close(timeout=2)

    def test_keyed_coalescer_merges_and_flushes_with_bounded_keys(self):
        calls = []
        coalescer = KeyedCoalescer(
            interval=10,
            key=lambda item: item[0],
            merge=lambda first, second: (first[0], first[1] + second[1]),
            callback=calls.append,
            max_pending_keys=1,
        )
        coalescer.push(("key", 1))
        coalescer.push(("key", 2))
        with self.assertRaises(RuntimeError):
            coalescer.push(("other", 3))
        self.assertEqual(coalescer.stats().merged, 1)
        coalescer.flush(1)
        coalescer.close()
        self.assertEqual(calls, [("key", 3)])

    def test_keyed_coalescer_starts_lazily_and_serializes_parallel_merges(self):
        calls = []
        coalescer = KeyedCoalescer(
            interval=60,
            key=lambda item: item[0],
            merge=lambda first, second: (first[0], first[1] + second[1]),
            callback=calls.append,
            max_pending_keys=1,
        )
        self.assertIsNone(coalescer._thread.ident)
        coalescer.flush(timeout=0.1)
        self.assertIsNone(coalescer._thread.ident)

        barrier = threading.Barrier(5)

        def push_many(_index):
            barrier.wait(timeout=1)
            for _ in range(10):
                coalescer.push(("key", 1))

        try:
            with ThreadPoolExecutor(max_workers=4) as producers:
                futures = [producers.submit(push_many, index) for index in range(4)]
                barrier.wait(timeout=1)
                for future in futures:
                    future.result(timeout=2)
            coalescer.flush(timeout=1)
            self.assertEqual(coalescer.stats().merged, 39)
            self.assertEqual(calls, [("key", 40)])
        finally:
            coalescer.close(timeout=1)

    def test_coalescer_rejects_async_key_and_merge_functions(self):
        async def async_key(value):
            return value

        with KeyedCoalescer(
            interval=10,
            key=async_key,
            merge=lambda first, _second: first,
            callback=lambda _value: None,
        ) as coalescer:
            with self.assertRaises(TypeError):
                coalescer.push("value")

        async def async_merge(first, _second):
            return first

        coalescer = KeyedCoalescer(
            interval=10,
            key=lambda value: value[0],
            merge=async_merge,
            callback=lambda _value: None,
        )
        coalescer.push(("key", 1))
        with self.assertRaises(TypeError):
            coalescer.push(("key", 2))
        coalescer.close(flush=False)


if __name__ == "__main__":
    unittest.main()
