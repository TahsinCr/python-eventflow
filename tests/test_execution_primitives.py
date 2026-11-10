"""Synchronous execution primitive behavior."""

from __future__ import annotations

import threading
import unittest

from _support import ManualClock

from eventflow import EventBus
from eventflow._compatibility import ExceptionGroup
from eventflow.bridges import AsyncEventQueue
from eventflow.cache import VersionedCache
from eventflow.concurrency import KeyedConcurrencyLimiter, KeyedRateLimiter
from eventflow.executors import BoundedWorkPool
from eventflow.observability import OperationStats
from eventflow.policy import PolicyEvaluator
from eventflow.registry import ServiceRegistry
from eventflow.resilience import CircuitBreaker, CooldownRegistry, RetryPolicy
from eventflow.scheduling import AsyncScheduler, KeyedCoalescer
from eventflow.scopes import ResourceScope
from eventflow.state import StateMachine, StateTransitionError, VersionedValue
from eventflow.tasks import AsyncTaskScope
from eventflow.timing import Deadline


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

    def test_bounded_work_pool_uses_configured_thread_name_prefix(self):
        pool = BoundedWorkPool(
            max_workers=1,
            max_pending=0,
            thread_name_prefix="MavDispatcher",
        )
        try:
            worker_name = pool.submit(lambda: threading.current_thread().name).result(1)
        finally:
            pool.shutdown()

        self.assertEqual(worker_name, "MavDispatcher_0")

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

    def test_cooldown_prunes_other_expired_keys_only_when_capacity_requires_it(self):
        clock = ManualClock(0.0)
        cooldowns = CooldownRegistry(max_entries=2, clock=clock)
        cooldowns.block("expired", 1)
        cooldowns.block("active", 10)
        clock.advance(1)

        self.assertEqual(cooldowns.block("active", 2), 9)
        self.assertEqual(cooldowns.block("new", 2), 2)
        self.assertFalse(cooldowns.is_blocked("expired"))
        self.assertTrue(cooldowns.is_blocked("active"))
        self.assertTrue(cooldowns.is_blocked("new"))

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

    def test_operation_stats_reject_invalid_public_record_values(self):
        observer = OperationStats()
        invalid_records = (
            ("", {}, ValueError),
            ("publish", {"status": "unknown"}, ValueError),
            ("publish", {"elapsed": True}, TypeError),
            ("publish", {"elapsed": float("nan")}, ValueError),
            ("publish", {"elapsed": -1}, ValueError),
            ("publish", {"queue_depth": True}, ValueError),
            ("publish", {"queue_depth": -1}, ValueError),
            ("publish", {"dropped": 1.5}, ValueError),
            ("publish", {"retries": True}, ValueError),
            ("publish", {"rejected": -1}, ValueError),
            ("publish", {"timeouts": 1.0}, ValueError),
            ("publish", {"shutdown_seconds": float("inf")}, ValueError),
        )

        for name, values, error_type in invalid_records:
            with self.subTest(name=name, values=values):
                with self.assertRaises(error_type):
                    observer.record(name, **values)
