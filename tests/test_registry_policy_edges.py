"""Edge cases for registry, policy, and work-pool primitives."""

from __future__ import annotations

import threading
import unittest

from eventflow.compatibility import ExceptionGroup
from eventflow.executors import BoundedWorkPool
from eventflow.observability import OperationStats
from eventflow.policy import PolicyEvaluator
from eventflow.registry import ServiceRegistry


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

    def test_policy_preserves_falsey_context_for_explicit_denies(self):
        class FalseyContext(dict):
            def __bool__(self):
                return False

        policy = PolicyEvaluator(default="allow")
        policy.add("blocked", lambda _s, _a, _r, context: "deny" if context["blocked"] else None)

        decision = policy.evaluate(
            subject="worker",
            action="read",
            resource="item",
            context=FalseyContext(blocked=True),
        )

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.matched, ("blocked",))

    def test_policy_rejects_outcomes_that_only_compare_equal_to_allow(self):
        class AllowImpostor:
            def __eq__(self, other):
                return other == "allow"

        with self.assertRaises(ValueError):
            PolicyEvaluator(default=AllowImpostor())

        policy = PolicyEvaluator()
        policy.add("invalid", lambda *_: AllowImpostor())
        with self.assertRaises(TypeError):
            policy.evaluate(subject="worker", action="read", resource="item")

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

    def test_bounded_pool_rejection_observer_runs_outside_lock_and_keeps_error(self):

        pool_ref = {}
        inspected = threading.Event()
        observer_threads = []
        observer = OperationStats()
        original_record = observer.record

        def record(operation, **metrics):
            if operation != "work_pool.submit":
                return original_record(operation, **metrics)

            def inspect_pool():
                pool_ref["pool"].stats()
                inspected.set()

            thread = threading.Thread(target=inspect_pool, daemon=True)
            observer_threads.append(thread)
            thread.start()
            if not inspected.wait(timeout=0.5):
                raise ValueError("reentrant stats call was blocked")
            raise LookupError("observer failed")

        observer.record = record  # type: ignore[method-assign]
        pool = BoundedWorkPool(max_workers=1, max_pending=0, observer=observer)
        pool_ref["pool"] = pool
        release, started = threading.Event(), threading.Event()

        def blocking_work():
            started.set()
            release.wait(timeout=2)

        try:
            pool.submit(blocking_work)
            self.assertTrue(started.wait(timeout=1))
            with self.assertRaisesRegex(RuntimeError, "capacity is full") as caught:
                pool.submit(lambda: None)
            self.assertIsInstance(caught.exception.__cause__, LookupError)
            self.assertTrue(inspected.is_set())
            for thread in observer_threads:
                thread.join(timeout=1)
                self.assertFalse(thread.is_alive())
        finally:
            release.set()
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
        self.assertFalse(pool.stats().closed)
        pool.shutdown()
        pool.shutdown()
        self.assertTrue(pool.stats().closed)
        with self.assertRaises(RuntimeError):
            pool.submit(lambda: 1)

    def test_work_pool_done_callback_rejects_waiting_for_its_own_shutdown(self):
        pool = BoundedWorkPool(max_workers=1, max_pending=0)
        started = threading.Event()
        release = threading.Event()
        callback_done = threading.Event()
        errors = []

        def work():
            started.set()
            release.wait(1)
            return 1

        future = pool.submit(work)
        self.assertTrue(started.wait(1))

        def shutdown_from_callback(_future):
            try:
                pool.shutdown(wait=True)
            except RuntimeError as error:
                errors.append(error)
            finally:
                callback_done.set()

        future.add_done_callback(shutdown_from_callback)
        release.set()
        try:
            self.assertEqual(future.result(timeout=1), 1)
            self.assertTrue(callback_done.wait(1))
            self.assertEqual(len(errors), 1)
            self.assertEqual(
                str(errors[0]), "A BoundedWorkPool worker cannot wait for its own shutdown"
            )
            self.assertFalse(pool.stats().closed)
        finally:
            pool.shutdown(wait=True)
