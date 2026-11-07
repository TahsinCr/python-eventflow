"""Synchronous event bus behavior and lifecycle contracts."""

from __future__ import annotations

import asyncio
import contextvars
import gc
import threading
import unittest
import warnings
from collections import deque
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import cast
from unittest.mock import patch

from _support import _FailingAppendHistory, _FailingQueryHistory, _RecordingQueryHistory

from eventflow import (
    AsyncEventBus,
    AsyncEventBusActions,
    BaseEventBus,
    ErrorPolicy,
    EventBus,
    EventBusActions,
    EventBusClosedError,
    EventBusError,
    EventEngine,
    EventErrorContext,
    EventFilter,
    EventTimeoutContext,
    InvalidEventHandlerError,
    MemoryEventHistory,
    PublishResult,
    correlation_context,
    current_correlation_id,
)
from eventflow.compatibility import ExceptionGroup


class EventBusTests(unittest.TestCase):
    def test_executor_callbacks_receive_isolated_publish_contexts(self) -> None:
        marker = contextvars.ContextVar("executor-callback-marker", default=None)
        seen: list[tuple[int, str | None, str | None]] = []

        def callback(event: int) -> None:
            seen.append((event, current_correlation_id(), marker.get()))
            marker.set("worker-local")

        with ThreadPoolExecutor(max_workers=1) as executor:
            bus = EventBus[int](executor=executor)
            bus.subscribe(callback)
            try:
                with correlation_context("request-one"):
                    bus.publish(1)
                with correlation_context("request-two"):
                    bus.publish(2)
            finally:
                bus.close()

        self.assertEqual(seen, [(1, "request-one", None), (2, "request-two", None)])
        self.assertIsNone(marker.get())

    def test_falsey_event_filter_is_preserved(self) -> None:
        class FalseyFilter(EventFilter[int]):
            def __bool__(self) -> bool:
                return False

        seen: list[int] = []
        bus = EventBus[int]()
        bus.subscribe(seen.append, event_filter=FalseyFilter(event_type=int))

        result = bus.publish("not an int")  # type: ignore[arg-type]

        self.assertEqual(result.matched, 0)
        self.assertEqual(seen, [])

    def test_falsey_actions_are_preserved_by_bus_and_engine(self) -> None:
        class FalseyActions(EventBusActions[int]):
            def __bool__(self) -> bool:
                return False

        seen: list[int] = []
        actions = FalseyActions(on_before=seen.append)
        bus = EventBus[int](actions=actions)
        engine = EventEngine(actions=actions)

        bus.publish(1)
        engine.publish("channel", 2)

        self.assertEqual(seen, [1, 2])

    def test_parametrized_event_contracts_construct(self) -> None:
        event_filter = EventFilter[int](predicate=lambda value: value > 0)
        error = EventErrorContext[int](1, RuntimeError("broken"))
        timeout = EventTimeoutContext[int](event_filter, 0.1)

        self.assertTrue(event_filter.matches(1))
        self.assertEqual(str(error.error), "broken")
        self.assertEqual(timeout.timeout, 0.1)
        self.assertIsInstance(EventBusActions[int](), EventBusActions)
        self.assertIsInstance(AsyncEventBusActions[int](), AsyncEventBusActions)
        with self.assertRaises(AttributeError):
            event_filter.predicate = None  # type: ignore[misc]

    def test_event_filter_rejects_async_predicates(self) -> None:
        async def predicate(_event: int) -> bool:
            return True

        class AsyncPredicate:
            async def __call__(self, _event: int) -> bool:
                return True

        with self.assertRaisesRegex(TypeError, "must be synchronous"):
            EventFilter[int](predicate=predicate)
        with self.assertRaisesRegex(TypeError, "must be synchronous"):
            EventFilter[int](predicate=AsyncPredicate())  # type: ignore[arg-type]

        async def deferred_result() -> bool:
            return True

        result = deferred_result()
        sync_predicate = cast(Callable[[int], bool], lambda _event: result)
        event_filter = EventFilter[int](predicate=sync_predicate)
        with self.assertRaisesRegex(TypeError, "return a boolean synchronously"):
            event_filter.matches(1)
        self.assertIsNone(result.cr_frame)

    def test_bus_inherits_base_and_filters_by_type_and_predicate(self) -> None:
        bus = EventBus[object]()
        received: list[int] = []
        bus.subscribe(
            received.append,
            event_type=int,
            predicate=lambda value: value > 1,
        )

        self.assertIsInstance(bus, BaseEventBus)
        self.assertEqual(bus.publish("ignored").matched, 0)
        self.assertEqual(bus.publish(1).matched, 0)
        result = bus.publish(2)
        self.assertEqual(received, [2])
        self.assertEqual((result.matched, result.delivered, result.failed), (1, 1, 0))

    def test_once_subscription_and_idempotent_cancellation(self) -> None:
        bus = EventBus[int]()
        received: list[int] = []
        subscription = bus.subscribe(received.append, once=True)

        bus.publish(1)
        bus.publish(2)
        subscription.cancel()
        subscription.cancel()

        self.assertEqual(received, [1])
        self.assertFalse(subscription.active)
        self.assertEqual(bus.subscriber_count, 0)

    def test_final_subscription_claim_is_restored_when_removal_fails(self) -> None:
        bus = EventBus[int]()
        received: list[int] = []
        subscription = bus.subscribe(received.append, once=True)
        original_cancel = subscription._cancel
        attempts = 0

        def fail_once() -> None:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("subscription removal failed")
            original_cancel()

        with patch.object(subscription, "_cancel", side_effect=fail_once):
            with self.assertRaisesRegex(RuntimeError, "removal failed"):
                bus.publish(1)
            self.assertTrue(subscription.active)
            result = bus.publish(2)

        self.assertEqual(result.delivered, 1)
        self.assertEqual(received, [2])
        self.assertFalse(subscription.active)
        bus.close()

    def test_failed_history_append_does_not_increment_published(self) -> None:
        bus = EventBus[int](history=_FailingAppendHistory())
        with self.assertRaisesRegex(RuntimeError, "history append failed"):
            bus.publish(1)
        self.assertEqual(bus.stats.published, 0)
        bus.close()

    def test_publish_without_subscribers_records_event_and_stats(self) -> None:
        history = MemoryEventHistory[int](capacity=2)
        bus = EventBus[int](history=history)

        result = bus.publish(7)

        self.assertEqual((result.matched, result.delivered, result.failed), (0, 0, 0))
        self.assertEqual(history.query(), (7,))
        self.assertEqual((bus.stats.published, bus.stats.delivered, bus.stats.failed), (1, 0, 0))
        bus.close()

    def test_executor_rejection_preserves_finite_subscription_quota(self) -> None:
        received: list[int] = []
        with ThreadPoolExecutor(max_workers=1) as executor:
            bus = EventBus[int](executor=executor)
            subscription = bus.subscribe(received.append, times=2)
            original_submit = executor.submit
            attempts = 0

            def reject_once(function, /, *args, **kwargs):
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    raise RuntimeError("executor is shutting down")
                return original_submit(function, *args, **kwargs)

            with patch.object(executor, "submit", side_effect=reject_once):
                rejected = bus.publish(1)
                self.assertEqual((rejected.delivered, rejected.failed), (0, 1))
                self.assertTrue(subscription.active)
                self.assertEqual(bus.publish(2).delivered, 1)
                self.assertTrue(subscription.active)
                self.assertEqual(bus.publish(3).delivered, 1)
            self.assertEqual(received, [2, 3])
            self.assertFalse(subscription.active)
            bus.close()

    def test_wait_for_rejects_timeout_larger_than_float_range(self) -> None:
        bus = EventBus[int]()
        with self.assertRaises(ValueError):
            bus.wait_for(timeout=10**1000)
        bus.close()
        with self.assertRaisesRegex(ValueError, "positive and finite"):
            EventBus[int](shutdown_timeout=10**1000)

    def test_once_subscription_is_atomic_across_publisher_threads(self) -> None:
        bus = EventBus[int]()
        received: list[int] = []
        bus.subscribe(received.append, once=True)

        with ThreadPoolExecutor(max_workers=8) as publishers:
            tuple(publishers.map(bus.publish, range(100)))

        self.assertEqual(len(received), 1)

    def test_isolated_and_raising_error_policies(self) -> None:
        isolated = EventBus[int]()
        received: list[int] = []
        isolated.subscribe(lambda _event: (_ for _ in ()).throw(RuntimeError("broken")))
        isolated.subscribe(received.append)

        result = isolated.publish(3)
        self.assertEqual(received, [3])
        self.assertEqual((result.delivered, result.failed), (1, 1))
        self.assertEqual(isolated.stats.failed, 1)

        raising = EventBus[int](error_policy=ErrorPolicy.RAISE)
        raising.subscribe(lambda _event: (_ for _ in ()).throw(RuntimeError("broken")))
        with self.assertRaises(ExceptionGroup):
            raising.publish(3)

    def test_before_hook_failure_does_not_gate_sync_delivery(self) -> None:
        delivered: list[int] = []

        def reject(_event: int) -> None:
            raise PermissionError("not authorized")

        with EventBus[int](on_before=reject, error_policy=ErrorPolicy.RAISE) as bus:
            bus.subscribe(delivered.append)
            with self.assertRaises(ExceptionGroup):
                bus.publish(7)
        self.assertEqual(delivered, [7])

    def test_bounded_history_query_latest_and_replay(self) -> None:
        history = MemoryEventHistory[int](capacity=3)
        bus = EventBus[int](history=history)
        for value in range(5):
            bus.publish(value)

        self.assertEqual(history.query(), (2, 3, 4))
        self.assertEqual(
            history.latest(EventFilter(predicate=lambda value: value % 2 == 1)),
            3,
        )
        replayed: list[int] = []
        bus.subscribe(replayed.append, replay=2)
        self.assertEqual(replayed, [3, 4])

    def test_unfiltered_replay_limits_the_history_query(self) -> None:
        history = _RecordingQueryHistory()
        for value in range(5):
            history.append(value)
        bus = EventBus[int](history=history)
        replayed: list[int] = []

        bus.subscribe(replayed.append, replay=2)
        self.assertEqual(replayed, [3, 4])
        self.assertEqual(history.limits, [2])

        bus.subscribe(replayed.append, predicate=lambda value: value % 2 == 0, replay=1)
        self.assertEqual(replayed[-1:], [4])
        self.assertEqual(history.limits, [2, None])
        bus.close()

    def test_replay_setup_failures_do_not_leak_subscriptions(self) -> None:
        bus = EventBus[int](history=2)
        bus.publish(1)

        def broken_filter(_event: int) -> bool:
            raise RuntimeError("filter failed")

        with self.assertRaisesRegex(RuntimeError, "filter failed"):
            bus.subscribe(lambda _event: None, predicate=broken_filter, replay=1)
        self.assertEqual(bus.subscriber_count, 0)

        failing_history = EventBus[int](history=_FailingQueryHistory(2))
        with self.assertRaisesRegex(RuntimeError, "history query failed"):
            failing_history.subscribe(lambda _event: None, replay=1)
        self.assertEqual(failing_history.subscriber_count, 0)

    def test_capacity_arguments_reject_bool_and_fractional_values(self) -> None:
        with self.assertRaises(ValueError):
            EventBus[int](replay_buffer_limit=0.5)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            AsyncEventBus[int](replay_buffer_limit=True)
        with self.assertRaises(ValueError):
            MemoryEventHistory[int](capacity=1.5)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            MemoryEventHistory[int](capacity=True)

    def test_replay_wait_and_query_arguments_are_validated(self) -> None:
        bus_without_history = EventBus[int]()
        history = MemoryEventHistory[int](capacity=2)
        bus_with_history = EventBus[int](history=history)

        for raw_replay in (True, 1.5, -1):
            with self.assertRaisesRegex(ValueError, "non-negative integer"):
                bus_without_history.subscribe(
                    lambda _event: None,
                    replay=cast(int, raw_replay),
                )

        for raw_timeout in (True, float("nan"), float("inf"), -float("inf"), -0.1):
            with self.assertRaisesRegex(ValueError, "non-negative and finite"):
                bus_without_history.wait_for(timeout=cast(float, raw_timeout))
        self.assertIsNone(bus_without_history.wait_for(timeout=0))

        for raw_limit in (True, 1.5, 0, -1):
            limit = cast(int, raw_limit)
            with self.assertRaisesRegex(ValueError, "positive integer"):
                bus_without_history.query(limit=limit)
            with self.assertRaisesRegex(ValueError, "positive integer"):
                bus_with_history.query(limit=limit)
            with self.assertRaisesRegex(ValueError, "positive integer"):
                history.query(limit=limit)

    def test_unfiltered_limited_history_reads_do_not_copy_the_complete_buffer(self) -> None:
        class NoFullIterationDeque(deque[int]):
            def __iter__(self):  # type: ignore[override]
                raise AssertionError("limited reads copied the complete history")

        history = MemoryEventHistory[int](capacity=10_000)
        history._events = NoFullIterationDeque(  # type: ignore[attr-defined]
            range(10_000),
            maxlen=10_000,
        )
        self.assertEqual(history.query(limit=2), (9_998, 9_999))
        bus = EventBus[int](history=history)
        self.assertEqual(bus.latest(), 9_999)
        self.assertEqual(bus.query(limit=2), (9_998, 9_999))

        async_bus = AsyncEventBus[int](history=history)
        self.assertEqual(async_bus.latest(), 9_999)
        self.assertEqual(async_bus.query(limit=2), (9_998, 9_999))

    def test_history_filter_conflicts_are_validated_without_history(self) -> None:
        buses = (EventBus[int](), AsyncEventBus[int]())
        for bus in buses:
            with self.subTest(bus=type(bus).__name__):
                with self.assertRaisesRegex(ValueError, "Use event_filter"):
                    bus.latest(
                        event_filter=EventFilter[int](),
                        predicate=lambda _value: True,
                    )
                with self.assertRaisesRegex(ValueError, "Use event_filter"):
                    bus.query(
                        event_filter=EventFilter[int](),
                        predicate=lambda _value: True,
                    )

    def test_history_predicates_can_mutate_history_without_changing_snapshot(self) -> None:
        for operation in ("latest", "limited", "all"):
            with self.subTest(operation=operation):
                history = MemoryEventHistory[int](3)
                for value in range(3):
                    history.append(value)

                def predicate(value: int) -> bool:
                    history.clear()
                    history.append(99)
                    return value == 0

                event_filter = EventFilter(predicate=predicate)
                if operation == "latest":
                    self.assertEqual(history.latest(event_filter), 0)
                else:
                    limit = 1 if operation == "limited" else None
                    self.assertEqual(history.query(event_filter, limit=limit), (0,))
                self.assertEqual(history.query(), (99,))

    def test_history_predicates_do_not_block_another_writer(self) -> None:
        for operation in ("latest", "limited", "all"):
            with self.subTest(operation=operation):
                history = MemoryEventHistory[int](2)
                history.append(1)
                entered = threading.Event()
                written = threading.Event()

                def writer() -> None:
                    if entered.wait(2.0):
                        history.append(2)
                        written.set()

                def predicate(value: int) -> bool:
                    entered.set()
                    self.assertTrue(written.wait(2.0), "predicate holds history lock")
                    return True

                thread = threading.Thread(target=writer)
                thread.start()
                try:
                    event_filter = EventFilter(predicate=predicate)
                    if operation == "latest":
                        self.assertEqual(history.latest(event_filter), 1)
                    else:
                        limit = 1 if operation == "limited" else None
                        self.assertEqual(history.query(event_filter, limit=limit), (1,))
                finally:
                    entered.set()
                    thread.join(3.0)
                self.assertFalse(thread.is_alive())
                self.assertEqual(history.query(), (1, 2))

    def test_live_publish_waits_behind_subscription_replay(self) -> None:
        bus = EventBus[int](history=4)
        bus.publish(1)
        replay_started = threading.Event()
        release_replay = threading.Event()
        received: list[int] = []

        def handler(value: int) -> None:
            if value == 1:
                replay_started.set()
                release_replay.wait(1.0)
            received.append(value)

        subscriber = threading.Thread(target=lambda: bus.subscribe(handler, replay=1))
        subscriber.start()
        self.assertTrue(replay_started.wait(1.0))
        bus.publish(2)
        release_replay.set()
        subscriber.join(1.0)

        self.assertEqual(received, [1, 2])

    def test_replay_overflow_is_reported_instead_of_dropping_events(self) -> None:
        bus = EventBus[int](history=4, replay_buffer_limit=1)
        bus.publish(1)
        entered = threading.Event()
        release = threading.Event()

        def handler(value: int) -> None:
            if value == 1:
                entered.set()
                release.wait(1.0)

        subscriber = threading.Thread(target=lambda: bus.subscribe(handler, replay=1))
        subscriber.start()
        self.assertTrue(entered.wait(1.0))
        bus.publish(2)
        healthy: list[int] = []
        bus.subscribe(healthy.append)
        result = bus.publish(3)
        self.assertEqual(result.failed, 1)
        self.assertIsInstance(result.errors[0], BufferError)
        self.assertEqual(healthy, [3])
        release.set()
        subscriber.join(1.0)
        self.assertFalse(subscriber.is_alive())

    def test_close_reports_a_stuck_periodic_schedule(self) -> None:
        from eventflow import EventShutdownTimeoutError

        entered = threading.Event()
        release = threading.Event()
        bus = EventBus[int](shutdown_timeout=0.02)
        bus.subscribe(lambda _event: (entered.set(), release.wait(1.0)))
        bus.publish_every(1, 0.001)
        self.assertTrue(entered.wait(1.0))
        with self.assertRaises(EventShutdownTimeoutError):
            bus.close()
        release.set()
        bus.close()
        self.assertTrue(bus.closed)

    def test_close_does_not_wait_for_an_inflight_callback(self) -> None:
        bus = EventBus[int]()
        entered = threading.Event()
        release = threading.Event()
        finished = threading.Event()
        published: list[PublishResult] = []

        def handler(_event: int) -> None:
            entered.set()
            release.wait(1.0)
            finished.set()

        bus.subscribe(handler)
        publishing = threading.Thread(target=lambda: published.append(bus.publish(1)))
        publishing.start()
        try:
            self.assertTrue(entered.wait(1.0))
            bus.close()
            self.assertFalse(finished.is_set())
            with self.assertRaises(EventBusClosedError):
                bus.publish(2)
        finally:
            release.set()
            publishing.join(1.0)

        self.assertFalse(publishing.is_alive())
        self.assertTrue(finished.is_set())
        self.assertEqual(published[0].delivered, 1)

    def test_history_capacity_shorthand_and_bus_query_tools(self) -> None:
        bus = EventBus[int](history=2)
        for value in (1, 2, 3):
            bus.publish(value)

        self.assertEqual(bus.query(), (2, 3))
        self.assertEqual(bus.query(predicate=lambda value: value % 2 == 0), (2,))
        self.assertEqual(bus.latest(predicate=lambda value: value < 3), 2)

    def test_history_configuration_rejects_invalid_values_immediately(self) -> None:
        for constructor in (EventBus, AsyncEventBus):
            for invalid in (True, 1.5, "10", object()):
                with self.subTest(bus=constructor.__name__, history=invalid):
                    with self.assertRaises(TypeError):
                        constructor(history=invalid)  # type: ignore[arg-type]
            for invalid in (0, -1):
                with self.subTest(bus=constructor.__name__, history=invalid):
                    with self.assertRaises(ValueError):
                        constructor(history=invalid)

    def test_periodic_publish_rejects_boolean_interval(self) -> None:
        with self.assertRaises(ValueError):
            EventBus[int]().publish_every(1, True)

    def test_clear_does_not_stop_periodic_schedules(self) -> None:
        published = threading.Event()
        bus = EventBus[int](on_before=lambda _event: published.set())
        schedule = bus.publish_every(1, 0.01, immediately=False)
        try:
            bus.clear()
            self.assertTrue(published.wait(1.0))
        finally:
            bus.close()
        self.assertFalse(schedule.active)

    def test_error_and_timeout_actions_receive_context(self) -> None:
        errors: list[tuple[int, str]] = []
        timeouts: list[float | None] = []
        bus = EventBus[int](
            on_error=lambda context: errors.append((context.event, str(context.error))),
            on_timeout=lambda context: timeouts.append(context.timeout),
        )
        bus.subscribe(lambda _event: (_ for _ in ()).throw(RuntimeError("broken")))

        result = bus.publish(5)
        self.assertEqual(result.failed, 1)
        self.assertEqual(errors, [(5, "broken")])
        self.assertIsNone(bus.wait_for(timeout=0.001))
        self.assertEqual(timeouts, [0.001])

    def test_global_before_after_hooks_and_once_shortcut(self) -> None:
        calls: list[str] = []
        bus = EventBus[int](
            on_before=lambda event: calls.append(f"before:{event}"),
            on_after=lambda event, result: calls.append(f"after:{event}:{result.delivered}"),
        )
        bus.once(lambda event: calls.append(f"handler:{event}"))

        bus.publish(4)
        bus.publish(5)

        self.assertEqual(
            calls,
            ["before:4", "handler:4", "after:4:1", "before:5", "after:5:0"],
        )

    def test_subscription_times_is_atomic_across_publishers(self) -> None:
        bus = EventBus[int]()
        received: list[int] = []
        subscription = bus.subscribe(received.append, times=3)

        with ThreadPoolExecutor(max_workers=8) as publishers:
            tuple(publishers.map(bus.publish, range(100)))

        self.assertEqual(len(received), 3)
        self.assertFalse(subscription.active)

    def test_periodic_publish_runs_requested_times(self) -> None:
        bus = EventBus[str]()
        received: list[str] = []
        completed = threading.Event()

        def receive(event: str) -> None:
            received.append(event)
            if len(received) == 3:
                completed.set()

        bus.subscribe(receive)
        schedule = bus.publish_every("heartbeat", 0.001, times=3)

        self.assertTrue(completed.wait(1.0))
        waiting = bus.publish_every("late", 60.0, immediately=False)
        bus.close()
        self.assertEqual(received, ["heartbeat"] * 3)
        self.assertFalse(schedule.active)
        self.assertFalse(waiting.active)

    def test_periodic_publish_reports_failures_through_error_action(self) -> None:
        for policy in (ErrorPolicy.ISOLATE, ErrorPolicy.RAISE):
            with self.subTest(policy=policy):
                reported: list[Exception] = []
                notified = threading.Event()

                def on_error(context: EventErrorContext[int]) -> None:
                    reported.append(context.error)
                    notified.set()

                def fail(_event: int) -> None:
                    raise RuntimeError("periodic handler failed")

                bus = EventBus[int](error_policy=policy, on_error=on_error)
                bus.subscribe(fail)
                bus.publish_every(1, 0.001, times=1)
                self.assertTrue(notified.wait(1.0))
                bus.close()
                self.assertIsInstance(reported[0], RuntimeError)

    def test_periodic_schedule_capacity_is_bounded(self) -> None:
        bus = EventBus[str](max_schedules=1)
        schedule = bus.publish_every("first", 60.0, immediately=False)
        try:
            with self.assertRaisesRegex(EventBusError, "capacity"):
                bus.publish_every("second", 60.0, immediately=False)
        finally:
            schedule.cancel()
            bus.close()

    def test_periodic_thread_start_failure_releases_schedule_capacity(self) -> None:
        bus = EventBus[str](max_schedules=1)
        with patch("threading.Thread.start", side_effect=RuntimeError("start failed")):
            with self.assertRaisesRegex(RuntimeError, "start failed"):
                bus.publish_every("failed", 60.0, immediately=False)

        schedule = bus.publish_every("recovered", 60.0, immediately=False)
        schedule.cancel()
        bus.close()

    def test_close_waits_for_periodic_thread_exit(self) -> None:
        bus = EventBus[int]()
        entered = threading.Event()
        release = threading.Event()

        def block(_value: int) -> None:
            entered.set()
            release.wait(1.0)

        bus.subscribe(block)
        bus.publish_every(1, 1.0)
        self.assertTrue(entered.wait(1.0))
        closed = threading.Event()
        closer = threading.Thread(target=lambda: (bus.close(), closed.set()))
        closer.start()
        self.assertFalse(closed.wait(0.02))
        release.set()
        closer.join(1.0)
        self.assertFalse(closer.is_alive())

    def test_cancelled_periodic_thread_stays_owned_until_it_exits(self) -> None:
        from eventflow import EventShutdownTimeoutError

        entered = threading.Event()
        release = threading.Event()
        bus = EventBus[int](max_schedules=1, shutdown_timeout=0.02)

        def block(_value: int) -> None:
            entered.set()
            release.wait(1.0)

        bus.subscribe(block)
        schedule = bus.publish_every(1, 0.001)
        self.assertTrue(entered.wait(1.0))
        schedule.cancel()

        with self.assertRaisesRegex(EventBusError, "capacity"):
            bus.publish_every(2, 60.0, immediately=False)
        with self.assertRaises(EventShutdownTimeoutError):
            bus.close()

        release.set()
        bus.close()

    def test_clear_wakes_wait_for_without_timeout(self) -> None:
        bus = EventBus[int]()
        result: list[int | None] = []
        worker = threading.Thread(target=lambda: result.append(bus.wait_for()))
        worker.start()

        with bus._condition:
            registered = bus._condition.wait_for(
                lambda: bus.subscriber_count == 1,
                timeout=1.0,
            )
        self.assertTrue(registered)
        bus.clear()
        worker.join(1.0)

        self.assertFalse(worker.is_alive())
        self.assertEqual(result, [None])
        bus.close()

    def test_wait_for_does_not_finish_before_claimed_callback_records_value(self) -> None:
        bus = EventBus[int]()
        registered, entered, checked, release = (threading.Event() for _ in range(4))
        result: list[int | None] = []
        original_subscribe = bus.subscribe
        original_wait = bus._condition.wait_for

        def subscribe(callback, **options):
            def delayed(event):
                entered.set()
                self.assertTrue(release.wait(5))
                callback(event)

            handle = original_subscribe(delayed, **options)
            registered.set()
            return handle

        def wait_for(predicate, timeout=None):
            def observed():
                value = predicate()
                if entered.is_set():
                    checked.set()
                return value

            return original_wait(observed, timeout)

        waiter = threading.Thread(target=lambda: result.append(bus.wait_for(timeout=5)))
        publisher = threading.Thread(target=lambda: bus.publish(7))
        with (
            patch.object(bus, "subscribe", side_effect=subscribe),
            patch.object(bus._condition, "wait_for", side_effect=wait_for),
        ):
            waiter.start()
            try:
                self.assertTrue(registered.wait(5))
                publisher.start()
                self.assertTrue(entered.wait(5))
                # A notification may arrive after once-claim, before capture.
                with bus._condition:
                    bus._condition.notify_all()
                self.assertTrue(checked.wait(5))
            finally:
                release.set()
                if publisher.ident is not None:
                    publisher.join(5)
                bus.close()
                waiter.join(5)
        self.assertEqual(result, [7])

    def test_wait_for_receives_matching_event_and_close_wakes_waiter(self) -> None:
        bus = EventBus[int]()
        result: list[int | None] = []
        registered = threading.Event()
        original_subscribe = bus.subscribe

        def subscribe(*args, **kwargs):
            subscription = original_subscribe(*args, **kwargs)
            registered.set()
            return subscription

        waiter = threading.Thread(
            target=lambda: result.append(
                bus.wait_for(predicate=lambda value: value == 7, timeout=5.0)
            )
        )
        with patch.object(bus, "subscribe", side_effect=subscribe):
            waiter.start()
            self.assertTrue(registered.wait(5.0))
            bus.publish(6)
            bus.publish(7)
        waiter.join(timeout=5.0)
        self.assertEqual(result, [7])

        closed_result: list[int | None] = []
        registered.clear()
        closed_waiter = threading.Thread(
            target=lambda: closed_result.append(bus.wait_for(timeout=5.0))
        )
        with patch.object(bus, "subscribe", side_effect=subscribe):
            closed_waiter.start()
            self.assertTrue(registered.wait(5.0))
            bus.close()
        closed_waiter.join(timeout=5.0)
        self.assertEqual(closed_result, [None])
        with self.assertRaises(EventBusClosedError):
            bus.publish(8)

    def test_concurrent_publish_and_optional_executor(self) -> None:
        bus = EventBus[int]()
        received: list[int] = []
        received_lock = threading.Lock()

        def receive(value: int) -> None:
            with received_lock:
                received.append(value)

        bus.subscribe(receive)
        with ThreadPoolExecutor(max_workers=8) as publishers:
            tuple(publishers.map(bus.publish, range(200)))
        self.assertEqual(sorted(received), list(range(200)))
        self.assertEqual(bus.stats.delivered, 200)

        callback_threads: list[str] = []
        with ThreadPoolExecutor(max_workers=1, thread_name_prefix="EventWorker") as executor:
            threaded_bus = EventBus[int](executor=executor)
            threaded_bus.subscribe(
                lambda _event: callback_threads.append(threading.current_thread().name)
            )
            threaded_bus.publish(1)
        self.assertTrue(callback_threads[0].startswith("EventWorker"))

    def test_cancelled_queued_final_executor_delivery_restores_claim(self) -> None:
        class CancelNextExecutor(ThreadPoolExecutor):
            cancel_next = False
            cancelled_next = False

            def submit(self, fn, /, *args, **kwargs):
                future = super().submit(fn, *args, **kwargs)
                if self.cancel_next:
                    self.cancel_next = False
                    self.cancelled_next = future.cancel()
                return future

        worker_started = threading.Event()
        release_worker = threading.Event()

        def block_worker() -> None:
            worker_started.set()
            release_worker.wait(1)

        executor = CancelNextExecutor(max_workers=1)
        blocker = executor.submit(block_worker)
        self.assertTrue(worker_started.wait(1))
        bus = EventBus[int](executor=executor)
        received: list[int] = []
        bus.subscribe(received.append, times=1)

        try:
            executor.cancel_next = True
            cancelled = bus.publish(1)
            self.assertTrue(executor.cancelled_next)
            self.assertEqual(cancelled.delivered, 0)
            self.assertEqual(cancelled.failed, 1)

            release_worker.set()
            blocker.result(timeout=1)
            delivered = bus.publish(2)
            self.assertEqual(delivered.delivered, 1)
            self.assertEqual(received, [2])
        finally:
            release_worker.set()
            bus.close()
            executor.shutdown(wait=True, cancel_futures=True)

    def test_executor_handler_can_publish_reentrantly_with_one_worker(self) -> None:
        received: list[int] = []
        with ThreadPoolExecutor(max_workers=1) as executor:
            bus = EventBus[int](executor=executor)

            def handler(value: int) -> None:
                received.append(value)
                if value == 1:
                    bus.publish(2)

            bus.subscribe(handler)
            result = bus.publish(1)

        self.assertTrue(result.successful)
        self.assertEqual(received, [1, 2])

    def test_shared_executor_reentrancy_across_buses_does_not_deadlock(self) -> None:
        executor = ThreadPoolExecutor(max_workers=1)
        first = EventBus[int](executor=executor)
        second = EventBus[int](executor=executor)
        received: list[int] = []
        second.subscribe(received.append)
        first.subscribe(lambda value: second.publish(value))
        results = []
        publisher = threading.Thread(
            target=lambda: results.append(first.publish(42)),
            daemon=True,
        )

        publisher.start()
        publisher.join(timeout=1.0)
        if publisher.is_alive():
            executor.shutdown(wait=False, cancel_futures=True)
            publisher.join(timeout=1.0)
        self.assertFalse(publisher.is_alive(), "cross-bus publish deadlocked")
        executor.shutdown(wait=True)
        first.close()
        second.close()

        self.assertEqual(received, [42])
        self.assertTrue(results[0].successful)

    def test_executor_rejects_process_pool(self) -> None:
        with ProcessPoolExecutor(max_workers=1) as executor:
            with self.assertRaisesRegex(TypeError, "ThreadPoolExecutor"):
                EventBus[int](executor=executor)  # type: ignore[arg-type]

    def test_sync_bus_rejects_async_handler(self) -> None:
        async def handler(_event: int) -> None:
            pass

        with self.assertRaises(InvalidEventHandlerError):
            EventBus[int]().subscribe(handler)

    def test_sync_handlers_reject_awaitables_in_all_delivery_paths(self) -> None:
        async def handler(_event: int) -> None:
            pass

        def wrapper(event: int) -> object:
            return handler(event)

        with warnings.catch_warnings(record=True) as observed:
            warnings.simplefilter("always", RuntimeWarning)

            direct = EventBus[int]()
            direct_subscription = direct.subscribe(wrapper)  # type: ignore[arg-type]
            direct_result = direct.publish(1)
            self.assertEqual((direct_result.delivered, direct_result.failed), (0, 1))
            self.assertIsInstance(direct_result.errors[0], InvalidEventHandlerError)

            history = MemoryEventHistory[int](capacity=2)
            history.append(2)
            replay_bus = EventBus[int](history=history)
            replay_subscription = replay_bus.subscribe(
                wrapper,
                replay=1,
            )
            self.assertEqual(replay_bus.stats.delivered, 0)
            self.assertEqual(replay_bus.stats.failed, 1)

            with ThreadPoolExecutor(max_workers=1) as executor:
                executor_bus = EventBus[int](executor=executor)
                executor_subscription = executor_bus.subscribe(
                    wrapper,  # type: ignore[arg-type]
                )
                executor_result = executor_bus.publish(3)
                self.assertEqual((executor_result.delivered, executor_result.failed), (0, 1))
                self.assertIsInstance(executor_result.errors[0], InvalidEventHandlerError)
                executor_subscription.cancel()
                executor_bus.close()

            direct_subscription.cancel()
            replay_subscription.cancel()
            direct.close()
            replay_bus.close()
            gc.collect()

        self.assertFalse(any(issubclass(item.category, RuntimeWarning) for item in observed))

    def test_sync_actions_reject_and_close_awaitables(self) -> None:
        def async_result(*_args):
            return asyncio.sleep(0)

        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always", RuntimeWarning)

            before = EventBus[int](on_before=async_result)
            result = before.publish(1)
            self.assertEqual(result.failed, 1)
            self.assertIsInstance(result.errors[0], TypeError)

            after = EventBus[int](on_after=async_result)
            result = after.publish(1)
            self.assertEqual(result.failed, 1)
            self.assertIsInstance(result.errors[0], TypeError)

            on_error = EventBus[int](on_error=async_result)
            on_error.subscribe(lambda _event: 1 / 0)
            result = on_error.publish(1)
            self.assertEqual(result.failed, 2)
            self.assertTrue(any(isinstance(error, TypeError) for error in result.errors))

            on_timeout = EventBus[int](on_timeout=async_result)
            with self.assertRaisesRegex(TypeError, "awaitable"):
                on_timeout.wait_for(timeout=0.0)

        self.assertEqual(captured, [])

    def test_close_wins_if_periodic_thread_setup_races_with_close(self) -> None:
        bus = EventBus[int]()
        real_thread = threading.Thread
        close_finished = threading.Event()
        close_errors: list[BaseException] = []

        def close_before_thread_start(*args, **kwargs):
            def close_bus() -> None:
                try:
                    bus.close()
                except BaseException as error:
                    close_errors.append(error)
                finally:
                    close_finished.set()

            closer = real_thread(target=close_bus)
            closer.start()
            self.assertTrue(close_finished.wait(1.0))
            closer.join()
            self.assertEqual(close_errors, [])
            return real_thread(*args, **kwargs)

        try:
            with patch(
                "eventflow.event_bus.threading.Thread",
                side_effect=close_before_thread_start,
            ):
                with self.assertRaises(EventBusClosedError):
                    bus.publish_every(1, 60.0, immediately=False)
            self.assertEqual(bus._schedules, {})
        finally:
            bus.close()
