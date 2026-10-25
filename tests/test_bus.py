from __future__ import annotations

import asyncio
import gc
import threading
import time
import unittest
import warnings
from collections import deque
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import cast
from unittest.mock import patch

from eventflow import (
    AsyncEventBus,
    AsyncEventBusActions,
    BaseEventBus,
    DeliveryMode,
    ErrorPolicy,
    EventBus,
    EventBusActions,
    EventBusClosedError,
    EventBusError,
    EventFilter,
    EventErrorContext,
    EventTimeoutContext,
    InvalidEventHandlerError,
    MemoryEventHistory,
    PublishResult,
)
from eventflow.compatibility import ExceptionGroup


class _FailingQueryHistory(MemoryEventHistory[int]):
    def query(
        self,
        event_filter: EventFilter[int] | None = None,
        *,
        limit: int | None = None,
    ) -> tuple[int, ...]:
        raise RuntimeError("history query failed")


class _RecordingQueryHistory(MemoryEventHistory[int]):
    def __init__(self) -> None:
        super().__init__(capacity=10)
        self.limits: list[int | None] = []

    def query(
        self,
        event_filter: EventFilter[int] | None = None,
        *,
        limit: int | None = None,
    ) -> tuple[int, ...]:
        self.limits.append(limit)
        return super().query(event_filter, limit=limit)


class EventBusTests(unittest.TestCase):
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
        isolated.subscribe(
            lambda _event: (_ for _ in ()).throw(RuntimeError("broken"))
        )
        isolated.subscribe(received.append)

        result = isolated.publish(3)
        self.assertEqual(received, [3])
        self.assertEqual((result.delivered, result.failed), (1, 1))
        self.assertEqual(isolated.stats.failed, 1)

        raising = EventBus[int](error_policy=ErrorPolicy.RAISE)
        raising.subscribe(
            lambda _event: (_ for _ in ()).throw(RuntimeError("broken"))
        )
        with self.assertRaises(ExceptionGroup):
            raising.publish(3)

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

        subscriber = threading.Thread(
            target=lambda: bus.subscribe(handler, replay=1)
        )
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

    def test_error_and_timeout_actions_receive_context(self) -> None:
        errors: list[tuple[int, str]] = []
        timeouts: list[float | None] = []
        bus = EventBus[int](
            on_error=lambda context: errors.append(
                (context.event, str(context.error))
            ),
            on_timeout=lambda context: timeouts.append(context.timeout),
        )
        bus.subscribe(
            lambda _event: (_ for _ in ()).throw(RuntimeError("broken"))
        )

        result = bus.publish(5)
        self.assertEqual(result.failed, 1)
        self.assertEqual(errors, [(5, "broken")])
        self.assertIsNone(bus.wait_for(timeout=0.001))
        self.assertEqual(timeouts, [0.001])

    def test_global_before_after_hooks_and_once_shortcut(self) -> None:
        calls: list[str] = []
        bus = EventBus[int](
            on_before=lambda event: calls.append(f"before:{event}"),
            on_after=lambda event, result: calls.append(
                f"after:{event}:{result.delivered}"
            ),
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
        deadline = time.monotonic() + 1.0
        while schedule.active and time.monotonic() < deadline:
            time.sleep(0.001)
        self.assertEqual(received, ["heartbeat"] * 3)
        self.assertFalse(schedule.active)

        waiting = bus.publish_every("late", 60.0, immediately=False)
        bus.close()
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

    def test_wait_for_receives_matching_event_and_close_wakes_waiter(self) -> None:
        bus = EventBus[int]()
        result: list[int | None] = []

        waiter = threading.Thread(
            target=lambda: result.append(
                bus.wait_for(predicate=lambda value: value == 7, timeout=1.0)
            )
        )
        waiter.start()
        deadline = time.monotonic() + 1.0
        while bus.subscriber_count == 0 and time.monotonic() < deadline:
            time.sleep(0.001)
        bus.publish(6)
        bus.publish(7)
        waiter.join(timeout=1.0)
        self.assertEqual(result, [7])

        closed_result: list[int | None] = []
        closed_waiter = threading.Thread(
            target=lambda: closed_result.append(bus.wait_for(timeout=5.0))
        )
        closed_waiter.start()
        deadline = time.monotonic() + 1.0
        while bus.subscriber_count == 0 and time.monotonic() < deadline:
            time.sleep(0.001)
        bus.close()
        closed_waiter.join(timeout=1.0)
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
                self.assertEqual(
                    (executor_result.delivered, executor_result.failed), (0, 1)
                )
                self.assertIsInstance(
                    executor_result.errors[0], InvalidEventHandlerError
                )
                executor_subscription.cancel()
                executor_bus.close()

            direct_subscription.cancel()
            replay_subscription.cancel()
            direct.close()
            replay_bus.close()
            gc.collect()

        self.assertFalse(
            any(issubclass(item.category, RuntimeWarning) for item in observed)
        )


class AsyncEventBusTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_bus_filters_once_and_tracks_results(self) -> None:
        bus = AsyncEventBus[int]()
        received: list[int] = []

        async def handler(value: int) -> None:
            received.append(value)

        subscription = await bus.subscribe(
            handler,
            predicate=lambda value: value > 1,
            once=True,
        )
        self.assertEqual((await bus.publish(1)).matched, 0)
        result = await bus.publish(2)
        await bus.publish(3)

        self.assertEqual(received, [2])
        self.assertEqual((result.delivered, result.failed), (1, 0))
        self.assertFalse(subscription.active)

    async def test_async_once_is_atomic_across_concurrent_publishers(self) -> None:
        bus = AsyncEventBus[int](delivery_mode=DeliveryMode.CONCURRENT)
        received: list[int] = []

        async def handler(value: int) -> None:
            await asyncio.sleep(0)
            received.append(value)

        await bus.subscribe(handler, once=True)
        await asyncio.gather(*(bus.publish(value) for value in range(100)))
        self.assertEqual(len(received), 1)

    async def test_async_bus_rejects_sync_handler(self) -> None:
        bus = AsyncEventBus[int]()
        with self.assertRaises(InvalidEventHandlerError):
            await bus.subscribe(lambda _event: None)  # type: ignore[arg-type]

    async def test_sequential_and_concurrent_delivery_modes(self) -> None:
        sequential = AsyncEventBus[int]()
        order: list[str] = []

        async def first(_event: int) -> None:
            order.extend(("first:start", "first:end"))

        async def second(_event: int) -> None:
            order.append("second")

        await sequential.subscribe(first)
        await sequential.subscribe(second)
        await sequential.publish(1)
        self.assertEqual(order, ["first:start", "first:end", "second"])

        concurrent = AsyncEventBus[int](delivery_mode=DeliveryMode.CONCURRENT)
        release = asyncio.Event()
        started = asyncio.Event()

        async def blocked(_event: int) -> None:
            started.set()
            await release.wait()

        async def releasing(_event: int) -> None:
            await started.wait()
            release.set()

        await concurrent.subscribe(blocked)
        await concurrent.subscribe(releasing)
        result = await asyncio.wait_for(concurrent.publish(1), timeout=1.0)
        self.assertEqual(result.delivered, 2)

    async def test_async_history_replay_wait_timeout_and_close(self) -> None:
        history = MemoryEventHistory[int](capacity=2)
        bus = AsyncEventBus[int](history=history)
        await bus.publish(1)
        await bus.publish(2)
        replayed: list[int] = []

        async def receive(value: int) -> None:
            replayed.append(value)

        await bus.subscribe(receive, replay=2)
        self.assertEqual(replayed, [1, 2])

        waiting = asyncio.create_task(
            bus.wait_for(predicate=lambda value: value == 4, timeout=1.0)
        )
        await asyncio.sleep(0)
        await bus.publish(3)
        await bus.publish(4)
        self.assertEqual(await waiting, 4)
        self.assertIsNone(await bus.wait_for(timeout=0.001))

        close_waiter = asyncio.create_task(bus.wait_for())
        await asyncio.sleep(0)

        await bus.close()
        self.assertIsNone(await close_waiter)
        self.assertTrue(bus.closed)
        with self.assertRaises(EventBusClosedError):
            await bus.publish(5)

    async def test_async_replay_and_wait_arguments_are_validated(self) -> None:
        bus = AsyncEventBus[int]()

        async def receive(_event: int) -> None:
            return None

        for raw_replay in (True, 1.5, -1):
            with self.assertRaisesRegex(ValueError, "non-negative integer"):
                await bus.subscribe(receive, replay=cast(int, raw_replay))

        for raw_timeout in (True, float("nan"), float("inf"), -float("inf"), -0.1):
            with self.assertRaisesRegex(ValueError, "non-negative and finite"):
                await bus.wait_for(timeout=cast(float, raw_timeout))
        self.assertIsNone(await bus.wait_for(timeout=0))

    async def test_async_unfiltered_replay_limits_the_history_query(self) -> None:
        history = _RecordingQueryHistory()
        for value in range(5):
            history.append(value)
        bus = AsyncEventBus[int](history=history)
        replayed: list[int] = []

        async def receive(value: int) -> None:
            replayed.append(value)

        await bus.subscribe(receive, replay=2)
        self.assertEqual(replayed, [3, 4])
        self.assertEqual(history.limits, [2])

        await bus.subscribe(
            receive,
            predicate=lambda value: value % 2 == 0,
            replay=1,
        )
        self.assertEqual(replayed[-1:], [4])
        self.assertEqual(history.limits, [2, None])
        await bus.close()

    async def test_async_live_publish_waits_behind_subscription_replay(self) -> None:
        bus = AsyncEventBus[int](history=4)
        await bus.publish(1)
        replay_started = asyncio.Event()
        release_replay = asyncio.Event()
        received: list[int] = []

        async def handler(value: int) -> None:
            if value == 1:
                replay_started.set()
                await release_replay.wait()
            received.append(value)

        subscription = asyncio.create_task(bus.subscribe(handler, replay=1))
        await asyncio.wait_for(replay_started.wait(), 1.0)
        await bus.publish(2)
        release_replay.set()
        await asyncio.wait_for(subscription, 1.0)

        self.assertEqual(received, [1, 2])

    async def test_async_replay_overflow_isolated_from_healthy_subscribers(self) -> None:
        bus = AsyncEventBus[int](history=1, replay_buffer_limit=1)
        await bus.publish(1)
        entered = asyncio.Event()
        release = asyncio.Event()

        async def slow(value: int) -> None:
            if value == 1:
                entered.set()
                await release.wait()

        replay = asyncio.create_task(bus.subscribe(slow, replay=1))
        await entered.wait()
        healthy: list[int] = []

        async def receive(value: int) -> None:
            healthy.append(value)

        await bus.subscribe(receive)
        await bus.publish(2)
        result = await bus.publish(3)
        self.assertEqual(result.failed, 1)
        self.assertIsInstance(result.errors[0], BufferError)
        self.assertEqual(healthy, [2, 3])
        release.set()
        await replay

    async def test_cancelled_replay_removes_unreachable_subscription(self) -> None:
        bus = AsyncEventBus[int](history=2, replay_buffer_limit=4)
        await bus.publish(1)
        entered = asyncio.Event()

        async def handler(_event: int) -> None:
            entered.set()
            await asyncio.Event().wait()

        subscribing = asyncio.create_task(bus.subscribe(handler, replay=1))
        await asyncio.wait_for(entered.wait(), 1.0)
        subscribing.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await subscribing

        self.assertEqual(bus.subscriber_count, 0)
        await bus.close()

    async def test_replay_cleanup_survives_a_second_cancellation(self) -> None:
        bus = AsyncEventBus[int](history=2)
        await bus.publish(1)
        entered = asyncio.Event()

        async def handler(_event: int) -> None:
            entered.set()
            await asyncio.Event().wait()

        subscribing = asyncio.create_task(bus.subscribe(handler, replay=1))
        await asyncio.wait_for(entered.wait(), 1.0)
        await bus._lock.acquire()
        try:
            subscribing.cancel()
            await asyncio.sleep(0)
            subscribing.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await subscribing
        finally:
            bus._lock.release()

        await asyncio.sleep(0)
        self.assertEqual(bus.subscriber_count, 0)
        await bus.close()

    async def test_async_replay_setup_failures_do_not_leak_subscriptions(self) -> None:
        bus = AsyncEventBus[int](history=2)
        await bus.publish(1)

        def broken_filter(_event: int) -> bool:
            raise RuntimeError("filter failed")

        async def receive(_event: int) -> None:
            return None

        with self.assertRaisesRegex(RuntimeError, "filter failed"):
            await bus.subscribe(receive, predicate=broken_filter, replay=1)
        self.assertEqual(bus.subscriber_count, 0)

        failing_history = AsyncEventBus[int](history=_FailingQueryHistory(2))
        with self.assertRaisesRegex(RuntimeError, "history query failed"):
            await failing_history.subscribe(receive, replay=1)
        self.assertEqual(failing_history.subscriber_count, 0)

    async def test_async_close_can_resume_after_caller_cancellation(self) -> None:
        bus = AsyncEventBus[int]()

        async def receive(_event: int) -> None:
            return None

        subscription = await bus.subscribe(receive)
        original_cancel = subscription._cancel
        cancel_started = asyncio.Event()

        async def blocked_cancel() -> None:
            cancel_started.set()
            await asyncio.Event().wait()
            await original_cancel()

        subscription._cancel = blocked_cancel
        closing = asyncio.create_task(bus.close())
        await asyncio.wait_for(cancel_started.wait(), 1.0)
        closing.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await closing
        self.assertEqual(bus.subscriber_count, 1)
        self.assertFalse(subscription.active)

        subscription._cancel = original_cancel
        await bus.close()
        self.assertFalse(subscription.active)
        self.assertEqual(bus.subscriber_count, 0)

    async def test_async_close_does_not_wait_for_an_inflight_callback(self) -> None:
        bus = AsyncEventBus[int]()
        entered = asyncio.Event()
        release = asyncio.Event()
        finished = asyncio.Event()

        async def handler(_event: int) -> None:
            entered.set()
            await release.wait()
            finished.set()

        await bus.subscribe(handler)
        publishing = asyncio.create_task(bus.publish(1))
        try:
            await asyncio.wait_for(entered.wait(), 1.0)
            await bus.close()
            self.assertFalse(finished.is_set())
            with self.assertRaises(EventBusClosedError):
                await bus.publish(2)
        finally:
            release.set()
            await asyncio.gather(publishing, return_exceptions=True)
            await bus.close()

        self.assertTrue(finished.is_set())
        self.assertEqual(publishing.result().delivered, 1)

    async def test_async_wait_subscription_is_atomic_with_publish(self) -> None:
        bus = AsyncEventBus[int]()
        await bus._lock.acquire()
        waiting = asyncio.create_task(bus.wait_for(timeout=0.1))
        publishing = asyncio.create_task(bus.publish(7))
        await asyncio.sleep(0)
        bus._lock.release()

        await publishing
        self.assertEqual(await waiting, 7)

    async def test_async_wait_for_cleanup_survives_repeated_cancellation(self) -> None:
        bus = AsyncEventBus[int]()
        waiting = asyncio.create_task(bus.wait_for())
        await asyncio.sleep(0)
        self.assertEqual(bus.subscriber_count, 1)

        subscriber = next(iter(bus._subscribers.values()))
        original_cancel = subscriber.subscription._cancel
        cancel_started = asyncio.Event()
        cancel_finished = asyncio.Event()

        async def observed_cancel() -> None:
            cancel_started.set()
            await original_cancel()
            cancel_finished.set()

        subscriber.subscription._cancel = observed_cancel
        await bus._lock.acquire()
        try:
            waiting.cancel()
            await asyncio.wait_for(cancel_started.wait(), 1.0)
            waiting.cancel()
        finally:
            bus._lock.release()

        with self.assertRaises(asyncio.CancelledError):
            await waiting
        await asyncio.wait_for(cancel_finished.wait(), 1.0)
        await asyncio.sleep(0)
        self.assertEqual(bus.subscriber_count, 0)
        self.assertEqual(bus._waiters, set())
        await bus.close()

    async def test_async_error_and_timeout_actions(self) -> None:
        errors: list[tuple[int, str]] = []
        timeouts: list[float | None] = []

        async def on_error(context: EventErrorContext[int]) -> None:
            errors.append((context.event, str(context.error)))

        async def on_timeout(context: EventTimeoutContext[int]) -> None:
            timeouts.append(context.timeout)

        bus = AsyncEventBus[int](
            history=2,
            actions=AsyncEventBusActions(
                on_error=on_error,
                on_timeout=on_timeout,
            ),
        )

        async def broken(_event: int) -> None:
            raise RuntimeError("broken")

        await bus.subscribe(broken)
        result = await bus.publish(7)
        self.assertEqual(result.failed, 1)
        self.assertEqual(errors, [(7, "broken")])
        self.assertIsNone(await bus.wait_for(timeout=0.001))
        self.assertEqual(timeouts, [0.001])
        self.assertEqual(bus.latest(), 7)

    async def test_async_hooks_limited_subscription_and_once_shortcut(self) -> None:
        calls: list[str] = []

        async def on_before(event: int) -> None:
            calls.append(f"before:{event}")

        async def on_after(event: int, result: PublishResult) -> None:
            calls.append(f"after:{event}:{result.delivered}")

        async def receive(event: int) -> None:
            calls.append(f"handler:{event}")

        bus = AsyncEventBus[int](on_before=on_before, on_after=on_after)
        await bus.once(receive)
        await bus.publish(1)
        await bus.publish(2)
        self.assertEqual(
            calls,
            ["before:1", "handler:1", "after:1:1", "before:2", "after:2:0"],
        )

        limited: list[int] = []

        async def receive_limited(event: int) -> None:
            limited.append(event)

        subscription = await bus.subscribe(receive_limited, times=3)
        await asyncio.gather(*(bus.publish(value) for value in range(100)))
        self.assertEqual(len(limited), 3)
        self.assertFalse(subscription.active)

    async def test_async_periodic_publish_runs_requested_times(self) -> None:
        bus = AsyncEventBus[str]()
        received: list[str] = []
        completed = asyncio.Event()

        async def receive(event: str) -> None:
            received.append(event)
            if len(received) == 3:
                completed.set()

        await bus.subscribe(receive)
        schedule = await bus.publish_every("heartbeat", 0.001, times=3)

        await asyncio.wait_for(completed.wait(), timeout=1.0)
        for _ in range(10):
            if not schedule.active:
                break
            await asyncio.sleep(0)
        self.assertEqual(received, ["heartbeat"] * 3)
        self.assertFalse(schedule.active)

        waiting = await bus.publish_every("late", 60.0, immediately=False)
        await bus.close()
        self.assertFalse(waiting.active)

    async def test_async_periodic_publish_reports_failures_through_error_action(
        self,
    ) -> None:
        for policy in (ErrorPolicy.ISOLATE, ErrorPolicy.RAISE):
            with self.subTest(policy=policy):
                reported: list[Exception] = []
                notified = asyncio.Event()

                async def on_error(context: EventErrorContext[int]) -> None:
                    reported.append(context.error)
                    notified.set()

                async def fail(_event: int) -> None:
                    raise RuntimeError("periodic handler failed")

                bus = AsyncEventBus[int](error_policy=policy, on_error=on_error)
                await bus.subscribe(fail)
                await bus.publish_every(1, 0.001, times=1)
                await asyncio.wait_for(notified.wait(), 1.0)
                await bus.close()
                self.assertIsInstance(reported[0], RuntimeError)

    async def test_async_periodic_schedule_capacity_is_bounded(self) -> None:
        bus = AsyncEventBus[str](max_schedules=1)
        schedule = await bus.publish_every("first", 60.0, immediately=False)
        try:
            with self.assertRaisesRegex(EventBusError, "capacity"):
                await bus.publish_every("second", 60.0, immediately=False)
        finally:
            await schedule.cancel()
            await bus.close()

    async def test_cancel_before_periodic_task_starts_releases_capacity(self) -> None:
        bus = AsyncEventBus[str](max_schedules=1)
        try:
            schedule = await bus.publish_every("first", 60.0, immediately=False)
            task = bus._schedules[schedule.id][1]

            await schedule.cancel()
            self.assertNotIn(schedule.id, bus._schedules)
            await asyncio.gather(task, return_exceptions=True)

            self.assertTrue(task.done())
            replacement = await bus.publish_every(
                "second", 60.0, immediately=False
            )
            await replacement.cancel()
            await bus.close()
            self.assertEqual(bus._schedules, {})
        finally:
            await bus.close()

    async def test_subscription_cancel_can_retry_after_lock_wait_is_cancelled(self) -> None:
        bus = AsyncEventBus[int]()

        async def handler(_event: int) -> None:
            pass

        subscription = await bus.subscribe(handler)
        await bus._lock.acquire()
        try:
            cancelling = asyncio.create_task(subscription.cancel())
            await asyncio.sleep(0)
            cancelling.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await cancelling
        finally:
            bus._lock.release()

        try:
            await subscription.cancel()
            self.assertFalse(subscription.active)
            self.assertEqual(bus.subscriber_count, 0)
        finally:
            await bus.close()

    async def test_concurrent_subscription_cancellation_removes_once(self) -> None:
        bus = AsyncEventBus[int]()

        async def handler(_event: int) -> None:
            pass

        subscription = await bus.subscribe(handler)
        await bus._lock.acquire()
        try:
            first = asyncio.create_task(subscription.cancel())
            second = asyncio.create_task(subscription.cancel())
            await asyncio.sleep(0)
            self.assertFalse(subscription.active)
        finally:
            bus._lock.release()
        await asyncio.gather(first, second)

        self.assertEqual(bus.subscriber_count, 0)
        await bus.close()

    async def test_cancelled_periodic_task_stays_owned_until_it_exits(self) -> None:
        bus = AsyncEventBus[int](max_schedules=1)
        entered = asyncio.Event()
        cancellation_received = asyncio.Event()
        release = asyncio.Event()
        finished = asyncio.Event()

        async def block(_value: int) -> None:
            entered.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancellation_received.set()
                await release.wait()
                raise
            finally:
                finished.set()

        await bus.subscribe(block)
        schedule = await bus.publish_every(1, 0.001)
        await asyncio.wait_for(entered.wait(), 1.0)
        await schedule.cancel()
        await asyncio.wait_for(cancellation_received.wait(), 1.0)

        with self.assertRaisesRegex(EventBusError, "capacity"):
            await bus.publish_every(2, 60.0, immediately=False)

        release.set()
        await bus.close()
        self.assertTrue(finished.is_set())

    async def test_periodic_cleanup_survives_repeated_cancellation(self) -> None:
        bus = AsyncEventBus[int](max_schedules=1)
        entered = asyncio.Event()
        cancellation_received = asyncio.Event()

        async def block(_value: int) -> None:
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancellation_received.set()
                raise

        await bus.subscribe(block)
        schedule = await bus.publish_every(1, 0.001)
        await asyncio.wait_for(entered.wait(), 1.0)
        schedule_task = bus._schedules[schedule.id][1]

        await bus._lock.acquire()
        try:
            await schedule.cancel()
            await asyncio.wait_for(cancellation_received.wait(), 1.0)
            schedule_task.cancel()
        finally:
            bus._lock.release()

        await asyncio.gather(schedule_task, return_exceptions=True)
        self.assertNotIn(schedule.id, bus._schedules)
        replacement = await bus.publish_every(2, 60.0, immediately=False)
        await replacement.cancel()
        await bus.close()

    async def test_concurrent_callback_cancellation_is_a_delivery_failure(self) -> None:
        bus = AsyncEventBus[int](delivery_mode=DeliveryMode.CONCURRENT)
        received: list[int] = []

        async def cancel_self(_event: int) -> None:
            raise asyncio.CancelledError

        async def receive(event: int) -> None:
            received.append(event)

        await bus.subscribe(cancel_self)
        await bus.subscribe(receive)

        result = await bus.publish(7)
        self.assertEqual((result.matched, result.delivered, result.failed), (2, 1, 1))
        self.assertIsInstance(result.errors[0], EventBusError)
        self.assertEqual(received, [7])
        self.assertEqual(
            (bus.stats.published, bus.stats.delivered, bus.stats.failed),
            (1, 1, 1),
        )

    async def test_sequential_callback_cancellation_aborts_publish(self) -> None:
        bus = AsyncEventBus[int]()
        received: list[int] = []

        async def cancel_self(_event: int) -> None:
            raise asyncio.CancelledError

        async def receive(event: int) -> None:
            received.append(event)

        await bus.subscribe(cancel_self)
        await bus.subscribe(receive)

        with self.assertRaises(asyncio.CancelledError):
            await bus.publish(7)
        self.assertEqual(received, [])

    async def test_concurrent_publish_cancellation_still_propagates(self) -> None:
        bus = AsyncEventBus[int](delivery_mode=DeliveryMode.CONCURRENT)
        entered = asyncio.Event()
        cancelled = asyncio.Event()

        async def wait_for_release(_event: int) -> None:
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise

        await bus.subscribe(wait_for_release)
        publishing = asyncio.create_task(bus.publish(1))
        await asyncio.wait_for(entered.wait(), 1.0)
        publishing.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await publishing
        self.assertTrue(cancelled.is_set())

    async def test_threadsafe_publish_uses_owning_loop(self) -> None:
        bus = AsyncEventBus[int]()
        received: list[int] = []

        async def receive(value: int) -> None:
            received.append(value)

        await bus.subscribe(receive)

        def publish_from_thread() -> None:
            result = bus.publish_threadsafe(9).result(timeout=1.0)
            self.assertEqual(result.delivered, 1)

        await asyncio.to_thread(publish_from_thread)
        self.assertEqual(received, [9])
