from __future__ import annotations

import asyncio
import threading
import time
import unittest
from collections import deque
from concurrent.futures import ThreadPoolExecutor

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

    def test_periodic_schedule_capacity_is_bounded(self) -> None:
        bus = EventBus[str](max_schedules=1)
        schedule = bus.publish_every("first", 60.0, immediately=False)
        try:
            with self.assertRaisesRegex(EventBusError, "capacity"):
                bus.publish_every("second", 60.0, immediately=False)
        finally:
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

    async def test_async_wait_subscription_is_atomic_with_publish(self) -> None:
        bus = AsyncEventBus[int]()
        await bus._lock.acquire()
        waiting = asyncio.create_task(bus.wait_for(timeout=0.1))
        publishing = asyncio.create_task(bus.publish(7))
        await asyncio.sleep(0)
        bus._lock.release()

        await publishing
        self.assertEqual(await waiting, 7)

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

    async def test_async_periodic_schedule_capacity_is_bounded(self) -> None:
        bus = AsyncEventBus[str](max_schedules=1)
        schedule = await bus.publish_every("first", 60.0, immediately=False)
        try:
            with self.assertRaisesRegex(EventBusError, "capacity"):
                await bus.publish_every("second", 60.0, immediately=False)
        finally:
            await schedule.cancel()
            await bus.close()

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
