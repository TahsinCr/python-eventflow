"""Async event bus behavior and lifecycle contracts."""

from __future__ import annotations

import asyncio
import unittest
from typing import cast
from unittest.mock import patch

from eventflow import (
    AsyncEventEngine,
    AsyncEventBus,
    AsyncEventBusActions,
    DeliveryMode,
    ErrorPolicy,
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

from _support import _FailingAppendHistory, _FailingQueryHistory, _RecordingQueryHistory


class AsyncEventBusTests(unittest.IsolatedAsyncioTestCase):
    async def test_falsey_event_filter_is_preserved(self) -> None:
        class FalseyFilter(EventFilter[int]):
            def __bool__(self) -> bool:
                return False

        seen: list[int] = []

        async def receive(event: int) -> None:
            seen.append(event)

        bus = AsyncEventBus[int]()
        await bus.subscribe(receive, event_filter=FalseyFilter(event_type=int))

        result = await bus.publish("not an int")  # type: ignore[arg-type]

        self.assertEqual(result.matched, 0)
        self.assertEqual(seen, [])

    async def test_falsey_actions_are_preserved_by_bus_and_engine(self) -> None:
        class FalseyActions(AsyncEventBusActions[int]):
            def __bool__(self) -> bool:
                return False

        seen: list[int] = []

        async def on_before(event: int) -> None:
            seen.append(event)

        actions = FalseyActions(on_before=on_before)
        bus = AsyncEventBus[int](actions=actions)
        engine = AsyncEventEngine(actions=actions)

        await bus.publish(1)
        await engine.publish("channel", 2)

        self.assertEqual(seen, [1, 2])

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

    async def test_failed_history_append_does_not_increment_published(self) -> None:
        bus = AsyncEventBus[int](history=_FailingAppendHistory())
        with self.assertRaisesRegex(RuntimeError, "history append failed"):
            await bus.publish(1)
        self.assertEqual(bus.stats.published, 0)
        await bus.close()

    async def test_publish_without_subscribers_records_event_and_stats(self) -> None:
        history = MemoryEventHistory[int](capacity=2)
        bus = AsyncEventBus[int](history=history)

        result = await bus.publish(7)

        self.assertEqual((result.matched, result.delivered, result.failed), (0, 0, 0))
        self.assertEqual(history.query(), (7,))
        self.assertEqual((bus.stats.published, bus.stats.delivered, bus.stats.failed), (1, 0, 0))
        await bus.close()

    async def test_final_finite_claim_is_restored_when_cancellation_interrupts_removal(
        self,
    ) -> None:
        for delivery_mode in (DeliveryMode.SEQUENTIAL, DeliveryMode.CONCURRENT):
            for once, times in ((True, None), (False, 1), (False, 2)):
                with self.subTest(delivery_mode=delivery_mode, once=once, times=times):
                    bus = AsyncEventBus[int](delivery_mode=delivery_mode)
                    received: list[int] = []

                    async def receive(event: int) -> None:
                        received.append(event)

                    subscription = await bus.subscribe(receive, once=once, times=times)
                    if times == 2:
                        await bus.publish(0)
                    removal_started = asyncio.Event()
                    release_removal = asyncio.Event()
                    original_cancel = subscription._cancel
                    attempts = 0

                    async def cancel_once() -> None:
                        nonlocal attempts
                        attempts += 1
                        if attempts == 1:
                            removal_started.set()
                            await release_removal.wait()
                        await original_cancel()

                    try:
                        with patch.object(subscription, "_cancel", new=cancel_once):
                            publishing = asyncio.create_task(bus.publish(1))
                            await asyncio.wait_for(removal_started.wait(), timeout=1.0)
                            publishing.cancel()
                            with self.assertRaises(asyncio.CancelledError):
                                await publishing

                            self.assertTrue(subscription.active)
                            self.assertEqual(bus.subscriber_count, 1)
                            release_removal.set()
                            await bus.publish(2)
                            await bus.publish(3)

                        expected = [0, 2] if times == 2 else [2]
                        self.assertEqual(received, expected)
                        self.assertFalse(subscription.active)
                        self.assertEqual(bus.subscriber_count, 0)
                    finally:
                        release_removal.set()
                        await bus.close()

    async def test_async_wait_for_rejects_timeout_larger_than_float_range(self) -> None:
        bus = AsyncEventBus[int]()
        with self.assertRaises(ValueError):
            await bus.wait_for(timeout=10**1000)
        with self.assertRaisesRegex(ValueError, "positive and finite"):
            await bus.publish_every(1, 10**1000)
        await bus.close()

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

    async def test_task_creation_rejection_closes_delivery_and_periodic_coroutines(self) -> None:
        loop = asyncio.get_running_loop()
        rejected = []

        def reject_task(coroutine, *, name=None, context=None):
            rejected.append(coroutine)
            raise RuntimeError("task creation rejected")

        concurrent = AsyncEventBus[int](
            delivery_mode=DeliveryMode.CONCURRENT,
            max_concurrency=2,
        )
        delivered = []

        async def callback(event: int) -> None:
            delivered.append(event)

        for _ in range(3):
            await concurrent.subscribe(callback)

        with patch.object(loop, "create_task", side_effect=reject_task):
            with self.assertRaisesRegex(RuntimeError, "creation rejected"):
                await concurrent.publish(1)

        self.assertEqual(len(rejected), 1)
        self.assertIsNone(rejected[0].cr_frame)
        self.assertEqual(delivered, [])
        await concurrent.close()

        periodic = AsyncEventBus[int]()
        with patch.object(loop, "create_task", side_effect=reject_task):
            with self.assertRaisesRegex(RuntimeError, "creation rejected"):
                await periodic.publish_every(1, 1)

        self.assertEqual(len(rejected), 2)
        self.assertIsNone(rejected[1].cr_frame)
        self.assertEqual(periodic._schedules, {})
        await periodic.close()

    async def test_async_periodic_publish_rejects_boolean_interval(self) -> None:
        with self.assertRaises(ValueError):
            await AsyncEventBus[int]().publish_every(1, True)

    async def test_async_clear_does_not_stop_periodic_schedules(self) -> None:
        published = asyncio.Event()

        async def before(_event):
            published.set()

        bus = AsyncEventBus[int](on_before=before)
        schedule = await bus.publish_every(1, 0.01, immediately=False)
        try:
            await bus.clear()
            await asyncio.wait_for(published.wait(), 1.0)
        finally:
            await schedule.cancel()
            await bus.close()
        self.assertFalse(schedule.active)

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

        waiting = asyncio.create_task(bus.wait_for(predicate=lambda value: value == 4, timeout=1.0))
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
        self.assertTrue(subscription.active)

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

    async def test_before_hook_failure_does_not_gate_async_delivery(self) -> None:
        delivered: list[int] = []

        async def reject(_event: int) -> None:
            raise PermissionError("not authorized")

        async def receive(event: int) -> None:
            delivered.append(event)

        async with AsyncEventBus[int](
            on_before=reject,
            error_policy=ErrorPolicy.RAISE,
        ) as bus:
            await bus.subscribe(receive)
            with self.assertRaises(ExceptionGroup):
                await bus.publish(7)
        self.assertEqual(delivered, [7])

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
            self.assertTrue(task.done())
            replacement = await bus.publish_every("second", 60.0, immediately=False)
            replacement_task = bus._schedules[replacement.id][1]
            await bus.close()
            self.assertTrue(replacement_task.done())
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
