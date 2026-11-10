"""Indexed routing for keyed EventBus subscriptions."""

from __future__ import annotations

import threading
import unittest
from concurrent.futures import ThreadPoolExecutor

from eventflow import AsyncEventQueue, EventBus, EventFilter, QueueOverflowError
from eventflow.engine import EventEngine


class EventBusRoutingTests(unittest.TestCase):
    def test_keyed_and_wildcard_subscribers_keep_registration_order(self) -> None:
        seen: list[tuple[str, tuple[str, int]]] = []
        with EventBus[tuple[str, int]](routing_key=lambda event: event[0]) as bus:
            bus.subscribe(lambda event: seen.append(("wildcard-1", event)))
            bus.subscribe(lambda event: seen.append(("keyed-1", event)), routing_key="a")
            bus.subscribe(lambda event: seen.append(("other", event)), routing_key="b")
            bus.subscribe(lambda event: seen.append(("wildcard-2", event)))
            bus.subscribe(lambda event: seen.append(("keyed-2", event)), routing_key="a")

            result = bus.publish(("a", 1))

        self.assertEqual(
            seen,
            [
                ("wildcard-1", ("a", 1)),
                ("keyed-1", ("a", 1)),
                ("wildcard-2", ("a", 1)),
                ("keyed-2", ("a", 1)),
            ],
        )
        self.assertEqual(result.matched, 4)
        self.assertEqual(result.delivered, 4)

    def test_routing_index_preserves_order_after_cancellations(self) -> None:
        seen: list[int] = []
        expected: list[int] = []

        with EventBus[int](routing_key=lambda event: event % 7) as bus:
            for index in range(280):
                route = index % 4
                if route == 0:
                    subscription = bus.subscribe(lambda _event, index=index: seen.append(index))
                    selected = True
                else:
                    key = 3 if route == 1 else index % 7
                    subscription = bus.subscribe(
                        lambda _event, index=index: seen.append(index), routing_key=key
                    )
                    selected = key == 3
                if index % 9 == 0:
                    subscription.cancel()
                elif selected:
                    expected.append(index)

            for index in range(280, 300):
                bus.subscribe(lambda _event, index=index: seen.append(index), routing_key=3)
                expected.append(index)

            result = bus.publish(10)

        self.assertEqual(seen, expected)
        self.assertEqual(result.matched, len(expected))
        self.assertEqual(result.delivered, len(expected))

    def test_routing_merge_preserves_order_with_skewed_indexes(self) -> None:
        for wildcard_count, keyed_count in ((3, 257), (257, 3)):
            seen: list[int] = []
            expected: list[int] = []

            with self.subTest(wildcard_count=wildcard_count, keyed_count=keyed_count):
                with EventBus[int](routing_key=lambda event: event % 2) as bus:
                    subscriptions = []
                    for index in range(wildcard_count + keyed_count):
                        route_key = None if index < wildcard_count else 0
                        subscription = bus.subscribe(
                            lambda _event, value=index: seen.append(value),
                            routing_key=route_key,
                        )
                        subscriptions.append((subscription, route_key, index))

                    for index, (subscription, route_key, value) in enumerate(subscriptions):
                        if index % 17 == 0:
                            subscription.cancel()
                        elif route_key is None or route_key == 0:
                            expected.append(value)

                    result = bus.publish(0)

                self.assertEqual(seen, expected)
                self.assertEqual(result.matched, len(expected))
                self.assertEqual(result.delivered, len(expected))

    def test_routing_order_survives_concurrent_publish_and_subscription_churn(self) -> None:
        seen: dict[int, list[str]] = {}
        seen_lock = threading.Lock()

        def record(name: str):
            def callback(event: int) -> None:
                with seen_lock:
                    seen.setdefault(event, []).append(name)

            return callback

        with EventBus[int](routing_key=lambda event: event % 2) as bus:
            bus.subscribe(record("wildcard-1"))
            bus.subscribe(record("keyed-1"), routing_key=0)
            bus.subscribe(record("wildcard-2"))
            bus.subscribe(record("keyed-2"), routing_key=0)
            barrier = threading.Barrier(5)

            def publish_many(worker: int) -> None:
                barrier.wait(timeout=5)
                for offset in range(100):
                    event = worker * 1_000 + offset
                    result = bus.publish(event)
                    stable_count = 4 if event % 2 == 0 else 2
                    if result.matched < stable_count or result.delivered < stable_count:
                        raise AssertionError("concurrent publish lost a stable subscriber")

            def churn() -> None:
                barrier.wait(timeout=5)
                for _ in range(250):
                    subscription = bus.subscribe(lambda _event: None, routing_key=0)
                    subscription.cancel()

            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(publish_many, worker) for worker in range(4)]
                futures.append(executor.submit(churn))
                for future in futures:
                    future.result(timeout=10)

        self.assertEqual(len(seen), 400)
        for event, callbacks in seen.items():
            expected = (
                ["wildcard-1", "keyed-1", "wildcard-2", "keyed-2"]
                if event % 2 == 0
                else ["wildcard-1", "wildcard-2"]
            )
            self.assertEqual(callbacks, expected)

    def test_routed_replay_returns_latest_matching_events_and_applies_filter(self) -> None:
        seen: list[tuple[str, int]] = []
        with EventBus[tuple[str, int]](history=8, routing_key=lambda event: event[0]) as bus:
            for event in (("b", 1), ("a", 1), ("a", 2), ("b", 2), ("a", 3)):
                bus.publish(event)

            bus.subscribe(
                seen.append,
                routing_key="a",
                event_filter=EventFilter(predicate=lambda event: event[1] >= 2),
                replay=2,
            )

        self.assertEqual(seen, [("a", 2), ("a", 3)])

    def test_once_wait_for_and_engine_forward_routing_key(self) -> None:
        seen: list[tuple[str, int]] = []
        once_seen: list[tuple[str, int]] = []
        with EventBus[tuple[str, int]](routing_key=lambda event: event[0]) as bus:
            bus.once(seen.append, routing_key="a")
            bus.publish(("b", 0))
            bus.publish(("a", 1))
            bus.publish(("a", 2))
        self.assertEqual(seen, [("a", 1)])

        engine = EventEngine()
        engine.add(
            "messages",
            EventBus[tuple[str, int]](routing_key=lambda event: event[0]),
        )
        try:
            engine.subscribe("messages", seen.append, routing_key="b")
            engine.once("messages", once_seen.append, routing_key="b")
            publisher = threading.Timer(
                0.01,
                lambda: (
                    engine.publish("messages", ("a", 3)),
                    engine.publish("messages", ("b", 4)),
                    engine.publish("messages", ("b", 5)),
                ),
            )
            publisher.start()
            waited = engine.wait_for("messages", routing_key="b", timeout=1)
            publisher.join(timeout=1)
            self.assertEqual(waited, ("b", 4))
            self.assertFalse(publisher.is_alive())
        finally:
            engine.stop()
        self.assertEqual(seen, [("a", 1), ("b", 4), ("b", 5)])
        self.assertEqual(once_seen, [("b", 4)])

    def test_wait_for_captures_only_the_requested_key(self) -> None:
        with EventBus[tuple[str, int]](routing_key=lambda event: event[0]) as bus:
            publisher = threading.Timer(
                0.01,
                lambda: (bus.publish(("b", 1)), bus.publish(("a", 2))),
            )
            publisher.start()
            result = bus.wait_for(routing_key="a", timeout=1)
            publisher.join(timeout=1)

        self.assertEqual(result, ("a", 2))
        self.assertFalse(publisher.is_alive())

    def test_routing_key_is_lazy_without_indexed_subscribers(self) -> None:
        extracted: list[int] = []
        with EventBus[int](routing_key=lambda event: extracted.append(event) or event) as bus:
            bus.publish(1)
            bus.subscribe(lambda _event: None)
            bus.publish(2)
        self.assertEqual(extracted, [])

    def test_invalid_routing_configuration_is_rejected(self) -> None:
        with EventBus[int]() as bus:
            with self.assertRaises(ValueError):
                bus.subscribe(lambda _event: None, routing_key="a")

        with EventBus[int](routing_key=lambda _event: "a") as bus:
            with self.assertRaises(TypeError):
                bus.subscribe(lambda _event: None, routing_key=[])

        with EventBus[int](routing_key=lambda _event: []) as bus:
            bus.subscribe(lambda _event: None, routing_key="a")
            with self.assertRaises(TypeError):
                bus.publish(1)

        with EventBus[int](routing_key=lambda _event: None) as bus:
            bus.subscribe(lambda _event: None, routing_key="a")
            with self.assertRaises(TypeError):
                bus.publish(1)


class AsyncEventQueueRoutingTests(unittest.IsolatedAsyncioTestCase):
    async def test_eventflow_filters_run_before_bounded_queue(self) -> None:
        bus = EventBus[tuple[str, int]](routing_key=lambda event: event[0])
        queue = AsyncEventQueue(
            bus,
            event_type=tuple,
            predicate=lambda event: event[1] > 0,
            routing_key="a",
            capacity=1,
            overflow="error",
        )
        try:
            bus.publish(("b", 1))
            bus.publish(("a", 0))
            bus.publish(("a", 1))
            overflow = bus.publish(("a", 2))

            self.assertEqual(queue.stats().size, 1)
            self.assertIsInstance(overflow.errors[0], QueueOverflowError)
            self.assertEqual(await queue.receive(), ("a", 1))
            self.assertEqual(queue.stats().received, 1)
        finally:
            await queue.aclose()
            bus.close()

    async def test_event_filter_is_applied_before_bounded_queue(self) -> None:
        bus = EventBus[tuple[str, int]]()
        queue = AsyncEventQueue(
            bus,
            event_filter=EventFilter(predicate=lambda event: event[1] > 0),
            capacity=1,
            overflow="error",
        )
        try:
            bus.publish(("vehicle", 0))
            bus.publish(("vehicle", 1))
            overflow = bus.publish(("vehicle", 2))

            self.assertEqual(queue.stats().size, 1)
            self.assertIsInstance(overflow.errors[0], QueueOverflowError)
            self.assertEqual(await queue.receive(), ("vehicle", 1))
        finally:
            await queue.aclose()
            bus.close()


if __name__ == "__main__":
    unittest.main()
