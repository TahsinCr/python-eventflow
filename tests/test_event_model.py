"""Compare EventBus to a small reference model driven by repeatable traces."""

import unittest
from collections import deque
from random import Random

from eventflow import EventBus, EventBusClosedError


class EventModelTests(unittest.TestCase):
    def test_routed_dispatch_replay_and_history_match_reference(self) -> None:
        for seed in range(10):
            with (
                self.subTest(seed=seed),
                EventBus[int](history=13, routing_key=lambda n: n % 3) as bus,
            ):
                rng = Random(seed)
                history: deque[int] = deque(maxlen=13)
                subscriptions = {}
                expected: list[tuple[int, int]] = []
                received: list[tuple[int, int]] = []
                for index in range(250):
                    with self.subTest(step=index):
                        operation = rng.randrange(12)
                        if operation < 6:
                            event = rng.randrange(90)
                            history.append(event)
                            deliveries = [
                                (identifier, event)
                                for identifier, (_, route, remainder) in subscriptions.items()
                                if route in (None, event % 3) and event % 5 == remainder
                            ]
                            expected.extend(deliveries)
                            self.assertEqual(bus.publish(event).delivered, len(deliveries))
                        elif operation < 9:
                            route = rng.choice((None, 0, 1, 2))
                            remainder = rng.randrange(5)
                            replay = rng.randrange(5)
                            matches = [
                                event
                                for event in history
                                if route in (None, event % 3) and event % 5 == remainder
                            ]
                            if replay:
                                expected.extend((index, event) for event in matches[-replay:])
                            handle = bus.subscribe(
                                lambda event, index=index: received.append((index, event)),
                                routing_key=route,
                                predicate=lambda event, remainder=remainder: event % 5 == remainder,
                                replay=replay,
                            )
                            subscriptions[index] = (handle, route, remainder)
                        elif operation == 9 and subscriptions:
                            identifier = rng.choice(tuple(subscriptions))
                            handle, _, _ = subscriptions.pop(identifier)
                            handle.cancel()
                        elif operation == 10:
                            bus.clear()
                            subscriptions.clear()
                        else:
                            limit = rng.randrange(1, 8)
                            matches = [event for event in history if event % 2 == 0]
                            self.assertEqual(
                                bus.query(predicate=lambda event: event % 2 == 0, limit=limit),
                                tuple(matches[-limit:]),
                            )
                        self.assertEqual(received, expected)
                        received.clear()
                        expected.clear()
                        self.assertEqual(bus.subscriber_count, len(subscriptions))
                        self.assertEqual(bus.query(), tuple(history))

                bus.close()
                self.assertEqual(bus.subscriber_count, 0)
                self.assertTrue(all(not handle.active for handle, _, _ in subscriptions.values()))
                with self.assertRaises(EventBusClosedError):
                    bus.publish(0)
