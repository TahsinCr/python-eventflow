from __future__ import annotations

import unittest

from benchmarks.benchmark_native import RoutingEvent, _subscribe_fixture
from benchmarks.native.python_kernels import (
    filter_by_type,
    merge_subscribers,
    merge_subscribers_sorted,
    merge_subscribers_two_pointer,
)
from eventflow.filtering import EventFilter
from eventflow.history import MemoryEventHistory


class _Event:
    pass


class _Selected(_Event):
    pass


class _SelectedChild(_Selected):
    pass


class _Other(_Event):
    pass


class NativeBenchmarkReferenceTests(unittest.TestCase):
    def test_single_key_routing_fixture_publishes_on_its_registered_key(self) -> None:
        bus = _subscribe_fixture(64, merge=None, key_count=1, wildcard_percent=0)
        try:
            result = bus.publish(RoutingEvent(0, 17))
        finally:
            bus.close()

        self.assertEqual(result.matched, 64)
        self.assertEqual(result.delivered, 64)

    def test_filter_uses_isinstance_and_preserves_event_order(self) -> None:
        events = (_Event(), _Selected(), _SelectedChild(), _Event())

        result = filter_by_type(events, _Selected)

        self.assertEqual(result, events[1:3])
        self.assertIs(result[0], events[1])
        self.assertIs(result[1], events[2])

    def test_type_filter_preserves_history_limits_and_type_tuples(self) -> None:
        events = (
            _Selected(),
            _Other(),
            _SelectedChild(),
            _Selected(),
            _Other(),
            _SelectedChild(),
        )
        history = MemoryEventHistory[_Event](capacity=len(events))
        for event in events:
            history.append(event)

        for event_type in (_Selected, (_Selected, _Other)):
            event_filter = EventFilter[_Event](event_type=event_type)
            for limit in (None, 1, 2, 10, 10**100):
                with self.subTest(event_type=event_type, limit=limit):
                    expected = history.query(event_filter, limit=limit)
                    actual = filter_by_type(events, event_type, limit)
                    self.assertEqual(len(actual), len(expected))
                    self.assertTrue(
                        all(
                            item is reference
                            for item, reference in zip(actual, expected, strict=True)
                        )
                    )

        for invalid_limit in (0, -1, True):
            with self.subTest(invalid_limit=invalid_limit):
                with self.assertRaises(ValueError):
                    filter_by_type(events, _Selected, invalid_limit)

    def test_merge_matches_registration_order_and_empty_fast_paths(self) -> None:
        subscribers = {index: object() for index in (2, 5, 9, 14, 21)}
        unkeyed = {2: subscribers[2], 9: subscribers[9], 21: subscribers[21]}
        keyed = {5: subscribers[5], 14: subscribers[14]}

        merged = merge_subscribers(unkeyed, keyed)

        self.assertEqual(merged, tuple(subscribers.values()))
        self.assertTrue(
            all(
                actual is expected
                for actual, expected in zip(merged, subscribers.values(), strict=True)
            )
        )
        self.assertEqual(merge_subscribers({}, keyed), tuple(keyed.values()))
        self.assertEqual(merge_subscribers(unkeyed, {}), tuple(unkeyed.values()))
        self.assertEqual(merge_subscribers({}, {}), ())

    def test_python_merge_algorithms_match_for_skew_and_unbounded_ids(self) -> None:
        identifiers = (1, 3, 8, 10**100, 10**100 + 2)
        subscribers = {identifier: object() for identifier in identifiers}
        cases = (
            (
                {1: subscribers[1], 8: subscribers[8]},
                {
                    3: subscribers[3],
                    10**100: subscribers[10**100],
                    10**100 + 2: subscribers[10**100 + 2],
                },
            ),
            (
                {
                    1: subscribers[1],
                    3: subscribers[3],
                    8: subscribers[8],
                    10**100: subscribers[10**100],
                },
                {10**100 + 2: subscribers[10**100 + 2]},
            ),
            ({}, {3: subscribers[3], 10**100: subscribers[10**100]}),
            ({1: subscribers[1]}, {}),
            ({}, {}),
        )
        algorithms = (merge_subscribers, merge_subscribers_two_pointer, merge_subscribers_sorted)

        for unkeyed, keyed in cases:
            expected = merge_subscribers(unkeyed, keyed)
            for algorithm in algorithms:
                with self.subTest(algorithm=algorithm.__name__, size=len(unkeyed) + len(keyed)):
                    actual = algorithm(unkeyed, keyed)
                    self.assertEqual(len(actual), len(expected))
                    self.assertTrue(
                        all(
                            item is reference
                            for item, reference in zip(actual, expected, strict=True)
                        )
                    )
