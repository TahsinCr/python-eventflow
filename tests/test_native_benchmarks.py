from __future__ import annotations

import unittest

from benchmarks.native.python_kernels import filter_by_type, merge_subscribers


class NativeBenchmarkReferenceTests(unittest.TestCase):
    def test_filter_uses_isinstance_and_preserves_event_order(self) -> None:
        class Event:
            pass

        class Selected(Event):
            pass

        class SelectedChild(Selected):
            pass

        events = (Event(), Selected(), SelectedChild(), Event())

        result = filter_by_type(events, Selected)

        self.assertEqual(result, events[1:3])
        self.assertIs(result[0], events[1])
        self.assertIs(result[1], events[2])

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
