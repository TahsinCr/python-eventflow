"""Behavior of bounded history using shared immutable blocks."""

import threading
import unittest
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
from weakref import ref

from eventflow import EventBus, EventFilter, MemoryEventHistory


class BlockHistoryTests(unittest.TestCase):
    def test_retention_and_query_order_across_partial_blocks(self):
        even = EventFilter[int](predicate=lambda event: event % 2 == 0)
        for capacity, block_size in ((1, 1), (5, 2), (7, 7), (13, 4)):
            with self.subTest(capacity=capacity, block_size=block_size):
                history = MemoryEventHistory[int](capacity, block_size=block_size)
                expected = deque(maxlen=capacity)
                for event in range(50):
                    if event == 25:
                        expected.clear()
                        history.clear()
                        self.assertIsNone(history.latest())
                    history.append(event)
                    expected.append(event)
                    matches = [value for value in expected if value % 2 == 0]
                    self.assertEqual(len(history), len(expected))
                    self.assertEqual(history.query(), tuple(expected))
                    self.assertEqual(history.query(limit=3), tuple(expected)[-3:])
                    self.assertEqual(history.query(even, limit=3), tuple(matches[-3:]))
                    self.assertEqual(history.latest(), event)
                    self.assertEqual(history.latest(even), matches[-1] if matches else None)

    def test_eviction_and_clear_release_payloads(self):
        class Payload:
            pass

        history = MemoryEventHistory[Payload](3, block_size=2)
        payloads = [Payload() for _ in range(5)]
        references = [ref(payload) for payload in payloads]
        for payload in payloads:
            history.append(payload)
            history.query()
        del payload, payloads
        self.assertEqual(
            [reference() is None for reference in references], [True, True, False, False, False]
        )
        history.clear()
        self.assertTrue(all(reference() is None for reference in references))

    def test_type_membership_is_evaluated_again_after_class_change(self):
        class First:
            pass

        class Second:
            pass

        event = First()
        history = MemoryEventHistory[object](4, block_size=2)
        history.append(event)
        history.append(Second())
        first = EventFilter(event_type=First)
        self.assertEqual(history.query(first), (event,))
        event.__class__ = Second
        self.assertEqual(history.query(first), ())

    def test_failed_seal_preserves_the_window_and_allows_retry(self):
        class FailingBlocks(deque):
            failed = False

            def append(self, value):
                if not self.failed:
                    self.failed = True
                    raise MemoryError("cannot seal block")
                super().append(value)

        history = MemoryEventHistory[int](3, block_size=2)
        for event in range(3):
            history.append(event)
        store = history._events
        with patch.object(store, "blocks", FailingBlocks(store.blocks)):
            with self.assertRaisesRegex(MemoryError, "cannot seal block"):
                history.append(3)
            self.assertEqual(history.query(), (0, 1, 2))
            history.append(3)
            self.assertEqual(history.query(), (1, 2, 3))

    def test_replay_snapshot_survives_eviction_and_delivers_live_events_last(self):
        history = MemoryEventHistory[int](16, block_size=4)
        entered = threading.Event()
        resume = threading.Event()
        received = []

        def matches(event: int) -> bool:
            if event == 13:
                entered.set()
                if not resume.wait(10):
                    raise TimeoutError("replay filter was not released")
            return event % 2 == 1

        with EventBus[int](history=history) as bus, ThreadPoolExecutor(1) as pool:
            for event in range(16):
                bus.publish(event)
            replay = pool.submit(bus.subscribe, received.append, predicate=matches, replay=3)
            try:
                self.assertTrue(entered.wait(10))
                for event in range(16, 34):
                    bus.publish(event)
                history.clear()
            finally:
                resume.set()
            replay.result(timeout=10).cancel()
        self.assertEqual(received, [11, 13, 15, *range(17, 34, 2)])
