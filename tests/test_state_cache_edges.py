"""Edge cases for deadlines, state, and cache primitives."""

from __future__ import annotations

import unittest

from eventflow.cache import VersionedCache
from eventflow.state import StateMachine, StateTransitionError, VersionedValue
from eventflow.timing import Deadline
from _support import ManualClock


class TimingStateCacheTests(unittest.TestCase):
    def test_deadline_validation_and_unlimited_child(self):
        clock = ManualClock(4.0)
        deadline = Deadline.after(0, clock=clock)
        self.assertEqual(deadline.remaining, 0)
        self.assertTrue(deadline.expired)
        self.assertIs(deadline.child(None), deadline)
        absolute = Deadline.at(8, clock=clock)
        self.assertEqual(absolute.remaining, 4)
        clock.set(9)
        self.assertEqual(absolute.remaining, 0)
        with self.assertRaises(ValueError):
            Deadline.after(10**1000)
        with self.assertRaises(TypeError):
            Deadline.at(True)
        with self.assertRaises(ValueError):
            Deadline.at(float("inf"))
        with self.assertRaises(ValueError):
            Deadline.after(1, clock=lambda: float("nan"))
        bad_clock_deadline = Deadline.at(1, clock=lambda: float("inf"))
        with self.assertRaises(ValueError):
            _ = bad_clock_deadline.remaining
        with self.assertRaises(ValueError):
            Deadline(float("nan"))

    def test_state_graph_guards_observers_and_compare_and_set_validation(self):
        guarded = StateMachine("new", {"new": {"ready"}}, guard=lambda *_: False)
        self.assertFalse(guarded.can_transition("ready"))
        self.assertFalse(guarded.can_transition("missing"))
        with self.assertRaises(StateTransitionError):
            guarded.transition("ready")

        closed = []

        async def async_guard(*_args):
            return True

        async_guard_result = async_guard("old", "new")
        machine = StateMachine("old", {"old": {"new"}}, guard=lambda *_: async_guard_result)
        with self.assertRaisesRegex(TypeError, "synchronous"):
            machine.transition("new")
        self.assertIsNone(async_guard_result.cr_frame)

        async def async_observer(*_args):
            return None

        observer_result = async_observer(None, None)

        def observe(previous, current):
            closed.append((previous, current))
            return observer_result

        machine = StateMachine(
            0,
            {0: {1}},
            observer=observe,
        )
        with self.assertRaisesRegex(TypeError, "synchronous"):
            machine.transition(1)
        self.assertEqual(machine.state, 1)
        self.assertIsNone(observer_result.cr_frame)
        self.assertEqual(len(closed), 1)

        for constructor in (
            lambda: StateMachine(0, []),
            lambda: StateMachine(0, {1: {2}}),
            lambda: StateMachine(0, {0: {1}}, terminal={3}),
            lambda: StateMachine(0, {0: {1}}, guard=object()),
            lambda: StateMachine(0, {0: {1}}, observer=object()),
        ):
            with self.assertRaises((TypeError, ValueError)):
                constructor()
        for expected in (True, -1, 1.5):
            with self.assertRaises((TypeError, ValueError)):
                machine.compare_and_transition(expected, 1)

        value = VersionedValue("start")
        self.assertTrue(value.compare_and_set(0, "done"))
        self.assertFalse(value.compare_and_set(0, "stale"))
        for expected in (True, -1, 0.5):
            with self.assertRaises((TypeError, ValueError)):
                value.compare_and_set(expected, "invalid")

    def test_cache_bounds_validation_removal_and_version_misses(self):
        with self.assertRaises(ValueError):
            VersionedCache(max_entries=0)
        with self.assertRaises(ValueError):
            VersionedCache(max_entries=1, max_bytes=0, getsizeof=lambda *_: 1)
        with self.assertRaises(ValueError):
            VersionedCache(max_entries=1, max_bytes=2)
        with self.assertRaises((TypeError, ValueError)):
            VersionedCache(max_entries=1, ttl=0)
        with self.assertRaises(TypeError):
            VersionedCache(max_entries=1, getsizeof=object())
        with self.assertRaises(TypeError):
            VersionedCache(max_entries=1, clock=object())

        cache = VersionedCache(
            max_entries=2,
            max_bytes=2,
            getsizeof=lambda _key, value: len(value),
        )
        self.assertFalse(cache.put("large", "abc", version=1))
        self.assertTrue(cache.put("item", "x", version=2))
        self.assertIsNone(cache.get("item", version=1))
        self.assertEqual(cache.get("item", version=2), "x")
        self.assertEqual(cache.remove("item"), "x")
        self.assertIsNone(cache.remove("item"))
        self.assertEqual(cache.stats().misses, 1)
        cache.put("next", "y", version=1)
        cache.clear()
        self.assertEqual(cache.stats().entries, 0)
        with self.assertRaises(ValueError):
            cache.put("bad", "x", version=True)
        with self.assertRaises(ValueError):
            cache.get("bad", version=-1)
