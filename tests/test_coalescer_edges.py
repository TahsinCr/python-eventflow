"""Keyed coalescer behavior and edge contracts."""

from __future__ import annotations

import heapq
import inspect
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from eventflow._compatibility import ExceptionGroup
from eventflow.observability import OperationStats
from eventflow.scheduling import KeyedCoalescer


class CoalescerTests(unittest.TestCase):
    def test_coalescer_context_manager_flushes_and_closes_pending_work(self):
        calls = []
        with KeyedCoalescer(
            interval=60,
            key=lambda item: item[0],
            merge=lambda first, second: (first[0], first[1] + second[1]),
            callback=calls.append,
        ) as coalescer:
            coalescer.push(("key", 2))
            self.assertFalse(coalescer.stats().closed)

        self.assertEqual(calls, [("key", 2)])
        self.assertTrue(coalescer.stats().closed)

    def test_coalescer_public_flush_delivers_each_merged_key_once(self):
        calls = []
        coalescer = KeyedCoalescer(
            interval=60,
            key=lambda item: item[0],
            merge=lambda first, second: (first[0], first[1] + second[1]),
            callback=calls.append,
            max_pending_keys=3,
        )
        try:
            for item in (("a", 1), ("b", 2), ("a", 3), ("c", 4), ("b", 5)):
                coalescer.push(item)
            coalescer.flush(timeout=1)

            self.assertCountEqual(calls, [("a", 4), ("b", 7), ("c", 4)])
            self.assertEqual(coalescer.stats().delivered, 3)
            self.assertEqual(coalescer.stats().merged, 2)
        finally:
            coalescer.close(timeout=1)

    def test_keyed_coalescer_merges_and_flushes_with_bounded_keys(self):
        calls = []
        coalescer = KeyedCoalescer(
            interval=10,
            key=lambda item: item[0],
            merge=lambda first, second: (first[0], first[1] + second[1]),
            callback=calls.append,
            max_pending_keys=1,
        )
        coalescer.push(("key", 1))
        coalescer.push(("key", 2))
        with self.assertRaises(RuntimeError):
            coalescer.push(("other", 3))
        self.assertEqual(coalescer.stats().merged, 1)
        coalescer.flush(1)
        coalescer.close()
        self.assertEqual(calls, [("key", 3)])

    def test_keyed_coalescer_selects_due_deadlines_without_ordering_keys(self):
        coalescer = KeyedCoalescer(
            interval=60,
            key=lambda item: item[0],
            merge=lambda _first, second: second,
            callback=lambda _item: None,
            max_pending_keys=3,
        )
        keys = (object(), object(), object())
        try:
            for index, key in enumerate(keys):
                coalescer.push((key, index))
            with coalescer._condition:
                first_deadline = time.monotonic() - 2
                deadlines = (first_deadline, first_deadline, first_deadline - 1)
                for key, deadline in zip(keys, deadlines, strict=True):
                    coalescer._pending[key].deadline = deadline
                coalescer._due_heap = [
                    (deadline, coalescer._pending[key].sequence, key)
                    for key, deadline in zip(keys, deadlines, strict=True)
                ]
                heapq.heapify(coalescer._due_heap)
                self.assertEqual(coalescer._next_due(), (keys[2], (keys[2], 2)))
                self.assertEqual(coalescer._next_due(), (keys[0], (keys[0], 0)))
                self.assertEqual(coalescer._next_due(), (keys[1], (keys[1], 1)))
        finally:
            coalescer.close(flush=False, timeout=1)

    def test_keyed_coalescer_starts_lazily_and_serializes_parallel_merges(self):
        calls = []
        coalescer = KeyedCoalescer(
            interval=60,
            key=lambda item: item[0],
            merge=lambda first, second: (first[0], first[1] + second[1]),
            callback=calls.append,
            max_pending_keys=1,
        )
        self.assertIsNone(coalescer._thread.ident)
        coalescer.flush(timeout=0.1)
        self.assertIsNone(coalescer._thread.ident)

        barrier = threading.Barrier(5)

        def push_many(_index):
            barrier.wait(timeout=1)
            for _ in range(10):
                coalescer.push(("key", 1))

        try:
            with ThreadPoolExecutor(max_workers=4) as producers:
                futures = [producers.submit(push_many, index) for index in range(4)]
                barrier.wait(timeout=1)
                for future in futures:
                    future.result(timeout=2)
            coalescer.flush(timeout=1)
            self.assertEqual(coalescer.stats().merged, 39)
            self.assertEqual(calls, [("key", 40)])
        finally:
            coalescer.close(timeout=1)

    def test_concurrent_coalescer_flush_timeout_keeps_other_flush_active(self):
        started, release = threading.Event(), threading.Event()
        calls = []

        def deliver(value):
            calls.append(value)
            if value == "first":
                started.set()
                release.wait(timeout=2)

        coalescer = KeyedCoalescer(
            interval=60,
            key=lambda value: value,
            merge=lambda _first, second: second,
            callback=deliver,
            max_pending_keys=2,
        )
        with ThreadPoolExecutor(max_workers=2) as workers:
            try:
                coalescer.push("first")
                short_flush = workers.submit(coalescer.flush, 0.5)
                self.assertTrue(started.wait(timeout=1))
                coalescer.push("second")
                long_flush = workers.submit(coalescer.flush, 2)
                with coalescer._condition:
                    self.assertTrue(
                        coalescer._condition.wait_for(
                            lambda: coalescer._flush_requests == 2, timeout=1
                        )
                    )
                with self.assertRaises(TimeoutError):
                    short_flush.result(timeout=1)
                self.assertTrue(coalescer._flushing)
                release.set()
                long_flush.result(timeout=1)
                self.assertEqual(calls, ["first", "second"])
            finally:
                release.set()
                coalescer.close(timeout=1)

    def test_coalescer_timed_close_retry_keeps_first_flush_policy(self):
        started, release = threading.Event(), threading.Event()
        calls = []

        def deliver(value):
            calls.append(value)
            if value == "first":
                started.set()
                release.wait(timeout=2)

        coalescer = KeyedCoalescer(
            interval=60,
            key=lambda value: value,
            merge=lambda _first, second: second,
            callback=deliver,
            max_pending_keys=2,
        )
        coalescer.push("first")
        coalescer.push("second")
        with ThreadPoolExecutor(max_workers=1) as workers:
            try:
                closing = workers.submit(coalescer.close, flush=True, timeout=2)
                self.assertTrue(started.wait(timeout=1))
                with self.assertRaises(TimeoutError):
                    coalescer.close(flush=False, timeout=0)
                release.set()
                closing.result(timeout=1)
                self.assertEqual(calls, ["first", "second"])
            finally:
                release.set()
                coalescer.close(timeout=1)

    def test_coalescer_rejects_async_key_and_merge_functions(self):
        async def async_key(value):
            return value

        with KeyedCoalescer(
            interval=10,
            key=async_key,
            merge=lambda first, _second: first,
            callback=lambda _value: None,
        ) as coalescer:
            with self.assertRaises(TypeError):
                coalescer.push("value")

        async def async_merge(first, _second):
            return first

        coalescer = KeyedCoalescer(
            interval=10,
            key=lambda value: value[0],
            merge=async_merge,
            callback=lambda _value: None,
        )
        coalescer.push(("key", 1))
        with self.assertRaises(TypeError):
            coalescer.push(("key", 2))
        coalescer.close(flush=False)


class CoalescerEdgeTests(unittest.TestCase):
    def test_observer_failure_is_reported_after_pending_work_and_worker_continues(self):
        calls = []
        observer = OperationStats()
        coalescer = KeyedCoalescer(
            interval=60,
            key=lambda value: value,
            merge=lambda _first, second: second,
            callback=calls.append,
            observer=observer,
        )
        try:
            with patch.object(
                observer,
                "record",
                side_effect=[RuntimeError("observer"), None, None],
            ):
                coalescer.push("first")
                coalescer.push("second")
                with self.assertRaises(ExceptionGroup) as raised:
                    coalescer.flush(timeout=1)
                self.assertEqual(str(raised.exception.exceptions[0]), "observer")
                self.assertCountEqual(calls, ["first", "second"])

                coalescer.push("third")
                coalescer.flush(timeout=1)

            self.assertEqual(calls, ["first", "second", "third"])
        finally:
            coalescer.close(timeout=1)

    def test_callback_failures_are_visible_and_pending_work_can_be_discarded(self):
        reported = []

        def broken(_value):
            raise ValueError("delivery")

        coalescer = KeyedCoalescer(
            interval=10,
            key=lambda value: value,
            merge=lambda _first, second: second,
            callback=broken,
            on_error=reported.append,
        )
        coalescer.push("error")
        with self.assertRaises(ExceptionGroup):
            coalescer.flush(1)
        self.assertEqual(len(reported), 1)
        coalescer.close()

        discarded = KeyedCoalescer(
            interval=10,
            key=lambda value: value,
            merge=lambda _first, second: second,
            callback=lambda _value: None,
        )
        discarded.push("pending")
        discarded.close(flush=False)
        self.assertEqual(discarded.stats().discarded, 1)

    def test_coalescer_closes_awaitable_delivery_and_error_callbacks(self):
        callback_coroutines = []
        observer_coroutines = []

        async def async_callback(_value):
            return None

        async def async_observer(_error):
            return None

        def callback(value):
            coroutine = async_callback(value)
            callback_coroutines.append(coroutine)
            return coroutine

        def on_error(error):
            coroutine = async_observer(error)
            observer_coroutines.append(coroutine)
            return coroutine

        coalescer = KeyedCoalescer(
            interval=60,
            key=lambda value: value,
            merge=lambda _first, second: second,
            callback=callback,
            on_error=on_error,
        )
        try:
            coalescer.push("value")
            with self.assertRaises(ExceptionGroup) as raised:
                coalescer.flush(timeout=1)

            self.assertEqual(
                [type(error) for error in raised.exception.exceptions], [TypeError, TypeError]
            )
            self.assertEqual(len(callback_coroutines), 1)
            self.assertEqual(len(observer_coroutines), 1)
            self.assertEqual(inspect.getcoroutinestate(callback_coroutines[0]), inspect.CORO_CLOSED)
            self.assertEqual(inspect.getcoroutinestate(observer_coroutines[0]), inspect.CORO_CLOSED)
            self.assertEqual(coalescer.stats().failures, 1)
        finally:
            coalescer.close(timeout=1)
