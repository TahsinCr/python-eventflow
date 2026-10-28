import asyncio
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from eventflow import CallbackSubscription
from eventflow.pipeline import AsyncPipeline, Pipeline, PipelineExecutionError


class PipelineTests(unittest.TestCase):
    def test_registry_order_decorator_identity_and_atomic_errors(self):
        pipeline = Pipeline[str]()
        self.assertEqual(pipeline.run(""), "")

        def middle(value):
            return value + "m"

        self.assertIs(pipeline.stage("middle")(middle), middle)
        pipeline.add("first", lambda value: value + "f", before="middle")
        pipeline.add("last", lambda value: value + "l", after="middle")
        self.assertEqual(pipeline.stage_names, ("first", "middle", "last"))
        self.assertEqual(pipeline.run(""), "fml")
        for name, before, after, error in (
            ("", None, None, ValueError),
            ("middle", None, None, ValueError),
            ("unknown", "missing", None, KeyError),
            ("cycle", "middle", "last", ValueError),
            ("self", "self", None, ValueError),
        ):
            with self.subTest(name=name), self.assertRaises(error):
                pipeline.add(name, middle, before=before, after=after)
            self.assertEqual(pipeline.run(""), "fml")
        pipeline.disable("middle")
        pipeline.replace("middle", lambda value: value + "M")
        self.assertEqual(pipeline.run(""), "fl")
        pipeline.enable("middle")
        self.assertEqual(pipeline.run(""), "fMl")
        pipeline.remove("middle")
        self.assertEqual(pipeline.run(""), "fl")
        for operation in (pipeline.remove, pipeline.enable, pipeline.disable):
            with self.assertRaises(KeyError):
                operation("missing")
        with self.assertRaises(KeyError):
            pipeline.replace("missing", middle)
        with self.assertRaises(TypeError):
            pipeline.add("bad", None)

    def test_sync_awaitable_rejection_closes_coroutine(self):
        pipeline = Pipeline[int]()

        async def asynchronous(value):
            return value

        with self.assertRaises(TypeError):
            pipeline.add("async", asynchronous)
        created = []

        def wrapper(value):
            result = asynchronous(value)
            created.append(result)
            return result

        pipeline.add("wrapped", wrapper)
        with self.assertRaises(PipelineExecutionError) as caught:
            pipeline.run(1)
        self.assertIsInstance(caught.exception.__cause__, TypeError)
        self.assertIsNone(created[0].cr_frame)

    def test_fail_fast_preserves_context_and_cause(self):
        pipeline = Pipeline[int]()
        original = ValueError("broken")
        pipeline.add("first", lambda value: value + 1)

        def fail(_value):
            raise original

        pipeline.add("failed", fail)
        pipeline.add("unreachable", lambda value: self.fail("called after failure"))
        with self.assertRaises(PipelineExecutionError) as caught:
            pipeline.run(4)
        self.assertIs(caught.exception.__cause__, original)
        self.assertEqual((caught.exception.name, caught.exception.context), ("failed", 5))

    def test_middleware_around_short_circuit_recovery_and_mutations(self):
        pipeline = Pipeline[int]()
        calls = []
        pipeline.add("double", lambda value: value * 2)

        def outer(value, next_):
            calls.append("before")
            result = next_(value + 1)
            calls.append("after")
            return result + 1

        self.assertIs(pipeline.middleware("outer")(outer), outer)
        pipeline.add_middleware("inner", lambda value, next_: next_(value + 2))
        self.assertEqual(pipeline.run(1), 9)
        self.assertEqual(calls, ["before", "after"])
        pipeline.replace_middleware("inner", lambda value, next_: 100)
        self.assertEqual(pipeline.run(1), 101)
        pipeline.disable_middleware("inner")
        self.assertEqual(pipeline.run(1), 5)
        pipeline.enable_middleware("inner")
        self.assertEqual(pipeline.remove_middleware("outer"), outer)
        self.assertEqual(pipeline.middleware_names, ("inner",))
        self.assertEqual(pipeline.run(1), 100)
        pipeline.replace_middleware("inner", lambda value, next_: next_(value))
        pipeline.replace("double", lambda value: 1 / 0)

        def recover(value, next_):
            try:
                return next_(value)
            except PipelineExecutionError:
                return 42

        pipeline.add_middleware("recover", recover, before="inner")
        self.assertEqual(pipeline.run(1), 42)
        with self.assertRaises(ValueError):
            pipeline.add_middleware("cycle", recover, before="recover", after="inner")

    def test_continuation_is_single_use_and_cannot_escape(self):
        pipeline = Pipeline[int]()
        pipeline.add("inc", lambda value: value + 1)

        def twice(value, next_):
            next_(value)
            return next_(value)

        pipeline.add_middleware("twice", twice)
        with self.assertRaises(PipelineExecutionError) as caught:
            pipeline.run(0)
        self.assertIsInstance(caught.exception.__cause__, RuntimeError)
        saved = []
        pipeline.replace_middleware("twice", lambda value, next_: saved.append(next_) or value)
        pipeline.run(0)
        with self.assertRaises(RuntimeError):
            saved[0](0)

    def test_running_thread_uses_original_snapshot(self):
        pipeline = Pipeline[int]()
        entered, release = threading.Event(), threading.Event()

        def blocking(value):
            entered.set()
            self.assertTrue(release.wait(2))
            return value + 1

        pipeline.add("blocking", blocking)
        pipeline.add("last", lambda value: value * 2)
        with ThreadPoolExecutor(1) as pool:
            running = pool.submit(pipeline.run, 1)
            self.assertTrue(entered.wait(2))
            try:
                pipeline.replace("blocking", lambda value: value + 10)
                pipeline.disable("last")
                self.assertEqual(pipeline.run(1), 11)
            finally:
                release.set()
            self.assertEqual(running.result(2), 4)

    def test_multiple_threads_and_decorators_share_one_registry(self):
        pipeline = Pipeline[int]()
        callback = CallbackSubscription(1, lambda: None, lambda value: value + 1)
        hooks = []

        @callback.on_before
        def before(context):
            hooks.append(context.event)

        pipeline.stage("policy")(callback.invoke)
        with ThreadPoolExecutor(4) as pool:
            self.assertEqual(list(pool.map(pipeline.run, range(20))), list(range(1, 21)))
        self.assertEqual(len(hooks), 20)

    def test_middleware_continuation_rejects_other_threads(self):
        pipeline = Pipeline[int]()

        def middleware(value, next_):
            with ThreadPoolExecutor(1) as pool:
                return pool.submit(next_, value).result()

        pipeline.add_middleware("thread", middleware)
        with self.assertRaises(PipelineExecutionError) as caught:
            pipeline.run(1)
        self.assertIsInstance(caught.exception.__cause__, RuntimeError)

    def test_disabled_observability_does_not_read_clock(self):
        pipeline = Pipeline[int]()
        pipeline.add("identity", lambda value: value)
        with (
            patch("eventflow.pipeline.sync.perf_counter", side_effect=AssertionError),
            patch("eventflow.pipeline.base.perf_counter", side_effect=AssertionError),
        ):
            self.assertEqual(pipeline.run(1), 1)


class AsyncPipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_decorators_direct_registration_and_async_middleware(self):
        pipeline = AsyncPipeline[int]()

        async def first(value):
            return value + 1

        self.assertIs(pipeline.stage("first")(first), first)
        pipeline.add("second", first, after="first")

        @pipeline.middleware("around")
        async def around(value, next_):
            return (await next_(value)) * 2

        self.assertEqual(await pipeline.run(1), 6)
        pipeline.disable("first")
        self.assertEqual(await pipeline.run(1), 4)
        pipeline.enable("first")
        pipeline.remove("second")
        pipeline.replace("first", first)
        self.assertEqual(await pipeline.run(1), 4)

    async def test_wrong_runtime_result_is_rejected_without_thread(self):
        pipeline = AsyncPipeline[int]()
        pipeline.add("sync", lambda value: value)
        with self.assertRaises(PipelineExecutionError) as caught:
            await pipeline.run(1)
        self.assertIsInstance(caught.exception.__cause__, TypeError)

    async def test_missing_await_in_async_stage_closes_nested_coroutine(self):
        pipeline = AsyncPipeline[int]()
        created = []

        async def result(value):
            return value

        async def wrong(value):
            operation = result(value)
            created.append(operation)
            return operation

        pipeline.add("wrong", wrong)
        with self.assertRaises(PipelineExecutionError):
            await pipeline.run(1)
        self.assertIsNone(created[0].cr_frame)

    async def test_snapshot_concurrent_runs_and_cancellation_cleanup(self):
        pipeline = AsyncPipeline[int]()
        entered, release, cleaned = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def block(value):
            entered.set()
            try:
                await release.wait()
                return value + 1
            finally:
                cleaned.set()

        pipeline.add("first", block)
        pipeline.add("last", block)
        running = asyncio.create_task(pipeline.run(1))
        await entered.wait()

        async def replacement(value):
            return value + 10

        pipeline.replace("first", replacement)
        pipeline.remove("last")
        self.assertEqual(await asyncio.gather(pipeline.run(2), pipeline.run(3)), [12, 13])
        release.set()
        self.assertEqual(await running, 3)
        pipeline.replace("first", block)
        release.clear()
        entered.clear()
        cleaned.clear()
        running = asyncio.create_task(pipeline.run(1))
        await entered.wait()
        running.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await running
        self.assertTrue(cleaned.is_set())

    async def test_async_continuations_single_use_and_task_owned(self):
        pipeline = AsyncPipeline[int]()

        async def twice(value, next_):
            await next_(value)
            return await next_(value)

        pipeline.add_middleware("twice", twice)
        with self.assertRaises(PipelineExecutionError):
            await pipeline.run(1)

        async def detached(value, next_):
            return await asyncio.create_task(next_(value))

        pipeline.replace_middleware("twice", detached)
        with self.assertRaises(PipelineExecutionError):
            await pipeline.run(1)
        saved = []

        async def save(value, next_):
            saved.append(next_)
            return value

        pipeline.replace_middleware("twice", save)
        await pipeline.run(1)
        with self.assertRaises(RuntimeError):
            await saved[0](1)


class AsyncPipelineOwnershipTests(unittest.TestCase):
    def test_different_loop_is_rejected(self):
        pipeline = AsyncPipeline[int]()
        self.assertEqual(asyncio.run(pipeline.run(1)), 1)
        with self.assertRaises(RuntimeError):
            asyncio.run(pipeline.run(1))
