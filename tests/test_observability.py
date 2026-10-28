import asyncio
import unittest
from unittest.mock import patch

from eventflow.pipeline import AsyncPipeline, Pipeline, PipelineExecutionError
from eventflow.observability import PipelineStats
from eventflow.context import correlation_context


class ObservabilityTests(unittest.TestCase):
    def test_counts_stage_and_run_durations_with_fake_clock(self):
        stats = PipelineStats()
        pipeline = Pipeline[int](observer=stats)
        pipeline.add("inc", lambda value: value + 1)
        with (
            patch("eventflow.pipeline.sync.perf_counter", side_effect=[1.0, 2.0]),
            patch("eventflow.pipeline.base.perf_counter", side_effect=[5.0, 9.0]),
        ):
            self.assertEqual(pipeline.run(1), 2)
        snapshot = stats.snapshot
        self.assertEqual(snapshot.runs.calls, 1)
        self.assertEqual(snapshot.runs.total_seconds, 8)
        self.assertEqual(snapshot.stages["inc"].total_seconds, 3)
        with self.assertRaises(TypeError):
            snapshot.stages["new"] = snapshot.runs
        stats.reset()
        self.assertEqual(stats.snapshot.runs.calls, 0)

    def test_observer_correlation_and_middleware_short_circuit(self):
        seen = []
        pipeline = Pipeline[int](observer=seen.append)
        pipeline.add("never", lambda value: self.fail("short-circuit failed"))
        pipeline.add_middleware("cached", lambda value, next_: 100)
        with correlation_context("request"):
            self.assertEqual(pipeline.run(1), 100)
        self.assertEqual([item.kind for item in seen], ["middleware", "run"])
        self.assertTrue(all(item.correlation_id == "request" for item in seen))

    def test_observer_failure_preserves_original_error(self):
        original = ValueError("stage")

        def observer(_event):
            raise RuntimeError("observer")

        pipeline = Pipeline[int](observer=observer)

        def fail(value):
            raise original

        pipeline.add("fail", fail)
        with self.assertRaises(PipelineExecutionError) as caught:
            pipeline.run(1)
        self.assertIs(caught.exception.__cause__, original)
        self.assertEqual(len(caught.exception.observer_errors), 2)
        pipeline.replace("fail", lambda value: value)
        with self.assertRaises(PipelineExecutionError) as caught:
            pipeline.run(1)
        self.assertEqual(caught.exception.phase, "observer")

        async def bad(_event):
            pass

        with self.assertRaises(TypeError):
            Pipeline(observer=bad)


class AsyncObservabilityTests(unittest.IsolatedAsyncioTestCase):
    async def test_error_and_cancel_metrics(self):
        stats = PipelineStats()
        pipeline = AsyncPipeline[int](observer=stats)

        async def fail(value):
            raise ValueError("failed")

        pipeline.add("stage", fail)
        with self.assertRaises(PipelineExecutionError):
            await pipeline.run(1)

        async def cancel(value):
            raise asyncio.CancelledError

        pipeline.replace("stage", cancel)
        with self.assertRaises(asyncio.CancelledError):
            await pipeline.run(1)
        self.assertEqual(stats.snapshot.runs.failed, 1)
        self.assertEqual(stats.snapshot.runs.cancelled, 1)
        self.assertEqual(stats.snapshot.stages["stage"].calls, 2)
