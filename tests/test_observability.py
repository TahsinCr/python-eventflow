import asyncio
import threading
import unittest
from unittest.mock import patch

from eventflow.pipeline import AsyncPipeline, Pipeline, PipelineExecutionError
from eventflow.observability import PipelineObservation, PipelineStats
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

    def test_snapshots_are_independent_of_later_updates_and_reset(self):
        stats = PipelineStats()
        stats(PipelineObservation("run", None, 1.0, "success"))
        stats(PipelineObservation("stage", "stable", 0.5, "success"))
        before = stats.snapshot

        stats(PipelineObservation("run", None, 2.0, "error"))
        stats(PipelineObservation("stage", "dynamic", 0.25, "cancelled"))
        updated = stats.snapshot
        stats.reset()

        self.assertEqual(before.runs.calls, 1)
        self.assertEqual(before.runs.successful, 1)
        self.assertEqual(before.stages["stable"].calls, 1)
        self.assertNotIn("dynamic", before.stages)
        self.assertEqual(updated.runs.calls, 2)
        self.assertEqual(updated.runs.failed, 1)
        self.assertEqual(updated.stages["dynamic"].cancelled, 1)
        self.assertEqual(stats.snapshot.runs.calls, 0)
        self.assertEqual(stats.snapshot.stages, {})

    def test_concurrent_metric_updates_are_not_lost(self):
        stats = PipelineStats()
        workers = 8
        observations_per_worker = 1_000
        start = threading.Barrier(workers + 1)

        def record(worker: int) -> None:
            start.wait()
            for _ in range(observations_per_worker):
                stats(PipelineObservation("stage", f"worker-{worker}", 0.001, "success"))

        threads = [threading.Thread(target=record, args=(worker,)) for worker in range(workers)]
        for thread in threads:
            thread.start()
        start.wait()
        yield_to_workers = threading.Event()
        while any(thread.is_alive() for thread in threads):
            _ = stats.snapshot
            yield_to_workers.wait(0.0001)
        for thread in threads:
            thread.join()

        final = stats.snapshot
        self.assertEqual(
            sum(metric.calls for metric in final.stages.values()),
            workers * observations_per_worker,
        )
        for worker in range(workers):
            self.assertEqual(final.stages[f"worker-{worker}"].calls, observations_per_worker)

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

    async def test_observer_failure_does_not_replace_pipeline_cancellation(self):
        original = asyncio.CancelledError("stage cancelled")
        observer_errors = []
        pipeline = AsyncPipeline[int](
            observer=lambda observation: self._fail_on_cancellation(observation, observer_errors)
        )

        async def cancel(value: int) -> int:
            raise original

        pipeline.add("cancel", cancel)
        with self.assertRaises(asyncio.CancelledError) as caught:
            await pipeline.run(1)

        self.assertIs(caught.exception, original)
        self.assertEqual(str(caught.exception.__cause__), "observer failed")
        self.assertGreaterEqual(len(observer_errors), 1)

    @staticmethod
    def _fail_on_cancellation(observation, errors):
        if observation.status == "cancelled":
            error = RuntimeError("observer failed")
            errors.append(error)
            raise error
