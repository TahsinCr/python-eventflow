from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def _run_benchmark(module: str, *arguments: str, timeout: int = 20):
    return subprocess.run(
        [sys.executable, "-m", module, *arguments],
        cwd=REPOSITORY_ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


class BenchmarkTests(unittest.TestCase):
    def test_bridge_benchmark_reports_direct_portal_and_queue_roundtrips(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            output = Path(temporary_directory) / "bridges.json"
            _run_benchmark(
                "benchmarks.benchmark_bridges",
                "--iterations",
                "1",
                "--output",
                str(output),
            )
            results = json.loads(output.read_text(encoding="utf-8"))["results"]

        by_case = {result["case"]: result for result in results}
        self.assertEqual(
            set(by_case),
            {
                "async-direct-await",
                "loop-portal-roundtrip",
                "sync-bus-to-async-queue-roundtrip",
            },
        )
        self.assertTrue(all(result["ops_per_second"] > 0 for result in by_case.values()))

    def test_coalescer_benchmark_measures_worker_flush_and_merge_cases(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            output = Path(temporary_directory) / "coalescer.json"
            _run_benchmark(
                "benchmarks.benchmark_coalescer",
                "--key-counts",
                "8",
                "16",
                "--samples",
                "1",
                "--producers",
                "2",
                "--merge-updates",
                "2",
                "--output",
                str(output),
            )

            report = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(report["benchmark"], "keyed-coalescer-worker-lifecycle")
        self.assertEqual(
            {result["case"] for result in report["results"]},
            {"single-producer", "multi-producer", "merge-heavy", "concurrent-flush"},
        )
        self.assertTrue(all(result["median_total_seconds"] > 0 for result in report["results"]))

    def test_runtime_report_covers_contention_memory_and_schedule_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            output = Path(temporary_directory) / "runtime.json"
            _run_benchmark(
                "benchmarks.benchmark_runtime",
                "--iterations",
                "1",
                "--soak-operations",
                "2",
                "--samples",
                "1",
                "--publisher-counts",
                "1",
                "--schedule-counts",
                "1",
                "--output",
                str(output),
            )

            results = json.loads(output.read_text(encoding="utf-8"))["results"]

        by_case = {result["case"]: result for result in results}
        self.assertIn("sync-publisher-contention/1", by_case)
        self.assertIn("async-publisher-contention/1/max-concurrency=None", by_case)
        self.assertIn("sync-memory-soak/history=0", by_case)
        self.assertIn("async-memory-soak/history=128", by_case)
        self.assertEqual(
            by_case["sync-periodic-schedules/1"]["remaining_threads_after_close_peak"], 0
        )
        self.assertEqual(
            by_case["async-periodic-schedules/1"]["remaining_tasks_after_cancel_peak"], 0
        )

    def test_keyed_limiter_benchmark_reports_waiter_samples(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            output = Path(temporary_directory) / "limiter.json"
            _run_benchmark(
                "benchmarks.benchmark_keyed_limiter",
                "--waiters",
                "8",
                "16",
                "--repeats",
                "1",
                "--output",
                str(output),
            )
            report = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(report["benchmark"], "keyed-concurrency-same-key-waiters")
        self.assertEqual([result["waiters"] for result in report["results"]], [8, 16])
        self.assertTrue(all(result["p50_seconds"] > 0 for result in report["results"]))

    def test_rate_limiter_benchmark_reports_wait_and_cancellation_distributions(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            output = Path(temporary_directory) / "rate-limiter.json"
            _run_benchmark(
                "benchmarks.benchmark_rate_limiter",
                "--workers",
                "4",
                "--acquisitions",
                "2",
                "--rate",
                "10000",
                "--repeats",
                "1",
                "--cancel-fraction",
                "0.25",
                "--output",
                str(output),
            )
            report = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(report["benchmark"], "keyed-rate-limiter-wait-fairness")
        self.assertEqual(report["completed_acquisitions"], 6)
        self.assertEqual(report["observed_cancel_fraction"], 0.25)
        self.assertGreater(report["acquisition_wait_seconds"]["p99"], 0)
        self.assertGreater(report["worker_completion_seconds"]["max_to_median_ratio"], 0)

    def test_publish_and_pipeline_reports_include_error_and_middleware_cases(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            callback_output = Path(temporary_directory) / "callbacks.json"
            pipeline_output = Path(temporary_directory) / "pipeline.json"
            for module, output in (
                ("benchmarks.benchmark_publish", callback_output),
                ("benchmarks.benchmark_pipeline", pipeline_output),
            ):
                _run_benchmark(
                    module,
                    "--iterations",
                    "1",
                    "--output",
                    str(output),
                )

            callback_cases = {
                result["case"]
                for result in json.loads(callback_output.read_text(encoding="utf-8"))["results"]
            }
            pipeline_cases = {
                result["case"]
                for result in json.loads(pipeline_output.read_text(encoding="utf-8"))["results"]
            }

        self.assertIn("sync/callback-error-rate-100%", callback_cases)
        self.assertIn("async-concurrent/callback-error-rate-10%", callback_cases)
        self.assertIn("sync/20/middleware", pipeline_cases)
        self.assertIn("async/20/middleware", pipeline_cases)

    def test_stream_report_includes_equivalent_native_baseline(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            output = Path(temporary_directory) / "stream.json"
            _run_benchmark(
                "benchmarks.benchmark_stream",
                "--iterations",
                "1",
                "--items",
                "5",
                "--output",
                str(output),
            )

            results = json.loads(output.read_text(encoding="utf-8"))["results"]

        by_case = {result["case"]: result for result in results}
        self.assertEqual(
            set(by_case),
            {
                "direct-generator",
                "native-map-filter",
                "unbuffered",
                "buffer-16",
                "native-batch-64",
                "batch_within-64-burst",
            },
        )
        for result in by_case.values():
            self.assertEqual(result["items_per_operation"], 5)
            self.assertGreater(result["items_per_second"], 0)

    def test_replay_benchmark_includes_sparse_filtered_history(self) -> None:
        result = _run_benchmark(
            "benchmarks.benchmark_replay",
            "--events",
            "20",
            "--replay",
            "1",
            "--iterations",
            "1",
            "--samples",
            "1",
            timeout=10,
        )
        self.assertIn("Sparse filtered history query", result.stdout)


if __name__ == "__main__":
    unittest.main()
