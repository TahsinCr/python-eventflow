from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from benchmarks._measure import source_provenance

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def _run_benchmark(module: str, *arguments: str, timeout: int = 20):
    result = subprocess.run(
        [sys.executable, "-m", module, *arguments],
        cwd=REPOSITORY_ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if result.returncode:
        raise AssertionError(
            f"{module} exited with {result.returncode}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


class BenchmarkTests(unittest.TestCase):
    def assert_source_metadata(self, document: dict[str, object]) -> None:
        self.assertIn("gil_enabled", document)
        source = document["source"]
        self.assertIsInstance(source, dict)
        self.assertIn("revision", source)
        self.assertIn("source_dirty", source)
        self.assertRegex(source["source_sha256"], r"^[0-9a-f]{64}$")

    def test_bridge_benchmark_reports_same_and_cross_thread_queue_roundtrips(self) -> None:
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
                "same-thread-waiting-consumer-roundtrip",
                "cross-thread-waiting-consumer-roundtrip",
            },
        )
        self.assertTrue(all(result["ops_per_second"] > 0 for result in by_case.values()))

    def test_multi_publisher_bridge_benchmark_reports_scaling_and_source(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            output = Path(temporary_directory) / "multi-publisher.json"
            _run_benchmark(
                "benchmarks.benchmark_bridge_publishers",
                "--publishers",
                "2",
                "4",
                "--iterations",
                "12",
                "--repeats",
                "1",
                "--output",
                str(output),
            )
            document = json.loads(output.read_text(encoding="utf-8"))

        self.assert_source_metadata(document)
        results = document["results"]
        self.assertEqual([result["publishers"] for result in results], [2, 4])
        for result in results:
            self.assertEqual(result["operations_per_sample"], result["publishers"] * 12)
            self.assertGreater(result["median_cpu_utilization_percent"], 0)
            sample = result["samples"][0]
            self.assertEqual(sample["delivered_events"], result["operations_per_sample"])
            self.assertGreater(sample["throughput_ops_per_second"], 0)
            self.assertGreater(sample["cpu_seconds"], 0)
            self.assertGreater(sample["cpu_utilization_percent"], 0)
            latency = sample["roundtrip_latency_us"]
            self.assertLessEqual(latency["p50"], latency["p95"])
            self.assertLessEqual(latency["p95"], latency["p99"])
            self.assertLessEqual(latency["p99"], latency["max"])

    def test_source_fingerprint_tracks_code_and_ignores_benchmark_results(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            (root / "eventflow").mkdir()
            (root / "benchmarks" / "results").mkdir(parents=True)
            (root / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
            (root / "eventflow" / "__init__.py").write_text("value = 1\n", encoding="utf-8")
            benchmark = root / "benchmarks" / "benchmark.py"
            benchmark.write_text("value = 1\n", encoding="utf-8")

            before = source_provenance(root)
            (root / "benchmarks" / "results" / "run.json").write_text("{}\n", encoding="utf-8")
            after_result = source_provenance(root)
            benchmark.write_text("value = 2\n", encoding="utf-8")
            after_source_change = source_provenance(root)

        self.assertEqual(before["source_sha256"], after_result["source_sha256"])
        self.assertNotEqual(before["source_sha256"], after_source_change["source_sha256"])

    @unittest.skipUnless(shutil.which("git"), "Git is required for source status coverage")
    def test_source_dirty_flag_ignores_results_and_detects_code_changes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            (root / "eventflow").mkdir()
            (root / "benchmarks" / "results").mkdir(parents=True)
            (root / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
            (root / "eventflow" / "__init__.py").write_text("value = 1\n", encoding="utf-8")
            benchmark = root / "benchmarks" / "benchmark.py"
            benchmark.write_text("value = 1\n", encoding="utf-8")
            subprocess.run(["git", "init", "-q"], cwd=root, check=True)
            subprocess.run(
                ["git", "add", "pyproject.toml", "eventflow", "benchmarks"], cwd=root, check=True
            )
            subprocess.run(
                [
                    "git",
                    "-c",
                    "user.name=EventFlow Tests",
                    "-c",
                    "user.email=tests@example.invalid",
                    "-c",
                    "commit.gpgsign=false",
                    "commit",
                    "-qm",
                    "benchmark source fixture",
                ],
                cwd=root,
                check=True,
            )

            clean = source_provenance(root)
            (root / "benchmarks" / "results" / "run.json").write_text("{}\n", encoding="utf-8")
            with_result = source_provenance(root)
            benchmark.write_text("value = 2\n", encoding="utf-8")
            with_source_change = source_provenance(root)

        self.assertFalse(clean["source_dirty"])
        self.assertFalse(with_result["source_dirty"])
        self.assertTrue(with_source_change["source_dirty"])

    def test_comparison_benchmark_reports_candidate_and_baseline_sources(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            baseline = root / "baseline"
            baseline.mkdir()
            shutil.copytree(
                REPOSITORY_ROOT / "eventflow",
                baseline / "eventflow",
                ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
            )
            output = root / "comparison.json"
            _run_benchmark(
                "benchmarks.benchmark_compare",
                "--baseline",
                str(baseline),
                "--iterations",
                "1",
                "--samples",
                "1",
                "--output",
                str(output),
                timeout=60,
            )
            document = json.loads(output.read_text(encoding="utf-8"))

        self.assert_source_metadata(document)
        baseline_source = document["baseline_source"]
        self.assertIsInstance(baseline_source, dict)
        self.assertRegex(baseline_source["source_sha256"], r"^[0-9a-f]{64}$")

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
        self.assert_source_metadata(report)
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
                "1",
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
        self.assert_source_metadata(report)
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
        self.assert_source_metadata(report)
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
