from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class BenchmarkTests(unittest.TestCase):
    def test_runtime_report_covers_contention_memory_and_schedule_cleanup(self) -> None:
        repository = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as temporary_directory:
            output = Path(temporary_directory) / "runtime.json"
            subprocess.run(
                [
                    sys.executable,
                    "-m",
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
                ],
                cwd=repository,
                check=True,
                capture_output=True,
                text=True,
                timeout=20,
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

    def test_publish_and_pipeline_reports_include_error_and_middleware_cases(self) -> None:
        repository = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as temporary_directory:
            callback_output = Path(temporary_directory) / "callbacks.json"
            pipeline_output = Path(temporary_directory) / "pipeline.json"
            for module, output in (
                ("benchmarks.benchmark_publish", callback_output),
                ("benchmarks.benchmark_pipeline", pipeline_output),
            ):
                subprocess.run(
                    [
                        sys.executable,
                        "-m",
                        module,
                        "--iterations",
                        "1",
                        "--output",
                        str(output),
                    ],
                    cwd=repository,
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=20,
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
        repository = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as temporary_directory:
            output = Path(temporary_directory) / "stream.json"
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "benchmarks.benchmark_stream",
                    "--iterations",
                    "1",
                    "--items",
                    "5",
                    "--output",
                    str(output),
                ],
                cwd=repository,
                check=True,
                capture_output=True,
                text=True,
            )

            results = json.loads(output.read_text(encoding="utf-8"))["results"]

        by_case = {result["case"]: result for result in results}
        self.assertEqual(
            set(by_case),
            {"direct-generator", "native-map-filter", "unbuffered", "buffer-16"},
        )
        for result in by_case.values():
            self.assertEqual(result["items_per_operation"], 5)
            self.assertGreater(result["items_per_second"], 0)

    def test_replay_benchmark_includes_sparse_filtered_history(self) -> None:
        repository = Path(__file__).resolve().parents[1]
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "benchmarks.benchmark_replay",
                "--events",
                "20",
                "--replay",
                "1",
                "--iterations",
                "1",
                "--samples",
                "1",
            ],
            cwd=repository,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
        self.assertIn("Sparse filtered history query", result.stdout)


if __name__ == "__main__":
    unittest.main()
