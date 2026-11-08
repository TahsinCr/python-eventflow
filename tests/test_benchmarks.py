from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from benchmarks._runner import benchmark_metadata, source_provenance

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


class BenchmarkMetadataTests(unittest.TestCase):
    def test_source_fingerprint_tracks_code_and_ignores_result_files(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            (root / "eventflow").mkdir()
            (root / "benchmarks" / "results").mkdir(parents=True)
            (root / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
            (root / "eventflow" / "__init__.py").write_text("value = 1\n", encoding="utf-8")
            source = root / "benchmarks" / "benchmark.py"
            source.write_text("value = 1\n", encoding="utf-8")

            before = source_provenance(root)
            (root / "benchmarks" / "results" / "run.json").write_text("{}\n", encoding="utf-8")
            after_result = source_provenance(root)
            source.write_text("value = 2\n", encoding="utf-8")
            after_source_change = source_provenance(root)

        self.assertEqual(before["source_sha256"], after_result["source_sha256"])
        self.assertNotEqual(before["source_sha256"], after_source_change["source_sha256"])

    @unittest.skipUnless(shutil.which("git"), "Git is required for dirty-source coverage")
    def test_source_dirty_flag_ignores_results_and_detects_code_changes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            (root / "eventflow").mkdir()
            (root / "benchmarks" / "results").mkdir(parents=True)
            (root / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
            (root / "eventflow" / "__init__.py").write_text("value = 1\n", encoding="utf-8")
            source = root / "benchmarks" / "benchmark.py"
            source.write_text("value = 1\n", encoding="utf-8")
            subprocess.run(["git", "init", "-q"], cwd=root, check=True)
            subprocess.run(
                ["git", "add", "pyproject.toml", "eventflow", "benchmarks"],
                cwd=root,
                check=True,
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
            source.write_text("value = 2\n", encoding="utf-8")
            with_source_change = source_provenance(root)

        self.assertFalse(clean["source_dirty"])
        self.assertFalse(with_result["source_dirty"])
        self.assertTrue(with_source_change["source_dirty"])

    def test_metadata_records_backend_gil_and_source_identity(self) -> None:
        metadata = benchmark_metadata(REPOSITORY_ROOT)

        self.assertEqual(metadata["eventflow_implementation"], "python")
        self.assertIn(metadata["eventflow_gil"], {"enabled", "disabled", "unavailable"})
        self.assertIn(metadata["eventflow_source_dirty"], {"true", "false", "unknown"})
        self.assertRegex(metadata["eventflow_source_sha256"], r"^[0-9a-f]{64}$")


@unittest.skipUnless(importlib.util.find_spec("pyperf"), "pyperf development extra is required")
class PyperfRunnerTests(unittest.TestCase):
    def test_sync_and_async_results_use_pyperf_json_and_source_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            package = root / "benchmark_smoke"
            package.mkdir()
            (package / "__init__.py").write_text("", encoding="utf-8")
            (package / "run.py").write_text(
                "from benchmarks._runner import (bench_async, bench_sync, create_runner, "
                "forward_cli_arguments)\n"
                "runner = create_runner('benchmark runner smoke test', "
                "add_cmdline_args=forward_cli_arguments('case', 'items'))\n"
                "runner.argparser.add_argument('--case', default='default')\n"
                "runner.argparser.add_argument('--items', type=int, nargs='+', default=[1])\n"
                "args = runner.parse_args()\n"
                "case_name = '-'.join(map(str, args.items))\n"
                "bench_sync(runner, f'sync-smoke-{args.case}-{case_name}', "
                "lambda: sum((1, 2, 3)))\n"
                "async def async_smoke():\n"
                "    return sum((1, 2, 3))\n"
                "bench_async(runner, 'async-smoke', async_smoke)\n",
                encoding="utf-8",
            )
            output = root / "smoke.json"
            environment = os.environ.copy()
            paths = [str(root), str(REPOSITORY_ROOT)]
            if environment.get("PYTHONPATH"):
                paths.append(environment["PYTHONPATH"])
            environment["PYTHONPATH"] = os.pathsep.join(paths)
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "benchmark_smoke.run",
                    "--processes=1",
                    "--values=1",
                    "--warmups=0",
                    "--loops=1",
                    "--case=custom",
                    "--items",
                    "2",
                    "3",
                    "--output",
                    str(output),
                ],
                cwd=REPOSITORY_ROOT,
                env=environment,
                check=False,
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode:
                self.fail(
                    f"pyperf smoke failed\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
                )
            document = json.loads(output.read_text(encoding="utf-8"))

        benchmarks = document["benchmarks"]
        names = {item["metadata"]["name"] for item in benchmarks}
        self.assertEqual(names, {"sync-smoke-custom-2-3", "async-smoke"})
        metadata = document["metadata"]
        self.assertEqual(metadata["eventflow_implementation"], "python")
        self.assertIn(metadata["eventflow_gil"], {"enabled", "disabled", "unavailable"})
        self.assertRegex(metadata["eventflow_source_sha256"], r"^[0-9a-f]{64}$")

    def test_async_pipeline_benchmarks_create_pipeline_in_each_worker_loop(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            output = Path(temporary_directory) / "pipeline.json"
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "benchmarks.benchmark_pipeline",
                    "--processes=1",
                    "--values=2",
                    "--warmups=1",
                    "--loops=1",
                    "--output",
                    str(output),
                ],
                cwd=REPOSITORY_ROOT,
                check=False,
                capture_output=True,
                text=True,
                timeout=60,
            )
            if result.returncode:
                self.fail(
                    f"pipeline benchmark failed\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
                )
            document = json.loads(output.read_text(encoding="utf-8"))

        names = {item["metadata"]["name"] for item in document["benchmarks"]}
        self.assertEqual(len(names), 21)
        self.assertIn("async/20/middleware", names)


if __name__ == "__main__":
    unittest.main()
