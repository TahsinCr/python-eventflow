from __future__ import annotations

import unittest

from benchmarks._runner import _is_source_path


class BenchmarkSourceTests(unittest.TestCase):
    def test_fingerprint_includes_benchmark_helper_and_core_sources(self) -> None:
        for path in (
            "pyproject.toml",
            "benchmarks/_support.py",
            "benchmarks/benchmark_publish.py",
            "eventflow/_core.c",
            "eventflow/_core.pyi",
            "eventflow/py.typed",
        ):
            with self.subTest(path=path):
                self.assertTrue(_is_source_path(path))

    def test_fingerprint_excludes_generated_result_artifacts(self) -> None:
        self.assertFalse(_is_source_path("benchmarks/results/current_summary.csv"))
        self.assertFalse(_is_source_path("benchmarks/results/baseline.json"))
