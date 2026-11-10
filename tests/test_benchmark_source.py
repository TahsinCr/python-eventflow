from __future__ import annotations

import unittest

from benchmarks._runner import _is_source_path


class BenchmarkSourceTests(unittest.TestCase):
    def test_fingerprint_includes_benchmark_helper_and_native_sources(self) -> None:
        for path in (
            "benchmarks/_support.py",
            "benchmarks/native/kernels.c",
            "benchmarks/native/kernels.pyx",
            "benchmarks/native/build.py",
            "benchmarks/native/provenance.py",
            "benchmarks/native/python_kernels.py",
            "benchmarks/native/validate.py",
            "benchmarks/benchmark_native.py",
        ):
            with self.subTest(path=path):
                self.assertTrue(_is_source_path(path))

    def test_fingerprint_excludes_generated_result_artifacts(self) -> None:
        self.assertFalse(_is_source_path("benchmarks/results/native-cpython.json"))
        self.assertFalse(_is_source_path("benchmarks/results/current_summary.csv"))
