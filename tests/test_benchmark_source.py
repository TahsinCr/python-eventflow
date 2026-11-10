from __future__ import annotations

import unittest

from benchmarks._runner import _is_source_path


class BenchmarkSourceTests(unittest.TestCase):
    def test_fingerprint_includes_native_kernel_sources(self) -> None:
        for path in (
            "benchmarks/native/kernels.c",
            "benchmarks/native/kernels.pyx",
            "benchmarks/native/build.py",
            "benchmarks/benchmark_native.py",
        ):
            with self.subTest(path=path):
                self.assertTrue(_is_source_path(path))

    def test_fingerprint_excludes_generated_result_artifacts(self) -> None:
        self.assertFalse(_is_source_path("benchmarks/results/native-cpython.json"))
        self.assertFalse(_is_source_path("benchmarks/results/current_summary.csv"))
