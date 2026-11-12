from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from benchmarks.summarize_native import summarize_files, write_summary


class NativeBenchmarkSummaryTests(unittest.TestCase):
    def test_pyperf_metadata_fallback_does_not_treat_event_count_as_algorithm(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            source = Path(temporary_directory) / "run.json"
            source.write_text(
                json.dumps(
                    {
                        "metadata": {
                            "eventflow_revision": "revision-a",
                            "eventflow_source_sha256": "source-a",
                            "eventflow_native_backend": "c",
                            "subscriptions": 2048,
                        },
                        "benchmarks": [
                            {
                                "metadata": {
                                    "name": "history_filter_only/100000/single_type/limit_all",
                                    "candidate_algorithm": 100000,
                                    "candidate_implementation": "c",
                                },
                                "runs": [{"values": [0.000001, 0.000002]}],
                            },
                            {
                                "metadata": {
                                    "name": "routing_latency_publish/c_key_lookup/4/p95/"
                                    "subscriptions_2048",
                                    "candidate_algorithm": "c_key_lookup",
                                    "measurement": "publish_latency",
                                    "percentile": 95,
                                },
                                "runs": [{"values": [0.0000003, 0.0000004]}],
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )

            rows = summarize_files([source])

        history = next(row for row in rows if row["benchmark"].startswith("history_"))
        legacy = next(row for row in rows if row["measurement"].startswith("legacy_"))
        self.assertEqual(history["candidate_algorithm"], "c")
        self.assertEqual(history["subscriptions"], 2048)
        self.assertEqual(history["sample_count"], 2)
        self.assertEqual(legacy["candidate_algorithm"], "c_key_lookup")
        self.assertEqual(legacy["legacy_statistic"], "p95")
        self.assertEqual(legacy["sample_scope"], "pyperf_percentile_replicates")
        self.assertEqual(legacy["p95_us"], "")

    def test_raw_latency_rows_derive_percentiles_from_same_samples(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            source = root / "latency.json"
            summary = root / "summary.csv"
            source.write_text(
                json.dumps(
                    {
                        "schema": "eventflow.native-latency.v1",
                        "metadata": {
                            "eventflow_revision": "revision-a",
                            "eventflow_source_sha256": "source-a",
                            "eventflow_native_backend": "c",
                            "eventflow_native_binary_sha256": "binary-a",
                            "eventflow_native_build_manifest_sha256": "manifest-a",
                            "eventflow_gil": "enabled",
                        },
                        "results": [
                            {
                                "benchmark": "routing_latency/c_key_lookup/all/2",
                                "candidate_algorithm": "c_key_lookup",
                                "candidate_implementation": "c",
                                "latency_profile": "all",
                                "filter_mode": "deliver_all",
                                "key_count": 7,
                                "wildcard_percent": 20,
                                "subscriptions": 2048,
                                "selected_subscriptions": 1600,
                                "publishers": 2,
                                "throughput": {
                                    "publish_count": 4,
                                    "wall_ns": 2_000_000,
                                    "process_cpu_ns": 3_000_000,
                                },
                                "publish_latency_ns": {"0": [100, 200], "1": [300, 400]},
                                "lock_metrics": {
                                    "bus_lock": {
                                        "0": [
                                            {"wait_ns": 10, "hold_ns": 20},
                                            {"wait_ns": 30, "hold_ns": 40},
                                        ],
                                        "1": [
                                            {"wait_ns": 50, "hold_ns": 60},
                                            {"wait_ns": 70, "hold_ns": 80},
                                        ],
                                    }
                                },
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            write_summary([source], summary)
            first = summary.read_bytes()
            write_summary([source], summary)
            second = summary.read_bytes()
            with summary.open(newline="", encoding="utf-8") as stream:
                rows = list(csv.DictReader(stream))

        self.assertEqual(first, second)
        aggregate = next(
            row
            for row in rows
            if row["measurement"] == "publish_latency" and row["sample_scope"] == "aggregate"
        )
        worker = next(
            row
            for row in rows
            if row["measurement"] == "publish_latency" and row["sample_scope"] == "worker_0"
        )
        wait = next(row for row in rows if row["metric"] == "bus_lock_wait_ns")
        throughput = next(row for row in rows if row["measurement"] == "throughput")
        self.assertEqual(aggregate["sample_count"], "4")
        self.assertEqual(aggregate["p50_us"], "0.2")
        self.assertEqual(aggregate["p95_us"], "0.4")
        self.assertEqual(aggregate["max_us"], "0.4")
        self.assertEqual(worker["sample_count"], "2")
        self.assertEqual(worker["p50_us"], "0.1")
        self.assertEqual(wait["sample_count"], "4")
        self.assertEqual(throughput["sample_count"], "4")
        self.assertEqual(throughput["events_per_second"], "2000")
        self.assertEqual(aggregate["binary_sha256"], "binary-a")
        self.assertEqual(aggregate["build_manifest_sha256"], "manifest-a")

    def test_summary_rejects_mixed_source_identities(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            paths = []
            for revision in ("revision-a", "revision-b"):
                path = root / f"{revision}.json"
                path.write_text(
                    json.dumps(
                        {
                            "metadata": {
                                "eventflow_revision": revision,
                                "eventflow_source_sha256": f"sha-{revision}",
                            },
                            "benchmarks": [],
                        }
                    ),
                    encoding="utf-8",
                )
                paths.append(path)

            with self.assertRaisesRegex(ValueError, "different source identities"):
                summarize_files(paths)


if __name__ == "__main__":
    unittest.main()
