"""Create a deterministic CSV summary from source-pinned native benchmark JSON."""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import statistics
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

CSV_FIELDS = (
    "source_file",
    "suite",
    "implementation",
    "candidate_algorithm",
    "benchmark",
    "measurement",
    "metric",
    "legacy_statistic",
    "event_type_shape",
    "query_limit",
    "latency_profile",
    "filter_mode",
    "key_count",
    "wildcard_percent",
    "subscriptions",
    "selected_subscriptions",
    "publishers",
    "sample_scope",
    "sample_count",
    "mean_us",
    "median_us",
    "p50_us",
    "p95_us",
    "p99_us",
    "max_us",
    "stdev_us",
    "cv_percent",
    "events_per_second",
    "wall_seconds",
    "process_cpu_seconds",
    "source_revision",
    "source_sha256",
    "binary_sha256",
    "build_manifest_sha256",
    "python_version",
    "gil",
    "cpu_affinity",
    "raw_json",
)

_SUBSCRIPTIONS_PATTERN = re.compile(r"(?:^|/)subscriptions_(\d+)(?:/|$)")


def _text(value: object) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None


def _integer(value: object) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return None


def _number(value: float | int) -> str:
    return format(value, ".12g")


def _source_metadata(document: dict[str, Any], path: Path) -> dict[str, Any]:
    metadata = document.get("metadata")
    if not isinstance(metadata, dict):
        raise ValueError(f"{path}: missing suite metadata")
    revision = _text(metadata.get("eventflow_revision"))
    source_sha256 = _text(metadata.get("eventflow_source_sha256"))
    if not revision or not source_sha256:
        raise ValueError(f"{path}: source revision and SHA-256 metadata are required")
    return metadata


def _shared_metadata(documents: Sequence[tuple[Path, dict[str, Any]]]) -> None:
    identities = {
        (
            document["metadata"].get("eventflow_revision"),
            document["metadata"].get("eventflow_source_sha256"),
        )
        for _, document in documents
    }
    if len(identities) != 1:
        details = ", ".join(path.name for path, _ in documents)
        raise ValueError(f"benchmark JSON files use different source identities: {details}")


def _base_row(path: Path, metadata: dict[str, Any], name: str) -> dict[str, str | int | float]:
    return {
        "source_file": path.name,
        "suite": _text(metadata.get("eventflow_native_matrix")) or "native",
        "implementation": _text(metadata.get("eventflow_native_backend"))
        or _text(metadata.get("eventflow_implementation"))
        or "unknown",
        "candidate_algorithm": "",
        "benchmark": name,
        "measurement": "",
        "metric": "",
        "legacy_statistic": "",
        "event_type_shape": "",
        "query_limit": "",
        "latency_profile": "",
        "filter_mode": "",
        "key_count": "",
        "wildcard_percent": "",
        "subscriptions": "",
        "selected_subscriptions": "",
        "publishers": "",
        "sample_scope": "",
        "sample_count": 0,
        "mean_us": "",
        "median_us": "",
        "p50_us": "",
        "p95_us": "",
        "p99_us": "",
        "max_us": "",
        "stdev_us": "",
        "cv_percent": "",
        "events_per_second": "",
        "wall_seconds": "",
        "process_cpu_seconds": "",
        "source_revision": metadata["eventflow_revision"],
        "source_sha256": metadata["eventflow_source_sha256"],
        "binary_sha256": _text(metadata.get("eventflow_native_binary_sha256")) or "",
        "build_manifest_sha256": _text(
            metadata.get("eventflow_native_build_manifest_sha256")
        )
        or "",
        "python_version": _text(metadata.get("python_version")) or "",
        "gil": _text(metadata.get("eventflow_gil")) or "",
        "cpu_affinity": _text(metadata.get("cpu_affinity")) or "",
        "raw_json": path.name,
    }


def _copy_dimension(row: dict[str, Any], item: dict[str, Any], suite: dict[str, Any]) -> None:
    for key in (
        "event_type_shape",
        "query_limit",
        "latency_profile",
        "filter_mode",
        "selected_subscriptions",
        "publishers",
    ):
        value = item.get(key)
        if value is None:
            value = suite.get(key)
        if value is not None:
            row[key] = value
    row["key_count"] = item.get("key_count", suite.get("key_count", ""))
    row["wildcard_percent"] = item.get("wildcard_percent", suite.get("wildcard_percent", ""))

    count = _integer(item.get("subscriptions"))
    if count is None:
        count = _integer(suite.get("subscriptions"))
    if count is None:
        match = _SUBSCRIPTIONS_PATTERN.search(str(item.get("name", "")))
        count = int(match.group(1)) if match else None
    row["subscriptions"] = "" if count is None else count

    algorithm = _text(item.get("candidate_algorithm"))
    if algorithm is not None and algorithm.isdecimal():
        algorithm = None
    if algorithm is None:
        algorithm = _text(item.get("candidate_implementation"))
    if algorithm is None:
        algorithm = _text(suite.get("eventflow_native_backend"))
    row["candidate_algorithm"] = algorithm or ""


def _stats(values: Sequence[float]) -> dict[str, str | int]:
    if not values:
        raise ValueError("cannot summarize an empty sample group")
    ordered = sorted(values)
    average = statistics.fmean(ordered)
    deviation = statistics.stdev(ordered) if len(ordered) > 1 else 0.0

    def nearest_rank(percentile: int) -> float:
        return ordered[math.ceil(percentile * len(ordered) / 100) - 1]

    def to_us(value: float) -> str:
        return _number(value * 1_000_000)

    return {
        "sample_count": len(ordered),
        "mean_us": to_us(average),
        "median_us": to_us(statistics.median(ordered)),
        "p50_us": to_us(nearest_rank(50)),
        "p95_us": to_us(nearest_rank(95)),
        "p99_us": to_us(nearest_rank(99)),
        "max_us": to_us(ordered[-1]),
        "stdev_us": to_us(deviation),
        "cv_percent": _number(deviation / average * 100) if average else "0",
    }


def _pyperf_rows(path: Path, document: dict[str, Any]) -> list[dict[str, Any]]:
    suite = document["metadata"]
    benchmarks = document.get("benchmarks")
    if not isinstance(benchmarks, list):
        raise ValueError(f"{path}: missing pyperf benchmark list")
    rows: list[dict[str, Any]] = []
    for benchmark in benchmarks:
        item = benchmark.get("metadata", {})
        name = _text(item.get("name"))
        if not name:
            raise ValueError(f"{path}: benchmark has no name")
        row = _base_row(path, suite, name)
        _copy_dimension(row, item, suite)
        measure = _text(item.get("measurement")) or name.partition("/")[0]
        statistic = _integer(item.get("percentile"))
        values = [
            value
            for run in benchmark.get("runs", ())
            for value in run.get("values", ())
            if isinstance(value, (int, float)) and not isinstance(value, bool)
        ]
        if not values:
            raise ValueError(f"{path}: benchmark {name!r} has no measured values")
        if measure in {"publish_latency", "routing_lock_wait"}:
            row["measurement"] = "legacy_pyperf_percentile_estimate"
            row["legacy_statistic"] = f"p{statistic}" if statistic is not None else "unknown"
            row["sample_scope"] = "pyperf_percentile_replicates"
            row["sample_count"] = len(values)
        else:
            row["measurement"] = measure
            row["sample_scope"] = "pyperf_values"
            row.update(_stats([float(value) for value in values]))
        rows.append(row)
    return rows


def _latency_rows(path: Path, document: dict[str, Any]) -> list[dict[str, Any]]:
    suite = document["metadata"]
    rows: list[dict[str, Any]] = []
    for result in document.get("results", ()):
        name = str(result["benchmark"])
        base = _base_row(path, suite, name)
        _copy_dimension(base, result, suite)
        base.update(
            {
                "candidate_algorithm": result["candidate_algorithm"],
                "implementation": result["candidate_implementation"],
                "latency_profile": result["latency_profile"],
                "filter_mode": result["filter_mode"],
                "key_count": result["key_count"],
                "wildcard_percent": result["wildcard_percent"],
                "subscriptions": result["subscriptions"],
                "selected_subscriptions": result["selected_subscriptions"],
                "publishers": result["publishers"],
            }
        )

        throughput = result["throughput"]
        throughput_row = dict(base)
        duration = throughput["wall_ns"] / 1_000_000_000
        throughput_row.update(
            {
                "measurement": "throughput",
                "metric": "un-instrumented_publish_batch",
                "sample_scope": "batch",
                "sample_count": throughput["publish_count"],
                "events_per_second": _number(throughput["publish_count"] / duration),
                "wall_seconds": _number(duration),
                "process_cpu_seconds": _number(
                    throughput["process_cpu_ns"] / 1_000_000_000
                ),
            }
        )
        rows.append(throughput_row)

        latency_groups = result["publish_latency_ns"]
        all_latency = [sample for group in latency_groups.values() for sample in group]
        scopes = [("aggregate", all_latency)] + [
            (f"worker_{worker}", samples)
            for worker, samples in sorted(latency_groups.items(), key=lambda item: int(item[0]))
        ]
        for scope, samples in scopes:
            latency_row = dict(base)
            latency_row.update(
                {
                    "measurement": "publish_latency",
                    "metric": "perf_counter_ns_around_publish",
                    "sample_scope": scope,
                    **_stats([sample / 1_000_000_000 for sample in samples]),
                }
            )
            rows.append(latency_row)

        for phase, worker_samples in sorted(result["lock_metrics"].items()):
            for metric in ("wait_ns", "hold_ns"):
                groups = {
                    worker: [sample[metric] for sample in samples]
                    for worker, samples in worker_samples.items()
                }
                all_samples = [sample for group in groups.values() for sample in group]
                scopes = [("aggregate", all_samples)] + [
                    (f"worker_{worker}", samples)
                    for worker, samples in sorted(groups.items(), key=lambda item: int(item[0]))
                ]
                for scope, samples in scopes:
                    lock_row = dict(base)
                    lock_row.update(
                        {
                            "measurement": "lock_metrics",
                            "metric": f"{phase}_{metric}",
                            "sample_scope": scope,
                            **_stats([sample / 1_000_000_000 for sample in samples]),
                        }
                    )
                    rows.append(lock_row)
    return rows


def summarize_files(paths: Iterable[Path]) -> list[dict[str, Any]]:
    documents: list[tuple[Path, dict[str, Any]]] = []
    for path in sorted((Path(path).resolve() for path in paths), key=lambda item: item.name):
        document = json.loads(path.read_text(encoding="utf-8"))
        _source_metadata(document, path)
        documents.append((path, document))
    if not documents:
        raise ValueError("provide at least one benchmark JSON file")
    _shared_metadata(documents)

    rows: list[dict[str, Any]] = []
    for path, document in documents:
        if document.get("schema") == "eventflow.native-latency.v1":
            rows.extend(_latency_rows(path, document))
        else:
            rows.extend(_pyperf_rows(path, document))
    return sorted(
        rows,
        key=lambda row: (
            row["source_file"],
            row["benchmark"],
            row["sample_scope"],
            row["measurement"],
            row["metric"],
        ),
    )


def write_summary(paths: Iterable[Path], output: Path) -> None:
    rows = summarize_files(paths)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=CSV_FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("json_files", nargs="+", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    write_summary(args.json_files, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
