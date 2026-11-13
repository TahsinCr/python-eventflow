# EventFlow benchmark results

This directory retains one benchmark snapshot: the latest source-fingerprinted
runtime comparison recorded on 2026-11-13. Earlier dated result archives were
removed because they measured superseded source or benchmark paths. Benchmark
implementations and workload definitions remain under `benchmarks/`.

## Retained data

| Result set | Files | Coverage |
| --- | ---: | --- |
| Runtime history query | 4 pyperf JSON files and one CSV summary | Python and Cython runtime paths, measured in both execution orders |
| Runtime routing | 2 CSV summaries | Python and C production paths, initial and escalated process samples |

The full per-publish routing JSON samples were not committed because the 20
files total about 313 MB. The retained CSV summaries contain per-process
percentiles, throughput, lock measurements, source identity, runtime binary
hashes and build-manifest details.

## Measurement identity

- **Source revision:** `33725bf`
- **Source fingerprint:** `7634a58c2f3992d2bb1f64b8b7a6d5b5e54b5f738329a8a8c7a95e2771fbd250`
- **Working tree:** dirty during measurement; use the recorded fingerprint and
  binary/build-manifest hashes to identify this exact experiment.
- **Interpreter:** CPython 3.14.10, GIL enabled
- **Host:** Intel Core i7-12700H, Linux x86_64, 20 logical CPUs available

These are one-host measurements. They are not a complete performance survey of
every EventFlow feature, operating system, Python version or application
workload.

## Runtime boundaries

The standard runtime remains pure Python. Native kernels are experimental and
opt-in:

- Cython handles only unlimited, exact `EventFilter` type-only history queries.
  Limited queries, predicates and filter subclasses use Python.
- C handles routing only when both keyed and wildcard subscription indexes are
  populated. Empty-index fast paths remain in Python.
- A requested native extension that is unavailable fails during import;
  EventFlow does not silently fall back to Python in a forced native run.
- On free-threaded interpreters, the loader verifies GIL state before and after
  importing each native extension.

## Results

### `MemoryEventHistory.query()`

The workload queried 100,000 stored events with an unlimited, single-type
`EventFilter`. Each pyperf run used rigorous mode and retained 120 measurements.
The candidate order was reversed to expose order effects.

| Run order | Python | Cython | Relative result |
| --- | ---: | ---: | --- |
| Python, then Cython | 5.71 ms (CV 3.9%) | 3.06 ms (CV 3.7%) | Cython 1.86× faster |
| Cython, then Python | 6.12 ms (CV 5.8%) | 3.10 ms (CV 4.1%) | Python 1.98× slower |

pyperf did not meet its strict 1% stability target. The similar result after
reversing order is useful one-host evidence for this specific unlimited
type-only query, not a general EventFlow speedup guarantee.

### Production `EventBus` routing

The matched workload used 2,048 subscriptions, 653 selected callbacks, seven
routing keys, 20% wildcard subscriptions and one or four publisher threads.
Five independent processes ran at 2,000 publishes per publisher. Because a
P99 coefficient of variation exceeded 10%, all candidates were repeated for
five processes at 5,000 publishes per publisher.

| Publishers | Python P50 / P99 | C P50 / P99 | Python / C throughput | P99 CV, Python / C |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 490 / 1,057 µs | 457 / 977 µs | 1,859 / 2,045 publishes/s | 21.1% / 26.0% |
| 4 | 2,519 / 12,831 µs | 2,181 / 23,672 µs | 1,270 / 1,472 publishes/s | 19.4% / 30.6% |

At four publishers, C had 13.4% lower P50 and 15.9% higher throughput, but its
median P99 was 84.5% higher. P99 variation remained above the 10% threshold for
both backends after escalation, so this comparison is inconclusive and does
not justify making C the default. Instrumented lock measurements are
diagnostic only and are not end-to-end publish timings.

## Result files

- [Python-first history run](native/native_runtime_history_query_2026-11-13_7634a58c_py31410_gil-on_python_first.json)
- [Cython-second history run](native/native_runtime_history_query_2026-11-13_7634a58c_py31410_gil-on_cython_second.json)
- [Cython-first history run](native/native_runtime_history_query_2026-11-13_7634a58c_py31410_gil-on_cython_reverse_first.json)
- [Python-second history run](native/native_runtime_history_query_2026-11-13_7634a58c_py31410_gil-on_python_reverse_second.json)
- [History CSV summary](native/native_runtime_history_query_2026-11-13_7634a58c_py31410_gil-on_summary.csv)
- [Initial routing stage](native/native_runtime_routing_2026-11-13_7634a58c_py31410_gil-on_initial_summary.csv)
- [Escalated routing stage](native/native_runtime_routing_2026-11-13_7634a58c_py31410_gil-on_escalation_summary.csv)

## Reproduction guidance

Build optional extensions outside the checkout and keep the interpreter, source
fingerprint, build manifest, host and backend order with every result:

```bash
python -m benchmarks.native.build --build-dir /tmp/eventflow-native
```

Use `python -m benchmarks.benchmark_native --help` for the history benchmark
options and `python -m benchmarks.native_latency --help` for routing-latency
options. Set the relevant `EVENTFLOW_EXPERIMENTAL_*_BACKEND` variable before
importing EventFlow to select an experimental runtime kernel. Keep Python as
the production default until matched, repeated measurements on target workloads
show a stable end-to-end gain.
