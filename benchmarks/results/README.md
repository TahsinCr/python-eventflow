# EventFlow performance report

**Snapshot:** 2026-11-09 · **Package:** EventFlow 1.3.2 · **Measured source:**
`0bf7ac756d52a1c897cad6f6e4229aa2a088954f`

This report is the current reference for benchmark results in this directory.
Every result created for this snapshot records the same clean source revision
and source fingerprint. Benchmark records from earlier source revisions and
older harnesses were removed, leaving only the current baseline and its
matched GIL-on/free-threaded comparison.

## What was measured

The current suite has 13 pyperf modules and 142 workload cases on CPython 3.14.9
with the GIL enabled. A matched 3.15.2 GIL-on/free-threaded run adds 21 cases for
each build across bridges, publisher scaling and lifecycle work. That makes
184 case/build results in the current CSV; it does not mean every public method
was timed individually.

| Area | Module(s) | Cases | Representative paths |
| --- | --- | ---: | --- |
| Event publication and engines | `benchmark_publish.py` | 27 | Sync/async, 0–100 subscribers, filters, isolated callback errors, named engine channels |
| Thread/async bridges | `benchmark_bridges.py`, `benchmark_bridge_publishers.py` | 12 | Direct await, loop portal, same/cross-thread queue handoff, publisher scaling and process CPU |
| History and replay | `benchmark_replay.py` | 10 | Latest, full/window/sparse query, bus query and replay subscription setup |
| Pipelines | `benchmark_pipeline.py` | 21 | Sync/async add, decorator, middleware and metrics paths at multiple stage counts |
| Async streams | `benchmark_stream.py` | 6 | Native controls, fused transforms, buffering and timed batching |
| Runtime load and schedules | `benchmark_runtime.py` | 20 | Sync/async publisher contention, retained history/payloads and periodic registration |
| Keyed coordination | `benchmark_keyed_limiter.py`, `benchmark_rate_limiter.py`, `benchmark_coalescer.py` | 16 | Waiter fan-in, token-bucket contention, keyed updates, merges and flush |
| Components | `benchmark_components.py` | 13 | Cache hit/miss/update, versioned state, state transitions, registry, policy, envelope, context and metrics |
| Resilience | `benchmark_resilience.py` | 8 | Sync/async retry success and retry, cooldown and circuit-breaker calls |
| Callbacks and lifecycle | `benchmark_lifecycle.py` | 9 | Callback policy/hooks, resource scopes, task scope, single-flight and bounded work-pool submission |
| **Total on CPython 3.14.9** | **13 modules** | **142** | **Current broad coverage screen** |

The suite measures representative hot paths and complete operations. It does
not try to benchmark every validation branch, error-hook combination,
cancellation race or API accessor as a separate case; those are correctness
concerns, not useful standalone timing claims.

## Method and evidence limits

### Broad coverage screen

- Host: Intel Core i7-12700H, 20 logical CPUs, Arch Linux x86_64.
- Interpreter: CPython 3.14.9; pyperf 2.11.0.
- Each case used 3 worker processes, 3 values per process, 1 warmup and a
  50 ms minimum sample. Serial cases were pinned to CPU 4; thread-scaling and
  coalescer cases were pinned to CPUs 4–7.
- The custom workload sizes were bounded for this screen: coalescer key counts
  16/256/1,024; keyed-limiter waiters 10/100/1,000; rate-limiter 8 workers × 4
  acquisitions; runtime used a 25-event batch, 2,000-operation soak and one
  schedule-registration loop.

This is a **coverage screen**, not a release performance gate. Pyperf warned
that a number of cases did not establish its strict 95% certainty target for
less than 1% variation. The CSV therefore includes sample count, standard
deviation and coefficient of variation; use those alongside each mean. The
runtime schedule cases were especially noisy with one registration loop and
are excluded from the conclusions below. Across the 184 CSV rows, 107 have a
coefficient of variation at or above 1%, including 21 at or above 5%. Treat the
broad matrix as a workload map; use repeated, focused runs before acting on a
small difference.

### GIL-on and free-threaded comparison

- Interpreters: CPython 3.15.2 and CPython 3.15.2t, on the same host.
- Both builds used 4 worker processes, 5 values per process, 2 warmups and a
  100 ms minimum sample. Bridge-publisher workers used CPUs 4–7; the other
  modules used CPU 4.
- The runner recorded the GIL state from the running interpreter. The
  free-threaded workers inherited `PYTHON_GIL=0`.
- The paired subset contains `benchmark_bridge_publishers.py` (8 cases),
  `benchmark_bridges.py` (4 cases) and `benchmark_lifecycle.py` (9 cases) for
  each build. The two Python 3.15 builds are directly comparable; do not treat
  the 3.14 broad screen as a GIL-on/off comparison.

Several paired cases still have more than 1% variation, particularly the loop
portal and bounded work-pool paths. The observed differences are local evidence
for these workloads, not guarantees for other machines or production traffic.

### Reproducibility and files

Each pyperf file records interpreter, observed GIL state, affinity, revision,
source cleanliness and source SHA-256. `eventflow_source_dirty=false` means the
measured source files were clean; result JSON and the generated CSV are
excluded from the source fingerprint.

- [`current_performance_summary_2026-11-09.csv`](current_performance_summary_2026-11-09.csv)
  has one row per measured case: raw sample count, mean, median, standard
  deviation, coefficient of variation, minimum and maximum, with build and
  source metadata.
- `coverage_matrix_benchmark_<module>_cpython_3.14.9_gil_on_2026-11-09.json`
  contains each module's broad-screen data.
- `paired_benchmark_{bridge_publishers,bridges,lifecycle}_cpython_3.15.2*_2026-11-09.json`
  contains the matched GIL-on and free-threaded runs.
- Earlier-source result files and custom-harness records were removed. The
  retained pyperf JSON files all measure the source revision named at the top
  of this report.

To repeat one module with the broad-screen settings, install the `dev` extra
and run it from the repository root, for example:

```bash
python -m benchmarks.benchmark_components \
  --processes=3 --values=3 --warmups=1 --min-time=0.05 --affinity=4 \
  --output benchmarks/results/local_components.json
```

For GIL comparisons, run the same module and options with CPython 3.15.2 and
3.15.2t. Keep both outputs, host, affinity and GIL metadata; do not compare
different workload parameters as if they were a controlled pair.

## Representative CPython 3.14.9 results

These are mean normalized times from 9 measured values unless otherwise
indicated. The corresponding standard deviation and CV are in the CSV; values
with high CV or pyperf warnings should be treated as directional only.

| Workload | Mean | CV | Interpretation |
| --- | ---: | ---: | --- |
| Sync publish, 0 subscribers | 0.856 µs | 0.31% | Empty dispatch path |
| Sync publish, 10 subscribers | 9.908 µs | 0.66% | Callback delivery included |
| Sync publish, 100 subscribers | 69.624 µs | 0.26% | Callback delivery included |
| Async sequential publish, 10 subscribers | 6.875 µs | 0.80% | Inline sequential callback path |
| Async concurrent publish, 10 subscribers | 45.269 µs | 0.31% | Task scheduling and concurrent delivery included |
| Sync engine publish, no subscribers | 1.434 µs | 2.30% | Named-channel lookup plus bus dispatch |
| History latest | 0.216 µs | 0.54% | In-memory latest-item path |
| History window query | 0.722 µs | 0.18% | Small requested window |
| Sparse filter over 100,000 history events | 9.314 ms | 0.67% | Full scan; predicate runs in Python |
| Cache hit / miss | 0.409 / 0.362 µs | 0.76 / 0.23% | One hot key; not an eviction workload |
| State compare-and-transition | 1.722 µs | 0.60% | Snapshot plus successful version check |
| Policy evaluation, 0 / 16 rules | 0.787 / 7.862 µs | 1.09 / 1.02% | Rules are invoked synchronously |
| Retry success / one retry | 0.635 / 2.797 µs | 0.27 / 0.41% | Zero configured backoff; no real waiting |
| Callback invoke / predicate and hooks | 2.833 / 3.532 µs | 2.88 / 1.66% | Direct standalone callback policy path |
| Bounded work-pool submit and wait | 16.337 µs | 0.65% | One worker; includes thread handoff and `Future.result()` |
| Single-flight, 8 same-key waiters | 105.437 µs | 0.65% | Full burst latency, not per-waiter latency |
| Async stream, 5,000 items, unbuffered / buffer-16 | 1.196 / 3.091 ms | 1.04 / 1.14% | Full consumption; buffering provides bounded prefetch, not CPU speed |

These data show operation costs and scaling shape; they do not identify a
single universal bottleneck. The engine overhead estimate is based on a
named-channel case and a corresponding direct-bus case, so treat the difference
as approximate rather than a paired subtraction.

## CPython 3.15.2 GIL comparison

The GIL-on and free-threaded results below use the same CPython release and
workload parameters. Times are means from 20 values. “Process CPU” is CPU time
per delivered event; it can increase while wall time decreases.

| Workload | GIL on | GIL disabled | Observed change |
| --- | ---: | ---: | ---: |
| Same-thread queue roundtrip | 11.76 µs | 13.38 µs | +13.8% wall time |
| Cross-thread queue roundtrip | 48.41 µs | 65.42 µs | +35.1% wall time |
| 16-publisher wall time/event | 23.01 µs | 23.55 µs | +2.3%; effectively close at this sample size |
| 16-publisher process CPU/event | 26.30 µs | 88.10 µs | 3.35× CPU time without the GIL |
| Bounded work-pool submit/result | 18.10 µs | 50.42 µs | 2.79× slower; free-threaded CV was 9.12% |
| Single-flight, 8 same-key waiters | 127.68 µs | 147.65 µs | +15.6% wall time |

At 2 and 4 publishers, the free-threaded build had lower wall time (about 30%
and 32%) but higher process CPU time. By 16 publishers, wall time was nearly
the same and CPU cost was much higher. The bridge queue's cross-thread handoff
was slower in the free-threaded build. This rules out the assumption that
free-threading or a native rewrite automatically speeds every EventFlow path;
the expected workload and CPU budget matter.

The results do not support moving all dispatch or coordination code to C. The
measured paths spend substantial time in callback invocation, event-loop
scheduling, thread wakeups, locks and ownership cleanup. A native loop would
still cross into Python for user callbacks and would not remove those scheduling
costs. Any future native or Cython work should be limited to a CPU-heavy,
measured loop whose inputs and outputs can stay native across the boundary.

## What remains unmeasured

- Free-threaded comparisons cover bridges, bridge publisher scaling and
  lifecycle scenarios only; the other 10 modules were not rerun on 3.15.2t.
- The broad screen does not cover every callback error/hook combination,
  timeout/cancellation race, engine shutdown failure, filter type, cache TTL or
  eviction pattern, or pipeline ordering graph.
- Runtime schedule-registration results had high variance at one loop and are
  not used to rank implementations. A follow-up should isolate timer/thread
  startup from steady-state scheduling and gather a larger sample.
- The suite does not measure production payload distributions, long-duration
  memory retention, startup/import cost, multi-host variance or end-to-end
  application latency.
- A full application test suite was not rerun for this benchmark-only update.
  The six tests in `tests/test_benchmarks.py` passed on Python 3.10.22 and
  3.13.15; Ruff passed; Pyright reported zero diagnostics on Python 3.10.
