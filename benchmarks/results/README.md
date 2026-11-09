# EventFlow performance report

**Snapshot:** 2026-11-09 · **Package:** EventFlow 1.3.2 · **Measured source:**
`4af998df85e8fa779e4fec709a01765ed5018ca3` · **Source SHA-256:**
`5276989cf35716861fd9104c3a795ff961acdceca1c45172b1fcf0052d38bdae`

This report and the 31 pyperf JSON files cover one clean source revision. The
summary CSV contains 409 case/build rows from a broad benchmark screen, focused
interpreter comparisons, one-operation latency distributions and repeated
large-history measurements. Previous-source result JSON was removed so it
cannot be mistaken for this baseline.

## Scope

The suite has 13 modules. The CPython 3.14.9 broad screen covers 180 cases and
maps representative costs; it is not a release performance gate.

| Area | Module(s) | Cases | Representative paths |
| --- | --- | ---: | --- |
| Event publication and engines | `benchmark_publish.py` | 31 | Sync/async fan-out, filters, callback errors, yielding callbacks, named channels |
| Thread/async bridges | `benchmark_bridges.py`, `benchmark_bridge_publishers.py` | 14 | Direct await, loop portal, same/cross-thread queue handoff, publisher scaling and process CPU |
| History and replay | `benchmark_replay.py` | 36 | 1k/10k/100k histories, latest/query/filter/replay paths |
| Pipelines | `benchmark_pipeline.py` | 21 | Sync/async add, decorator, middleware and metrics at multiple stage counts |
| Async streams | `benchmark_stream.py` | 6 | Native controls, fused transforms, buffering and timed batching |
| Runtime load and schedules | `benchmark_runtime.py` | 22 | Sync/async publisher contention, retained history/payloads and periodic registration |
| Keyed coordination | `benchmark_keyed_limiter.py`, `benchmark_rate_limiter.py`, `benchmark_coalescer.py` | 20 | Waiter fan-in, token-bucket contention, keyed updates, merges and flush |
| Components | `benchmark_components.py` | 13 | Cache, versioned state, transitions, registry, policy, envelope, context and metrics |
| Resilience | `benchmark_resilience.py` | 8 | Sync/async retry, cooldown and circuit-breaker paths |
| Callbacks and lifecycle | `benchmark_lifecycle.py` | 9 | Callback hooks, resource/task scopes, single-flight and bounded work submission |
| **Total** | **13 modules** | **180** | **CPython 3.14.9 broad screen** |

The 14 bridge cases combine 10 publisher-count cases and 4 queue/portal cases.
The 20 coordination cases combine 16 coalescer, 3 keyed-limiter and 1
rate-limiter cases.

## Measurement matrix

| Set | Interpreter and settings | Cases | Purpose |
| --- | --- | ---: | --- |
| Broad screen | CPython 3.14.9, GIL enabled; 3 processes × 3 values, 1 warmup, 50 ms minimum sample | 180 | Coverage across all 13 modules |
| Python floor focus | CPython 3.10.22; same short-screen settings | 81 | Bridge, publisher, publish and replay comparisons at the minimum supported version |
| GIL pair | CPython 3.15.2 and 3.15.2t; 4 processes × 5 values, 2 warmups, 100 ms minimum sample | 54 per build | Matched bridge, publisher, lifecycle and publish workloads |
| Single-operation distributions | CPython 3.10.22, 3.14.9, 3.15.2 and 3.15.2t; 5 processes × 100 values, 5 warmups, `loops=1` | 4 cases per build | 500 individual direct, portal, same-thread and cross-thread roundtrip observations per interpreter |
| Large-history repeat | CPython 3.10.22 and 3.14.9; 5 processes × 10 values, 2 warmups, 100 ms minimum sample | 12 per build | Repeated query/filter/replay measurements at 100,000 events |

The 3.15.2 and 3.15.2t pair uses the same CPython release and workload
parameters. Free-threaded workers ran with `PYTHON_GIL=0`, and the runner
recorded the interpreter's observed GIL state. Python 3.10 and 3.14 runs are
version comparisons, not controlled GIL comparisons.

The broad screen measures representative hot paths and complete operations.
It does not separately time every validation branch, error-hook combination,
cancellation race, filter type, cache state or pipeline ordering graph.

## Result stability and reproduction

The CSV was regenerated from all 31 JSON files. It records sample count, mean,
median, standard deviation, coefficient of variation (CV), minimum and maximum,
plus interpreter, GIL state, affinity and source metadata. Across 409 rows, 179
have CV at or above 1%; 39 are at or above 5%. This includes exploratory
single-operation distributions. Use CV and raw JSON alongside each mean; do
not rank small differences from noisy rows.

Every result records the same revision and SHA-256, with
`eventflow_source_dirty=false`. Generated JSON and the summary CSV are excluded
from the source fingerprint. The host was an Intel Core i7-12700H with 20
logical CPUs running Arch Linux x86_64; pyperf version was 2.11.0. Affinity was
CPU 4 for serial workloads and CPUs 4–7 for thread-scaled workloads.

- [`current_performance_summary_2026-11-09.csv`](current_performance_summary_2026-11-09.csv)
  contains all 409 measured case/build rows.
- `coverage_4af998d_benchmark_<module>_cpython_3.14.9_gil_on_2026-11-09.json`
  contains the 13-module broad screen.
- `focus_4af998d_benchmark_<module>_cpython_3.10.22_gil_on_2026-11-09.json`
  contains the four Python 3.10 focus modules.
- `paired_4af998d_benchmark_<module>_cpython_3.15.2*_2026-11-09.json`
  contains the four matched GIL-on and free-threaded modules.
- `latency_4af998d_bridges_cpython_<version>_2026-11-09.json` contains the
  four one-operation distributions for each interpreter mode.
- `repeat_4af998d_replay100k_cpython_<version>_2026-11-09.json` contains the
  repeated 100k-event history measurements.

To repeat a broad-screen module, install the `dev` extra and run it from the
repository root, for example:

```bash
python -m benchmarks.benchmark_components \
  --processes=3 --values=3 --warmups=1 --min-time=0.05 --affinity=4 \
  --output benchmarks/results/local_components.json
```

For a controlled GIL comparison, run the same module, options and host under
CPython 3.15.2 and 3.15.2t. Keep both JSON files; do not compare unlike sample
settings or workload sizes as if they were a pair.

## Bridge and thread handoff

The cross-thread benchmark now runs a dedicated publisher thread that calls
`EventBus.publish()` directly. It waits for an acknowledgement from the async
consumer; the benchmark no longer routes each event through an extra
`queue.Queue` and per-event future. Earlier measurements included that harness
overhead and were removed.

The roundtrip includes synchronous bus dispatch, queue insertion, the
thread-to-event-loop wakeup, consumer receipt and its acknowledgement. It is
not one-way queue insertion latency. The corrected matched 3.15.2 measurements
are:

| Workload | GIL enabled | GIL disabled | CV, enabled / disabled |
| --- | ---: | ---: | ---: |
| Same-thread queue roundtrip | 8.70 µs | 9.79 µs | 0.89% / 0.66% |
| Cross-thread queue roundtrip | 26.08 µs | 21.62 µs | 4.60% / 5.44% |
| Loop portal roundtrip | 28.51 µs | 36.74 µs | 1.56% / 3.20% |

The free-threaded queue roundtrip is about 17% lower in this short paired
screen, but this is a single-host result with 20 measured values per case. It
supports further targeted investigation, not a general speedup guarantee.

The one-operation cross-thread distributions show substantial scheduler
variation:

| Interpreter | p50 | p95 | p99 | CV |
| --- | ---: | ---: | ---: | ---: |
| CPython 3.10.22 | 83.88 µs | 103.00 µs | 115.83 µs | 29.4% |
| CPython 3.14.9 | 55.21 µs | 88.54 µs | 193.23 µs | 52.7% |
| CPython 3.15.2, GIL enabled | 47.50 µs | 71.02 µs | 83.64 µs | 17.7% |
| CPython 3.15.2t, GIL disabled | 37.40 µs | 63.41 µs | 84.31 µs | 24.8% |

Each percentile is an order statistic over 500 single-operation samples from
one local run, not a production latency percentile or service-level guarantee.
The high CVs make the tails exploratory. A cProfile inspection of the corrected
path showed substantial time in asyncio loop/selector iterations and the
publisher thread; it is instrumented and too short to assign reliable
percentages. It confirms that the measurement includes event-loop and thread
handoff work, but does not identify a safe isolated runtime hot loop.

## Publisher scaling and free-threading

These paired means are wall time and process CPU time per delivered event.
Lower wall time may require more CPU:

| Publishers | GIL on wall | GIL off wall | GIL on CPU | GIL off CPU |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 27.49 µs | 22.07 µs | 30.75 µs | 35.60 µs |
| 2 | 25.23 µs | 16.95 µs | 30.14 µs | 42.96 µs |
| 4 | 21.78 µs | 15.37 µs | 25.90 µs | 50.74 µs |
| 8 | 19.17 µs | 19.77 µs | 22.39 µs | 73.72 µs |
| 16 | 17.85 µs | 21.08 µs | 20.81 µs | 79.06 µs |

Free-threading used less wall time at 1–4 publishers in this sample, with the
1-publisher row noisy (CV about 5–6%). At 2 and 4 publishers it reduced wall
time by about 33% and 30%, while raising process CPU by about 43% and 96%. At 8
and 16 publishers, wall time was close or higher and CPU cost was about 3.3×
and 3.8×. This is a concurrency/CPU-budget tradeoff, not an across-the-board
optimization.

## History and replay scaling

The 100k-event repeats used 50 measured values per case:

| Interpreter | Match position | Mean | CV |
| --- | --- | ---: | ---: |
| CPython 3.10.22 | Newest item | 0.608 ms | 3.35% |
| CPython 3.10.22 | Oldest item | 21.845 ms | 1.05% |
| CPython 3.10.22 | No match | 22.253 ms | 1.91% |
| CPython 3.14.9 | Newest item | 1.138 ms | 1.63% |
| CPython 3.14.9 | Oldest item | 9.312 ms | 0.25% |
| CPython 3.14.9 | No match | 9.301 ms | 0.42% |

The full scans were about 2.4× faster on CPython 3.14.9 than 3.10.22 for this
workload. The newest-match time goes in the other direction. Filtered queries
take a consistent history snapshot before evaluating the user predicate, so
snapshot copying remains even when the match is near the end. The predicate
also executes in Python. Preserve these semantics unless a measured
alternative keeps the same consistency and callback-locking guarantees.

## Async callback fan-out

The synthetic callback awaits `asyncio.sleep(0)` once. This measures task and
event-loop scheduling overhead; it does not model network or disk I/O.

| Interpreter | Callbacks | Sequential | Concurrent |
| --- | ---: | ---: | ---: |
| CPython 3.14.9 | 100 | 267.78 µs | 450.31 µs |
| CPython 3.15.2, GIL enabled | 100 | 248.51 µs | 379.23 µs |
| CPython 3.15.2t, GIL disabled | 100 | 262.74 µs | 460.89 µs |

Sequential dispatch is faster for this yield-once workload. Real I/O may benefit
from overlap, and requires its own controlled benchmark before dispatch
defaults or behavior change.

## Conclusions and limits

- The prior cross-thread benchmark included an unnecessary request-queue and
  future handoff. Removing it cut the measured 3.14.9 cross-thread roundtrip
  from the earlier roughly 55 µs screen to about 30 µs. This is a measurement
  correction, not an EventFlow runtime speedup.
- The corrected bridge benchmark isolates the actual synchronous publisher,
  queue delivery, event-loop wakeup and async consumer acknowledgement. The
  paired result suggests a possible free-threaded wall-time benefit, but
  repeated multi-host evidence is still needed.
- Free-threaded publisher scaling can reduce wall time at moderate concurrency
  while increasing CPU use substantially. Select it according to workload and
  CPU budget.
- Filtered history cost grows with history size, while snapshot consistency
  and arbitrary Python predicates limit safe optimization options.
- No runtime EventFlow implementation code was changed. The measurements do
  not yet establish a stable CPU-heavy hot loop that justifies C/Cython or a
  semantic change. The next useful bridge step is a separately measured
  producer publish duration and consumer wake/ack duration, followed by a
  repeated run on a second machine before considering runtime changes.
- The free-threaded matrix covers bridges, publisher scaling, lifecycle and
  publish, not all 13 modules. This work did not model application payloads,
  real I/O, long-term memory retention, startup/import time, multi-host variance
  or end-to-end production latency.
- This benchmark-only work did not rerun the full application test suite or
  hosted GitHub Actions. All 13 benchmark modules completed on the broad
  screen; the bridge change passed focused smoke runs, Ruff, formatting and
  Pyright. No push, release or publication was performed.
