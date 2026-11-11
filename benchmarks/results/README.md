# EventFlow performance report

**Snapshot:** 2026-11-09 · **Package:** EventFlow 1.3.2 · **Measured source:**
`4af998df85e8fa779e4fec709a01765ed5018ca3` · **Source SHA-256:**
`5276989cf35716861fd9104c3a795ff961acdceca1c45172b1fcf0052d38bdae`

This file contains six source-pinned snapshots. The 2026-11-09 broad screen
and its 31 pyperf JSON files cover the source revision above. The initial
native comparison uses a separate 2026-11-10 revision and its own 12 JSON files
and CSV. A reversed-order follow-up uses another clean 2026-11-10 source
revision, six JSON files and a separate CSV. The second-round routing matrix
uses the Python two-pointer production merge at a newer source revision, with
six JSON files and its own CSV. Compare results only within the source and
interpreter matrix named in each section. A separate history query comparison
records clean pre-optimization and optimized Python sources with two JSON files
and its own CSV.

The 2026-11-09 summary CSV contains 409 case/build rows from a broad benchmark
screen, focused interpreter comparisons, one-operation latency distributions
and repeated large-history measurements. Previous-source result JSON was
removed so it cannot be mistaken for that baseline.

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
- No runtime EventFlow implementation code was changed in the 2026-11-09
  broad-screen work. The native candidate measurements and their limits are
  reported separately below.
- The free-threaded matrix covers bridges, publisher scaling, lifecycle and
  publish, not all 13 modules. This work did not model application payloads,
  real I/O, long-term memory retention, startup/import time, multi-host variance
  or end-to-end production latency.
- At the time of the 2026-11-09 broad screen, the full application test suite
  and hosted GitHub Actions were not rerun. No push, release or publication was
  performed.

## Native Python, Cython and C comparison (2026-11-10)

**Measured source:** `b16851c7613374f6c51ad06249c321fdc1e3bbd7` ·
**Source SHA-256:** `c3a0d1f254f75bf46773ba39abe863291e635bd4a7bd0b27a8d93c056b363538` ·
**Working source state:** clean · **Results:** 12 pyperf JSON files, 240 case
rows, 40 measured values per case

The EventFlow runtime remained Python in every run. C and Cython extensions
were experimental benchmark backends, built outside the checkout and excluded
from the wheel. This comparison measured two candidate areas: type-only
`MemoryEventHistory` filtering and the keyed subscriber snapshot used by a
synchronous `EventBus` publish.

### Workloads and validation

- History sizes were 1,000, 10,000 and 100,000 events. The selected event type
  occurred every 2nd or 8th record. Each candidate first called
  `MemoryEventHistory.query()` to obtain the same lock-protected snapshot, then
  filtered by event type. Its returned objects and order were checked against
  `query(EventFilter(event_type=...))`; subclass instances were included in
  the match set.
- Routing used 64, 512 and 2,048 subscribers, seven route keys and 20%
  wildcard subscriptions. The tests timed the subscriber snapshot and the
  complete synchronous `EventBus.publish()` path. The publish cases used
  no-op callbacks or event-type filters that rejected delivery; publish result
  counts were checked against the expected route selection.
- Every case used five pyperf worker processes, eight measured values per
  process, two warmups, a 100 ms minimum sample and CPU affinity 4. That is 40
  measured values per case. The CPython 3.10.18 environment had pyperf 2.10.0;
  3.14.9 and both 3.15.2 builds used pyperf 2.11.0.
- All result files identify the same clean source revision and hash. On
  CPython 3.15.2t, `PYTHON_GIL=0` was set and the worker confirmed that the GIL
  remained disabled after importing each native candidate. CPython 3.10 does
  not expose the runtime GIL query used by the metadata helper; its GIL is
  enabled by the interpreter build.

### 100,000-event type-filter query

Times are means. The production path and each candidate use the same
lock-protected snapshot. The Python candidate replaces the per-event
`EventFilter.matches()` call with a direct type check; Cython and C compile
that filtering loop.

| Interpreter | Production `query(filter)` | Python type-filter candidate | Cython | C |
| --- | ---: | ---: | ---: | ---: |
| CPython 3.10.18 | 23.84 ms | 6.69 ms | 3.74 ms | 4.09 ms |
| CPython 3.14.9 | 9.45 ms | 6.17 ms | 3.75 ms | 3.94 ms |
| CPython 3.15.2, GIL on | 10.08 ms | 6.17 ms | 4.28 ms | 4.47 ms |
| CPython 3.15.2t, GIL off | 14.67 ms | 10.01 ms | 8.18 ms | 8.42 ms |

The Cython filter ran 1.22–1.79x faster than the equivalent Python candidate;
C ran 1.19–1.64x faster. Relative to the current production query, the Cython
candidate was 1.79–6.38x faster across these interpreter builds. The benefit
was smaller on free-threaded CPython 3.15.2t, where even the Python kernel was
slower than its GIL-enabled counterpart. The result applies to a filter whose
only criterion is event type; arbitrary predicates and additional filter
fields were not moved into native code.

### 2,048-subscriber routing snapshot

| Interpreter | Production Python | Cython | C |
| --- | ---: | ---: | ---: |
| CPython 3.10.18 | 163.43 µs | 17.32 µs | 11.90 µs |
| CPython 3.14.9 | 118.30 µs | 17.93 µs | 8.14 µs |
| CPython 3.15.2, GIL on | 146.06 µs | 21.57 µs | 12.67 µs |
| CPython 3.15.2t, GIL off | 196.37 µs | 32.08 µs | 14.34 µs |

C was 1.46–2.24x faster than Cython for this isolated merge. Cython still cut
the snapshot time by 6.1–9.5x against the Python candidate. These kernel
ratios overstate the gain in a complete publish because callback delivery also
costs time.

### Complete publish with 2,048 subscriptions

The production Python row is the reference. Cython and C use the native
snapshot merge while retaining the EventFlow bus and callback path.

| Interpreter | Workload | Production Python | Cython | C |
| --- | --- | ---: | ---: | ---: |
| CPython 3.10.18 | No-op callbacks | 1.561 ms | 1.386 ms | 1.368 ms |
| CPython 3.14.9 | No-op callbacks | 554.81 µs | 454.92 µs | 446.93 µs |
| CPython 3.15.2, GIL on | No-op callbacks | 539.09 µs | 405.35 µs | 397.39 µs |
| CPython 3.15.2t, GIL off | No-op callbacks | 632.64 µs | 463.91 µs | 443.05 µs |
| CPython 3.10.18 | Filters reject delivery | 605.18 µs | 453.10 µs | 442.09 µs |
| CPython 3.14.9 | Filters reject delivery | 258.74 µs | 160.07 µs | 149.24 µs |
| CPython 3.15.2, GIL on | Filters reject delivery | 284.37 µs* | 151.05 µs | 144.56 µs* |
| CPython 3.15.2t, GIL off | Filters reject delivery | 348.84 µs | 180.75 µs | 161.44 µs |

For no-op callback publishes, the native candidates reduced latency by 11–30%
across the four interpreter modes. When the route was selected but event
filters rejected all callbacks, the measured reduction was 25–54%. The 3.15.2
GIL-on filtered production row had 11.89% coefficient of variation (CV), and
its C candidate had 9.07% CV; treat those two values as exploratory. The
independent Python candidate for that case was 278.94 µs at 0.79% CV, which
still shows a large native difference but needs a repeat run.

Cython and C were close in complete publishes: C was about 1–4% faster in most
stable GIL-enabled cases and about 4–11% faster in the filtered cases. Cython
was faster for history filtering on every interpreter. C therefore wins the
isolated route merge, but the full-path gain over Cython is small; these data
do not justify the extra C maintenance burden as the default implementation.

### Stability, test coverage and limits

Across the 240 result rows, 71 had CV at or above 1%; 14 had CV at or above
5%. The noisy rows are listed in the machine-readable summary with each mean,
median, standard deviation, minimum, maximum, sample count and source metadata.
The host was Arch Linux x86_64 on an Intel Core i7-12700H (20 logical CPUs),
with serial runs pinned to CPU 4. The CPU used the powersave governor with
turbo enabled. This is one machine, not a multi-host result.

The full test suite passed 399 tests on CPython 3.10.18 (one expected skip),
3.14.9, 3.15.2 with the GIL and 3.15.2t with the GIL disabled. Branch coverage
on CPython 3.14.9 was 93.71% of statements and 84.19% of branches, with 91.40%
combined coverage. The lowest combined module was `resilience.py` at 86.26%;
`bridges.py` was 87.64% and `timing.py` 87.78%. This work added focused tests
for retry deadline capping, observer failures and cancellation, awaitable
cleanup, and callback predicate behavior.

This native comparison changes no EventFlow runtime code. It does not model
CPU-heavy user callbacks, external I/O, concurrent subscription mutation,
custom history providers, startup cost, package install overhead or another
machine. No C/Cython prototypes were built for async queues, pipelines, locks
or lifecycle operations: their Python callback, event-loop and synchronization
costs are not represented by these candidate kernels. The prior 13-module
Python benchmark matrix remains the broad workload screen; this native matrix
is intentionally limited to two code paths with measured CPU-loop potential.

The results support a Cython prototype for a type-only history fast path and
routing merge if runtime integration is pursued. First compare it with a pure
Python fast path in the production implementation, then repeat the complete
publish measurements with representative callback work and on a second host.
Consider C for the route merge only if the remaining few-percent full-publish
difference is worth its extra maintenance and free-threading-specific code.

### Follow-up comparison harness

The follow-up work keeps the EventFlow runtime unchanged and expands the
benchmark-only candidates so algorithm choice can be compared separately from
implementation language:

- Routing includes the production `heapq.merge`, a Python two-pointer merge,
  Python `sorted()`, the original Cython key-then-lookup loop, a Cython
  `dict.items()` loop, and the C two-pointer kernel. The C kernel handles
  arbitrary-size Python integer identifiers with a native-size fast path and
  Python integer comparison fallback.
- History measures snapshot creation separately from filtering and the combined
  path. It covers single and tuple event types, limits of 1 and 10 as well as
  the unlimited path, and a production type-plus-predicate control. Native
  candidates remain type-only and are checked against `MemoryEventHistory`
  query order and object identity.
- Routing snapshots cover 0, 1, 4, 8, 64, 512 and 2,048 subscriptions across
  1, 7 and 31 keys with 0%, 20% and 50% wildcard subscriptions. Full-publish
  cases include no-op callbacks, a small deterministic CPU callback and a
  filter-rejected delivery. The `decision` matrix selects representative large
  history and routing cases; `full` runs the complete matrix; `smoke` only
  checks benchmark registration and worker argument forwarding.
- `benchmarks/native/validate.py` checks filter limits and tuple types, merge
  order and identity over randomized maps and very large IDs, then exercises
  concurrent publish and subscription churn through an `EventEngine`. The
  validators run on CPython 3.10, 3.14, 3.15 with the GIL and 3.15t without it.
- `benchmarks/native/build.py` writes a build manifest beside the extensions.
  Each native pyperf file records the manifest and binary SHA-256, CPython and
  Cython versions, compiler identity and effective compiler/linker flags. The
  generated files stay outside the checkout and are not part of the package.

Install the optional benchmark dependencies and build candidates outside the
repository:

```bash
python -m pip install '.[native-bench]'
python benchmarks/native/build.py --build-dir /tmp/eventflow-native-build
PYTHONPATH="/tmp/eventflow-native-build:$PWD" python -m benchmarks.native.validate cython
PYTHONPATH="/tmp/eventflow-native-build:$PWD" python -m benchmarks.native.validate c
PYTHONPATH="/tmp/eventflow-native-build:$PWD" python -m benchmarks.benchmark_native \
  --implementation cython --matrix decision -o /tmp/eventflow-native-cython.json
```

Run the benchmark command separately with `--implementation python` and `c`
to produce the comparable result files. Use `--matrix smoke` to check setup;
smoke timings are not performance evidence. The original 2026-11-10 result
tables above remain historical measurements of the earlier, narrower matrix.

### Native result files

- [`native_summary_2026-11-10.csv`](native/native_summary_2026-11-10.csv)
  contains all 240 case rows and CV statistics. Its `backend` column is
  derived from the result-file suffix; the C and Cython JSON files do not
  contain a per-case `candidate_implementation` field.
- CPython 3.10.18: [Python](native/native_2026-11-10_b16851c_py31018_gil-on_python.json),
  [Cython](native/native_2026-11-10_b16851c_py31018_gil-on_cython.json),
  [C](native/native_2026-11-10_b16851c_py31018_gil-on_c.json).
- CPython 3.14.9: [Python](native/native_2026-11-10_b16851c_py3149_gil-on_python.json),
  [Cython](native/native_2026-11-10_b16851c_py3149_gil-on_cython.json),
  [C](native/native_2026-11-10_b16851c_py3149_gil-on_c.json).
- CPython 3.15.2, GIL on: [Python](native/native_2026-11-10_b16851c_py3152_gil-on_python.json),
  [Cython](native/native_2026-11-10_b16851c_py3152_gil-on_cython.json),
  [C](native/native_2026-11-10_b16851c_py3152_gil-on_c.json).
- CPython 3.15.2t, GIL off: [Python](native/native_2026-11-10_b16851c_py3152t_gil-off_python.json),
  [Cython](native/native_2026-11-10_b16851c_py3152t_gil-off_cython.json),
  [C](native/native_2026-11-10_b16851c_py3152t_gil-off_c.json).

The Python, Cython and C values are also reproducible with `pyperf compare_to`
using each interpreter's three files from `benchmarks/results`. For example:

```bash
python -m pyperf compare_to \
  -b routing_publish/publish_noop_callbacks/2048 \
  native/native_2026-11-10_b16851c_py3149_gil-on_python.json \
  native/native_2026-11-10_b16851c_py3149_gil-on_cython.json \
  native/native_2026-11-10_b16851c_py3149_gil-on_c.json
```

## Native candidate repeat run (2026-11-10)

**Measured source:** `588284149cb55bb39ebe0fbff40c443c69925bdc` ·
**Source SHA-256:**
`1593cefb0b50138e2fc936e67c4116d0832466efd29156a4d5da51d430240c47` ·
**Working source state:** clean · **Interpreter:** CPython 3.14.9, GIL enabled ·
**pyperf:** 2.11.0 · **Cython:** 3.3.1 · **C compiler:** GCC 16.2.1

This second measurement pass repeats the expanded candidate matrix on the same
machine and interpreter as the first pass, but reverses candidate order to
reduce order-related bias. The first pass ran Python, Cython, then C; the repeat
ran C, Cython, then Python. Each pass used three worker processes, five values
per process, one warmup and a 50 ms minimum sample. That gives 15 measured
values per case in each pass and 30 pooled values per case. The worker pinned
serial work to CPU 4 on an Intel Core i7-12700H (20 logical CPUs), Arch Linux
x86_64; the CPU governor was `powersave`.

The CSV contains 346 rows across the two passes: 173 workload/backend
combinations in each order. All result files identify the same clean source
revision;
the native JSON metadata also records the Cython/compiler build manifest and
binary SHA-256. This is a repeated single-host measurement, not independent
hardware replication. Across 173 pooled cases, 168 had a coefficient of
variation (CV) of at least 1%, and 86 had CV of at least 5%. Differences near
those variation levels should not be treated as stable wins.

### History filtering at 100,000 events

The unlimited case selected every second event of one type. “Filter only”
excludes snapshot creation; “snapshot + filter” includes a full history
snapshot and type filtering. The production row is the complete
`MemoryEventHistory.query(EventFilter(...))` operation.

| Path | Mean | CV |
| --- | ---: | ---: |
| Production query | 9.73 ms | 1.9% |
| Python type filter only | 4.83 ms | 2.1% |
| Cython type filter only | 2.21 ms | 3.5% |
| C type filter only | 2.50 ms | 1.1% |
| Python snapshot + type filter | 6.85 ms | 13.4% |
| Cython snapshot + type filter | 3.84 ms | 9.5% |
| C snapshot + type filter | 4.09 ms | 5.5% |

Cython's filter loop was about 2.2 times faster than the Python type-only
candidate. With snapshot creation included, Cython was about 1.8 times faster
than the Python candidate and 2.5 times faster than the production query in
this run. The combined candidates have noticeably higher CV, so repeat on
another host before using these ratios to justify a runtime path. For limits of
1 and 10, the complete production and candidate paths clustered around
1.34–1.42 ms; copying the history snapshot dominated and the native filter did
not show a clear end-to-end gain.

### Routing with 2,048 subscribers

The selected route had seven keys and 20% wildcard subscriptions. Of 2,048
registered subscribers, 653 were selected. The snapshot measurement isolates
the merge; publish measurements include the regular EventFlow bus and callback
path.

| Candidate | Snapshot | No-op publish | Light CPU callback | Filter rejects delivery |
| --- | ---: | ---: | ---: | ---: |
| Production `heapq.merge` | 114.39 µs | 568.05 µs | 1.874 ms | 264.38 µs |
| Python two-pointer | 52.49 µs | 511.89 µs | 1.800 ms | 200.58 µs |
| Python `sorted()` | 53.44 µs | 520.50 µs | 1.899 ms | 204.17 µs |
| Cython key lookup | 18.18 µs | 501.85 µs | 1.866 ms | 165.08 µs |
| Cython `dict.items()` | 22.04 µs | 497.12 µs | 1.818 ms | 167.40 µs |
| C two-pointer | 7.00 µs | 453.82 µs | 1.716 ms | 149.91 µs |

The isolated kernel shows a large algorithm and language effect: the Python
two-pointer merge was about 2.2 times faster than production `heapq.merge`,
while C was about 2.6 times faster than the Cython key-lookup variant. The full
publish path narrows that gap. Against production, C averaged about 20% lower
latency for no-op callbacks and 43% lower latency when filters rejected all
delivery. With a small CPU callback, C averaged 8% lower latency, while Cython
was within 3% of production. The two Cython iteration variants were close on
full publishes; for the isolated snapshot, `dict.items()` was about 21% slower
than key lookup.

The selected full-publish cases still varied: CV was 4.6–9.8% for no-op
callbacks, 7.6–11.5% for the light CPU callback and 1.2–11.6% for rejected
delivery, depending on candidate. The size of the C win in the no-op and
filtered cases merits a repeat on a second host and with representative user
callbacks before adopting it. The light-callback result suggests callback cost
can consume most of the routing improvement.

### Interpretation and next evidence

- The result supports testing a direct Python two-pointer merge in the actual
  EventBus before selecting a native backend. It nearly halves the measured
  route-snapshot time in this workload and has a much smaller maintenance and
  packaging cost than C or Cython.
- C produced a measurable full-publish difference in this particular 3.14.9
  workload, especially when selected callbacks were filtered out. The benchmark
  does not include cross-host variation, extension installation/startup costs,
  or the long-term cost of shipping and maintaining a native extension.
- Cython wins the history filter loop and improves the combined snapshot/filter
  candidate, but limits of 1 and 10 remain dominated by snapshot creation.
  This does not establish that a native history API would be worthwhile.
- Keep all candidates benchmark-only for now. Before runtime integration,
  repeat the decision cases on a second host, include application-like callback
  work, and run the publish comparisons on the supported Python range and
  free-threaded builds. Preserve the current bus ordering and concurrency
  semantics in any prototype.
- The expanded matrix still covers only two native candidate areas: history
  type filtering and synchronous keyed routing. It does not compare C/Cython
  implementations for the async bus, queues, pipelines, streams, lifecycle,
  locks or the rest of the library.

### Follow-up result files

- [`native_followup_2026-11-10_5882841_py3149_gil-on_summary.csv`](native/native_followup_2026-11-10_5882841_py3149_gil-on_summary.csv)
  contains the 346 summary rows across both orders, sample statistics,
  source fingerprints and native build provenance.
- CPython 3.14.9, GIL enabled: [Python](native/native_followup_2026-11-10_5882841_py3149_gil-on_python.json),
  [Cython](native/native_followup_2026-11-10_5882841_py3149_gil-on_cython.json),
  [C](native/native_followup_2026-11-10_5882841_py3149_gil-on_c.json).
- Reversed-order repeat: [Python](native/native_followup_2026-11-10_5882841_py3149_gil-on_python_repeat2.json),
  [Cython](native/native_followup_2026-11-10_5882841_py3149_gil-on_cython_repeat2.json),
  [C](native/native_followup_2026-11-10_5882841_py3149_gil-on_c_repeat2.json).


## Second-round native routing and quality review (2026-11-10)

**Measured source:** `d9ffb1ad40887067832f5e65a4a517df68ca5072` ·
**Source SHA-256:**
`a1081799198f144acb917403155565c548146df67c688dbc5973c1dd25a73208` ·
**Working source state:** clean · **Interpreter:** CPython 3.14.9, GIL enabled ·
**pyperf:** 2.11.0 · **Cython:** 3.3.1 · **C compiler:** GCC 16.2.1

This pass measures the real EventFlow Python two-pointer routing merge and the
benchmark-only Cython and C candidates on the same host. It runs the candidates
in forward order (Python, Cython, C) and reverse order (C, Cython, Python) to
expose order effects. Each JSON file contains three worker processes and seven
measured values per process, after three warmups, with a 30 ms minimum sample.
There are 21 values for each workload in each order: 63 workload/backend rows
per order, 126 summary rows and 2,646 measured values in total. The host was an
Intel Core i7-12700H with 20 logical CPUs on Arch Linux x86_64; workers were
pinned to CPUs 4–7 and the governor was `powersave`.

Across the 63 paired workload/backend cases, forward-order CV was at least 1%
for 43 cases and at least 5% for 25; reverse-order CV crossed those thresholds
for 29 and 24 cases. Pooling the two orders gave 45/63 cases at CV ≥1% and
28/63 at CV ≥5%. The absolute shift between paired order means had a 2.18%
median and 4.17% average; 17/63 cases shifted by at least 5%, and 7/63 by at
least 10%. Keep the order-specific values in the CSV when judging a candidate.

The primary comparison uses 2,048 subscribers, seven route keys, 20% wildcard
subscriptions and a route selecting 653 callbacks. Publish paths include the
EventFlow bus and callbacks. The C and Cython modules remain optional benchmark
candidates; they are not imported by the runtime or included in the package.
The native binaries, compiler flags and source hashes are recorded in the JSON
metadata and summary CSV.

### Audit report verification and corrections

The earlier reversed-order follow-up reported 346 rows: 173 workload/backend
cases in each order, with 15 observations per case and order. Recomputing the
statistics from those raw JSON files confirms the pooled counts: 168/173 cases
had CV at least 1%, and 86/173 had CV at least 5%. The per-order counts were
64/173 and 78/173 at CV ≥5%, and 134/173 and 148/173 at CV ≥1%. Comparing the
two run means also shows a median order shift of 2.59%, a mean shift of 3.37%,
and 34/173 cases with at least a 5% shift (four were at least 10%). These
results support the report's caution about small differences and show why the
per-order results belong alongside pooled statistics.

One criticism in that report was based on a mistaken reading of the routing
event fixture. `RoutingEvent` is constructed as `(key, payload)`, so
`RoutingEvent(0, 17)` has route key `0`. The single-key fixture therefore does
match the event and is not an invalid workload. In the 2,048-subscriber matrix,
the one-key/no-wildcard case selects all 2,048 subscribers; it is a valid
full-fan-out control, not a selective-routing case. The focused fixture test
also confirms that the one-key, zero-wildcard setup selects its keyed
subscribers.

### Current production merge and full publish

The production Python path now uses the tested two-pointer merge. The
`python_heapq` rows below are explicit benchmark references for the previous
merge algorithm, while `routing_snapshot_production` and
`routing_publish_production` call the current EventFlow implementation.
Numbers in paired columns are forward-order mean / reverse-order mean. All
publish times are per event.

| Candidate | Snapshot, µs | No-op publish, µs | Light CPU callback, µs | Filter rejects, µs |
| --- | ---: | ---: | ---: | ---: |
| Python `heapq` reference | 110.81 / 110.49 | 565.13 / 545.91 | 1,766.44 / 1,754.72 | 273.42 / 253.34 |
| Python two-pointer candidate | 52.55 / 51.42 | 523.31 / 488.30 | 1,720.71 / 1,707.03 | 193.77 / 192.43 |
| Current EventFlow Python | 54.35 / 51.77 | 493.65 / 486.70 | 1,708.82 / 1,715.34 | 192.03 / 190.72 |

The `heapq` and two-pointer candidate rows both use the benchmark's
`CandidateBus`, which overrides `_subscriber_snapshot()` and calls the selected
merge kernel. That common harness makes candidate-to-candidate comparison more
useful for estimating the algorithm change, though it still includes the
subclass and kernel-call overhead. The current production row calls
`EventBus._subscriber_snapshot()` directly. It did not run the pre-change
EventBus source in this exact harness, so a production-to-`heapq` full-publish
ratio is not a clean source-to-source A/B result.

Within the common candidate harness, the Python two-pointer candidate averaged
51.99 µs per snapshot versus 110.65 µs for `heapq` (53.0% lower). Full publish
averages were 505.81 µs versus 555.52 µs for no-op callbacks (9.0% lower),
1.714 ms versus 1.761 ms with the light CPU callback (2.7% lower), and 193.10
µs versus 263.38 µs when filters rejected delivery (26.7% lower). The last
comparison is particularly noisy: the heapq filter-rejected case had 20.4% CV
in the forward pass, although the reversed pass was 1.4%. The current direct
production path measured 53.06 µs for its snapshot and 490.18 / 1,712.08 /
191.37 µs for those three publish cases. Its same-algorithm two-pointer
`CandidateBus` row is about 3% slower on no-op publish, illustrating the small
harness-call difference between the paths. These measurements support the
algorithm choice, but are not an exact old-production versus new-production
percentage claim.

### Native candidates against the current Python path

The table gives the mean per order for the current Python baseline and the
native candidates on the same 2,048-subscriber, seven-key workload. Native
candidates also run through `CandidateBus`, so these are benchmark integration
comparisons, not source-to-source production builds. The Cython `dict.items()`
variant is included because it was stable in both orders; the Cython key-lookup
no-op publish had a 13.96% reverse-order CV and should be treated as noisy.

| Candidate | No-op publish, µs | Light CPU callback, µs | Filter rejects, µs |
| --- | ---: | ---: | ---: |
| Current EventFlow Python | 493.65 / 486.70 | 1,708.82 / 1,715.34 | 192.03 / 190.72 |
| Cython key lookup | 449.61 / 467.49 | 1,676.36 / 1,678.73 | 157.65 / 157.67 |
| Cython `dict.items()` | 455.80 / 456.16 | 1,677.46 / 1,688.57 | 165.13 / 164.11 |
| C two-pointer | 443.17 / 438.04 | 1,666.37 / 1,671.91 | 146.58 / 146.34 |

Across the two orders, Cython `dict.items()` averaged about 7.0% lower latency
for no-op publish, 1.7% for the light callback and 14.0% for filter-rejected
publish. C averaged 10.1%, 2.5% and 23.5% lower latency for those same cases.
The candidate harness overhead is proportionally material to the small light
callback deltas; do not treat the measured 1.7–2.5% as a confirmed native
runtime gain. The gap between C and Cython is smaller in complete publish than
in the isolated merge, and the light callback consumes most of the merge
improvement. These data show native potential, but do not justify native
runtime dependencies or distribution complexity by themselves.

The isolated snapshot means were 53.06 µs for production Python, 51.99 µs for
the two-pointer candidate, 18.52 µs for Cython key lookup and 6.84 µs for C.
The Python replacement already captures most of the benefit over the old
`heapq` merge. The native kernels are faster in isolation, but complete publish
results are the relevant boundary for a runtime decision.

The additional route shapes help identify when a native merge matters:

| Workload | Current EventFlow Python | Cython key lookup | C two-pointer |
| --- | ---: | ---: | ---: |
| 2,048 subscribers, one key, no wildcards (all callbacks selected) | 1.398 ms | 1.408 ms | 1.405 ms |
| 2,048 subscribers, 31 keys, 50% wildcards | 814.46 µs | 754.28 µs | 736.15 µs |

The one-key workload is a full-fan-out control and shows no native benefit once
callback dispatch dominates. The 31-key workload showed about 7.4% lower latency
with Cython and 9.6% with C than current Python, with low serial-run variation.
These profiles are still synthetic and single-host.

### Concurrent publisher measurements

The contention matrix publishes from 1, 4 or 8 threads against 2,048
subscriptions. Values below are the mean elapsed microseconds per publish,
averaged across the forward and reverse passes. Each individual case had 21
samples per pass.

| Publishers | Current EventFlow Python | Cython key lookup | C two-pointer |
| ---: | ---: | ---: | ---: |
| 1 | 541.81 µs | 484.29 µs | 474.36 µs |
| 4 | 1.011 ms | 791.50 µs | 774.46 µs |
| 8 | 988.64 µs | 887.52 µs | 814.45 µs |

These contention rows are not stable enough for an implementation decision:
pooled CV ranged from 13.3% to 26.2% for the displayed cases. The benchmark
reports average time per publish in a contended batch; it does not report
individual request p95/p99 latency or isolate lock-wait time. The apparent
candidate differences need a quieter repeat, preferably on a second host, and
an explicit latency-distribution workload before making a concurrency claim.

### Test and implementation status

The Python production change replaces `heapq.merge` with an in-place two-pointer
walk over the sorted keyed and wildcard subscription indexes. Existing
no-routing, keyed-only and wildcard-only fast paths remain. The result order is
still registration order. New regression coverage exercises skewed index sizes,
cancellation, callback ordering and concurrent publish/subscription churn.
A focused benchmark fixture test ensures the one-key route selects the expected
keyed subscriptions.

The full 408-test suite passed on CPython 3.10.18 (one expected skip), 3.14.9,
3.15.2 with the GIL and 3.15.2t with the GIL disabled. Ruff and Pyright passed,
including Pyright configured for Python 3.10 and 3.14. Current-source native
build and semantic validation passed on CPython 3.14.9. The same C and Cython
kernel sources passed validation earlier on Python 3.10, 3.14, 3.15 GIL-on and
3.15t GIL-off; after the routing-matrix CLI update, those four native build
checks were not all repeated at the exact `d9ffb1a` source revision. Hosted CI
was not run because these local commits were not pushed.

### Decision and remaining evidence

- Keep the Python two-pointer merge in production. It reduces the targeted
  merge and full-publish costs compared with the previous Python `heapq` path,
  has direct regression coverage, and adds no dependency or packaging burden.
- Keep C and Cython as benchmark-only candidates. The C advantage is measurable
  in selected serial publishes, while Cython is close on full delivery and has
  lower development cost. The remaining evidence is not broad enough to choose
  a native runtime backend.
- Repeat the focused matrix on a second host and across the supported interpreter
  range before estimating portable gains. Include realistic callback work,
  per-operation latency distributions and separately measured lock waiting.
- This benchmark still covers only keyed synchronous routing. It does not
  compare native implementations for async buses, queues, pipelines, streams,
  lifecycle, history APIs or the rest of EventFlow.
- No push, release or PyPI publication was performed.

### Second-round result files

- [`native_routing_2026-11-10_d9ffb1a_py3149_gil-on_summary.csv`](native/native_routing_2026-11-10_d9ffb1a_py3149_gil-on_summary.csv)
  contains the 126 rows, per-order statistics, source fingerprints and native
  build provenance.
- Forward order: [Python](native/native_routing_2026-11-10_d9ffb1a_py3149_gil-on_python_forward.json),
  [Cython](native/native_routing_2026-11-10_d9ffb1a_py3149_gil-on_cython_forward.json),
  [C](native/native_routing_2026-11-10_d9ffb1a_py3149_gil-on_c_forward.json).
- Reverse order: [C](native/native_routing_2026-11-10_d9ffb1a_py3149_gil-on_c_reverse.json),
  [Cython](native/native_routing_2026-11-10_d9ffb1a_py3149_gil-on_cython_reverse.json),
  [Python](native/native_routing_2026-11-10_d9ffb1a_py3149_gil-on_python_reverse.json).


## Python type-only history query fast path (2026-11-10)

**Baseline source:** `a9af2fc8a16d1169c3a49c6b7893eb6d4909ba4a` ·
**Optimized source:** `297dbb939b6385b6a1bb5fd5b7becacdefee340` ·
**Interpreter:** CPython 3.14.9, GIL enabled · **pyperf:** 2.11.0 ·
**Working source state:** clean for both runs

The report identified large type-only `MemoryEventHistory.query()` calls as a
worthwhile Python optimization candidate. Before changing the query, a focused
`--matrix history` was added to the existing native benchmark runner so these
measurements can run without routing cases. Both runs use the same 100,000-event
history with one matching event every two records, 3 worker processes, 7 values
per process, 3 warmups, a 30 ms minimum sample and CPUs 4–7. There are 21
measured values per workload in each source revision. The JSON files record
separate clean source fingerprints; the CSV contains 42 rows, per-run
statistics and both revisions.

The production path now uses a direct `isinstance` loop only when passed an
exact `EventFilter` with an event type and no predicate. It keeps the existing
lock-protected history snapshot, scans backward for limited queries, then
reverses the matches so results remain in publication order. Predicates and
`EventFilter` subclasses continue through `matches()`; this preserves custom
filter behavior.

| Production query | Before | After | Change |
| --- | ---: | ---: | ---: |
| Single event type, unlimited | 9.60 ms (CV 0.70%) | 6.23 ms (CV 1.73%) | 1.54× faster; 35.1% lower |
| Tuple of event types, unlimited | 11.14 ms (CV 3.06%) | 7.61 ms (CV 2.68%) | 1.47× faster; 31.7% lower |
| Single type, `limit=1` | 1.42 ms (CV 5.60%) | 1.30 ms (CV 4.05%) | 8.4% lower; snapshot cost dominates |
| Single type, `limit=10` | 1.37 ms (CV 0.82%) | 1.37 ms (CV 1.18%) | No measurable change |

The unlimited type-only improvements are stable in both samples and match
`pyperf compare_to` results. Limited queries still copy a 100,000-item snapshot
before filtering, so they remain around 1.3–1.4 ms. The `limit=1` difference is
small relative to run variation, and the tuple `limit=10` baseline was noisy
(CV 18.22%); neither supports a broad limited-query speed claim.

Predicate queries remained effectively unchanged: the unbounded case moved
from 12.42 ms (CV 0.96%) to 12.35 ms (CV 1.70%), and `limit=10` moved from
1.36 ms (CV 3.75%) to 1.35 ms (CV 4.80%). The fast path intentionally does not
cover user predicates, because they must still execute once for each candidate
event and preserve the existing awaitable and strict-boolean checks. The test
suite also verifies event subclasses, tuple types, identity, result order,
limits and custom `EventFilter.matches()` overrides.

This is a Python-only specialization for the built-in memory history. It does
not change custom history providers, native runtime dependencies or package
contents. The 100,000-event history is much larger than the default history
capacity of 1,000; workloads with smaller histories may see smaller absolute
gains, and were not measured in this focused before/after comparison.

Reproduce the focused matrix with the `dev` and `native-bench` extras available:

```bash
python -m benchmarks.benchmark_native \
  --implementation=python --matrix=history \
  --processes=3 --values=7 --warmups=3 --min-time=0.03 --affinity=4-7 \
  --output benchmarks/results/native/history.json
```

### History fast-path result files

- [`native_history_2026-11-10_type_filter_summary.csv`](native/native_history_2026-11-10_type_filter_summary.csv)
  contains the 42 pre/post rows and source fingerprints.
- [Before optimization](native/native_history_2026-11-10_a9af2fc_py3149_gil-on_pre_python_fast_path.json)
  and [after optimization](native/native_history_2026-11-10_297dbb9_py3149_gil-on_post_python_fast_path.json)
  contain the raw pyperf results.

### Native history candidates after the Python fast path

The focused matrix was repeated after the Python optimization on clean source
revision `7150827` with CPython 3.14.9, the GIL enabled, an Intel Core i7-12700H,
and CPUs 4–7. Each case used three processes with seven measured values per
process, three warmups and a 30 ms minimum value duration. Cython ran before C
in the first pass; C ran before Cython in the reversed pass. A Python production
query run after both native passes checks for timing drift.

The Python column measures `MemoryEventHistory.query()` with its actual
`EventFilter`. Each native candidate first takes the same Python history
snapshot, then applies its C or Cython type-filter kernel. It therefore measures
snapshot plus native filtering, not a native history implementation or a
replacement production backend.

| Query over 100,000 events, one match in two | Python production before → post-native repeat | Cython snapshot + filter, forward / reversed | C snapshot + filter, forward / reversed |
| --- | ---: | ---: | ---: |
| Single event type, unlimited | 6.23 ms (CV 1.73%) → 7.45 ms (CV 12.35%) | 3.62 ms (CV 6.32%) / 3.49 ms (CV 4.80%) | 4.34 ms (CV 25.06%) / 4.01 ms (CV 2.26%) |
| Tuple of event types, unlimited | 7.61 ms (CV 2.68%) → 8.44 ms (CV 10.20%) | 4.85 ms (CV 1.51%) / 4.82 ms (CV 1.97%) | 5.44 ms (CV 19.66%) / 5.43 ms (CV 4.36%) |

Across the two orders, Cython averaged 3.55 ms for the single-type case and
4.84 ms for the tuple case; C averaged 4.18 ms and 5.43 ms. Against the earlier
stable Python production run, those means are respectively about 43% and 36%
lower for Cython, and 33% and 29% lower for C. Treat these as candidate
measurements, not expected application-level speedups: the later Python control
was 19.5% and 11.0% slower and had 10–12% CV, while the forward C full-path
cases were also noisy. The observed run-to-run spread limits the precision of
these ratios. No C or Cython code was moved into EventFlow's runtime.

- [`Cython, first order`](native/native_history_2026-11-10_7150827_py3149_gil-on_cython.json)
  and [`C, first order`](native/native_history_2026-11-10_7150827_py3149_gil-on_c.json)
- [`C, reversed order`](native/native_history_2026-11-10_7150827_py3149_gil-on_c_reverse.json)
  and [`Cython, reversed order`](native/native_history_2026-11-10_7150827_py3149_gil-on_cython_reverse.json)
- [Python control after native runs](native/native_history_2026-11-10_7150827_py3149_gil-on_python_after_native.json)
- [Combined history measurements and source fingerprints](native/native_history_2026-11-10_type_filter_summary.csv)

The combined CSV contains 115 rows: the original 42 pre/post Python rows, 52
native candidate rows across both orders, and 21 post-native Python control
rows. It includes the per-case CV, interpreter, source hash, and native build
metadata.

## Focused routing latency and C tuple history rerun (2026-11-11)

This follow-up uses one clean source revision for the expanded routing latency
matrix and all three history implementations.

| Property | Value |
| --- | --- |
| Source revision | <code>1fbef306cf2d3da238629543da208e9e87f674a2</code> |
| Source SHA-256 | <code>cf0213094b56b02230b5c4dbae89745aba8927a090e6154b183a9726893d5e42</code> |
| Working tree | Clean; <code>eventflow_source_dirty=false</code> in all six pyperf files |
| Interpreter | CPython 3.14.9, GIL enabled |
| Host | Intel Core i7-12700H, Linux x86_64, glibc 2.44 |
| Compiler / Cython | GCC 16.2.1 / Cython 3.3.1 |
| pyperf | 2.11.0 |

The Python, Cython and C runs were made serially on the same machine. They are
one-host measurements from one day; the pyperf metadata retains the individual
run environment and native artifact identities.

### C tuple indexing for unbounded history filters

The new C candidate walks an input tuple with direct indexed access instead of
the generic <code>PyIter_Next()</code> path. It is registered only for unbounded
queries; the validator checks event identity, order, type tuples, limits and
concurrent routing behavior before timing.

Each query contains 100,000 events with one match in two. <code>filter only</code>
times the type-filter kernel on an existing snapshot. <code>snapshot + filter</code>
includes a fresh Python history snapshot on every operation. These rows compare
candidate kernels; they are not a native implementation of
<code>MemoryEventHistory.query()</code>.

| Backend | Candidate | Type selection | Filter only | Snapshot + filter |
| --- | --- | --- | ---: | ---: |
| Python | Python type fast path | One event type | 4.75 ms (CV 4.9%) | 6.22 ms (CV 9.2%) |
| Cython | Cython type filter | One event type | 2.08 ms (CV 1.7%) | 3.50 ms (CV 2.3%) |
| C | Generic iterator | One event type | 2.42 ms (CV 0.8%) | 4.07 ms (CV 6.1%) |
| C | Direct tuple indexing | One event type | 2.16 ms (CV 4.6%) | 3.72 ms (CV 6.3%) |
| Python | Python type fast path | Type tuple | 6.33 ms (CV 10.9%) | 7.62 ms (CV 10.6%) |
| Cython | Cython type filter | Type tuple | 3.47 ms (CV 0.6%) | 4.92 ms (CV 0.8%) |
| C | Generic iterator | Type tuple | 3.81 ms (CV 0.6%) | 4.80 ms (CV 2.2%) |
| C | Direct tuple indexing | Type tuple | 3.47 ms (CV 0.7%) | 4.36 ms (CV 2.3%) |

Direct tuple indexing reduced the C kernel mean by about 11% for one type and
9% for a type tuple. Including the snapshot, the reductions were about 9% and
9%. The direct C candidate and Cython were close for the single-type case;
the C tuple candidate was lower for the tuple case in this run. These are
candidate measurements, not application-wide speedups.

Snapshot creation alone measured 1.22–1.34 ms. It remains a substantial floor
for small limited queries, so changing the filtering loop cannot remove most of
their cost. The C tuple candidate deliberately skips limits rather than
pretending to benchmark an unsupported path.

### Per-publish latency under contention

The routing fixture has 2,048 subscriptions. The base profile uses seven keys,
20% wildcard subscriptions and delivers to 653 callbacks. The filter-rejection
profile uses the same routing distribution but rejects all 653 selected
subscriptions by event type. The broad-key profile uses 31 keys and 50%
wildcards. Every profile was measured with one, four and eight publisher
threads, using the same event and callback behavior across implementations.

The tables show the **median of nine pyperf values** for each reported
percentile. One pyperf value is itself a nearest-rank percentile over 100
publishes per publisher: 100, 400 or 800 observations depending on the thread
count. <code>P99 CV</code> is the coefficient of variation across those nine
percentile estimates. It does not describe the variation among individual raw
publish calls. The companion CSV records mean, median, standard deviation and
CV for every percentile and benchmark name.

#### All callbacks delivered: seven keys, 20% wildcards

| Path | Publishers | P50 | P95 | P99 | P99 CV |
| --- | ---: | ---: | ---: | ---: | ---: |
| Production Python | 1 | 485 µs | 494 µs | 510 µs | 16.4% |
| Production Python | 4 | 488 µs | 5.68 ms | 22.96 ms | 14.0% |
| Production Python | 8 | 4.52 ms | 4.96 ms | 5.69 ms | 25.7% |
| Python two-pointer | 1 | 481 µs | 497 µs | 506 µs | 2.1% |
| Python two-pointer | 4 | 486 µs | 5.71 ms | 27.29 ms | 17.3% |
| Python two-pointer | 8 | 4.41 ms | 5.09 ms | 6.49 ms | 19.3% |
| C key lookup | 1 | 444 µs | 540 µs | 907 µs | 16.6% |
| C key lookup | 4 | 438 µs | 5.55 ms | 26.59 ms | 25.8% |
| C key lookup | 8 | 3.89 ms | 4.65 ms | 6.36 ms | 49.2% |
| Cython key lookup | 1 | 449 µs | 458 µs | 476 µs | 2.4% |
| Cython key lookup | 4 | 450 µs | 5.68 ms | 22.14 ms | 24.6% |
| Cython key lookup | 8 | 4.01 ms | 4.94 ms | 7.18 ms | 53.1% |
| Cython item iteration | 1 | 460 µs | 579 µs | 906 µs | 33.2% |
| Cython item iteration | 4 | 458 µs | 5.60 ms | 27.28 ms | 23.5% |
| Cython item iteration | 8 | 4.14 ms | 4.59 ms | 5.85 ms | 15.3% |

#### All callbacks delivered: 31 keys, 50% wildcards

| Path | Publishers | P50 | P95 | P99 | P99 CV |
| --- | ---: | ---: | ---: | ---: | ---: |
| Production Python | 1 | 798 µs | 820 µs | 881 µs | 36.3% |
| Production Python | 4 | 817 µs | 11.18 ms | 31.33 ms | 31.2% |
| Production Python | 8 | 7.75 ms | 8.77 ms | 10.64 ms | 19.5% |
| Python two-pointer | 1 | 795 µs | 823 µs | 835 µs | 1.6% |
| Python two-pointer | 4 | 824 µs | 7.53 ms | 28.56 ms | 42.9% |
| Python two-pointer | 8 | 7.94 ms | 8.34 ms | 9.23 ms | 15.4% |
| C key lookup | 1 | 732 µs | 748 µs | 761 µs | 2.3% |
| C key lookup | 4 | 740 µs | 6.98 ms | 32.72 ms | 36.7% |
| C key lookup | 8 | 7.05 ms | 7.48 ms | 8.24 ms | 23.4% |
| Cython key lookup | 1 | 747 µs | 789 µs | 801 µs | 12.0% |
| Cython key lookup | 4 | 915 µs | 9.68 ms | 37.87 ms | 23.4% |
| Cython key lookup | 8 | 7.21 ms | 7.93 ms | 8.23 ms | 11.8% |
| Cython item iteration | 1 | 749 µs | 789 µs | 800 µs | 5.6% |
| Cython item iteration | 4 | 774 µs | 11.37 ms | 33.56 ms | 30.8% |
| Cython item iteration | 8 | 7.11 ms | 7.82 ms | 9.39 ms | 12.1% |

#### All selected callbacks rejected by event type: seven keys, 20% wildcards

| Path | Publishers | P50 | P95 | P99 | P99 CV |
| --- | ---: | ---: | ---: | ---: | ---: |
| Production Python | 1 | 191 µs | 200 µs | 229 µs | 12.1% |
| Production Python | 4 | 192 µs | 231 µs | 10.66 ms | 32.9% |
| Production Python | 8 | 202 µs | 1.88 ms | 18.02 ms | 47.1% |
| Python two-pointer | 1 | 190 µs | 197 µs | 210 µs | 6.7% |
| Python two-pointer | 4 | 191 µs | 385 µs | 11.13 ms | 18.9% |
| Python two-pointer | 8 | 1.51 ms | 3.88 ms | 17.65 ms | 25.1% |
| C key lookup | 1 | 144 µs | 154 µs | 157 µs | 18.4% |
| C key lookup | 4 | 145 µs | 182 µs | 5.35 ms | 35.3% |
| C key lookup | 8 | 145 µs | 189 µs | 10.74 ms | 56.2% |
| Cython key lookup | 1 | 156 µs | 162 µs | 173 µs | 12.4% |
| Cython key lookup | 4 | 156 µs | 180 µs | 5.68 ms | 14.6% |
| Cython key lookup | 8 | 157 µs | 180 µs | 20.78 ms | 51.7% |
| Cython item iteration | 1 | 161 µs | 168 µs | 203 µs | 17.8% |
| Cython item iteration | 4 | 161 µs | 215 µs | 5.40 ms | 46.8% |
| Cython item iteration | 8 | 164 µs | 945 µs | 11.41 ms | 60.6% |

The native rows use the same benchmark fixture interface, while the production
Python row calls EventFlow's current <code>EventBus</code>. The small
differences between these paths should not be treated as a drop-in runtime A/B
result.

Across the clean measurements, C's median P50 was around 8% below the production
Python control in the one-publisher all-delivered 7-key and 31-key profiles.
At four and eight publishers, C sometimes had a lower median P50, but its P99
was not consistently lower. The Cython key-lookup candidate was close to
Python on the 7-key profile and modestly lower on the 31-key single-publisher
case. Filter rejection was faster than full delivery, as expected, because no
callbacks ran.

The variation is too high for a backend decision. Several contended P99
estimates have CVs above 30%, and some P50 estimates vary substantially too.
The 4/8-publisher results reflect GIL scheduling, OS thread scheduling and bus
work together. They do not show that C or Cython removes the tail-latency cost.

#### Separate bus-lock acquisition measurements

The lock-wait series wraps the bus lock and runs separately from publish
latency. It reports the median P50/P95/P99 estimates for the all-delivered
7-key profile. This wrapper is experimental and can perturb the observed
lock path.

| Path | Publishers | P50 | P95 | P99 | P99 CV |
| --- | ---: | ---: | ---: | ---: | ---: |
| Production Python | 1 | 0.26 µs | 0.40 µs | 0.57 µs | 6.6% |
| Production Python | 4 | 0.28 µs | 0.59 µs | 2.16 µs | 295.0% |
| Production Python | 8 | 0.33 µs | 1.21 µs | 2.12 µs | 154.8% |
| Python two-pointer | 1 | 0.27 µs | 0.88 µs | 1.15 µs | 45.0% |
| Python two-pointer | 4 | 0.28 µs | 1.28 µs | 2.18 µs | 297.5% |
| Python two-pointer | 8 | 0.31 µs | 1.77 µs | 2.14 µs | 197.8% |
| C key lookup | 1 | 0.27 µs | 0.47 µs | 1.45 µs | 28.0% |
| C key lookup | 4 | 0.27 µs | 0.64 µs | 2.09 µs | 19.4% |
| C key lookup | 8 | 0.30 µs | 1.32 µs | 2.05 µs | 8.8% |
| Cython key lookup | 1 | 0.26 µs | 0.37 µs | 0.58 µs | 15.7% |
| Cython key lookup | 4 | 0.27 µs | 0.52 µs | 2.15 µs | 5.8% |
| Cython key lookup | 8 | 0.29 µs | 0.93 µs | 2.01 µs | 298.0% |
| Cython item iteration | 1 | 0.27 µs | 0.42 µs | 0.66 µs | 16.4% |
| Cython item iteration | 4 | 0.27 µs | 0.59 µs | 2.31 µs | 198.1% |
| Cython item iteration | 8 | 0.31 µs | 1.25 µs | 2.13 µs | 20.5% |

The central lock-wait estimates are in the sub-microsecond to low-microsecond
range, while publish P99 estimates reach multiple milliseconds. This points
toward scheduler/GIL and full dispatch effects being relevant, but it is not a
causal profile: the lock instrument is separate and the lock P99 CV is very
high under contention.

### Build, binary size and import cost

One clean CPython 3.14.9 build produced both optional extensions. The import
column is the extension module's <code>self</code> time from
<code>-X importtime</code>, measured in 20 separate interpreter processes; the
OS file cache was not controlled.

| Artifact | ABI | Binary size | Import self median | Import self P95 | CV |
| --- | --- | ---: | ---: | ---: | ---: |
| C | <code>cpython-314-x86_64-linux-gnu</code> | 40,848 bytes | 127 µs | 136 µs | 4.3% |
| Cython | <code>cpython-314-x86_64-linux-gnu</code> | 625,512 bytes | 229.5 µs | 240 µs | 3.6% |

Building both extensions together took 7.111 seconds in one clean run. This is
not an isolated C-versus-Cython build-time comparison. The standard EventFlow
wheel remains pure Python and contains neither extension; no native wheel was
built, so there is no integrated-wheel size or startup-cost result.

In this build, the Cython binary was 15.3 times larger than the C binary and
its measured import self time was about 1.8 times higher. These are local
artifact costs, not a complete user-startup comparison.

The <code>cpython-314</code> filename confirms this measurement produced a
CPython 3.14-specific ABI artifact. Native builds were not repeated for Python
3.10, 3.11, 3.12, 3.13, 3.15, free-threaded CPython, Windows or macOS. The
current CI's free-threaded Python runtime job does not validate these benchmark
extensions. Cross-ABI and cross-platform native support remains an integration
gate.

### Result files and reproduction

- [Combined 321-row CSV with per-percentile variance and provenance](native/native_routing_history_2026-11-11_1fbef30_summary.csv)
- [Python history JSON](native/native_history_2026-11-11_1fbef30_py3149_gil-on_python.json), [Cython history JSON](native/native_history_2026-11-11_1fbef30_py3149_gil-on_cython.json) and [C history JSON](native/native_history_2026-11-11_1fbef30_py3149_gil-on_c.json)
- [Python routing latency JSON](native/native_routing_latency_2026-11-11_1fbef30_py3149_gil-on_python.json), [Cython routing latency JSON](native/native_routing_latency_2026-11-11_1fbef30_py3149_gil-on_cython.json) and [C routing latency JSON](native/native_routing_latency_2026-11-11_1fbef30_py3149_gil-on_c.json)
- [Native build and import-cost observations](native/native_build_cost_2026-11-11_1fbef30_py3149_gil-on.json)

The history command used three processes, five values per process, two
warmups, a 50 ms minimum value duration and CPU 4:

~~~bash
python -m benchmarks.benchmark_native \
  --implementation=python --matrix=history \
  --processes=3 --values=5 --warmups=2 --min-time=0.05 \
  --affinity=4 --quiet --output history.json
~~~

The routing command used three processes, three values per process, one
warmup, 100 publishes per publisher and CPUs 4–7:

~~~bash
python -m benchmarks.benchmark_native \
  --implementation=python --matrix=latency \
  --processes=3 --values=3 --warmups=1 --loops=100 \
  --affinity=4-7 --quiet --output routing-latency.json
~~~

For C and Cython, set <code>PYTHONPATH</code> to the clean native build
directory and repeat with <code>--implementation=c</code> and
<code>--implementation=cython</code>. Keep each implementation's JSON; the
summary CSV is derived from those raw files.

### Audit recommendation status

| Audit item | Result |
| --- | --- |
| Exclude generated benchmark data from the sdist | Done. The archive retains benchmark and test sources, but excludes generated <code>benchmarks/results/</code> files. This follows the report's reproducible-source option; another paragraph in the report prefers excluding all benchmark material, so this distribution-policy recommendation is internally inconsistent. |
| Measure publish P50/P95/P99 and separate lock wait | Done for the three listed profiles on one host. Multi-host or different-day repetition remains open. |
| Compare C tuple access with generic C iteration | Done for unlimited tuple history candidates; the candidate is excluded from limited-query comparisons. |
| Measure native build/import/distribution cost | Partially done. One CPython 3.14 Linux build, two extension sizes and 20-process import samples are recorded. There is no isolated per-extension build time or native wheel build. |
| Validate native ABI, GIL-free and operating-system coverage | Not done. Only CPython 3.14.9 with the GIL on, Linux x86_64 was built and validated. |
| Integrate a native runtime backend | Not done, consistent with the report's evidence gate. Python production paths and public APIs are unchanged. |

The next useful decision experiment is a repeated latency run on a second host
or on a separate day with controlled CPU frequency and a larger sample budget.
Native integration should wait until the end-to-end gain remains material in
the target workload and the supported ABI/platform matrix is proven.

The verified source archive was 271,728 bytes with 120 members; all 413 tests
passed when run from the extracted archive. A wheel built from that archive
was 105,854 bytes, had the pure-Python <code>py3-none-any</code> tag and
contained no native extension files.
