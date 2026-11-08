# Historical measurements and current benchmark workflow

Measured on 2026-10-28. Baseline source: `14fe966`; final runtime source: `e033f1a`. No remote service is involved.

Files in this directory are historical records. Existing JSON files keep the
schema and measurements produced by the earlier EventFlow benchmark harness;
they have not been rewritten as pyperf data. New runs use pyperf's standard
JSON format. Each worker records the EventFlow implementation, observed GIL
mode, Git revision, source-only dirty state, and a SHA-256 fingerprint of the
runtime package, benchmark Python files and `pyproject.toml`. Generated result
files are excluded from the fingerprint. Source metadata is collected outside
the timed work. Use pyperf's `compare_to` command for new result comparisons.

## Method

- Python: 3.10.18 (main, Aug 13 2026, 01:16:48) [GCC 16.1.1 20260728]
- Platform: Linux-7.2.11-arch1-1-x86_64-with-glibc2.44
- Independent publish runs: 2,000 timed operations per case after warm-up.
- Adjacent publish repeat: 5,000 operations per case in separate baseline/current processes.
- Controlled comparison: both versions loaded in one process, 9 alternating batches of 250 operations, reversing old/new order between batches. The table reports the median batch mean.
- Pipeline: 5,000 operations; configuration and decorator registration are outside timing.
- Stream: 100 complete consumptions of 1,000 items; latency is per whole stream.
- Independent-run p50/p95/p99 sample operation latencies; throughput uses median operation latency.
- Peak traced allocations use a separate pass (up to 100 operations). They are not process RSS.
- No strict timing gate or universal deployment guarantee is inferred from local microbenchmarks.

## Existing publish path

Time-separated measurements showed clock/scheduler drift: the initial baseline versus a later independent run reached +15.6% in one case. An adjacent-process repeat moved that case below baseline but produced a different +17.6% outlier. Raw JSON retains both observations. The controlled alternating comparison below did not reproduce a persistent regression. The sync/async publish method bodies are unchanged; the later synchronous fix affects wait registration/clear only.

| Case | Baseline (us/op) | Extended (us/op) | Change |
| --- | ---: | ---: | ---: |
| sync/0 | 2.717 | 2.739 | +0.8% |
| sync/1 | 4.357 | 4.384 | +0.6% |
| sync/10 | 17.412 | 17.388 | -0.1% |
| sync/100 | 135.208 | 136.236 | +0.8% |
| sync/filter-type | 17.555 | 17.576 | +0.1% |
| sync/filter-predicate | 22.117 | 22.193 | +0.3% |
| async-sequential/0 | 2.839 | 2.816 | -0.8% |
| async-sequential/1 | 3.805 | 3.824 | +0.5% |
| async-sequential/10 | 10.190 | 10.225 | +0.3% |
| async-sequential/100 | 70.268 | 69.473 | -1.1% |
| async-concurrent/0 | 4.362 | 4.247 | -2.6% |
| async-concurrent/1 | 17.019 | 16.784 | -1.4% |
| async-concurrent/10 | 65.514 | 64.589 | -1.4% |
| async-concurrent/100 | 504.239 | 504.049 | -0.0% |

Controlled changes range from -2.6% to +0.8%. This supports preserving the default dispatch path on this machine; it is not a statistical production guarantee.

## New pipelines

| Case | p50 (us) | p95 (us) | p99 (us) | Peak traced bytes |
| --- | ---: | ---: | ---: | ---: |
| sync/1/add | 1.298 | 1.460 | 1.916 | 192 |
| sync/1/decorator | 1.372 | 1.637 | 1.726 | 192 |
| sync/1/metrics | 9.271 | 9.759 | 13.438 | 1016 |
| sync/5/add | 4.630 | 4.847 | 5.795 | 192 |
| sync/5/decorator | 4.678 | 4.928 | 7.040 | 192 |
| sync/5/metrics | 28.490 | 30.277 | 35.727 | 1560 |
| sync/20/add | 16.912 | 18.567 | 21.498 | 192 |
| sync/20/decorator | 16.038 | 17.263 | 20.547 | 192 |
| sync/20/metrics | 100.201 | 105.929 | 113.489 | 3600 |
| async/1/add | 2.010 | 2.153 | 2.281 | 1352 |
| async/1/decorator | 2.003 | 2.106 | 2.196 | 608 |
| async/5/add | 6.301 | 6.556 | 8.358 | 608 |
| async/5/decorator | 6.578 | 6.990 | 8.150 | 608 |
| async/20/add | 21.902 | 23.974 | 27.385 | 608 |
| async/20/decorator | 22.009 | 23.660 | 33.049 | 608 |

Decorator and imperative registration use the same runtime plan; small independent-run timing differences are measurement variation. Metrics allocate observations and update aggregates only when enabled.

## Streams

`stream.json` was refreshed with 100 iterations of 1,000 items per case on
Python 3.10.18/Linux. `native-map-filter` applies the same mapping and predicate
as the EventFlow cases using a plain async generator; compare it with
`unbuffered` to estimate stream API overhead. `direct-generator` remains a
raw-source lower bound and has no mapping or filtering.

| Case | p50 per 1,000 items (us) | Items/s | Peak traced bytes |
| --- | ---: | ---: | ---: |
| direct-generator | 167.654 | 5945727 | 1352 |
| native-map-filter | 174.118 | 5742308 | 752 |
| unbuffered | 4024.101 | 248499 | 5096 |
| buffer-16 | 7106.649 | 140710 | 6652 |

The raw generator has no transformation or ownership machinery. The unbuffered case includes map/filter and owned closure. Buffering additionally runs one producer, a bounded queue and backpressure; it is a resource/decoupling choice, not a speedup for a trivial CPU-only source.

## Stream map/filter fusion (2026-11-02)

On Python 3.14.8 and 3.10.18/Linux, sequential 30-iteration runs over 5,000
items measured the implementation before and after fusing chained
`AsyncStream.map()` and `filter()` callbacks. The complete per-case data is in
[`stream_fusion.json`](stream_fusion.json).

| Python | Case | Before p50 (us) | After p50 (us) | Before items/s | After items/s | Peak traced bytes |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 3.14.8 | native map/filter | 1,306 | 1,347 | 3,828,776 | 3,704,893 | 1,176 -> 1,176 |
| 3.14.8 | EventFlow unbuffered | 8,396 | 5,809 | 595,313 | 860,265 | 7,132 -> 6,016 |
| 3.14.8 | EventFlow buffer-16 | 14,528 | 12,159 | 344,153 | 410,951 | 13,321 -> 12,149 |
| 3.10.18 | native map/filter | 889 | 849 | 5,622,292 | 5,888,699 | 752 -> 752 |
| 3.10.18 | EventFlow unbuffered | 17,333 | 12,417 | 288,300 | 402,587 | 4,397 -> 3,804 |
| 3.10.18 | EventFlow buffer-16 | 31,361 | 27,042 | 159,409 | 184,881 | 7,361 -> 6,515 |

For unbuffered streams, p50 fell by 30.8% and throughput rose by 44.5% on
Python 3.14.8; on Python 3.10.18, p50 fell by 28.4% and throughput rose by
39.6%. Peak traced allocations fell by 15.7% and 13.5%, respectively. EventFlow
still measured slower than a native generator while maintaining single-consumer
ownership, closure and error-propagation behavior. Buffering is slower for this
CPU-only source; its purpose is bounded prefetch and producer/consumer
decoupling.

The timed batch path measured about 3.18 ms per 5,000 items on Python 3.14.8
and 6.19 ms on Python 3.10.18, versus about 0.79 ms and 0.69 ms respectively
for native size-only batching. The extra coordination serves its deadline and
bounded producer/consumer handoff; this change did not alter that path. These
are local sequential measurements, not production guarantees.

## AsyncStream root read (2026-11-07)

Compared baseline `cbdf55b` with optimized commit `caacb2f` on Linux x86_64,
using CPython 3.10.18 and 3.14.9. Each case consumed 5,000 items over 40
iterations. The optimized path reads the root iterator directly for fused
`map()`/`filter()` chains, skipping one nested `AsyncStream.__anext__()` call
per item. Runs used separate sequential processes; the table reports whole
stream latency and `tracemalloc` peak, not retained memory or process RSS.

| Python | Case | Before p50 (us) | After p50 (us) | Before items/s | After items/s | Peak traced bytes |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 3.10.18 | unbuffered | 19,140.583 | 16,287.464 | 261,207 | 306,930 | 3,804 -> 4,540 |
| 3.10.18 | buffer-16 | 40,366.206 | 37,810.560 | 123,862 | 132,181 | 7,204 -> 7,657 |
| 3.14.9 | unbuffered | 8,794.800 | 8,050.361 | 568,470 | 620,730 | 6,016 -> 7,264 |
| 3.14.9 | buffer-16 | 18,054.025 | 17,685.558 | 276,936 | 282,485 | 12,149 -> 13,405 |

Unbuffered p50 fell by 14.9% on Python 3.10.18 and 8.5% on Python 3.14.9;
buffer-16 p50 fell by 6.3% and 2.0%. The latency improvement comes with a
small increase in peak traced allocations: 736/1,248 bytes for unbuffered
reads and 453/1,256 bytes for buffer-16. This trade-off is specific to this
CPU-only synthetic source and these interpreters. Raw reports, including
p95/p99 and all benchmark cases, are in
[`stream_root_read.json`](stream_root_read.json).

## EventFilter boolean fast path (2026-11-09)

The `history/query-sparse-filter` case scans a 100,000-event history from newest
to oldest for the oldest matching event (`event == 0`) with a replay limit of
one. On CPython 3.14.9/Linux x86_64, pinned to logical CPU 4, the pyperf 2.11.0
mean fell from 42.5 ms (0.4 ms standard deviation) to 9.48 ms (0.16 ms), a
4.48x speedup. `EventFilter.matches()` now returns exact `bool` predicate
results without calling `inspect.isawaitable`; other return types retain the
existing awaitable validation and error behavior. The query remains an O(n)
scan and no history index was added.

Each run used 10 worker processes, 5 values per process, 2 warmups and a 0.2 s
minimum sample duration. pyperf warned that the samples did not establish its
95% certainty target for less than 1% variation. Treat this as strong,
workload-specific local evidence, not a stable cross-machine timing guarantee.
The raw before/after JSON records include revision, source fingerprint, GIL
state, affinity and individual values:
[`history_sparse_filter_before_2026-11-09.json`](history_sparse_filter_before_2026-11-09.json),
[`history_sparse_filter_after_2026-11-09.json`](history_sparse_filter_after_2026-11-09.json).

The baseline is clean revision `2f13c2e`; the after record carries the same
revision plus a dirty-source fingerprint for the measured working tree. The
benchmark exercises the history/filter path only. A native extension was not
introduced: each scanned event still invokes the caller's Python predicate,
so moving the loop to C would retain that callback boundary while adding a
second backend and packaging cost.

## AsyncStream primitive transform fast path (2026-11-09)

The benchmark consumes 5,000 integer items through the same synchronous map and
boolean filter, with and without a 16-item buffer. On CPython 3.14.9/Linux
x86_64, pinned to logical CPU 4, pyperf 2.11.0 measured these whole-stream
means:

| Case | Before | After | Change |
| --- | ---: | ---: | ---: |
| unbuffered | 9.89 ms (0.20 ms SD) | 6.09 ms (0.20 ms SD) | 1.63x faster |
| buffer-16 | 19.9 ms (0.3 ms SD) | 16.0 ms (0.4 ms SD) | 1.25x faster |
| native map/filter control | 2.01 ms (0.02 ms SD) | 1.97 ms (0.03 ms SD) | 1.02x faster |

Runs used 10 worker processes, 5 values per process, 2 warmups, a 0.2 s
minimum sample duration and 5,000 items. `AsyncStream` now skips
`inspect.isawaitable()` only for exact `int` and `bool` transformation results;
subclasses and all other objects still use the previous awaitable check. The
existing async map/filter path is unchanged. The native generator control
remained within 2%, while both EventFlow cases improved. Buffering still costs
more for this CPU-only workload; its purpose is bounded prefetch and
producer/consumer decoupling.

pyperf warned that the sample series did not establish its 95% certainty
target for less than 1% variation. Treat these as workload-specific local
measurements rather than stable timing guarantees. The JSON records retain raw
samples and source fingerprints:
[`stream_transform_before_2026-11-09.json`](stream_transform_before_2026-11-09.json),
[`stream_transform_after_2026-11-09.json`](stream_transform_after_2026-11-09.json).

The before snapshot already contains the EventFilter boolean fast path; the
after snapshot adds the stream fast path. Both raw files record the same base
revision and distinct source hashes, which make the changed runtime state
auditable. The measured stream scenarios do not call EventFilter.

## Runtime load and resource checks

`runtime_load.json`, `callback_errors.json` and `pipeline_diagnostics.json` were
measured on Python 3.10.18/Linux on 2026-11-02. Runtime contention rates use the
median of five batches, with 200 publishes per publisher in each batch. The
memory soak sends 100,000 distinct 4 KiB payloads per case and compares traced
memory after the first half with memory at completion. Schedule registration
uses a 3,600-second interval so the measurement observes registered threads or
tasks without including periodic event delivery. Callback and middleware files
contain 500 and 1,000 timed publish/run operations per case, respectively.

| Scenario | Workload | Measured result |
| --- | --- | ---: |
| Sync contention | 1 publisher | 65,052 publishes/s |
| Sync contention | 4 publishers | 64,650 publishes/s |
| Sync contention | 16 publishers | 58,032 publishes/s |
| Async contention | 4 publishers, 4 subscribers, max concurrency 1 | 18,390 publishes/s |
| Async contention | 4 publishers, 4 subscribers, max concurrency 4 | 26,783 publishes/s |
| Async contention | 4 publishers, 4 subscribers, unlimited concurrency | 26,854 publishes/s |
| Memory soak, no history | Sync and async, 100,000 events with one handler | -48 B late traced growth; 9,123 B peak |
| Memory soak, capacity 128 | Sync and async, 100,000 events with one handler | -20 B late traced growth; about 526 KiB peak |
| Periodic registration | 50 sync schedules | 3.117 ms median; 50 worker threads; 0 remained after close |
| Periodic registration | 50 async schedules | 0.323 ms median; 50 tasks; 0 remained after cancellation |

`callback_errors.json` measures 0%, 10% and 100% failing callbacks in sync,
async sequential and async concurrent modes. For example, sync p50 was 17.350,
17.851 and 19.862 us at those error rates. `pipeline_diagnostics.json` now
measures 1, 5 and 20 pass-through middleware layers: sync p50 was 2.929 to
23.280 us, and async p50 was 4.449 to 47.653 us. Per-case percentiles,
throughput and traced peaks are retained in the JSON files.

These measurements show bounded memory settling after the history reaches its
configured capacity and schedule cleanup returning tracked resources to zero
for these workloads. Error-rate p95 can vary substantially between runs; use
the raw distributions and repeat the benchmark on the target environment before
making capacity decisions. The soak is a 100,000-event local run, not a
production-scale or long-duration guarantee; traced allocations exclude native
allocations and process RSS.

Replay uses the unchanged benchmark; see `replay_before.txt` and `replay_after.txt`. JSON files retain complete distributions and environment details.

## Keyed concurrency waiter contention

`keyed_limiter_contention.json` compares the shared-future broadcast path at
`cf89090` with the FIFO waiter queue. Each run holds one permit, queues 100,
1,000 or 5,000 same-key waiters, and yields once inside every permit holder so
remaining waiters must requeue as work progresses. Timing starts after all
waiters are queued. On Python 3.10.18/Linux, the p50 was 26.0 ms versus 1.53 ms
at 100 waiters (three samples each), 2.60 s versus 14.2 ms at 1,000 (two
baseline samples and three FIFO samples), and 77.26 s versus 76.0 ms at 5,000
(one baseline sample and three FIFO samples). Python 3.14.8/Linux measured
8.15 ms and 48.0 ms at 1,000 and 5,000 waiters. These local measurements
isolate a high-contention case and are not production guarantees.

Run the same-key contention workload with the current pyperf harness:

```bash
python -m benchmarks.benchmark_keyed_limiter --fast --waiters 100 1000 5000 --output keyed_limiter.json
```

## v1.3.1 performance measurements (2026-11-04)

Paired local comparisons used Linux 7.2.11 x86_64 with CPython 3.10.22 and
3.14.9. Each baseline and optimized source tree was loaded from its recorded
Git revision in one process, then measured in alternating order. The table
shows median per-operation latency across 13 timed batches after warm-up (up to
1,000 calls per implementation). Sync cases used 5,000 operations per batch,
async cases 800, and thread-handoff cases 500. Publish used one successful
subscriber with no error callback; pipelines had five trivial stages with
observers disabled; retries made one successful attempt without observers; the
cache case hit one non-expiring entry; policy cases used either no rules or four
abstaining rules. The portal returned an immediate coroutine and the work pool
ran a completed no-op. Lower latency is better, so negative percentages mean
latency fell. Percentages apply to these synthetic workloads on this machine;
they are not production guarantees.

| Workload | Baseline → optimized | Python 3.10.22, µs/op | Python 3.14.9, µs/op |
| --- | --- | ---: | ---: |
| Sync publish, one subscriber | `2b21a5a` → `3fa09f5` | 3.934 → 3.696 (-6.0%) | 2.420 → 2.340 (-3.3%) |
| Sequential async publish, one subscriber | `2b21a5a` → `3fa09f5` | 3.462 → 3.274 (-5.4%) | 2.051 → 1.846 (-10.0%) |
| Sync pipeline, five stages | `3fa09f5` → `84831bb` | 3.912 → 3.472 (-11.3%) | 1.923 → 1.823 (-5.2%) |
| Async pipeline, five stages | `3fa09f5` → `84831bb` | 5.085 → 4.695 (-7.7%) | 2.838 → 2.629 (-7.4%) |
| Sync retry, one attempt, no observer | `84831bb` → `37515e2` | 1.259 → 0.685 (-45.6%) | 0.878 → 0.418 (-52.4%) |
| Async retry, one attempt, no observer | `84831bb` → `37515e2` | 1.030 → 0.494 (-52.0%) | 0.723 → 0.288 (-60.2%) |
| Non-expiring cache hit | `37515e2` → `984ef59` | 0.547 → 0.414 (-24.3%) | 0.312 → 0.259 (-16.9%) |
| Policy evaluation, no rules | `984ef59` → `c9fea34` | 0.882 → 0.751 (-14.8%) | 0.827 → 0.687 (-17.0%) |
| Policy evaluation, four abstaining rules | `984ef59` → `c9fea34` | 1.657 → 1.458 (-12.0%) | 1.347 → 1.186 (-12.0%) |
| `LoopPortal.call()`, immediate coroutine | `c9fea34` → `fe05ac5` | 81.873 → 77.615 (-5.2%) | 69.568 → 64.722 (-7.0%) |
| `BoundedWorkPool.submit()`, completed no-op | `fe05ac5` → `b2db9ef` | 51.365 → 55.620 (+8.3%) | 46.523 → 45.193 (-2.9%) |

The `BoundedWorkPool` timing varied in direction between interpreters, so no
latency improvement is claimed for it; the code change removes one temporary
one-item list per accepted submission. The bus, pipeline, retry, cache, policy
and `LoopPortal` measurements showed lower medians on both tested interpreters.

## CooldownRegistry existing-key update (2026-11-07)

Compared baseline commit `f4b318e` with optimized commit `a72ae0f` on Linux
7.2.14-arch1-1 x86_64. Each registry was filled with active 3,600-second
deadlines, then repeatedly called `block(0, 3600)` with no waiters. Each run
used 200 warm-up calls followed by nine timed batches; the median latency per
call is shown below. Batch sizes vary to keep runtimes practical at larger
registries. The two interpreter runs were separate executions on the same
machine, not interleaved. These synthetic results describe the repeated-key
path only and are not production guarantees.

| Entries | Calls per batch | Python 3.10.18, µs/call | Python 3.14.9, µs/call |
| ---: | ---: | ---: | ---: |
| 10 | 5,000 | 3.335 → 2.447 (-26.6%) | 1.767 → 1.211 (-31.5%) |
| 100 | 3,000 | 5.923 → 2.380 (-59.8%) | 3.741 → 1.200 (-67.9%) |
| 1,000 | 1,000 | 29.717 → 2.353 (-92.1%) | 25.510 → 1.187 (-95.3%) |
| 4,096 | 500 | 111.236 → 2.391 (-97.9%) | 100.599 → 1.190 (-98.8%) |

Normal updates to a live key now inspect only that key. A full expiry prune
still occurs when a new key needs space in a full registry; explicit `prune()`
and `stats()` also retain their whole-registry scan behavior.

## KeyedCoalescer due-item drain (2026-11-07)

Compared baseline commit `1ace5eb` with optimized commit `3edae1f` on Linux
7.2.14-arch1-1 x86_64. Each sample filled an internal pending map with expired
entries, then timed `_next_due()` until all entries had been selected. Setup,
heap construction, worker-thread startup, and callback execution were excluded.
The run used seven samples at 128 and 1,024 keys, five at 4,096 keys, and one
at 16,384 keys; the table reports the median total drain time. Each interpreter
was run separately on the same machine. This isolates due-item selection cost
and is not an end-to-end or production guarantee.

| Pending keys | Python 3.10.18, ms/drain | Python 3.14.9, ms/drain |
| ---: | ---: | ---: |
| 128 | 1.121 → 0.143 (-87.2%) | 0.619 → 0.080 (-87.1%) |
| 1,024 | 62.191 → 1.197 (-98.1%) | 36.538 → 0.837 (-97.7%) |
| 4,096 | 986.010 → 5.215 (-99.5%) | 556.092 → 3.636 (-99.3%) |
| 16,384 | 15,640.381 → 22.651 (-99.9%) | 8,964.437 → 15.833 (-99.8%) |

The previous minimum scan gave quadratic total work for a full drain. The
deadline heap makes selection logarithmic per item; flush retains insertion
order and compacts stale heap entries while draining. Keys are never compared,
including when deadlines tie.

## Reproduce

```bash
python -m benchmarks.benchmark_publish --fast --output publish.json
python -m benchmarks.benchmark_bridge_publishers --fast --publishers 2 4 8 16 --output bridge-publishers.json
python -m benchmarks.benchmark_pipeline --fast --output pipeline.json
python -m benchmarks.benchmark_stream --fast --items 1000 --output stream.json
python -m benchmarks.benchmark_runtime --fast --soak-operations 1000 --output runtime.json
python -m benchmarks.benchmark_replay --fast --events 100000 --replay 25 --output replay.json
python -m pyperf compare_to baseline.json candidate.json --table
```

## Whole-process peak RSS

These RSS values were collected with the retired custom benchmark runner in a
fresh parent process. They cover the entire interpreter process and cannot be
attributed only to a buffer or pipeline. Publish used 500 operations in both
source trees; pipeline used 1,000 operations; stream used 20 consumptions of
1,000 items. They remain historical observations, not current pyperf results.

| Process | Peak RSS KiB |
| --- | ---: |
| baseline_publish | 22208 |
| extended_publish | 21884 |
| pipeline | 22396 |
| stream | 21852 |

Traced allocation peaks and process RSS are different measurements. Reproduce each run from the corresponding checkout in a fresh parent process:

For new memory measurements, run a pyperf workload with `--tracemalloc` or
`--track-memory`. These capture different quantities and should not be mixed
with the historical process-RSS figures above.

## AsyncEventQueue waiter handoff (2026-11-07)

Compared baseline `a6fc5ca` with the same-thread waiter wake optimization on
Linux x86_64. Both revisions used the corrected benchmark harness: one
persistent async consumer was already waiting in `receive()` before each
synchronous publish, and it rearmed before the next operation. The cross-thread
case used one persistent publisher thread. Each process ran 1,000 timed
operations per case; seven independent processes alternated baseline and
optimized order for each Python version. The table reports the median of the
seven per-process percentile values. Full per-run data and the exact workload
are in [`bridge_waiter_handoff_2026-11-07.json`](bridge_waiter_handoff_2026-11-07.json).

Keep existing measurement records unchanged. Future benchmark output records
both the base revision and a fingerprint of the exact source tree used. For
before/after runs, use `baseline_source` and `source` in the comparison JSON to
identify each tree; the source-only dirty flag does not count result files or
documentation edits.

| Python | Case | Before P50 (us) | After P50 (us) | Change | Before P95 (us) | After P95 (us) |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 3.10.18 | same-thread waiting consumer | 31.796 | 21.892 | -31.1% | 33.845 | 22.635 |
| 3.10.18 | cross-thread waiting consumer | 72.484 | 72.785 | +0.4% | 101.255 | 178.838 |
| 3.14.9 | same-thread waiting consumer | 18.772 | 11.058 | -41.1% | 20.893 | 11.478 |
| 3.14.9 | cross-thread waiting consumer | 49.270 | 48.589 | -1.4% | 124.122 | 123.667 |

The same-thread path avoids the thread-safe loop wakeup and completes its
waiter on the owning event-loop thread. Cross-thread publishers retain
`call_soon_threadsafe()` for safety; these measurements show no material P50
change there. Tail percentiles vary between runs, especially in the threaded
case. These are local round-trip measurements that include consumer scheduling
and benchmark acknowledgements, not guarantees for a deployed workload.

## CPython 3.15.2 free-threading comparison (2026-11-07)

Compared regular CPython 3.15.2 with CPython 3.15.2t on the same Linux x86_64
host and benchmark harness. The runtime mode was checked with
`sys._is_gil_enabled()` before each process. Seven 1,000-operation processes
per mode alternated order. The table reports the median of per-process latency
percentiles; full samples are included in the JSON record above.

| Bridge case | GIL enabled P50 (us) | GIL disabled P50 (us) | Change | GIL enabled P95 (us) | GIL disabled P95 (us) |
| --- | ---: | ---: | ---: | ---: | ---: |
| same-thread waiting consumer | 12.402 | 13.848 | +11.7% | 12.862 | 14.315 |
| cross-thread waiting consumer | 50.457 | 41.083 | -18.6% | 83.958 | 49.508 |

This workload uses one synchronous publisher thread and one async consumer; it
does not measure CPU-bound scaling across many publisher threads. The free-
threaded build was slower on the same-thread case and faster on the cross-thread
round-trip in these local samples. The measurements do not establish a general
speedup for EventFlow or Python code, and traced allocation peaks are not RSS.

## Multi-publisher bridge handoff (2026-11-07)

Measured CPython 3.14.9 with the GIL enabled and CPython 3.14.3t with the GIL
disabled on the same Linux x86_64 host. Each count used 500 sends per publisher
and five samples. A publisher waited for an async consumer acknowledgement
before sending its next event. Throughput is the median across samples;
round-trip P50/P95/P99 and process CPU use are the medians of the corresponding
per-sample values. CPU use is normalized to one core and can exceed 100% when
threads execute in parallel. Each run delivered all 75,000 measured events.

| Python | Publishers | Throughput (ops/s) | CPU (% of one core) | P50 (us) | P95 (us) | P99 (us) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 3.14.9, GIL on | 2 | 19,762 | 112.4 | 79.6 | 205.2 | 289.4 |
| 3.14.9, GIL on | 4 | 13,580 | 110.3 | 275.4 | 466.8 | 627.0 |
| 3.14.9, GIL on | 8 | 14,169 | 109.9 | 518.3 | 946.9 | 1,302.7 |
| 3.14.9, GIL on | 16 | 12,677 | 105.3 | 1,128.9 | 2,156.5 | 2,936.9 |
| 3.14.3t, GIL off | 2 | 38,452 | 217.8 | 31.3 | 155.7 | 223.9 |
| 3.14.3t, GIL off | 4 | 67,781 | 359.8 | 41.0 | 125.1 | 237.1 |
| 3.14.3t, GIL off | 8 | 63,834 | 477.2 | 109.6 | 200.6 | 287.9 |
| 3.14.3t, GIL off | 16 | 64,266 | 525.2 | 232.4 | 353.4 | 442.4 |

In these samples, GIL-disabled throughput rose from two to four publishers and
then largely plateaued; the GIL-enabled run did not improve with additional
publishers, and its tail latency grew. This is useful evidence for the target
handoff path, not a general interpreter or deployment comparison: the runs are
short local measurements and were not isolated from host scheduling noise.
Both JSON files identify commit `a7fd6c377a9e3707406651866c3fbf12e65dd034`,
record a clean source state, and share source fingerprint
`ff604a13c3d82eb9f12d5b38657dcde257ad4da63202b3575834e6c8c98774ee`:
[`GIL enabled`](bridge_publishers_cpython_3.14.9_gil_on_2026-11-07.json) ·
[`GIL disabled`](bridge_publishers_cpython_3.14.3t_gil_off_2026-11-07.json).

## Pyperf bridge baseline: CPython 3.15.2 (2026-11-08)

Captured the current bridge workloads on CPython 3.15.2 with the GIL enabled
and CPython 3.15.2t with the GIL disabled. Both builds ran on the same Linux
x86_64 host (Intel Core i7-12700H, kernel 7.2.14, glibc 2.44) with pyperf
2.11.0. Pyperf used 20 worker processes, five values per process, two warmups,
and a 0.2-second minimum value duration. `PYTHON_GIL=0` was set for the
free-threaded workers and the recorded GIL metadata confirms both modes.

The round-trip workload was pinned to CPU 4, one P-core, to measure handoff
latency without cross-core migration. The publisher-scaling workload was pinned
to CPUs 0, 2, 4, 6, 8, and 10, one logical thread from each P-core, leaving
room for concurrent publisher threads. No system-wide performance tuning was
applied. All four records use the same clean source revision
`68889c517f1090fb69a58ebeb38feaccd6dd5b6f` and source fingerprint
`d458ff18c45eb516a406daa77233898ad68b402f74392cd6c060238b2e54cb4a`.

The files contain pyperf's raw values and environment metadata:

- [Round-trip, GIL enabled](bridge_roundtrip_cpython_3.15.2_gil_on_2026-11-08.json)
- [Round-trip, GIL disabled](bridge_roundtrip_cpython_3.15.2t_gil_off_2026-11-08.json)
- [Publisher scaling, GIL enabled](bridge_publishers_cpython_3.15.2_gil_on_2026-11-08.json)
- [Publisher scaling, GIL disabled](bridge_publishers_cpython_3.15.2t_gil_off_2026-11-08.json)

These pyperf measurements use explicit CPU affinity and should not be compared
directly with the earlier custom-runner observations above; the earlier runs
used different sampling and scheduling conditions.

### Waiting-consumer round-trip

Values below are pyperf comparison times per event; lower is faster. The
same-thread case publishes on the event-loop thread. The cross-thread case
publishes from a persistent worker thread and waits for the async consumer's
acknowledgement.

| Case | GIL enabled | GIL disabled | Comparison |
| --- | ---: | ---: | --- |
| Same-thread waiting consumer | 12.1 us | 13.7 us | 1.13x slower without the GIL |
| Cross-thread waiting consumer | 50.8 us | 67.5 us | 1.33x slower without the GIL |

The raw files also retain direct-await and `LoopPortal` cases; the table focuses
on the queue bridge, and `pyperf compare_to` found no significant difference
for the `LoopPortal` case.

### Concurrent publishers

The publisher benchmark reports wall-clock and process CPU time per delivered
event after queue and thread setup. CPU time can exceed wall time when several
threads execute in parallel.

| Publishers | Wall, GIL on | Wall, GIL off | Wall comparison | CPU, GIL on | CPU, GIL off |
| ---: | ---: | ---: | --- | ---: | ---: |
| 1 | 36.5 us | 35.0 us | 1.04x faster | 39.3 us | 53.4 us |
| 2 | 29.7 us | 17.3 us | 1.72x faster | 33.7 us | 43.0 us |
| 4 | 32.8 us | 9.26 us | 3.54x faster | 36.4 us | 40.1 us |
| 8 | 32.3 us | 10.7 us | 3.01x faster | 36.1 us | 49.8 us |
| 16 | 32.6 us | 11.8 us | 2.76x faster | 36.0 us | 61.3 us |

On this host, free-threaded CPython reduced wall time once multiple publishers
were active, while increasing process CPU time per event. Single-event
round-trip latency was slower in the free-threaded build under the isolated
single-core setup. Pyperf marked several publisher samples as unstable at its
1% variation target; the 4% single-publisher wall-time difference is therefore
not actionable. Treat the larger multi-publisher differences as a workload-
specific signal to reproduce on another machine before making implementation
decisions. These numbers do not establish a general speedup for EventFlow or
free-threaded Python.

Reproduce the publisher workload with the same sampling settings and choose
the interpreter explicitly for each build:

```bash
python -m benchmarks.benchmark_bridge_publishers \
  --publishers 1 2 4 8 16 \
  --processes 20 --values 5 --warmups 2 --min-time 0.2 \
  --affinity 0,2,4,6,8,10 --output gil-on.json

PYTHON_GIL=0 python -m benchmarks.benchmark_bridge_publishers \
  --publishers 1 2 4 8 16 \
  --processes 20 --values 5 --warmups 2 --min-time 0.2 \
  --affinity 0,2,4,6,8,10 --output gil-off.json

python -m pyperf compare_to gil-on.json gil-off.json --table
```
