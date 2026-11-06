# Local architecture-extension measurements

Measured on 2026-10-28. Baseline source: `14fe966`; final runtime source: `e033f1a`. No remote service is involved.

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

Reproduce the optimized measurement with:

```bash
python -m benchmarks.benchmark_keyed_limiter --waiters 100 1000 5000 --repeats 3 --output keyed_limiter.json
```

## Unreleased micro-optimizations (2026-11-04)

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
python -m benchmarks.benchmark_publish --iterations 2000 --output publish.json
python -m benchmarks.benchmark_compare --baseline /path/to/v1.1-checkout --iterations 250 --samples 9 --output comparison.json
python -m benchmarks.benchmark_pipeline --iterations 5000 --output pipeline.json
python -m benchmarks.benchmark_stream --iterations 100 --items 1000 --output stream.json
python -m benchmarks.benchmark_runtime --iterations 200 --samples 5 --soak-operations 100000 --output runtime.json
python -m benchmarks.benchmark_replay --events 100000 --replay 25 --iterations 100 --samples 7
```

## Whole-process peak RSS

Linux `resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss` was read in a fresh parent process after each child benchmark. Values below cover the entire interpreter/benchmark process; they cannot be attributed only to a buffer or pipeline. Raw commands are in `process_peak_rss.json`. Publish uses 500 operations in both source trees; pipeline uses 1,000, stream uses 20 consumptions of 1,000 items.

| Process | Peak RSS KiB |
| --- | ---: |
| baseline_publish | 22208 |
| extended_publish | 21884 |
| pipeline | 22396 |
| stream | 21852 |

Traced allocation peaks and process RSS are different measurements. Reproduce each run from the corresponding checkout in a fresh parent process:

```python
import resource, subprocess, sys

subprocess.run(
    [sys.executable, "-m", "benchmarks.benchmark_publish", "--iterations", "500"],
    check=True,
    stdout=subprocess.DEVNULL,
)
print(resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss)  # KiB on Linux
```
