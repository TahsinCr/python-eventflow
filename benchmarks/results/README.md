# Local architecture-extension measurements

Measured on 2026-10-28, starting from source commit `14fe966`. No remote service is involved.

## Method

- Python: 3.10.18 (main, Aug 13 2026, 01:16:48) [GCC 16.1.1 20260728]
- Platform: Linux-7.2.11-arch1-1-x86_64-with-glibc2.44
- Publish: 2,000 timed operations per case after warm-up; original bus implementations are unchanged.
- Pipeline: 5,000 operations; configuration and decorator registration happen before timing.
- Stream: 100 complete consumptions of 1,000 items; reported latency is per whole stream.
- p50/p95/p99 are sampled operation latencies. Throughput uses the median operation latency.
- Peak traced allocations are measured in a separate pass (up to 100 operations); they are not RSS.
- These are local microbenchmarks, with scheduler/clock noise; no strict timing gate is imposed.

## Existing publish path

| Case | Before p50 (us) | After p50 (us) | Change |
| --- | ---: | ---: | ---: |
| sync/0 | 2.788 | 2.828 | +1.4% |
| sync/1 | 4.366 | 4.415 | +1.1% |
| sync/10 | 16.765 | 16.941 | +1.0% |
| sync/100 | 135.175 | 146.524 | +8.4% |
| sync/filter-type | 17.186 | 17.092 | -0.5% |
| sync/filter-predicate | 22.266 | 22.291 | +0.1% |
| async-sequential/0 | 3.140 | 3.107 | -1.1% |
| async-sequential/1 | 3.989 | 3.890 | -2.5% |
| async-sequential/10 | 10.209 | 10.044 | -1.6% |
| async-sequential/100 | 67.970 | 70.810 | +4.2% |
| async-concurrent/0 | 4.329 | 4.166 | -3.8% |
| async-concurrent/1 | 17.235 | 16.796 | -2.5% |
| async-concurrent/10 | 65.313 | 63.283 | -3.1% |
| async-concurrent/100 | 511.476 | 510.737 | -0.1% |

No case exceeded a 10-15% median regression in this before/after run. The largest increase was +8.4% for sync/100; the underlying bus dispatch files were not modified. This is not a statistical production guarantee.

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

Decorator and imperative registration produce the same runtime plan. Metrics allocate observations and update aggregates only when enabled.

## Streams

| Case | p50 per 1,000 items (us) | Items/s | Peak traced bytes |
| --- | ---: | ---: | ---: |
| direct-generator | 77.424 | 12915141 | 1352 |
| unbuffered | 4074.789 | 245397 | 4449 |
| buffer-16 | 7289.113 | 137186 | 6588 |

The raw generator is a lower-bound reference with no transformation or ownership machinery. The unbuffered stream includes map/filter and owned closure; the buffered case additionally runs a producer, bounded queue and backpressure. Buffering is a resource/decoupling choice, not a speedup for a trivial CPU-only source.

Replay uses the unchanged replay benchmark: see `replay_before.txt` and `replay_after.txt`. Full JSON files retain each case and environment.

## Reproduce

```bash
python -m benchmarks.benchmark_publish --iterations 2000 --output publish.json
python -m benchmarks.benchmark_pipeline --iterations 5000 --output pipeline.json
python -m benchmarks.benchmark_stream --iterations 100 --items 1000 --output stream.json
python -m benchmarks.benchmark_replay --events 100000 --replay 25 --iterations 100 --samples 7
```
