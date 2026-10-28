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

| Case | p50 per 1,000 items (us) | Items/s | Peak traced bytes |
| --- | ---: | ---: | ---: |
| direct-generator | 77.424 | 12915141 | 1352 |
| unbuffered | 4074.789 | 245397 | 4449 |
| buffer-16 | 7289.113 | 137186 | 6588 |

The raw generator has no transformation or ownership machinery. The unbuffered case includes map/filter and owned closure. Buffering additionally runs one producer, a bounded queue and backpressure; it is a resource/decoupling choice, not a speedup for a trivial CPU-only source.

Replay uses the unchanged benchmark; see `replay_before.txt` and `replay_after.txt`. JSON files retain complete distributions and environment details.

## Reproduce

```bash
python -m benchmarks.benchmark_publish --iterations 2000 --output publish.json
python -m benchmarks.benchmark_compare --baseline /path/to/v1.1-checkout --iterations 250 --samples 9 --output comparison.json
python -m benchmarks.benchmark_pipeline --iterations 5000 --output pipeline.json
python -m benchmarks.benchmark_stream --iterations 100 --items 1000 --output stream.json
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
subprocess.run([sys.executable, "-m", "benchmarks.benchmark_publish", "--iterations", "500"], check=True, stdout=subprocess.DEVNULL)
print(resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss)  # KiB on Linux
```
