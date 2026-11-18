# Route-cache admission versus FIFO eviction

**Measurement date:** 2026-11-17
**Source revision:** `535be6a114060b9799edd0bf6aa3e2c106b9cb55`
**Decision:** Keep the existing 64-key admission-only cache. The FIFO candidate was measured and rejected; production behavior did not change.

## Question and decision

The route cache currently admits a key after repeated use and retains at most 64 keys. This experiment asked whether evicting the oldest cached key whenever a full cache sees a new key improves routing when the active key set shifts. The FIFO candidate used the same 64-key bound and changed only the Python and C route-cache paths.

FIFO helps repeated late-hot keys, including with many wildcard subscribers. Its cost is clear in uniform workloads: once the cache fills, new keys displace useful entries, hit rates fall, and publish latency, tail latency, CPU use, and retained cache size can increase. The regressions also appear with multiple publishers and on free-threaded CPython. The aggregate comparison is neutral or slower, so the production policy remains unchanged.

## Method

The benchmark uses the existing `benchmark_dispatch_pressure.py` runner and the same source revision for both candidates. The baseline checkout was clean. The FIFO worktree was dirty only in `eventflow/event_bus.py` and `eventflow/_core.c`; the exact patch and hashes are retained with the result files. Both candidates built and loaded the native C core.

The 60 profiles comprise 42 GIL-enabled single-publisher cases on CPython 3.14.10, 12 GIL-enabled concurrent-publisher cases on the same interpreter, and 6 cases on GIL-disabled CPython 3.15.2t. Profiles vary route cardinality (16, 64, 65, 80, 256, 1000), wildcard subscriber count (0, 100, 1024), uniform, late-hot, shifting-hot-key and Zipf access, none/low/high subscription churn, and 1/2/4/8 publishers where selected by the matrix.

Each candidate ran in three fresh rounds. Candidate order was baseline→FIFO, FIFO→baseline, then baseline→FIFO. Each pyperf invocation used three worker processes, three values per process, one warmup and a 30 ms minimum benchmark duration. Merging the three rounds gives 27 pyperf values per candidate and case. `pyperf compare_to --min-speed 5` was used for the paired result tables; its significance labels are screening evidence, not a universal performance guarantee.

Per-call P50/P95/P99 values come from a separate, uninstrumented 10,000-sample pass per profile and round. That collector always uses one publisher, including for profiles whose pyperf workload has multiple publishers; it must not be read as concurrent tail-latency measurement. Process CPU time was collected in another pass with each profile's configured publisher count. Subscribe/cancel churn occurs before each publish batch and is excluded from timed publish-only work. Lock wait/hold time was not instrumented.

The machine was Linux x86_64 with an Intel Core i7-12700H (20 logical CPUs), GCC 16.2.1 and pyperf 2.11.0. Single-publisher work was pinned to CPU 4; concurrent cases used CPUs 4–11. The CPU governor was `powersave`, with no isolated CPUs or privileged frequency changes. Results describe this host and configuration.

## Results

| Runtime and workload group | Cases | CV ≤ 5% | pyperf ≥ 5% faster | pyperf ≥ 5% slower | Not significant at 5% | Geometric mean |
|---|---:|---:|---:|---:|---:|---:|
| CPython 3.14.10, GIL enabled, one publisher | 42 | 42 | 12 | 14 | 16 | 1.02× slower |
| CPython 3.14.10, GIL enabled, concurrent publishers | 12 | 6 | 5 | 6 | 1 | 1.00× (neutral) |
| CPython 3.15.2t, GIL disabled | 6 | 5 | 3 | 3 | 0 | 1.02× faster |

`CV ≤ 5%` uses the coefficient of variation across the merged 27 pyperf values for both candidates. The pyperf result groups and CV answer different questions; cases can have a low CV without showing a significant candidate difference, or vice versa.

Representative cases show why the aggregate does not support adopting FIFO:

| Runtime / profile | Publishers in pyperf | Baseline → FIFO mean | Raw P95 / P99 (single-publisher collector) | CPU ns/publish (separate pass) | Cache hit rate | Shallow cache bytes |
|---|---:|---:|---:|---:|---:|---:|
| GIL on, 256 routes / 1024 wildcards / late-hot | 1 | 42.3 → 37.0 µs (−12.6%) | P95 43.9 → 38.6 µs; P99 45.3 → 39.9 µs | 41,923 → 36,475 (−13.0%) | 0% → 100% | 531,928 → 531,928 |
| GIL on, 65 routes / no wildcards / uniform | 1 | 432 → 586 ns (+35.8%) | P95 502 → 673 ns; P99 536 → 964 ns | 424 → 583 (+37.5%) | 98.5% → 0.6% | 7,640 → 10,064 |
| GIL on, 256 routes / no wildcards / uniform | 1 | 471 → 570 ns (+21.0%) | P95 561 → 645 ns; P99 594 → 977 ns | 467 → 565 (+21.0%) | 25.1% → 0.6% | 7,640 → 10,064 |
| GIL on, 256 routes / 1024 wildcards / late-hot | 8 | 43.5 → 37.7 µs (−13.3%) | P95 44.5 → 38.9 µs; P99 46.1 → 40.2 µs | 49,045 → 42,594 (−13.1%) | 0% → 100% | 531,928 → 531,928 |
| GIL on, 256 routes / 1024 wildcards / uniform | 8 | 42.1 → 45.4 µs (+7.7%) | P95 43.9 → 45.1 µs; P99 45.8 → 46.8 µs | 47,973 → 49,720 (+3.6%) | 25.1% → 0.6% | 531,928 → 534,352 |
| GIL off, 256 routes / no wildcards / uniform | 8 | 1.12 → 1.70 µs (+52.8%) | P95 774 → 901 ns; P99 813 → 1,352 ns | 7,950 → 8,905 (+12.0%) | 25.1% → 0.6% | 8,664 → 11,088 |
| GIL off, 256 routes / 1024 wildcards / late-hot | 8 | 22.4 → 11.7 µs (−48.0%) | P95 63.6 → 56.1 µs; P99 65.8 → 58.2 µs | 128,705 → 67,760 (−47.4%) | 0% → 100% | 532,952 → 532,952 |

For the last two rows, pyperf and CPU measurements use eight publishers, but P95/P99 still come from the separate one-publisher collector. The table's raw tail percentiles therefore describe per-call latency without concurrent contention. The GIL-disabled matrix has only six selected cases; its large late-hot win does not outweigh the uniform and Zipf regressions or establish a broad free-threaded benefit.

At 1000 routes with no wildcards, uniform access drops the measured cache hit rate from 6.4% to 0.6%; the single-publisher GIL-on pyperf mean rises 19.1% and P99 rises from 624 ns to 942 ns. Repeating only the late-hot key instead raises the hit rate from 0% to 100% and improves the mean by 8.4%. This workload sensitivity is the central result, not a reason to choose FIFO as a general policy.

The CSV includes all 60 cases with means, CV, P50/P95/P99, cache hits/misses, CPU time, shallow cache bytes and sampled RSS deltas. RSS deltas are coarse process snapshots and are not attributed to the route cache; use the shallow cache measurement for the narrow cache comparison. Full raw collectors remain in `/tmp/eventflow-p3-final-v2` and are not committed. Selected raw samples, all merged pyperf suites and the exact candidate patch are retained below.

## Retained evidence

- [`route-cache-summary.csv`](route-cache-summary.csv) — all 60 paired cases and diagnostic fields.
- [`route-cache-selected-latency.json.gz`](route-cache-selected-latency.json.gz) — selected raw single-publisher latency samples from each A/B round.
- [`route-cache-fifo-manifest.json`](route-cache-fifo-manifest.json) — runtime, source, workload and collection provenance, including hashes of the unretained intermediate set.
- [`route-cache-fifo-candidate.patch`](route-cache-fifo-candidate.patch) — the complete temporary FIFO candidate diff; it is evidence only and was not applied to production.
- `route-cache-gil-on-single-{base,fifo}.json`, `route-cache-gil-on-threaded-{base,fifo}.json`, and `route-cache-gil-off-315t-{base,fifo}.json` — merged pyperf datasets (27 values per case).
- `route-cache-compare-*.md` — `pyperf compare_to` screening tables for each runtime group.

No production cache code changed as a result of this experiment. Re-run the suite on additional hardware and representative application traffic before generalizing these results.
