# Filtered replay, history reads and cooldown wakeups

These measurements were collected for the 17 November 2026 audit on one Linux
x86-64 host with an Intel Core i7-12700H. The replay comparison used CPython
3.15.2 with the GIL enabled, the native extension loaded and pyperf 2.11.0.
Both sides used CPU 15, three worker processes, four values per worker, two
warmups and a minimum value duration of 25 ms.

## Replay and history

Each replay starts from 100,000 retained events, requests one event and includes
subscription and cancellation. Repeated subscriptions use an already captured
history snapshot. Both sides ran the same benchmark module. The base checkout
at `345ce16` has the new benchmark file copied into it, which accounts for
its dirty source flag. The fixed source is `a2ca48e`. Full revisions and source
fingerprints are in the datasets.

| Operation | Base mean | Fixed mean |
| --- | ---: | ---: |
| Sync replay, every event matches | 17.58 ms | 21.91 µs |
| Sync replay, only newest matches | 14.11 ms | 21.83 µs |
| Sync replay, only oldest matches | 13.36 ms | 13.68 ms |
| Sync replay, no match | 13.67 ms | 13.71 ms |
| Async replay, every event matches | 17.51 ms | 14.19 µs |
| Async replay, only newest matches | 14.14 ms | 14.39 µs |
| Async replay, only oldest matches | 13.40 ms | 13.74 ms |
| Async replay, no match | 13.42 ms | 13.73 ms |
| Warm history query for the last exact type match | 0.945 µs | 0.899 µs |
| Warm history query for all exact type matches | 0.794 µs | 7.20 ms |

Replay gains come from stopping when the window is full. Oldest-only and
no-match cases still scan the entire snapshot. A separate cold replay
diagnostic appended an event before timing each subscription to invalidate
the snapshot. The median fell from 10.91 ms to 0.76 ms across 60 samples per
side. A cold read still copies snapshot references to preserve a consistent
view while matching runs outside the publication lock.

An unlimited type query now reevaluates membership on each read. The base
result reused a precomputed tuple and could return obsolete matches. The
substantial cost of repeated full type queries is an explicit tradeoff. The
exact tail fast path keeps the narrow type query in the same range.

The memory diagnostic warmed the unfiltered snapshot, then queried 60,000
entries against 64 types in one inheritance chain. Extra retained Python
allocations after collection fell from 29.30 MiB to 0–32 bytes above the
snapshot baseline. The fixed case still allocated up to about 0.94 MiB
transiently during one result. These are `tracemalloc` readings from the
test process.

## Cooldown and async dispatch

Ten batches per side started 24 sync waiters on distinct cooldown keys.
Each batch extended one key 400 times with 1 ms between updates. Using CPUs
10–11, the median process CPU time fell from 132.54 ms to 12.03 ms. Median
voluntary context switches fell from 22,785 to 803. This controlled test
isolates notification fan-out under sustained multi-key contention.
The cold, memory and cooldown diagnostics compare `345ce16` and `b96bfcc`
in alternating process order. Source after `b96bfcc` does not change those
Python selection and wait paths.

Async dispatch with zero type matches took 12.8 µs at 64 subscribers,
85.3 µs at 512, and 336 µs at 2,048. The profile uses the sequential
async bus with integer events and subscriptions filtering for strings.
These measurements establish the scan cost for this workload. The async
dispatch architecture is unchanged.

## Data

- [Base replay pyperf samples](replay-history-base.json.gz)
- [Fixed replay pyperf samples](replay-history-fixed.json.gz)
- [Async type miss pyperf samples](async-type-misses.json.gz)
- [Cold replay, retained allocations and cooldown samples](replay-history-diagnostics.json)

```sh
python -m benchmarks.benchmark_replay --events 100000 --replay 1 --affinity 15 \
    --processes 3 --values 4 --warmups 2 --min-time 0.025 -o replay.json
python -m pyperf compare_to \
    benchmarks/results/v1.4.2/replay-history-base.json.gz \
    benchmarks/results/v1.4.2/replay-history-fixed.json.gz --table
```

The pyperf settings are intended for a focused comparison. Small differences
are sensitive to run conditions. The results do not characterize production
traffic, cross-platform behavior or free-threaded performance.
