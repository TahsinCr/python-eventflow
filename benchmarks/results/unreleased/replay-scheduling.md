# Publish lock acquisition and replay scheduling

The measurements use source revision `aa89fdb`, the native C backend on
CPython 3.15.2, and the same Linux/i7-12700H host and CPU affinity (4, 6, 8)
for both GIL settings. Each profile ran for 24 rounds.

```sh
taskset -c 4,6,8 python -m benchmarks.diagnose_replay_scheduling --output /tmp/gil-on.json
PYTHON_GIL=0 taskset -c 4,6,8 python3.15t -m benchmarks.diagnose_replay_scheduling --output /tmp/gil-off.json
```

Two publisher threads and zero to eight replay subscribers start each round
together. Each subscriber requests ten matching events. A history append
before the round invalidates the previous snapshot. Scheduling determines
whether later subscribers reuse a snapshot or rebuild it after a publish.

The instrumented run supplies a timer around the underlying RLock at bus
construction. It records the first actual acquisition made by each
`publish()`, including the timer, lock entry and scheduling cost.
Uninstrumented runs use the normal lock and supply end-to-end timings for
comparison. The two runs use separate thread schedules. Differences in
contended samples cannot be attributed solely to instrumentation.

## Synchronous replay with one million retained events

| GIL | Concurrent replay subscribers | Publish p95 without probe (ms) | Publish p95 with probe (ms) | First lock acquisition p95 (ms) |
| --- | ---: | ---: | ---: | ---: |
| on | 1 | 5.03 | 4.51 | 4.48 |
| on | 2 | 16.32 | 12.86 | 9.77 |
| on | 4 | 12.94 | 14.34 | 10.94 |
| on | 8 | 13.28 | 12.93 | 9.47 |
| off | 1 | 71.80 | 86.08 | 86.04 |
| off | 2 | 89.88 | 86.67 | 76.33 |
| off | 4 | 149.77 | 153.29 | 150.64 |
| off | 8 | 133.85 | 161.03 | 149.66 |

With zero replay subscribers at the same capacity, instrumentation changed
the median publish time from 0.55 to 2.09 µs with the GIL enabled and from
1.42 to 3.26 µs with it disabled. The measured lock time is an invocation
interval and includes scheduling around acquisition. It does not isolate
CPU time spent inside the lock implementation.

## Async replay and event loop delay

A callback is queued immediately before subscribing. The interval ends when
the event loop runs that callback. Warm rounds prebuild the snapshot; cold
rounds invalidate it with a publication. Callback work and replay depth
are held constant.

| GIL | Capacity | Warm delay p95 (ms) | Cold delay p95 (ms) |
| --- | ---: | ---: | ---: |
| on | 1,000 | 0.037 | 0.028 |
| on | 100,000 | 0.026 | 0.806 |
| on | 1,000,000 | 0.028 | 5.506 |
| off | 1,000 | 0.058 | 0.101 |
| off | 100,000 | 0.040 | 4.456 |
| off | 1,000,000 | 0.040 | 60.869 |

These samples show that the first publication lock acquisition can account
for a large part of the tail during concurrent replay and that a cold replay
can delay an already queued async callback. The p95 estimates use 48 publisher
observations or 24 loop observations per case. Real traffic, long-running
callbacks and process RSS were not measured. Keep the current architecture
until a representative workload can validate a change.

Raw profiles and source metadata are available for
[GIL enabled](replay-scheduling-gil-on.json.gz) and
[GIL disabled](replay-scheduling-gil-off.json.gz).
