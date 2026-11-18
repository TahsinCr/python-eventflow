# Async subscriber churn, replay responsiveness and fan-out

This diagnostic compares revisions `25344fa` and `80c1ef4` on an Intel Core
i7-12700H, Linux x86-64, CPython 3.15.2 pinned to CPU 2. The GIL-on and
GIL-off runs use separate interpreter builds. Each result describes a
controlled workload.

## Publication

Medians in microseconds per operation for 2,048 subscriptions.

| Workload | GIL on, base → current | GIL off, base → current |
| --- | ---: | ---: |
| Stable subscriptions, no type match | 1.59 → 1.49 | 2.91 → 2.74 |
| Stable subscriptions, 1 in 64 matches | 11.07 → 10.54 | 15.83 → 14.32 |
| All callbacks match, sequential | 451.37 → 396.86 | 646.99 → 554.82 |
| Add, publish without a match, cancel | 228.86 → 8.60 | 371.44 → 12.73 |

The churn case warms the candidate set and adds a nonmatching subscription
before each publication. The full scan stays available for custom matching
and dense candidate lists. This test does not imply that all subscription
changes cost the same. The broad probe had a noisy first GIL-on case with
eight nonmatching subscribers. A focused A/B/B/A rerun with 6,000 warmup
calls and 13 batches of 10,000 publications gave 2.38/2.37 µs on the base
and 2.37/2.36 µs on the candidate.

The separate fan-out probe compared each bus with a loop that calls the same
2,048 empty callbacks directly. GIL-off direct async calls took 268.86 µs on
the base and 283.43 µs on the candidate. Full async publication took
646.73 → 566.80 µs. This comparison includes matching, subscription state
and result handling. The difference is not a direct measurement of any
one internal step. The native synchronous bus measured 109.37 → 109.39 µs
in the same probe.

## Replay that needs an old match

Each trial asks for the oldest of 100,000 events using an equality predicate.
A callback queued immediately before replay measures event loop delay.
The table shows p95 across 20 trials in milliseconds on the current revision.

| Strategy | GIL on loop delay | GIL on replay | GIL off loop delay | GIL off replay |
| --- | ---: | ---: | ---: | ---: |
| Shared history | 9.577 | 9.611 | 12.436 | 12.483 |
| Shared history, yield every 512 records | 0.114 | 10.434 | 0.200 | 15.157 |
| Named channels, one target event in its own history | 0.030 | 0.026 | 0.037 | 0.031 |

Batching reduces the longest uninterrupted scan in this workload. The
predicate still runs on every inspected record, and giving other tasks
time can increase total replay latency. Named channel partitioning skips
unrelated records at the architecture level and gives each channel an
independent retention window. It fits applications whose replay is scoped
to a stable, known channel.

Run the [dispatch probe](history-dispatch-probe.py) and the
[replay and fan-out probe](async-replay-fanout-probe.py) on each checkout,
setting `PYTHONPATH` to its source and using the matching native build
and interpreter. Raw samples are available for
[GIL enabled](async-churn-replay-gil-on.json.gz) and
[GIL disabled](async-churn-replay-gil-off.json.gz).
