# Block history and async dispatch performance

CPython 3.15.2, Linux x86-64, Intel Core i7-12700H. The replay diagnostic
compares the default history with 256-event blocks at revision
`de504b3`, using the native C core and CPU affinity 4, 6 and 8. Each
profile used 20 rounds and two publishers. The dispatch, append and query
probe compares `2df0aad` and `de504b3` on CPU 2, reporting medians over
11 batches. The GIL settings use separate interpreter builds.

## Replay and publication

All values are p95 wall time in milliseconds. The publisher column uses
four concurrent replay subscribers and the uninstrumented publication
lock. The async columns time how long a callback queued immediately
before replay waits for its first turn.

| GIL | Capacity | Publish, deque → blocks | Cold async delay, deque → blocks | Warm async delay, deque → blocks |
| --- | ---: | ---: | ---: | ---: |
| on | 1,000 | 0.0046 → 0.0054 | 0.0470 → 0.0253 | 0.0582 → 0.0368 |
| on | 100,000 | 0.2287 → 0.0093 | 1.4567 → 0.0311 | 0.0468 → 0.0256 |
| on | 1,000,000 | 13.5571 → 0.0283 | 5.5109 → 0.0653 | 0.0277 → 0.0293 |
| off | 1,000 | 0.1460 → 0.1098 | 0.0896 → 0.0502 | 0.0586 → 0.0587 |
| off | 100,000 | 12.8033 → 0.1807 | 5.9275 → 0.0609 | 0.0386 → 0.0364 |
| off | 1,000,000 | 145.3421 → 0.8859 | 49.7978 → 0.2683 | 0.0382 → 0.0404 |

## Append, type query and async dispatch

All values are medians in microseconds per operation.

| Operation | GIL on, baseline → candidate | GIL off, baseline → candidate |
| --- | ---: | ---: |
| Append to full default history | 0.109 → 0.111 | 0.188 → 0.191 |
| Append to full history, default → blocks | 0.109 → 0.235 | 0.188 → 0.343 |
| Full type query, 100k matches | 4388.460 → 3322.095 | 4914.693 → 3854.617 |
| Full type query, 100k misses | 3072.454 → 2926.515 | 6656.971 → 6344.623 |
| Async, 8 type misses | 2.355 → 2.357 | 4.041 → 4.049 |
| Async, 2048 type misses | 202.731 → 1.623 | 316.908 → 2.870 |
| Async, 2048 subscribers, 1 in 64 matches | 210.055 → 11.179 | 327.272 → 15.588 |
| Async, 2048 unfiltered subscribers | 448.800 → 446.775 | 673.699 → 649.566 |
| Async, 2048 type misses with subscription churn | 226.298 → 223.195 | 370.831 → 368.498 |

The churn profile adds and removes one subscription around every publication.
The async gains apply to repeated built-in event types and regular type
filters. Other filters follow the existing matching path. Dense type queries
still reevaluate membership and use a temporary list before returning a
tuple. The traced peak was about 1.60 MB with either implementation in this
dense 100k workload. That measurement does not characterize all result sizes.

Block storage is most useful with large history and frequent snapshot
invalidation. Its higher append cost favors the default for publishing-heavy
workloads with little replay. Predicates that need to inspect most of the
history still take linear time. These single-host diagnostics do not
establish process RSS or production tail latency. Small percentage
differences should not be interpreted as gains or regressions without
additional repeats.

To reproduce replay, run `python -m benchmarks.diagnose_replay_scheduling
--rounds 20 --output PATH` once per backend, adding `--block-size 256` only
for blocks. For the second table, run the [dispatch probe](history-dispatch-probe.py)
from outside the checkouts with `PYTHONPATH` pointing at the source
being measured.

- [GIL enabled raw results](history-dispatch-gil-on.json.gz)
- [GIL disabled raw results](history-dispatch-gil-off.json.gz)
