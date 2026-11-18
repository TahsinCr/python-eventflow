# Retained benchmark results

This directory keeps benchmark evidence for EventFlow 1.3.2, 1.4.0 and
1.4.2. Each report identifies the revisions and workloads it measures.

The [16 November dispatch/history audit](v1.4.0/audit_2026-11-16.md) retains
source-hashed cache-policy and matched-buffer screening data, raw latency
diagnostics and their limitations. Production policies remain unchanged.

The [17 November route-cache FIFO experiment](v1.4.2/route-cache-fifo.md)
compares the existing admission-only policy with a bounded FIFO candidate on
GIL-enabled and free-threaded CPython. It records why the production policy was
kept, with source hashes, pyperf datasets, selected raw latency samples and a
full per-case CSV.

The [native C coverage review](v1.4.2/native-c-coverage.md) records `_core.c`
line, function and branch coverage for the measured revision and maps the
remaining gaps to their tested paths and reachability limits.

The [replay and history audit](v1.4.2/replay-history-audit.md) retains
before/after pyperf samples, cold replay and history memory diagnostics,
cooldown thread wakeups, and an async type-miss profile.

The [replay contention report](v1.4.2/replay-contention.md) measures
history up to one million events with two publishers, including snapshot
rebuilds, lock probes, publication tails and full type queries on CPython
with and without the GIL.

The [replay scheduling report](v1.4.2/replay-scheduling.md) measures
lock acquisition inside publication with multiple concurrent replay subscribers,
and callback delay during cold and warm async replay.

The [history and async dispatch comparison](v1.4.2/history-dispatch-optimization.md)
records the append cost of block storage, replay tail latency, full type query
timings and async dispatch with sparse matches and subscription churn.

The [async churn and replay comparison](v1.4.2/async-churn-replay.md)
measures subscription changes, sequential fan-out, cooperative replay and
partitioned histories on named channels.

## Datasets

- [`v1.3.2/`](v1.3.2/) contains the 2026-11-09 broad performance screen: 31
  pyperf JSON files and a 409-row CSV summary. The JSON files retain raw
  measurements and source/interpreter metadata.
- [`v1.4.0/`](v1.4.0/) contains all 1.4.0 results directly in one folder:
  13 benchmark-module JSON files with 183 cases (180 paired with 1.3.2), four
  focused history-query JSON files, one history summary CSV, and two routing
  summary CSVs.
- [`v1.4.2/`](v1.4.2/) contains the replay, history and async dispatch comparisons,
  native C coverage notes, and the route-cache FIFO experiment. See each
  report for its source revision, raw data and measurement limits.
- [`v1.3.2-v1.4.0-comparison.md`](v1.3.2-v1.4.0-comparison.md) explains the
  pure-Python release comparison, native backend evidence, and remaining
  measurement limits.

The 1.3.2 files were recovered from Git history at the last commit before the
old result archive was pruned. The 1.4.0 broad screen used the existing
benchmark modules and matching settings; its focused runtime measurements are
kept alongside it in the same version folder. Superseded prototype runs and
older overlapping archives are not retained here.
