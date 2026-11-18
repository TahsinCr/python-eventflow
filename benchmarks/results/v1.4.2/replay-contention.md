# Replay contention and history query measurements

Source: `4cefd15`. Both runs used CPython 3.15.2, the C core, Linux x86-64,
an Intel Core i7-12700H and CPU affinity 4, 6 and 8. One run used the
GIL-enabled build and the other the free-threaded build with the GIL disabled.
Each profile ran for 20 rounds with one replay subscriber and two publisher
threads. This is a wall-clock diagnostic on one host.

```sh
taskset -c 4,6,8 python -m benchmarks.diagnose_replay_contention --output /tmp/gil-on.csv
PYTHON_GIL=0 taskset -c 4,6,8 python3.15t -m benchmarks.diagnose_replay_contention --output /tmp/gil-off.csv
```

The 240 profiles per build cover four capacities (1k to 1M), five predicate
selectivities (0%, 0.1%, 1%, 10% and 100%), four replay windows (1, 10, 100
and 1,000) and three snapshot preparation policies (warm before every round,
leave cold every tenth round, or always leave cold). Publishers can
invalidate a prewarmed snapshot before the subscription acquires the lock.
The raw data records the actual rebuild as well as the preparation policy.
Each CSV contains 9,600 rows, one per publisher per round. Replay and snapshot
fields repeat for the two publishers in each round.

Snapshot duration measures the history query inside the publication lock,
including tuple creation when the cache is cold. The timed lock acquisition
is a separate probe on the real publication lock immediately before each
timed public `publish()` call. Probe values include scheduling delay and
probe overhead. They do not decompose a timed publication or measure the
entire subscription critical section. Snapshot bytes include the tuple and
its reference slots. They exclude payloads, temporary replay selection,
allocator overhead and process RSS.

Type query times come from a separate pass over a warmed history with all
`int` events matching. The publisher percentiles pool all profiles for a
capacity and actual snapshot state. They describe tails for this mix of
inputs and scheduling. Use the raw rows for individual profiles, which have
only 20 replay observations each.

## Measurements

| GIL | Capacity | Full type query median (ms) | Rebuild median (µs) | Reuse median (µs) | Cold publish p95/p99 (µs) | Cold lock probe p99 (µs) | Tuple size (MiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| on | 1,000 | 0.037 | 7.37 | 4.17 | 5.00 / 10.09 | 1.28 | 0.008 |
| on | 10,000 | 0.365 | 42.72 | 2.06 | 27.61 / 39.49 | 1.86 | 0.076 |
| on | 100,000 | 4.200 | 475.94 | 2.42 | 263.27 / 337.61 | 2.04 | 0.763 |
| on | 1,000,000 | 40.298 | 6069.13 | 3.99 | 4135.48 / 4853.76 | 5190.77 | 7.629 |
| off | 1,000 | 0.041 | 45.14 | 1.19 | 10.57 / 58.69 | 81.87 | 0.008 |
| off | 10,000 | 0.445 | 460.27 | 1.33 | 90.10 / 102.56 | 654.52 | 0.076 |
| off | 100,000 | 4.595 | 4986.96 | n/a | 953.02 / 1072.12 | 6920.10 | 0.763 |
| off | 1,000,000 | 65.132 | 56685.91 | 5.04 | 64434.91 / 69049.21 | 67127.58 | 7.629 |

The snapshot was reused in 4 of 4,800 replay rounds with
the GIL enabled and 200 with it disabled. Publisher tails
are shown for rounds with a real rebuild because reuse samples are uneven
across profiles. A publisher can win the lock before replay begins. A cold
snapshot therefore does not delay every publisher in these rounds.

## Decision

Keep the current snapshot and replay design. The publication lock preserves
a consistent boundary between history capture and replay registration.
Under this workload, tuple creation is measurable under that lock at large
capacities, and full type queries still pay for fresh type membership.
Changing cache or indexing policy would need more evidence from real traffic,
including multiple simultaneous replay subscribers and async workloads. See
the [previous audit](replay-history-audit.md) for matched before/after pyperf
data.

## Raw data

- GIL enabled: [CSV samples](replay-contention-gil-on.csv.gz) and [metadata and type query timings](replay-contention-gil-on.json).
- GIL disabled: [CSV samples](replay-contention-gil-off.csv.gz) and [metadata and type query timings](replay-contention-gil-off.json).
