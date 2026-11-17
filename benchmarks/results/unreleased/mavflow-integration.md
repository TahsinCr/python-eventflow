# Mavflow 2.0 integration and EventFlow comparison

## Compatibility boundary

- Read-only Mavflow `main`: `bf5bb0367883a2a0e4ae9036ccb639f7a42a7bd1`; the checkout was clean before and after testing. No Mavflow source, metadata, or dependency declaration was changed.
- Mavflow 2.0 declares `flowevent>=1.3.2,<1.4`. The current unreleased candidate is outside this declared range.
- Reference environment: normal installation of the archived Mavflow source resolved `flowevent==1.3.2` and `pymavlink==2.4.50`. The FlowEvent wheel SHA-256 was `db76006b29f846210d3d665e0b8ce0a7303da76cb5127e327ae6a5d791e169ab`.
- Diagnostic candidate: the current unreleased EventFlow working tree, built with package metadata 1.4.1 as a CPython 3.14 Linux native wheel (SHA-256 `ab355343f4de43e0d10c72052ba397f37c5660a7d703bcb92036f88155f1a06e`). This is not the published 1.4.1 artifact. Mavflow was installed normally first; EventFlow was then replaced with `pip install --no-deps`. This deliberately bypassed Mavflow's declared upper bound. Passing tests here are diagnostic evidence, not proof that users can install this combination through a resolver.
- Mavflow should widen its dependency range in a separate reviewed change after its maintainer accepts the compatibility evidence. A candidate is `flowevent>=1.3.2,<1.5`; this report does not change Mavflow or claim that range is already supported.

## Correctness checks

| Environment | Selected runtime/layout/dispatch/lifecycle tests | UDP loopback heartbeat | Filtered query, replay, cancel probe |
| --- | ---: | ---: | ---: |
| Normal resolver, EventFlow 1.3.2 | 69 passed | 1 passed | passed |
| Diagnostic local wheel, unreleased source with 1.4.1 metadata | 69 passed | 1 passed | passed |

The 69-test selection covered `test_runtime`, `test_package_layout`, `test_dispatch`, `test_concurrency_lifecycle`, and `test_lifecycle_subprocess`. The UDP test used loopback only. A direct `runtime.messages` probe published three `MavMessageEnvelope` values, queried the two matching heartbeat events from bounded history, replayed them to a new filtered subscription, and verified cancellation by confirming later publication was no longer delivered. No MAVLink hardware or external service was used.

The `AsyncEventQueue` bridge was exercised against `MavRuntime.messages` from a worker thread in both environments. EventFlow 1.3.2 passed the unfiltered bridge receive/close path; its constructor does not expose the newer `event_filter` option. The local candidate passed with a predicate filter: an `ATTITUDE` envelope was rejected before queue admission and a `HEARTBEAT` envelope was received without drops. Both runs used the async context manager for cleanup.

## Controlled `runtime.messages` profiles

The profile harness and raw samples are preserved in [`mavflow-integration.csv`](mavflow-integration.csv). Each table entry is the median of three fresh processes per environment. The candidate source status, package metadata, native backend, GIL state, and runtime are recorded per row.

- Host: Linux x86_64, 12th Gen Intel Core i7-12700H, CPython 3.14.10 with GIL enabled. The source EventFlow extension was compiled locally with GCC 16.2.1.
- Each process constructed `MavRuntime` but did not start a transport. Publishers called the actual `runtime.messages` EventBus from 1, 2, or 4 threads. Four subscribers used an EventFlow predicate to select half the event types; the bus retained at most 512 history items.
- Profiles used 1k, 10k, and 100k total publications. Churn levels were none, one add/cancel every 1,000 publications, or one every 100 publications. Three independent processes were run for each configuration.
- Per-event latency includes constructing the Mavflow envelope and calling `runtime.messages.publish`; it excludes socket receive, pymavlink decode and real transport. Throughput includes churn. CPU is process CPU time in the measured interval. RSS is the process high-water mark and includes imports and interpreter baseline. The raw CSV contains each sample, not only medians.
- The local candidate completed all 27 size/publisher/churn profiles in three fresh processes each (81 candidate processes). On 100k-event profiles, median throughput was 3.88x the 1.3.2 reference; median p95 and p99 latency ratios were 0.22x and 0.27x, and CPU-time ratio was 0.26x. This is a local, GIL-enabled signal for this workload, not a general Mavflow performance guarantee. The native C extension and other code changes since the 1.3.2 reference differ together, so the measurement does not isolate C as the only cause.
- Small 1k profiles have high run-to-run variation from process startup and short measurement windows. Even at larger sizes the report should be treated as a diagnostic comparison, not a release-grade pyperf result. No algorithm change is justified by this integration measurement alone.

| Events | Publishers | Churn | Reference msg/s | Candidate msg/s | Throughput delta | p95 us (ref / candidate) | p99 us (ref / candidate) | CPU s (ref / candidate) | Peak RSS MiB (ref / candidate) | Throughput CV % (ref / candidate) |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000 | 1 | none | 101,859 | 314,481 | +209% | 11.30 / 5.09 | 22.92 / 6.31 | 0.010 / 0.003 | 28.7 / 28.7 | 10.2 / 38.8% |
| 1,000 | 1 | 1 per 100 publishes | 98,467 | 295,924 | +201% | 11.41 / 4.92 | 23.08 / 7.70 | 0.010 / 0.003 | 28.5 / 28.6 | 15.9 / 5.4% |
| 1,000 | 1 | 1 per 1000 publishes | 97,770 | 321,251 | +229% | 11.76 / 4.79 | 22.49 / 6.32 | 0.010 / 0.003 | 28.5 / 28.9 | 21.2 / 13.5% |
| 1,000 | 2 | none | 92,911 | 199,881 | +115% | 18.31 / 5.29 | 26.36 / 9.72 | 0.011 / 0.005 | 28.6 / 28.7 | 8.9 / 16.9% |
| 1,000 | 2 | 1 per 100 publishes | 74,521 | 173,145 | +132% | 22.60 / 5.43 | 28.13 / 8.92 | 0.013 / 0.006 | 28.7 / 28.6 | 19.0 / 16.0% |
| 1,000 | 2 | 1 per 1000 publishes | 86,500 | 250,206 | +189% | 22.16 / 5.26 | 25.81 / 8.24 | 0.011 / 0.004 | 28.5 / 28.7 | 4.2 / 26.8% |
| 1,000 | 4 | none | 69,629 | 180,788 | +160% | 23.21 / 5.51 | 37.07 / 13.04 | 0.014 / 0.005 | 28.4 / 28.6 | 5.0 / 15.6% |
| 1,000 | 4 | 1 per 100 publishes | 55,979 | 208,469 | +272% | 23.59 / 5.33 | 37.69 / 11.45 | 0.017 / 0.005 | 28.5 / 28.4 | 5.3 / 13.7% |
| 1,000 | 4 | 1 per 1000 publishes | 68,011 | 176,091 | +159% | 23.05 / 5.47 | 30.13 / 13.26 | 0.014 / 0.005 | 28.6 / 28.6 | 9.6 / 5.8% |
| 10,000 | 1 | none | 109,648 | 426,314 | +289% | 10.38 / 2.27 | 12.67 / 4.23 | 0.091 / 0.023 | 28.7 / 29.0 | 1.6 / 1.5% |
| 10,000 | 1 | 1 per 100 publishes | 106,106 | 401,881 | +279% | 10.54 / 2.30 | 16.90 / 3.81 | 0.094 / 0.025 | 28.9 / 29.1 | 1.9 / 4.0% |
| 10,000 | 1 | 1 per 1000 publishes | 109,086 | 411,285 | +277% | 10.27 / 2.28 | 12.19 / 4.75 | 0.091 / 0.024 | 28.8 / 29.1 | 2.4 / 13.7% |
| 10,000 | 2 | none | 107,326 | 372,526 | +247% | 10.41 / 4.98 | 22.61 / 5.31 | 0.093 / 0.027 | 28.9 / 28.9 | 1.9 / 7.2% |
| 10,000 | 2 | 1 per 100 publishes | 100,767 | 357,477 | +255% | 13.61 / 5.03 | 22.43 / 5.39 | 0.099 / 0.028 | 28.9 / 29.0 | 3.2 / 8.1% |
| 10,000 | 2 | 1 per 1000 publishes | 104,880 | 402,094 | +283% | 10.45 / 2.34 | 22.47 / 4.79 | 0.094 / 0.025 | 29.0 / 29.0 | 3.7 / 9.8% |
| 10,000 | 4 | none | 99,262 | 350,693 | +253% | 17.74 / 4.88 | 23.11 / 5.40 | 0.099 / 0.028 | 28.9 / 28.8 | 7.9 / 6.8% |
| 10,000 | 4 | 1 per 100 publishes | 99,844 | 318,682 | +219% | 13.60 / 5.15 | 22.90 / 5.55 | 0.099 / 0.031 | 28.8 / 28.9 | 1.9 / 13.2% |
| 10,000 | 4 | 1 per 1000 publishes | 96,325 | 313,605 | +226% | 18.09 / 5.23 | 23.05 / 5.64 | 0.103 / 0.032 | 28.4 / 28.9 | 7.8 / 6.7% |
| 100,000 | 1 | none | 112,583 | 436,763 | +288% | 10.13 / 2.26 | 11.88 / 3.50 | 0.886 / 0.228 | 34.4 / 34.6 | 0.5 / 0.7% |
| 100,000 | 1 | 1 per 100 publishes | 108,089 | 406,362 | +276% | 10.36 / 2.31 | 12.13 / 3.99 | 0.923 / 0.245 | 34.0 / 34.3 | 0.9 / 0.4% |
| 100,000 | 1 | 1 per 1000 publishes | 108,077 | 424,184 | +292% | 10.39 / 2.28 | 13.50 / 3.61 | 0.923 / 0.235 | 34.1 / 34.6 | 1.3 / 0.3% |
| 100,000 | 2 | none | 106,785 | 428,345 | +301% | 10.46 / 2.27 | 20.47 / 4.71 | 0.929 / 0.233 | 34.1 / 34.5 | 3.7 / 1.8% |
| 100,000 | 2 | 1 per 100 publishes | 106,550 | 405,705 | +281% | 10.37 / 2.35 | 15.81 / 4.78 | 0.929 / 0.246 | 34.2 / 34.4 | 1.8 / 1.3% |
| 100,000 | 2 | 1 per 1000 publishes | 107,287 | 418,959 | +291% | 10.38 / 2.29 | 18.31 / 4.65 | 0.924 / 0.238 | 34.5 / 34.8 | 5.6 / 0.7% |
| 100,000 | 4 | none | 106,125 | 400,570 | +277% | 10.53 / 2.79 | 19.95 / 5.31 | 0.941 / 0.249 | 34.1 / 33.8 | 4.2 / 3.6% |
| 100,000 | 4 | 1 per 100 publishes | 104,241 | 406,072 | +290% | 10.60 / 2.41 | 21.10 / 5.10 | 0.951 / 0.246 | 33.9 / 33.9 | 2.1 / 5.2% |
| 100,000 | 4 | 1 per 1000 publishes | 105,536 | 400,672 | +280% | 10.55 / 2.53 | 19.14 / 5.30 | 0.946 / 0.250 | 33.7 / 33.8 | 2.3 / 4.1% |
## Decision

The tested Mavflow API usage worked with the declared 1.3.2 resolver result and with the diagnostic unreleased candidate. The `<1.4` dependency cap remains a real installation barrier and must not be hidden by the diagnostic environment. No Mavflow repository files or dependency declarations were changed. A separate Mavflow maintainer review is still required before widening its package constraint.
