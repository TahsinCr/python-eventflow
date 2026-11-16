# Mavflow 2.0 integration and EventFlow comparison

## Compatibility boundary

- Read-only Mavflow `main`: `bf5bb0367883a2a0e4ae9036ccb639f7a42a7bd1`; the checkout was clean before and after testing. No Mavflow source, metadata, or dependency declaration was changed.
- Mavflow 2.0 declares `flowevent>=1.3.2,<1.4`. This resolver range excludes EventFlow 1.4.2.
- Reference environment: normal installation of the archived Mavflow source resolved `flowevent==1.3.2` and `pymavlink==2.4.50`. The FlowEvent wheel SHA-256 was `db76006b29f846210d3d665e0b8ce0a7303da76cb5127e327ae6a5d791e169ab`.
- Diagnostic candidate: EventFlow 1.4.2 built from the prepared source distribution as a CPython 3.14 Linux native wheel (SHA-256 `831d8f42d81f51f624daeda761b1f9bc27f59bfd6672030fa9b229ffb5c18775`). Mavflow was installed normally first; EventFlow was then replaced with `pip install --no-deps`. This deliberately bypassed Mavflow's declared upper bound. Passing tests here are diagnostic evidence, not proof that users can install this combination through a resolver.
- Mavflow should widen its dependency range in a separate reviewed change after its maintainer accepts the compatibility evidence. A candidate is `flowevent>=1.3.2,<1.5`; this report does not change Mavflow or claim that range is already supported.

## Correctness checks

| Environment | Selected runtime/layout/dispatch/lifecycle tests | UDP loopback heartbeat | Filtered query, replay, cancel probe |
| --- | ---: | ---: | ---: |
| Normal resolver, EventFlow 1.3.2 | 69 passed | 1 passed | passed |
| Diagnostic local wheel, EventFlow 1.4.2 | 69 passed | 1 passed | passed |

The 69-test selection covered `test_runtime`, `test_package_layout`, `test_dispatch`, `test_concurrency_lifecycle`, and `test_lifecycle_subprocess`. The UDP test used loopback only. A direct `runtime.messages` probe published three `MavMessageEnvelope` values, queried the two matching heartbeat events from bounded history, replayed them to a new filtered subscription, and verified cancellation by confirming later publication was no longer delivered. No MAVLink hardware or external service was used.

## Controlled `runtime.messages` profiles

The profile harness and raw samples are preserved in [`mavflow-integration.csv`](mavflow-integration.csv). Each table entry is the median of three fresh processes per environment. The candidate/reference `flowevent` versions, native backend, GIL state, and runtime are recorded per row.

- Host: Linux x86_64, 12th Gen Intel Core i7-12700H, CPython 3.14.10 with GIL enabled. The source EventFlow extension was compiled locally with GCC 16.2.1.
- Each process constructed `MavRuntime` but did not start a transport. Publishers called the actual `runtime.messages` EventBus from 1, 2, or 4 threads. Four subscribers used an EventFlow predicate to select half the event types; the bus retained at most 512 history items.
- Profiles used 1k, 10k, and 100k total publications. Churn levels were none, one add/cancel every 1,000 publications, or one every 100 publications. Three independent processes were run for each configuration.
- Per-event latency includes constructing the Mavflow envelope and calling `runtime.messages.publish`; it excludes socket receive, pymavlink decode and real transport. Throughput includes churn. CPU is process CPU time in the measured interval. RSS is the process high-water mark and includes imports and interpreter baseline. The raw CSV contains each sample, not only medians.
- The final 1.4.2 wheel completed all 27 size/publisher/churn profiles in three fresh processes each (81 candidate processes). On 100k-event profiles, median throughput was 3.85x the 1.3.2 reference; median p95 and p99 latency ratios were 0.23x and 0.28x, and CPU-time ratio was 0.26x. This is a local, GIL-enabled signal for this workload, not a general Mavflow performance guarantee. The native C extension and other 1.3.2-to-1.4.2 code changes differ together, so the measurement does not isolate C as the only cause.
- Small 1k profiles have high run-to-run variation from process startup and short measurement windows. Even at larger sizes the report should be treated as a diagnostic comparison, not a release-grade pyperf result. No algorithm change is justified by this integration measurement alone.

| Events | Publishers | Churn | Reference msg/s | Candidate msg/s | Throughput delta | p95 us (ref / candidate) | p99 us (ref / candidate) | CPU s (ref / candidate) | Peak RSS MiB (ref / candidate) | Throughput CV % (ref / candidate) |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000 | 1 | none | 101,859 | 340,401 | +234% | 11.30 / 4.81 | 22.92 / 6.00 | 0.010 / 0.003 | 28.7 / 28.6 | 10.2 / 3.4% |
| 1,000 | 1 | 1 per 100 publishes | 98,467 | 313,983 | +219% | 11.41 / 4.61 | 23.08 / 6.38 | 0.010 / 0.003 | 28.5 / 28.6 | 15.9 / 1.0% |
| 1,000 | 1 | 1 per 1000 publishes | 97,770 | 291,288 | +198% | 11.76 / 5.07 | 22.49 / 7.65 | 0.010 / 0.003 | 28.5 / 28.6 | 21.2 / 8.4% |
| 1,000 | 2 | none | 92,911 | 282,175 | +204% | 18.31 / 5.12 | 26.36 / 8.44 | 0.011 / 0.003 | 28.6 / 28.6 | 8.9 / 28.4% |
| 1,000 | 2 | 1 per 100 publishes | 74,521 | 187,588 | +152% | 22.60 / 5.48 | 28.13 / 10.54 | 0.013 / 0.005 | 28.7 / 28.6 | 19.0 / 10.5% |
| 1,000 | 2 | 1 per 1000 publishes | 86,500 | 216,005 | +150% | 22.16 / 5.34 | 25.81 / 8.05 | 0.011 / 0.004 | 28.5 / 28.9 | 4.2 / 2.1% |
| 1,000 | 4 | none | 69,629 | 170,840 | +145% | 23.21 / 5.45 | 37.07 / 11.82 | 0.014 / 0.006 | 28.4 / 28.6 | 5.0 / 5.7% |
| 1,000 | 4 | 1 per 100 publishes | 55,979 | 184,071 | +229% | 23.59 / 5.42 | 37.69 / 12.86 | 0.017 / 0.005 | 28.5 / 28.5 | 5.3 / 2.5% |
| 1,000 | 4 | 1 per 1000 publishes | 68,011 | 166,782 | +145% | 23.05 / 5.58 | 30.13 / 13.12 | 0.014 / 0.006 | 28.6 / 28.6 | 9.6 / 9.2% |
| 10,000 | 1 | none | 109,648 | 424,360 | +287% | 10.38 / 2.30 | 12.67 / 4.03 | 0.091 / 0.023 | 28.7 / 29.0 | 1.6 / 1.8% |
| 10,000 | 1 | 1 per 100 publishes | 106,106 | 400,336 | +277% | 10.54 / 2.30 | 16.90 / 4.03 | 0.094 / 0.025 | 28.9 / 29.1 | 1.9 / 1.7% |
| 10,000 | 1 | 1 per 1000 publishes | 109,086 | 414,538 | +280% | 10.27 / 2.28 | 12.19 / 3.97 | 0.091 / 0.024 | 28.8 / 29.0 | 2.4 / 2.8% |
| 10,000 | 2 | none | 107,326 | 370,571 | +245% | 10.41 / 5.09 | 22.61 / 5.41 | 0.093 / 0.027 | 28.9 / 29.1 | 1.9 / 29.0% |
| 10,000 | 2 | 1 per 100 publishes | 100,767 | 347,045 | +244% | 13.61 / 5.16 | 22.43 / 5.55 | 0.099 / 0.029 | 28.9 / 29.1 | 3.2 / 7.9% |
| 10,000 | 2 | 1 per 1000 publishes | 104,880 | 377,746 | +260% | 10.45 / 4.60 | 22.47 / 5.28 | 0.094 / 0.026 | 29.0 / 29.1 | 3.7 / 7.2% |
| 10,000 | 4 | none | 99,262 | 320,276 | +223% | 17.74 / 5.26 | 23.11 / 5.79 | 0.099 / 0.031 | 28.9 / 28.6 | 7.9 / 13.6% |
| 10,000 | 4 | 1 per 100 publishes | 99,844 | 352,314 | +253% | 13.60 / 5.04 | 22.90 / 5.53 | 0.099 / 0.028 | 28.8 / 28.8 | 1.9 / 21.3% |
| 10,000 | 4 | 1 per 1000 publishes | 96,325 | 304,745 | +216% | 18.09 / 5.27 | 23.05 / 5.94 | 0.103 / 0.033 | 28.4 / 28.9 | 7.8 / 6.2% |
| 100,000 | 1 | none | 112,583 | 434,037 | +286% | 10.13 / 2.30 | 11.88 / 3.65 | 0.886 / 0.230 | 34.4 / 34.6 | 0.5 / 0.1% |
| 100,000 | 1 | 1 per 100 publishes | 108,089 | 404,961 | +275% | 10.36 / 2.32 | 12.13 / 3.92 | 0.923 / 0.246 | 34.0 / 34.7 | 0.9 / 0.3% |
| 100,000 | 1 | 1 per 1000 publishes | 108,077 | 419,980 | +289% | 10.39 / 2.30 | 13.50 / 3.82 | 0.923 / 0.238 | 34.1 / 34.5 | 1.3 / 0.2% |
| 100,000 | 2 | none | 106,785 | 430,201 | +303% | 10.46 / 2.29 | 20.47 / 4.05 | 0.929 / 0.232 | 34.1 / 34.5 | 3.7 / 1.5% |
| 100,000 | 2 | 1 per 100 publishes | 106,550 | 404,651 | +280% | 10.37 / 2.34 | 15.81 / 5.11 | 0.929 / 0.246 | 34.2 / 34.7 | 1.8 / 1.0% |
| 100,000 | 2 | 1 per 1000 publishes | 107,287 | 418,374 | +290% | 10.38 / 2.29 | 18.31 / 4.72 | 0.924 / 0.238 | 34.5 / 34.6 | 5.6 / 1.4% |
| 100,000 | 4 | none | 106,125 | 408,785 | +285% | 10.53 / 2.47 | 19.95 / 5.20 | 0.941 / 0.245 | 34.1 / 33.8 | 4.2 / 2.4% |
| 100,000 | 4 | 1 per 100 publishes | 104,241 | 394,693 | +279% | 10.60 / 2.54 | 21.10 / 5.19 | 0.951 / 0.253 | 33.9 / 33.9 | 2.1 / 2.4% |
| 100,000 | 4 | 1 per 1000 publishes | 105,536 | 404,720 | +283% | 10.55 / 2.49 | 19.14 / 5.31 | 0.946 / 0.247 | 33.7 / 33.9 | 2.3 / 2.8% |
## Decision

The tested Mavflow API usage worked with the declared 1.3.2 resolver result and with the diagnostic 1.4.2 wheel. The `<1.4` dependency cap remains a real installation barrier and must not be hidden by the diagnostic environment. No Mavflow repository files or dependency declarations were changed. A separate Mavflow maintainer review is still required before widening its package constraint.
