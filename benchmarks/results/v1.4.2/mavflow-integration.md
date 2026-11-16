# Mavflow 2.0 integration and EventFlow comparison

## Compatibility boundary

- Read-only Mavflow `main`: `bf5bb0367883a2a0e4ae9036ccb639f7a42a7bd1`; the checkout was clean before and after testing. No Mavflow source, metadata, or dependency declaration was changed.
- Mavflow 2.0 declares `flowevent>=1.3.2,<1.4`. This resolver range excludes EventFlow 1.4.2.
- Reference environment: normal installation of the archived Mavflow source resolved `flowevent==1.3.2` and `pymavlink==2.4.50`. The FlowEvent wheel SHA-256 was `db76006b29f846210d3d665e0b8ce0a7303da76cb5127e327ae6a5d791e169ab`.
- Diagnostic candidate: EventFlow 1.4.2 built from the prepared source distribution as a CPython 3.14 Linux native wheel (SHA-256 `006b390710a1c10ddc2dadbec88371a9d9ecca4241483ef784fd3b4381b953bc`). Mavflow was installed normally first; EventFlow was then replaced with `pip install --no-deps`. This deliberately bypassed Mavflow's declared upper bound. Passing tests here are diagnostic evidence, not proof that users can install this combination through a resolver.
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
- The final 1.4.2 wheel completed all 27 size/publisher/churn profiles in three fresh processes each (81 candidate processes). On 100k-event profiles, median throughput was 3.86x the 1.3.2 reference; median p95 and p99 latency ratios were 0.22x and 0.27x, and CPU-time ratio was 0.26x. This is a local, GIL-enabled signal for this workload, not a general Mavflow performance guarantee. The native C extension and other 1.3.2-to-1.4.2 code changes differ together, so the measurement does not isolate C as the only cause.
- Small 1k profiles have high run-to-run variation from process startup and short measurement windows. Even at larger sizes the report should be treated as a diagnostic comparison, not a release-grade pyperf result. No algorithm change is justified by this integration measurement alone.

| Events | Publishers | Churn | Reference msg/s | Candidate msg/s | Throughput delta | p95 us (ref / candidate) | p99 us (ref / candidate) | CPU s (ref / candidate) | Peak RSS MiB (ref / candidate) | Throughput CV % (ref / candidate) |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000 | 1 | none | 101,859 | 339,626 | +233% | 11.30 / 4.46 | 22.92 / 5.68 | 0.010 / 0.003 | 28.7 / 28.6 | 10.2 / 2.5% |
| 1,000 | 1 | 1 per 100 publishes | 98,467 | 334,944 | +240% | 11.41 / 2.94 | 23.08 / 5.42 | 0.010 / 0.003 | 28.5 / 28.5 | 15.9 / 6.1% |
| 1,000 | 1 | 1 per 1000 publishes | 97,770 | 330,513 | +238% | 11.76 / 4.83 | 22.49 / 5.44 | 0.010 / 0.003 | 28.5 / 28.7 | 21.2 / 33.5% |
| 1,000 | 2 | none | 92,911 | 194,316 | +109% | 18.31 / 5.44 | 26.36 / 8.82 | 0.011 / 0.005 | 28.6 / 28.8 | 8.9 / 17.3% |
| 1,000 | 2 | 1 per 100 publishes | 74,521 | 203,601 | +173% | 22.60 / 5.47 | 28.13 / 9.31 | 0.013 / 0.005 | 28.7 / 29.0 | 19.0 / 18.9% |
| 1,000 | 2 | 1 per 1000 publishes | 86,500 | 214,063 | +147% | 22.16 / 5.30 | 25.81 / 8.70 | 0.011 / 0.005 | 28.5 / 28.8 | 4.2 / 11.2% |
| 1,000 | 4 | none | 69,629 | 200,919 | +189% | 23.21 / 5.39 | 37.07 / 11.58 | 0.014 / 0.005 | 28.4 / 28.6 | 5.0 / 5.0% |
| 1,000 | 4 | 1 per 100 publishes | 55,979 | 163,991 | +193% | 23.59 / 5.51 | 37.69 / 12.86 | 0.017 / 0.006 | 28.5 / 28.6 | 5.3 / 16.8% |
| 1,000 | 4 | 1 per 1000 publishes | 68,011 | 187,196 | +175% | 23.05 / 5.47 | 30.13 / 13.10 | 0.014 / 0.005 | 28.6 / 28.5 | 9.6 / 7.0% |
| 10,000 | 1 | none | 109,648 | 421,496 | +284% | 10.38 / 2.28 | 12.67 / 4.84 | 0.091 / 0.024 | 28.7 / 29.0 | 1.6 / 3.9% |
| 10,000 | 1 | 1 per 100 publishes | 106,106 | 394,911 | +272% | 10.54 / 2.32 | 16.90 / 4.38 | 0.094 / 0.025 | 28.9 / 29.2 | 1.9 / 3.0% |
| 10,000 | 1 | 1 per 1000 publishes | 109,086 | 411,703 | +277% | 10.27 / 2.26 | 12.19 / 4.45 | 0.091 / 0.024 | 28.8 / 29.0 | 2.4 / 2.3% |
| 10,000 | 2 | none | 107,326 | 387,443 | +261% | 10.41 / 4.47 | 22.61 / 5.28 | 0.093 / 0.026 | 28.9 / 28.7 | 1.9 / 2.6% |
| 10,000 | 2 | 1 per 100 publishes | 100,767 | 324,750 | +222% | 13.61 / 5.18 | 22.43 / 5.58 | 0.099 / 0.031 | 28.9 / 29.2 | 3.2 / 3.8% |
| 10,000 | 2 | 1 per 1000 publishes | 104,880 | 360,800 | +244% | 10.45 / 5.06 | 22.47 / 5.38 | 0.094 / 0.027 | 29.0 / 29.0 | 3.7 / 8.4% |
| 10,000 | 4 | none | 99,262 | 300,350 | +203% | 17.74 / 5.26 | 23.11 / 5.63 | 0.099 / 0.033 | 28.9 / 29.0 | 7.9 / 10.5% |
| 10,000 | 4 | 1 per 100 publishes | 99,844 | 338,847 | +239% | 13.60 / 5.16 | 22.90 / 5.47 | 0.099 / 0.029 | 28.8 / 29.2 | 1.9 / 6.0% |
| 10,000 | 4 | 1 per 1000 publishes | 96,325 | 237,738 | +147% | 18.09 / 5.36 | 23.05 / 7.11 | 0.103 / 0.042 | 28.4 / 28.9 | 7.8 / 7.6% |
| 100,000 | 1 | none | 112,583 | 436,561 | +288% | 10.13 / 2.27 | 11.88 / 3.65 | 0.886 / 0.229 | 34.4 / 34.3 | 0.5 / 0.5% |
| 100,000 | 1 | 1 per 100 publishes | 108,089 | 408,728 | +278% | 10.36 / 2.30 | 12.13 / 3.99 | 0.923 / 0.244 | 34.0 / 34.5 | 0.9 / 1.4% |
| 100,000 | 1 | 1 per 1000 publishes | 108,077 | 422,151 | +291% | 10.39 / 2.29 | 13.50 / 3.82 | 0.923 / 0.236 | 34.1 / 34.4 | 1.3 / 0.5% |
| 100,000 | 2 | none | 106,785 | 424,073 | +297% | 10.46 / 2.30 | 20.47 / 4.55 | 0.929 / 0.235 | 34.1 / 34.5 | 3.7 / 2.3% |
| 100,000 | 2 | 1 per 100 publishes | 106,550 | 408,457 | +283% | 10.37 / 2.33 | 15.81 / 4.51 | 0.929 / 0.244 | 34.2 / 34.5 | 1.8 / 1.9% |
| 100,000 | 2 | 1 per 1000 publishes | 107,287 | 422,227 | +294% | 10.38 / 2.29 | 18.31 / 4.31 | 0.924 / 0.236 | 34.5 / 34.5 | 5.6 / 1.2% |
| 100,000 | 4 | none | 106,125 | 407,118 | +284% | 10.53 / 2.64 | 19.95 / 5.24 | 0.941 / 0.245 | 34.1 / 33.9 | 4.2 / 1.6% |
| 100,000 | 4 | 1 per 100 publishes | 104,241 | 402,516 | +286% | 10.60 / 2.47 | 21.10 / 5.19 | 0.951 / 0.249 | 33.9 / 33.7 | 2.1 / 2.1% |
| 100,000 | 4 | 1 per 1000 publishes | 105,536 | 402,115 | +281% | 10.55 / 2.70 | 19.14 / 5.24 | 0.946 / 0.249 | 33.7 / 33.9 | 2.3 / 0.6% |
## Decision

The tested Mavflow API usage worked with the declared 1.3.2 resolver result and with the diagnostic 1.4.2 wheel. The `<1.4` dependency cap remains a real installation barrier and must not be hidden by the diagnostic environment. No Mavflow repository files or dependency declarations were changed. A separate Mavflow maintainer review is still required before widening its package constraint.
