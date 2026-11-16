# Mavflow 2.0 integration and EventFlow comparison

## Compatibility boundary

- Read-only Mavflow `main`: `bf5bb0367883a2a0e4ae9036ccb639f7a42a7bd1`; the checkout was clean before and after testing. No Mavflow source, metadata, or dependency declaration was changed.
- Mavflow 2.0 declares `flowevent>=1.3.2,<1.4`. This resolver range excludes EventFlow 1.4.1 and a future 1.4.2.
- Reference environment: normal installation of the archived Mavflow source resolved `flowevent==1.3.2` and `pymavlink==2.4.50`. The FlowEvent wheel SHA-256 was `db76006b29f846210d3d665e0b8ce0a7303da76cb5127e327ae6a5d791e169ab`.
- Diagnostic candidate: EventFlow source commit `86eb3edaf32cacfe70882ea48f1a9e09928f3851`, package metadata version 1.4.1, built as a CPython 3.14 Linux native wheel (SHA-256 `93626f6eb85af5e0f4dd19fcb7b7914bede63f1c18d6e13391bf529b2dae0c6f`). Mavflow was installed normally first; EventFlow was then replaced with `pip install --no-deps`. This deliberately bypassed Mavflow's declared upper bound. Passing tests here are diagnostic evidence, not proof that users can install this combination through a resolver.
- Mavflow should widen its dependency range in a separate reviewed change after its maintainer accepts the compatibility evidence. A candidate is `flowevent>=1.3.2,<1.5`; this report does not change Mavflow or claim that range is already supported.

## Correctness checks

| Environment | Selected runtime/layout/dispatch/lifecycle tests | UDP loopback heartbeat | Filtered query, replay, cancel probe |
| --- | ---: | ---: | ---: |
| Normal resolver, EventFlow 1.3.2 | 69 passed | 1 passed | passed |
| Diagnostic local wheel, EventFlow 1.4.1 | 69 passed | 1 passed | passed |

The 69-test selection covered `test_runtime`, `test_package_layout`, `test_dispatch`, `test_concurrency_lifecycle`, and `test_lifecycle_subprocess`. The UDP test used loopback only. The additional probe published three `MavMessageEnvelope` values through `MavRuntime.messages`, queried the two matching heartbeat events from its bounded history, replayed them to a new filtered subscription, then verified cancellation. No MAVLink hardware or external service was used.

## Controlled `runtime.messages` profiles

The profile harness and raw samples are preserved in [`mavflow-integration.csv`](mavflow-integration.csv). Each table entry is the median of three fresh processes per environment. The candidate/reference `flowevent` versions, native backend, GIL state, and runtime are recorded per row.

- Host: Linux x86_64, 12th Gen Intel Core i7-12700H, CPython 3.14.10 with GIL enabled. The source EventFlow extension was compiled locally with GCC 16.2.1.
- Each process constructed `MavRuntime` but did not start a transport. Publishers called the actual `runtime.messages` EventBus from 1, 2, or 4 threads. Four subscribers used an EventFlow predicate to select half the event types; the bus retained at most 512 history items.
- Profiles used 1k, 10k, and 100k total publications. Churn levels were none, one add/cancel every 1,000 publications, or one every 100 publications. Three independent processes were run for each configuration.
- Per-event latency includes constructing the Mavflow envelope and calling `runtime.messages.publish`; it excludes socket receive, pymavlink decode and real transport. Throughput includes churn. CPU is process CPU time in the measured interval. RSS is the process high-water mark and includes imports and interpreter baseline. The raw CSV contains each sample, not only medians.
- On 100k-event profiles, median throughput was 3.84x the 1.3.2 reference; median p95 and p99 latency ratios were 0.22x and 0.27x, and CPU-time ratio was 0.26x. This is a local, GIL-enabled signal for this workload, not a general Mavflow performance guarantee. The native C extension and other 1.3.2-to-1.4.1 code changes differ together, so the measurement does not isolate C as the only cause.
- Small 1k profiles have high run-to-run variation from process startup and short measurement windows. Even at larger sizes the report should be treated as a diagnostic comparison, not a release-grade pyperf result. No algorithm change is justified by this integration measurement alone.

| Events | Publishers | Churn | Reference msg/s | Candidate msg/s | Throughput delta | p95 us (ref / candidate) | p99 us (ref / candidate) | CPU s (ref / candidate) | Peak RSS MiB (ref / candidate) | Throughput CV % (ref / candidate) |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000 | 1 | none | 101,859 | 336,262 | +230% | 11.30 / 4.45 | 22.92 / 5.50 | 0.010 / 0.003 | 28.7 / 28.6 | 10.2 / 21.7% |
| 1,000 | 1 | 1 per 100 publishes | 98,467 | 247,008 | +151% | 11.41 / 5.00 | 23.08 / 8.22 | 0.010 / 0.004 | 28.5 / 28.7 | 15.9 / 22.2% |
| 1,000 | 1 | 1 per 1k publishes | 97,770 | 198,098 | +103% | 11.76 / 5.29 | 22.49 / 8.78 | 0.010 / 0.005 | 28.5 / 28.6 | 21.2 / 34.2% |
| 1,000 | 2 | none | 92,911 | 216,579 | +133% | 18.31 / 5.33 | 26.36 / 7.96 | 0.011 / 0.004 | 28.6 / 28.7 | 8.9 / 4.2% |
| 1,000 | 2 | 1 per 100 publishes | 74,521 | 258,732 | +247% | 22.60 / 5.18 | 28.13 / 7.71 | 0.013 / 0.004 | 28.7 / 28.7 | 19.0 / 15.1% |
| 1,000 | 2 | 1 per 1k publishes | 86,500 | 187,077 | +116% | 22.16 / 5.43 | 25.81 / 9.29 | 0.011 / 0.005 | 28.5 / 28.8 | 4.2 / 11.5% |
| 1,000 | 4 | none | 69,629 | 184,457 | +165% | 23.21 / 5.47 | 37.07 / 12.57 | 0.014 / 0.005 | 28.4 / 28.7 | 5.0 / 4.6% |
| 1,000 | 4 | 1 per 100 publishes | 55,979 | 171,976 | +207% | 23.59 / 5.56 | 37.69 / 15.12 | 0.017 / 0.006 | 28.5 / 28.3 | 5.3 / 26.8% |
| 1,000 | 4 | 1 per 1k publishes | 68,011 | 164,752 | +142% | 23.05 / 5.54 | 30.13 / 13.01 | 0.014 / 0.006 | 28.6 / 28.6 | 9.6 / 4.1% |
| 10,000 | 1 | none | 109,648 | 421,408 | +284% | 10.38 / 2.28 | 12.67 / 4.92 | 0.091 / 0.024 | 28.7 / 29.2 | 1.6 / 2.0% |
| 10,000 | 1 | 1 per 100 publishes | 106,106 | 393,590 | +271% | 10.54 / 2.50 | 16.90 / 4.48 | 0.094 / 0.025 | 28.9 / 29.3 | 1.9 / 6.6% |
| 10,000 | 1 | 1 per 1k publishes | 109,086 | 401,671 | +268% | 10.27 / 2.32 | 12.19 / 4.86 | 0.091 / 0.025 | 28.8 / 29.1 | 2.4 / 13.3% |
| 10,000 | 2 | none | 107,326 | 337,307 | +214% | 10.41 / 5.20 | 22.61 / 5.49 | 0.093 / 0.029 | 28.9 / 29.0 | 1.9 / 7.2% |
| 10,000 | 2 | 1 per 100 publishes | 100,767 | 372,243 | +269% | 13.61 / 2.96 | 22.43 / 5.29 | 0.099 / 0.027 | 28.9 / 29.0 | 3.2 / 8.5% |
| 10,000 | 2 | 1 per 1k publishes | 104,880 | 358,447 | +242% | 10.45 / 5.03 | 22.47 / 5.41 | 0.094 / 0.028 | 29.0 / 29.0 | 3.7 / 6.5% |
| 10,000 | 4 | none | 99,262 | 289,020 | +191% | 17.74 / 5.48 | 23.11 / 6.52 | 0.099 / 0.034 | 28.9 / 28.8 | 7.9 / 22.3% |
| 10,000 | 4 | 1 per 100 publishes | 99,844 | 315,650 | +216% | 13.60 / 5.25 | 22.90 / 5.50 | 0.099 / 0.031 | 28.8 / 28.9 | 1.9 / 20.8% |
| 10,000 | 4 | 1 per 1k publishes | 96,325 | 285,002 | +196% | 18.09 / 5.27 | 23.05 / 5.72 | 0.103 / 0.035 | 28.4 / 28.8 | 7.8 / 22.2% |
| 100,000 | 1 | none | 112,583 | 437,174 | +288% | 10.13 / 2.27 | 11.88 / 3.59 | 0.886 / 0.228 | 34.4 / 34.6 | 0.5 / 0.5% |
| 100,000 | 1 | 1 per 100 publishes | 108,089 | 401,577 | +272% | 10.36 / 2.32 | 12.13 / 4.03 | 0.923 / 0.248 | 34.0 / 34.5 | 0.9 / 0.8% |
| 100,000 | 1 | 1 per 1k publishes | 108,077 | 415,343 | +284% | 10.39 / 2.30 | 13.50 / 4.30 | 0.923 / 0.239 | 34.1 / 34.3 | 1.3 / 0.7% |
| 100,000 | 2 | none | 106,785 | 423,481 | +297% | 10.46 / 2.30 | 20.47 / 4.71 | 0.929 / 0.235 | 34.1 / 34.4 | 3.7 / 3.5% |
| 100,000 | 2 | 1 per 100 publishes | 106,550 | 407,642 | +283% | 10.37 / 2.33 | 15.81 / 4.01 | 0.929 / 0.244 | 34.2 / 34.4 | 1.8 / 1.5% |
| 100,000 | 2 | 1 per 1k publishes | 107,287 | 417,741 | +289% | 10.38 / 2.31 | 18.31 / 4.43 | 0.924 / 0.238 | 34.5 / 34.6 | 5.6 / 1.1% |
| 100,000 | 4 | none | 106,125 | 407,032 | +284% | 10.53 / 2.39 | 19.95 / 5.32 | 0.941 / 0.245 | 34.1 / 33.8 | 4.2 / 2.1% |
| 100,000 | 4 | 1 per 100 publishes | 104,241 | 396,992 | +281% | 10.60 / 2.84 | 21.10 / 5.27 | 0.951 / 0.251 | 33.9 / 33.6 | 2.1 / 0.7% |
| 100,000 | 4 | 1 per 1k publishes | 105,536 | 379,918 | +260% | 10.55 / 4.64 | 19.14 / 5.36 | 0.946 / 0.263 | 33.7 / 33.7 | 2.3 / 8.0% |

## Decision

The tested Mavflow API usage worked under both the declared 1.3.2 resolver result and the forced local candidate. The `<1.4` dependency cap remains a real installation barrier and must not be hidden by the diagnostic environment. No Mavflow repository changes were made. Repeat this focused check against an actual 1.4.2 wheel after release metadata is prepared; that will test the final candidate artifact rather than the current 1.4.1 package metadata.
