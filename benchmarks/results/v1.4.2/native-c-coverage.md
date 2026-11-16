# Native C coverage and remaining branch risks

**Run date:** 2026-11-17
**Measured revision:** `dd5fa222a993020b8f85b8ce1d309896ca6798fd`
**`eventflow/_core.c` SHA-256:** `8b4da0702a342a184dce0945e2d10daed0e88e799a29badc990f9f5902099b1a`

The isolated GCC/gcovr build measured only `eventflow/_core.c`. It built and loaded the instrumented extension under Linux x86_64 and CPython 3.14.10 with the GIL enabled, then ran the complete native test suite. All 427 tests passed with no skips.

| C metric | Covered | Total | Coverage |
|---|---:|---:|---:|
| Lines | 1,315 | 1,587 | 82.9% |
| Functions | 79 | 79 | 100.0% |
| Branch arcs | 956 | 1,436 | 66.6% |

These are gcov branch arcs, not a count of high-level Boolean expressions. All functions execute, while many error, allocation, object traversal and platform-specific outcomes remain unobserved in this one Linux build. No arbitrary coverage threshold is used.

## Critical path coverage

| Path | Covered branch arcs | Relevant regression coverage | Remaining gap assessment |
|---|---:|---|---|
| `BusCore_publish` | 185 / 282 | `test_bus`, `test_routing_key`, `test_execution_primitives`, and native condition cleanup in `test_native_runtime` | Normal dispatch, hook failures, routing, and condition cleanup are exercised. Remaining arcs are combinations of error-state, allocation, fallback and cleanup outcomes; coverage alone does not identify a defect. |
| Subscriber claim and restore | `sub_claim_internal` 11 / 30; `sub_restore_claim_internal` 6 / 14 | `test_subscriber_claim_restore_integer_boundaries`, allocator-failure recovery tests, publish and replay tests | Quota and counter boundaries are covered. Exhaustive allocation-failure sequencing is not represented by ordinary successful calls. |
| Replay buffer and completion | `sub_buffer_if_replaying_internal` 11 / 20; `Subscriber_finish_replay` 7 / 12 | `test_subscriber_replay_methods_and_replaying_property`, plus bus history/replay tests | Buffering and normal completion are directly tested. Residual arcs are exceptional state and cleanup combinations. |
| Native route merge | `merge_subscriber_dicts_c` 16 / 30 | `test_routing_key` and dispatch-pressure cases at 64/65/80/256/1000 keys | This C helper merges native subscriber snapshots. The 64-key admission policy itself lives in Python `event_bus.py`; its behavior is tested separately and is not represented in this C percentage. |
| Operation statistics | `record` 95 / 114; `snapshot` 35 / 44; `reset` 13 / 20 | `test_core_extension_powers_event_bus_and_operation_stats`, `test_operation_stats_bound_cardinality_and_freeze_snapshots`, and native allocation-failure tests | Counter growth, large integers, snapshots, reset and failure recovery have deterministic tests. Remaining arcs include allocation rollback and invalid/empty state combinations. |
| GC traversal and clear | `Subscriber_traverse` 23 / 40; `BusCore_traverse` 40 / 68; `core_traverse` 85 / 164; `BusCore_clear` 31 / 32; `OperationStatsCore_clear` 5 / 6 | Full suite exercises object creation, release, and cleanup; native lifetime tests exercise direct core ownership | All traversal functions execute. `visitproc` early-error arcs are not injectable through normal Python APIs; platform GC and shutdown order also affect which arcs are reachable. |

## Interpretation and follow-up

The 66.6% branch figure is a useful map of unobserved C control flow, not evidence that the unvisited branches are safe or unsafe by themselves. The highest-value paths already have direct tests for callback/condition failure, replay state, integer boundaries, metrics snapshots and selected allocation failures. Remaining work is targeted: add deterministic coverage only where a public or safely injectable condition exists, and keep platform-only or CPython visitor failure paths explicit instead of manufacturing production hooks to raise the percentage.

The CI workflow uploads the isolated native C coverage directory as the `native-c-coverage` artifact. The exact local output for this run is `/tmp/eventflow-final-native-coverage`; it was not copied into the repository because the committed table and source fingerprint are the durable summary, while CI retains the full HTML/JSON detail on hosted runs.
