# Retained benchmark results

This directory keeps the latest useful benchmark evidence for EventFlow 1.3.2
and 1.4.0, plus a comparison of what the data can and cannot establish.

The [16 November dispatch/history audit](v1.4.0/audit_2026-11-16.md) retains
source-hashed cache-policy and matched-buffer screening data, raw latency
diagnostics and their limitations. Production policies remain unchanged.

The [17 November route-cache FIFO experiment](unreleased/route-cache-fifo.md)
compares the existing admission-only policy with a bounded FIFO candidate on
GIL-enabled and free-threaded CPython. It records why the production policy was
kept, with source hashes, pyperf datasets, selected raw latency samples and a
full per-case CSV.

The [native C coverage review](unreleased/native-c-coverage.md) records current
`_core.c` line, function and branch-arc coverage and maps the remaining gaps to
their tested paths and reachability limits.

## Datasets

- [`v1.3.2/`](v1.3.2/) contains the 2026-11-09 broad performance screen: 31
  pyperf JSON files and a 409-row CSV summary. The JSON files retain raw
  measurements and source/interpreter metadata.
- [`v1.4.0/`](v1.4.0/) contains all 1.4.0 results directly in one folder:
  13 benchmark-module JSON files with 183 cases (180 paired with 1.3.2), four
  focused history-query JSON files, one history summary CSV, and two routing
  summary CSVs.
- [`unreleased/`](unreleased/) contains current route-cache and native coverage
  experiments: 60 paired profiles, merged pyperf datasets for each runtime
  group, selected raw latency samples, a source/patch manifest, the decision
  report, native C coverage review, and Mavflow integration data.
- [`v1.3.2-v1.4.0-comparison.md`](v1.3.2-v1.4.0-comparison.md) explains the
  pure-Python release comparison, native backend evidence, and remaining
  measurement limits.

The 1.3.2 files were recovered from Git history at the last commit before the
old result archive was pruned. The 1.4.0 broad screen used the existing
benchmark modules and matching settings; its focused runtime measurements are
kept alongside it in the same version folder. Superseded prototype runs and
older overlapping archives are not retained here. No additional benchmark
runner or comparison script was added.
