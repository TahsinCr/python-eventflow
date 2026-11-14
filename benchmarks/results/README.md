# EventFlow benchmark results

Superseded benchmark archives and retired native-prototype result files
(`benchmarks/results/native/`) are not kept checked into this directory. Store
temporary pyperf JSON files here locally if needed (`benchmarks/results/` is
excluded from source distributions and from the pyperf source fingerprint).

## Comparing pure Python vs compiled `eventflow._core`

Install development dependencies (which builds `eventflow._core` in place if a C
compiler is available):

```bash
python -m pip install -e ".[dev]"
```

Run matching workloads with `EVENTFLOW_NO_EXTENSIONS=1` (pure-Python fallback)
and with default extensions enabled, then compare them using `pyperf`:

```bash
EVENTFLOW_NO_EXTENSIONS=1 python -m benchmarks.benchmark_publish --rigorous --output benchmarks/results/publish-python.json
python -m benchmarks.benchmark_publish --rigorous --output benchmarks/results/publish-core.json
python -m pyperf compare_to benchmarks/results/publish-python.json benchmarks/results/publish-core.json --table
```

Each JSON result records `eventflow_core` (`c` or `python`), `eventflow_gil`
(`enabled`, `disabled`, or `unavailable`), Git revision, source dirty status,
and the SHA-256 fingerprint of `pyproject.toml`, `eventflow/`, and
`benchmarks/`. Keep the interpreter build, GIL mode, machine load, and pyperf
flags identical between runs.
