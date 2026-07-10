# Ormdantic, SQLAlchemy, and SQLModel Benchmarks

This folder contains the reproducible benchmark report used by the README and
performance docs.

The suite compares Ormdantic, SQLAlchemy, and SQLModel on local SQLite file
databases. It measures:

- chunked write inserts;
- full-table counts;
- filtered counts;
- score-range counts;
- aggregate filtered projections;
- scalar projection reads;
- batched primary-key lookups.

Setup work is outside the timed section. Validation queries also run outside the
timed section, so each sample records only the measured operation.
Build the native extension with `--release` before recording report artifacts;
debug Rust builds are useful for development, but they are not representative
performance inputs.

## Run

```bash
uv sync --group dev --group benchmark
uv run --group dev maturin develop --release
uv run --group benchmark python -m benchmark.run
```

Or use the Makefile target:

```bash
make benchmark-report
```

The default run writes:

- `benchmark/results/default-orm-benchmark.json`
- `benchmark/charts/default/ormdantic-orm-benchmark-latency.svg`
- `benchmark/charts/default/ormdantic-orm-benchmark-speedup.svg`
- `benchmark/charts/default/ormdantic-orm-benchmark-summary.csv`
- docs-ready SVG copies under `docs/assets/benchmarks/default/`

## Huge Profile

Run the million-row profile when you want a real large local workload:

```bash
uv run --group dev maturin develop --release
uv run --group benchmark python -m benchmark.run --profile huge
```

Or use:

```bash
make benchmark-huge
```

The huge profile uses:

- `1,000,000` read rows;
- `1,000,000` write rows;
- `10,000` primary-key lookups;
- one measured iteration with no warmup.

It writes separate artifacts under `benchmark/results/huge-orm-benchmark.json`,
`benchmark/charts/huge/`, and `docs/assets/benchmarks/huge/`.

## Tune

Use smaller inputs for a fast local smoke run:

```bash
uv run --group benchmark python -m benchmark.run \
  --rows 5000 \
  --write-rows 5000 \
  --lookup-count 250 \
  --iterations 3 \
  --warmups 1
```

Benchmarks are machine-sensitive. Compare trends on the same hardware and
Python version, and treat the charts as a snapshot of these measured cases, not
as a claim that every query shape is faster.
