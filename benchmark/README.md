# Ormdantic vs SQLAlchemy Benchmarks

This folder contains the reproducible benchmark report used by the README and
performance docs.

The suite compares Ormdantic and SQLAlchemy through their normal async ORM APIs
on local SQLite file databases. It measures:

- full-table counts;
- filtered counts;
- score-range counts;
- aggregate filtered projections;
- batched primary-key lookups.

Setup work is outside the timed section. Validation queries also run outside the
timed section, so each sample records only the measured operation.

## Run

```bash
uv sync --group dev --group benchmark
uv run --group dev maturin develop
uv run --group benchmark python -m benchmark.run
```

Or use the Makefile target:

```bash
make benchmark-report
```

The default run writes:

- `benchmark/results/ormdantic-vs-sqlalchemy.json`
- `benchmark/charts/ormdantic-vs-sqlalchemy-latency.svg`
- `benchmark/charts/ormdantic-vs-sqlalchemy-speedup.svg`
- `benchmark/charts/ormdantic-vs-sqlalchemy-summary.csv`
- docs-ready SVG copies under `docs/assets/benchmarks/`

## Tune

Use smaller inputs for a fast local smoke run:

```bash
uv run --group benchmark python -m benchmark.run \
  --rows 5000 \
  --lookup-count 250 \
  --iterations 3 \
  --warmups 1
```

Benchmarks are machine-sensitive. Compare trends on the same hardware and
Python version, and treat the charts as a snapshot of these measured cases, not
as a claim that every query shape is faster.
