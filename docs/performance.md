# Performance

Ormdantic's performance strategy is to keep Python ergonomic and move repeated runtime work into Rust.

Use this page when you want to run benchmarks or understand where Ormdantic spends runtime work. It is not a promise that every query is faster than every alternative. Query shape, indexes, network latency, and database behavior still matter.

## What is measured

The benchmark suite covers:

- Python-facing serialization and hydration
- table-handle CRUD
- query-expression paths
- joined and select-in relationship loading
- nested loader graphs
- reflection and migration flows
- Rust SQL and DML compilation
- dialect rendering
- schema diffing
- hydration planning
- select-in merging
- native driver execution

## ORM comparison report

The repository also includes a reproducible comparison report under
`benchmark/`. It compares Ormdantic, SQLAlchemy, and SQLModel against local
SQLite file databases. The default profile is sized for quick local runs; the
huge profile uses million-row read and write datasets.
The charts show every measured case so improvements and regressions stay visible
instead of being hidden behind a single aggregate number.

![Ormdantic speedup over SQLAlchemy and SQLModel](assets/benchmarks/default/ormdantic-orm-benchmark-speedup.svg)

![Ormdantic, SQLAlchemy, and SQLModel median latency](assets/benchmarks/default/ormdantic-orm-benchmark-latency.svg)

Regenerate the report and SVGs:

```console
uv run --group dev maturin develop --release
uv run --group benchmark python -m benchmark.run
```

Use the release native extension for report artifacts. Debug Rust builds are
for development feedback and can make native write paths look artificially slow.

Run the million-row profile:

```console
uv run --group dev maturin develop --release
uv run --group benchmark python -m benchmark.run --profile huge
```

Million-row profile results:

![Million-row Ormdantic speedup over SQLAlchemy and SQLModel](assets/benchmarks/huge/ormdantic-orm-benchmark-speedup.svg)

![Million-row Ormdantic, SQLAlchemy, and SQLModel median latency](assets/benchmarks/huge/ormdantic-orm-benchmark-latency.svg)

The command writes JSON, CSV, and SVG outputs under `benchmark/`, plus docs-ready
SVG copies under `docs/assets/benchmarks/`.

## Run Python benchmarks

Run the CodSpeed-enabled Python benchmarks locally:

```console
uv run pytest tests/benchmarks --codspeed
```

The same benchmark tests also work with the local `pytest-benchmark` plugin:

```console
uv run pytest tests/benchmarks
```

## Run Rust benchmarks

Install the CodSpeed cargo subcommand once:

```console
cargo install cargo-codspeed --locked
```

Run Rust benchmarks:

```console
cargo codspeed build
cargo codspeed run
```

Without CodSpeed, use Criterion compatibility locally:

```console
cargo bench --workspace
```

## Read benchmark results

Look for regressions in:

- query compilation
- relationship loading
- hydration
- driver execution
- migration reflection

Small benchmark wins are less important than keeping the Python API predictable and the Rust boundary stable.
