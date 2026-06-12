# Performance

Ormdantic's performance strategy is to keep Python ergonomic and move repeated runtime work into Rust.

## What Is Measured

The benchmark suite covers:

- Python-facing serialization and hydration;
- table-handle CRUD;
- query-expression paths;
- joined and select-in relationship loading;
- nested loader graphs;
- reflection and migration flows;
- Rust SQL and DML compilation;
- dialect rendering;
- schema diffing;
- hydration planning;
- select-in merging;
- native driver execution.

## Python Benchmarks

Run the CodSpeed-enabled Python benchmarks locally:

```console
uv run pytest tests/benchmarks --codspeed
```

The same benchmark tests also work with the local `pytest-benchmark` plugin:

```console
uv run pytest tests/benchmarks
```

## Rust Benchmarks

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

## Reading Results

Look for regressions in:

- query compilation;
- relationship loading;
- hydration;
- driver execution;
- migration reflection.

Small benchmark wins are less important than keeping the Python API predictable and the Rust boundary stable.
