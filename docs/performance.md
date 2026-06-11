# Performance

Ormdantic benchmarks both sides of the runtime:

- Python-facing serialization and hydration, table-handle CRUD, query-expression paths, joined/select-in relationship loading, nested loader graphs, and reflection/migration flows.
- Rust SQL/DML/expression compilation, dialect rendering, schema diffing, hydration planning, select-in merging, nested duplicate folding, engine migration/reflection planning, Python bridge conversion, and runtime driver execution.

The release gate for these groups is `.github/workflows/codspeed.yml`. It runs the Python benchmark suite through `uv run pytest tests/benchmarks --codspeed` and the Rust workspace benches through `cargo codspeed build` plus `cargo codspeed run`.

## Python Benchmarks

Run the CodSpeed-enabled Python benchmarks locally:

```bash
uv run pytest tests/benchmarks --codspeed
```

The same benchmark tests also work with the local `pytest-benchmark` plugin:

```bash
uv run pytest tests/benchmarks
```

## Rust Benchmarks

Install the CodSpeed cargo subcommand once:

```bash
cargo install cargo-codspeed --locked
```

Run Rust benchmarks:

```bash
cargo codspeed build
cargo codspeed run
```

Without CodSpeed, use Criterion compatibility locally:

```bash
cargo bench --workspace
```

## CodSpeed MCP

This repository includes a Cursor MCP configuration for CodSpeed. After enabling MCP servers in Cursor, agents can use the CodSpeed MCP endpoint to inspect benchmark context and performance history.
