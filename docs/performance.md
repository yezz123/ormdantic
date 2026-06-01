# Performance

Ormdantic benchmarks both sides of the runtime:

- Python-facing serialization, hydration, table-handle CRUD, and query-expression paths.
- Rust SQL compilation and joined-query planning.

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
cargo bench -p ormdantic-sql
```

## CodSpeed MCP

This repository includes a Cursor MCP configuration for CodSpeed. After enabling MCP servers in Cursor, agents can use the CodSpeed MCP endpoint to inspect benchmark context and performance history.
