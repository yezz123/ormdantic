# Rust Crate Layout

## Goal

Plan a Rust workspace that can grow from a small PyO3 extension into a full ORM core without becoming a single oversized crate.

## Implemented Initial Layout

The migration now starts with a Cargo workspace at the repository root and a small set of focused crates:

```text
Cargo.toml
rust/
  crates/
    ormdantic-core/
    ormdantic-schema/
    ormdantic-hydrate/
    ormdantic-dialects/
    ormdantic-sql/
    ormdantic-py/
```

`pyproject.toml` points maturin at `rust/crates/ormdantic-py/Cargo.toml`, which keeps the Python package build tied to the PyO3 binding crate while the core crates stay independent from Python.

## Long-Term Workspace

```text
Cargo.toml
rust/
  crates/
    ormdantic-core/
    ormdantic-schema/
    ormdantic-sql/
    ormdantic-dialects/
    ormdantic-hydrate/
    ormdantic-py/
```

## Crate Responsibilities

### `ormdantic-core`

Shared primitives:

- identifiers
- errors
- result types
- feature flags
- stable internal traits

### `ormdantic-schema`

Rust-native metadata:

- table definitions
- fields and type categories
- primary keys
- indexes
- constraints
- relationships
- schema registry

### `ormdantic-sql`

SQL representation:

- SQL AST
- expression tree
- select/insert/update/delete/upsert builders
- bind parameter model
- query cache keys

### `ormdantic-dialects`

Database-specific behavior:

- identifier quoting
- placeholder style
- type rendering
- conflict/upsert syntax
- limit/offset syntax
- JSON operators
- returning support

### `ormdantic-hydrate`

Row-to-object payload shaping:

- column alias parsing
- nested object tree construction
- relationship deduplication
- JSON decoding
- optional batch streaming design

### `ormdantic-py`

PyO3 bindings:

- Python module exports
- conversion from Python metadata descriptors
- conversion of rows and values
- exception mapping
- compatibility shims for internal Python callers

## Initial Dependencies

Use conservative Rust dependencies:

- `pyo3` for Python bindings.
- `serde` and `serde_json` for metadata/value interchange.
- `thiserror` for structured Rust errors.
- `indexmap` if deterministic insertion order is needed.
- `smallvec` only after profiling shows allocation pressure.

Avoid introducing SQL parser dependencies unless Ormdantic needs to parse user-provided SQL. The core should generate SQL from an AST, not parse strings.

## Workspace Evolution Rules

- Begin with fewer crates and split only when compile boundaries or ownership become clear.
- Keep PyO3-specific code out of core logic.
- Keep dialect-specific syntax out of generic query planning.
- Keep Python compatibility decisions in `ormdantic-py` and Python wrapper modules.
- Avoid leaking Rust struct layout into the public Python API.

## Acceptance Criteria

- The first crate can build a Python extension locally.
- Hydration logic can be unit-tested without Python.
- PyO3 bindings can be integration-tested from Python.
- Future SQL and dialect crates can be added without moving public Python APIs.
