# Crate Workspace Kickoff

## Goal

Record the first concrete Rust workspace split after the initial flat hydration prototype.

## Current Workspace

The Rust code is now organized as a Cargo workspace from the project root:

```text
Cargo.toml
rust/
  crates/
    ormdantic-core/
    ormdantic-schema/
    ormdantic-hydrate/
    ormdantic-py/
```

`pyproject.toml` points maturin at `rust/crates/ormdantic-py/Cargo.toml`, so Python packaging still builds the private `ormdantic._ormdantic` extension module.

## Crate Boundaries

### `ormdantic-core`

Shared primitives that should stay independent from Python:

- common result alias
- shared errors
- future identifiers and stable internal traits

### `ormdantic-schema`

Rust metadata structures:

- `TableDef`
- `ColumnAlias`
- future field, relationship, index, and constraint descriptors

This crate is where normalized Python/Pydantic metadata should land before query planning or hydration uses it.

### `ormdantic-hydrate`

Pure Rust hydration planning:

- `FlatHydrationPlan`
- column alias parsing through `ormdantic-schema`
- primary-key alias validation through `ormdantic-core`

The current crate intentionally avoids PyO3 so hydration planning can be unit-tested without Python.

### `ormdantic-py`

Python binding crate:

- exposes `ormdantic._ormdantic`
- maps Rust errors to Python exceptions
- converts Python rows into Python dictionaries/lists
- keeps PyO3-specific code away from core crates

## Python Contract

The Python side still calls:

```python
hydrate_flat_payload(
    tablename=...,
    pk=...,
    columns=...,
    rows=...,
    is_array=...,
)
```

That contract remains private and stable for now. If the Rust extension is unavailable, `_hydration.py` still uses the Python fallback.

## Testing Boundary

Use:

- `cargo check --workspace` for all crates.
- `cargo test -p ormdantic-core -p ormdantic-schema -p ormdantic-hydrate` for pure Rust tests.
- Python integration tests for `ormdantic-py`, because PyO3 extension crates can require Python linker setup when compiled as Rust test binaries.

## Next Migration Boundaries

1. Add richer schema metadata in `ormdantic-schema`.
2. Move relationship result-shape planning into `ormdantic-hydrate`.
3. Add a Rust row planner for the current joined alias format.
4. Keep `ormdantic-py` as the only crate that touches Python objects.
5. Add future `ormdantic-sql` and `ormdantic-dialects` crates after hydration and schema metadata are stable.
