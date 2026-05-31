# Full Rust Core Checkpoint

## Goal

Record the vNext slice that moved Ormdantic to Pydantic v2 and replaced SQLAlchemy execution/DDL with Rust.

## Completed

- Pydantic v2 model introspection through `ormdantic/_introspect.py`.
- Native Rust execution through `ormdantic-engine`.
- Rust-backed async Python wrapper in `ormdantic/engine.py`.
- Rust DDL generation exposed through `ormdantic._ormdantic`.
- Rust query compilation for CRUD and current depth-based joins.
- Rust value conversion and snake case naming bridges.
- Rust nested row folding for relationship result payloads.
- SQLAlchemy dependency removed from Python runtime dependencies.

## Python Boundary

Python still owns:

- Pydantic model definitions.
- The `@database.table` decorator.
- Relationship discovery from Pydantic annotations.
- Final Pydantic model construction.

Everything else in the current ORM runtime now routes through Rust.

## Remaining Future Work

- Harden PostgreSQL parameter binding in the native engine.
- Add native integration tests for PostgreSQL in CI.
- Expand DDL support for advanced indexes, defaults, and check constraints.
- Add a public vNext migration guide for users relying on `_engine` or `_metadata`.
