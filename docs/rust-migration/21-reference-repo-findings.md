# Reference Repo Findings

## Goal

Summarize the implementation lessons from locally cloned references:

- `.references/pydantic-core`
- `.references/sqlalchemy`

These repositories are ignored and should not be committed.

## Pydantic-Core Lessons

`pydantic-core` uses maturin directly as the build backend and exposes a focused PyO3 extension module:

- `pyproject.toml` sets `build-backend = "maturin"` and configures `[tool.maturin]`.
- `Cargo.toml` builds a `cdylib` and uses `pyo3`.
- `src/lib.rs` defines the Python module with `#[pymodule]`.
- Core objects such as `SchemaValidator` and `SchemaSerializer` are `#[pyclass]` wrappers around Rust-owned internals.
- Python-visible functions and classes stay small; most logic lives behind Rust modules.

Useful Ormdantic takeaway:

- Start with one internal PyO3 module, not a full workspace.
- Keep the Python/Rust boundary small and batch-oriented.
- Let Rust own compiled state, and let Python keep the public API.
- Use a private module name such as `ormdantic._rust` or `ormdantic._ormdantic`.

## SQLAlchemy Lessons

SQLAlchemy separates concerns that Ormdantic should also keep separate:

- SQL expression and compiler behavior live under `sqlalchemy/sql`.
- Database-specific behavior lives under dialect packages like `dialects/postgresql` and `dialects/sqlite`.
- Engine/execution protocols are separated from SQL construction.
- Async support is an execution concern, not mixed into the SQL compiler itself.
- Dialects document transaction and DDL quirks extensively.

Useful Ormdantic takeaway:

- Do not mix dialect rules into generic query planning.
- Keep SQL generation separate from async execution.
- Preserve SQLAlchemy execution during the first sprint.
- Treat SQLite and PostgreSQL transaction behavior as real compatibility work, not just SQL syntax differences.

## First Sprint Decisions

The first implementation sprint should keep the plan narrow:

- Add a minimal PyO3/maturin skeleton.
- Prototype only flat row hydration first.
- Return Python dictionaries/lists, not Pydantic model instances.
- Keep SQLAlchemy execution unchanged while Rust query generation is introduced.
- Benchmark before claiming wins.

## Deferred Decisions

These should not be part of the first sprint:

- Full pydantic-core schema integration.
- Removing SQLAlchemy execution.
- Native Rust database execution.
- Pydantic v2 migration.
- SQLAlchemy-style session/unit-of-work.
