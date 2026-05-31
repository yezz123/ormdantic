# Migration History

This page preserves the important design context from Ormdantic's Rust migration. It is historical: the current architecture is described in [Rust Core](rust-core.md), [Dialect Support](dialect-support.md), and [vNext Migration](vnext-migration.md).

## Reference Lessons

The migration was informed by local reference checkouts of `pydantic-core` and SQLAlchemy. Those reference repositories are ignored and are not part of Ormdantic's source tree.

From `pydantic-core`, Ormdantic adopted these principles:

- Use maturin as the Python build backend.
- Keep one focused private PyO3 extension module.
- Keep Python-visible functions small and batch-oriented.
- Let Rust own compiled state while Python owns the public API.

From SQLAlchemy, Ormdantic adopted these boundaries:

- Keep SQL expression and compiler behavior separate from execution.
- Keep database-specific behavior in dialect layers.
- Treat async support as an execution concern.
- Document transaction, DDL, and dialect behavior as compatibility work rather than syntax-only work.

## Workspace Kickoff

The first Rust workspace split introduced pure Rust crates for shared primitives, schema metadata, and hydration planning, plus a private PyO3 binding crate:

- `ormdantic-core`
- `ormdantic-schema`
- `ormdantic-hydrate`
- `ormdantic-py`

This kept `ormdantic-py` as the only crate that touched Python objects and allowed hydration planning to be tested without a Python runtime.

## vNext Foundation

The vNext foundation expanded the workspace to include dialect and SQL compiler crates:

- `ormdantic-dialects`
- `ormdantic-sql`

At this stage Rust owned metadata validation, result-shape planning, and SQL compilation primitives. Python still owned Pydantic model introspection, CRUD orchestration, object construction, and the fallback runtime paths.

## Rust-Compiled Query Runtime

The next slice routed supported CRUD paths through Rust-compiled SQL:

- Find one by primary key.
- Find many with equality filters, ordering, limit, and offset.
- Count with equality filters.
- Insert, update, upsert, and delete by primary key.

Bind values were passed in the order returned by the Rust compiler, letting each dialect control placeholders instead of interpolating values into SQL strings.

## Dialect And Test Expansion

The dialect layer grew support for SQLAlchemy-style connection names and schemes:

- SQLite and `sqlite+aiosqlite`.
- PostgreSQL and `postgresql+asyncpg`.
- MySQL and common MySQL drivers such as `mysql+pymysql`.
- MariaDB and `mariadb+mariadbconnector`.
- SQL Server and `mssql+pyodbc`.
- Oracle and `oracle+oracledb`.

Rust integration tests were split across public crate test directories so each crate could verify its own contract.

## Query Builder Removal

Once Rust query compilation covered the current CRUD and depth-based relationship query shapes, Ormdantic removed the Python query-builder dependency and the old Python fallback query construction paths.

At this stage Python still owned Pydantic declarations, relationship discovery, and model construction. Rust owned current SQL generation and dialect rendering.

## Full Rust Core

The final vNext slice moved Ormdantic to a Rust-backed core:

- Pydantic v2 model introspection through `ormdantic._introspect`.
- Native Rust execution through `ormdantic-engine`.
- Rust-backed async wrapper classes in `ormdantic.engine`.
- Rust DDL generation through `ormdantic._ormdantic`.
- Rust query compilation for CRUD and depth-based joins.
- Rust value conversion, snake-case naming, and nested row folding.
- SQLAlchemy and PyPika removed from runtime dependencies.

Python remains the user-facing API and final model-construction layer. Rust handles the current internal runtime below that boundary.

## Remaining Work

The Rust migration leaves some future work intentionally outside this cleanup:

- Broaden native integration testing in CI.
- Expand DDL support for advanced defaults and migration workflows.
- Continue SQLAlchemy parity work for advanced query constructs, migrations, reflection, and association-style helpers.
