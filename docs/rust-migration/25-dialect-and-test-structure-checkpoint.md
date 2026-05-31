# Dialect And Test Structure Checkpoint

## Goal

Record the slice that broadened Rust dialect recognition and split Rust tests into maintainable integration test folders.

## Implemented

The Rust dialect layer now recognizes SQLAlchemy-style dialect names and connection schemes for:

- SQLite and `sqlite+aiosqlite`
- PostgreSQL and `postgresql+asyncpg`
- MySQL and common MySQL drivers such as `mysql+pymysql`
- MariaDB and `mariadb+mariadbconnector`
- SQL Server and `mssql+pyodbc`
- Oracle and `oracle+oracledb`

These dialects expose identifier quoting, placeholder style, and capability flags at the compiler layer. This does not mean every database has full runtime integration yet; it means the Rust compiler can now reason about these SQLAlchemy-style connection identifiers instead of treating them as unknown.

## Rust Test Structure

Rust now has integration test folders across the core crates:

- `rust/crates/ormdantic-core/tests/`
- `rust/crates/ormdantic-schema/tests/`
- `rust/crates/ormdantic-hydrate/tests/`
- `rust/crates/ormdantic-dialects/tests/`
- `rust/crates/ormdantic-sql/tests/`

The older inline tests remain useful for module-local cases, while integration tests cover public crate behavior and future cross-crate contracts.

## Python Cleanup

Removed an unused `OrmQuery.get_patch_queries()` stub and unused constructor state from `ormdantic/generator/_query.py`.

The old Python query-builder fallback was removed after Rust joined-select compilation covered current depth-based relationship queries.

## Remaining SQLAlchemy Parity Work

SQLAlchemy-level parity still requires:

- Rust relationship join compilation.
- Select-in relationship loading.
- Nested row folding in Rust.
- Transaction/session/unit-of-work decisions.
- Native migration/Alembic interop decisions.
- Driver-specific runtime verification beyond SQLite and PostgreSQL.

The next safe migration target is moving nested result folding fully into Rust. Python still owns Pydantic object construction.
