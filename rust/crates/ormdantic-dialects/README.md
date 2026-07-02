# ormdantic-dialects

SQL dialect support for Ormdantic.

`ormdantic-dialects` detects database kinds from names and SQLAlchemy-style URLs, then provides quoting, placeholder, insert-conflict upsert, returning, and JSON capability behavior to the SQL compiler and execution layer. SQL Server and Oracle use compiler-level `MERGE` upserts instead of insert conflict clauses.

## Public API

| Item                     | Purpose                                                            |
| ------------------------ | ------------------------------------------------------------------ |
| `DialectKind`            | Supported database kind enum with parsing from names or URLs.      |
| `Dialect`                | Trait implemented by each dialect.                                 |
| `SqliteDialect`          | SQLite quoting, placeholders, and capabilities.                    |
| `PostgresDialect`        | PostgreSQL quoting, placeholders, and capabilities.                |
| `MySqlDialect`           | MySQL quoting, placeholders, and capabilities.                     |
| `MariaDbDialect`         | MariaDB quoting, placeholders, and capabilities.                   |
| `MsSqlDialect`           | SQL Server quoting, placeholders, and capabilities.                |
| `OracleDialect`          | Oracle quoting, placeholders, and capabilities.                    |
| `AnyDialect`             | Runtime enum wrapper over all supported dialects.                  |
| `normalize_dialect_name` | Normalizes URL schemes and driver suffixes before dialect parsing. |

## Dependencies

Internal dependencies:

- `ormdantic-core`

External dependencies: none.

## Tests

The crate tests SQLAlchemy-style URL parsing, placeholder styles, quoting behavior, upsert conflict clauses, MERGE-only upsert rejection, and unknown dialect errors.
