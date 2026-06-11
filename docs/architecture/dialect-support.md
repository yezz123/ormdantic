# Dialect Support

| Dialect    | SQL compiler | Native execution           | CI                   |
| ---------- | ------------ | -------------------------- | -------------------- |
| SQLite     | Full         | Full                       | Required             |
| PostgreSQL | Full         | Full                       | Required service job |
| MySQL      | Full         | Full                       | Required service job |
| MariaDB    | Full         | Full                       | Required service job |
| SQL Server | Full         | Full with `mssql` feature  | Required service job |
| Oracle     | Full         | Full with `oracle` feature | Required service job |

## Notes

- SQLite is the local baseline for all ORM behavior tests.
- PostgreSQL, MySQL, MariaDB, SQL Server, and Oracle have gated Rust integration tests that run when their `ORMDANTIC_*_URL` variables are set.
- SQL Server and Oracle runtimes are available behind optional Cargo features for source builds because they add larger enterprise-driver dependencies; default Python extension builds include them.
- The dialect layer exposes backend bind-parameter limits, query compilation rejects oversized parameter sets before runtime execution, and select-in relationship loaders cap batches to the active backend limit.
- Runtime execution errors carry structured categories for common connection, syntax, constraint, transaction-conflict, timeout, and permission failures while preserving stable display messages.
- Live migration autogenerate reflects core table metadata across SQLite, PostgreSQL, MySQL, MariaDB, SQL Server, and Oracle, including schema-scoped table snapshots on schema-aware backends.
- `docker/databases/` provides a local Compose matrix for PostgreSQL, MySQL, MariaDB, SQL Server, and Oracle, plus a runner for the external migration smoke tests, relationship-loader stress suite, live autogenerate checks, and Rust engine tests.
