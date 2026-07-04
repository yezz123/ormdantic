# Dialect support

This page summarizes backend support across the Rust SQL compiler, native execution layer, upsert behavior, row-returning data manipulation language (DML), and continuous integration (CI) coverage.

| Dialect    | SQL compiler | Native execution           | Upsert strategy             | Row-returning DML | CI                   |
| ---------- | ------------ | -------------------------- | --------------------------- | ----------------- | -------------------- |
| SQLite     | Full         | Full                       | `ON CONFLICT`               | `RETURNING`       | Required             |
| PostgreSQL | Full         | Full                       | `ON CONFLICT`               | `RETURNING`       | Required service job |
| MySQL      | Full         | Full                       | `ON DUPLICATE KEY UPDATE`   | Not compiled      | Required service job |
| MariaDB    | Full         | Full                       | `ON DUPLICATE KEY UPDATE`   | `RETURNING`       | Required service job |
| SQL Server | Full         | Full with `mssql` feature  | `MERGE`                     | Not compiled      | Required service job |
| Oracle     | Full         | Full with `oracle` feature | `MERGE`                     | Not compiled      | Required service job |

## Support notes

- SQLite is the local baseline for all ORM behavior tests.
- PostgreSQL, MySQL, MariaDB, SQL Server, and Oracle have gated Rust integration tests that run when their `ORMDANTIC_*_URL` variables are set.
- SQL Server and Oracle runtimes are available behind optional Cargo features for source builds because they add larger enterprise-driver dependencies; default Python extension builds include them.
- SQL Server and Oracle upserts compile as `MERGE` because those backends do not support PostgreSQL-style insert conflict clauses.
- SQL Server `OUTPUT` and Oracle `RETURNING INTO` require backend-specific result-bind handling and are not advertised as generic `RETURNING` support by the SQL compiler.
- The dialect layer exposes backend bind-parameter limits, query compilation rejects oversized parameter sets before runtime execution, and select-in relationship loaders cap batches to the active backend limit.
- Runtime execution errors carry structured categories for common connection, syntax, constraint, transaction-conflict, timeout, and permission failures while preserving stable display messages.
- Live migration autogenerate reflects core table metadata across SQLite, PostgreSQL, MySQL, MariaDB, SQL Server, and Oracle, including schema-scoped table snapshots on schema-aware backends.
- `docker/databases/` provides a local Compose matrix for PostgreSQL, MySQL, MariaDB, SQL Server, and Oracle, plus a runner for the external migration smoke tests, relationship-loader stress suite, live autogenerate checks, and Rust engine tests.
