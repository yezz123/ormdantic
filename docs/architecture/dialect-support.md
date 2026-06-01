# Dialect Support

| Dialect | SQL compiler | Native execution | CI |
| --- | --- | --- | --- |
| SQLite | Full | Full | Required |
| PostgreSQL | Full | Full | Required service job |
| MySQL | Full | Full | Required service job |
| MariaDB | Full | Full | Required service job |
| SQL Server | Partial | Full with `mssql` feature | Optional/manual |
| Oracle | Partial | Full with `oracle` feature | Optional/manual |

## Notes

- SQLite is the local baseline for all ORM behavior tests.
- PostgreSQL, MySQL, and MariaDB have gated Rust integration tests that run when `ORMDANTIC_POSTGRES_URL`, `ORMDANTIC_MYSQL_URL`, or `ORMDANTIC_MARIADB_URL` are set.
- SQL Server and Oracle URLs are parsed and their SQL dialects are represented. Their native runtimes are available behind optional Cargo features because they add larger enterprise-driver dependencies.
