# Dialect Support

| Dialect | SQL compiler | Native execution | CI |
| --- | --- | --- | --- |
| SQLite | Full | Full | Required |
| PostgreSQL | Full | Full | Required service job |
| MySQL | Full | Full | Required service job |
| MariaDB | Full | Full | Required service job |
| SQL Server | Partial | Optional gate | Optional/manual |
| Oracle | Partial | Optional gate | Optional/manual |

## Notes

- SQLite is the local baseline for all ORM behavior tests.
- PostgreSQL, MySQL, and MariaDB have gated Rust integration tests that run when `ORMDANTIC_POSTGRES_URL`, `ORMDANTIC_MYSQL_URL`, or `ORMDANTIC_MARIADB_URL` are set.
- SQL Server and Oracle URLs are parsed and their SQL dialects are represented, but native runtime support is kept behind optional gates because wheel portability depends on driver/client setup.
