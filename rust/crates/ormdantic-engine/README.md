# ormdantic-engine

Native database execution for Ormdantic.

`ormdantic-engine` opens database connections from SQLAlchemy-style URLs, executes parameterized SQL, returns row sets, and provides basic transaction primitives used by the Python runtime.

## Public API

| Item               | Purpose                                                                                                  |
| ------------------ | -------------------------------------------------------------------------------------------------------- |
| `DbValue`          | Database bind/result value enum (`Null`, `Integer`, `Real`, `Text`, `Bool`).                             |
| `QueryResult`      | Column names and rows returned by statements that produce rows.                                          |
| `execute_url`      | One-shot execution by database URL.                                                                      |
| `NativeConnection` | Persistent connection with `open`, `execute`, `begin`, `commit`, `rollback`, `savepoint`, and `dialect`. |
| `returns_rows`     | Helper that detects row-returning SQL.                                                                   |
| `sql_error`        | Converts driver errors into the shared Ormdantic error type.                                             |

## Drivers And Features

| Feature    | Driver module       | Notes                                          |
| ---------- | ------------------- | ---------------------------------------------- |
| `sqlite`   | `drivers::sqlite`   | Uses `rusqlite` with bundled SQLite.           |
| `postgres` | `drivers::postgres` | Uses the `postgres` crate.                     |
| `mysql`    | `drivers::mysql`    | Uses the `mysql` crate.                        |
| `mariadb`  | `drivers::mysql`    | Uses the MySQL protocol.                       |
| `mssql`    | `drivers::mssql`    | SQL Server runtime support through `tiberius`. |
| `oracle`   | `drivers::oracle`   | Oracle runtime support through `oracle-rs`.    |

Default features are `sqlite`, `postgres`, and `mysql`. Use `all-engines` to enable every feature gate.

## Dependencies

Internal dependencies:

- `ormdantic-core`
- `ormdantic-dialects`

External dependencies:

- `mysql`
- `oracle-rs` (optional)
- `postgres`
- `rusqlite`
- `tiberius` (optional)
- `tokio` (optional)
- `tokio-util` (optional)

## Tests

SQLite is covered by local tests. PostgreSQL, MySQL, MariaDB, SQL Server, and Oracle integration tests are gated by `ORMDANTIC_POSTGRES_URL`, `ORMDANTIC_MYSQL_URL`, `ORMDANTIC_MARIADB_URL`, `ORMDANTIC_MSSQL_URL`, and `ORMDANTIC_ORACLE_URL`.

Run the enterprise runtimes with:

```bash
cargo test -p ormdantic-engine --features mssql,oracle
```
