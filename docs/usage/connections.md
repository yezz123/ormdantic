# Connections And Dialects

Ormdantic accepts SQLAlchemy-style database URLs, but execution is handled by Rust drivers.

## Supported URLs

```python
Ormdantic("sqlite:///app.sqlite3")
Ormdantic("postgresql://postgres:postgres@localhost:5432/postgres")
Ormdantic("postgresql+asyncpg://postgres:postgres@localhost:5432/postgres")
Ormdantic("mysql://root:password@localhost:3306/test")
Ormdantic("mysql+pymysql://root:password@localhost:3306/test")
Ormdantic("mariadb://root:password@localhost:3306/test")
Ormdantic("mssql+pyodbc://user:password@localhost:1433/test")
Ormdantic("oracle+oracledb://user:password@localhost:1521/service")
```

## Runtime Support

Default Python wheels are built from the private `ormdantic._ormdantic` extension with all runtime drivers compiled in.

| Dialect    | Native runtime | Notes                                                                                                            |
| ---------- | -------------- | ---------------------------------------------------------------------------------------------------------------- |
| SQLite     | Full           | Local baseline and default development engine.                                                                   |
| PostgreSQL | Full           | Parameter binding, row decoding, and transactions are supported.                                                 |
| MySQL      | Full           | Uses the Rust MySQL driver.                                                                                      |
| MariaDB    | Full           | Uses the MySQL protocol where compatible.                                                                        |
| SQL Server | Full           | Uses the pure Rust TDS driver. Add `trust_cert=true` for local/self-signed TLS test servers.                     |
| Oracle     | Full           | Uses the pure Rust Oracle TNS driver. Service names can be passed in the path or `service_name` query parameter. |

Source builds that bypass the Python extension can still choose Cargo features directly, but the standard maturin build includes the enterprise runtimes.

## Runtime Capabilities

Use `runtime_capabilities()` to inspect the database runtimes compiled into the installed extension:

```python
from ormdantic import runtime_capabilities

print(runtime_capabilities())
# {"sqlite": True, "postgresql": True, "mysql": True, "mariadb": True, "mssql": True, "oracle": True}
```

## Test Environment Variables

Native integration tests are gated by environment variables:

- `ORMDANTIC_POSTGRES_URL`
- `ORMDANTIC_MYSQL_URL`
- `ORMDANTIC_MARIADB_URL`
- `ORMDANTIC_MSSQL_URL`
- `ORMDANTIC_ORACLE_URL`
