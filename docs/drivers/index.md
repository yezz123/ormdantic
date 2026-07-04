# Drivers

Ormdantic talks to databases through native Rust drivers compiled into `ormdantic._ormdantic`. Use this section when you choose a backend, debug a connection URL, or need to know which database features stay backend-specific.

Check your installed runtime:

```python
from ormdantic import runtime_capabilities

runtime_capabilities()
```

The result tells you which drivers are available in the installed extension.

## Supported backends

| Backend | URL schemes | Notes |
| --- | --- | --- |
| SQLite | `sqlite://`, `sqlite:///` | Best for local files, tests, and embedded deployments. |
| PostgreSQL | `postgresql://`, `postgres://`, `postgresql+asyncpg://` | Strongest fit for advanced schema features. |
| MySQL | `mysql://`, `mysql+pymysql://` | Uses MySQL-specific DDL and reflection behavior. |
| MariaDB | `mariadb://`, `mariadb+mariadbconnector://` | Separate dialect name with MySQL-family execution. |
| SQL Server | `mssql://`, `mssql+pyodbc://` | Feature-gated in Rust builds. |
| Oracle | `oracle://`, `oracle+oracledb://` | Feature-gated in Rust builds. |

## What Ormdantic normalizes

Ormdantic normalizes:

- identifier quoting
- bind parameter syntax
- basic scalar value conversion
- common data definition language (DDL) operations
- table, column, index, and constraint snapshots
- migration history storage
- relationship loading query shapes

The goal is not to pretend every database is the same. The goal is to make the common path predictable.

## What remains backend-specific

Ormdantic documents instead of hiding:

- string key length rules
- exact decimal representation
- transaction DDL behavior
- native enum support
- tablespaces and filegroups
- identity and autoincrement behavior
- view and materialized view reflection
- conflict syntax and returning or output behavior

Read the page for your backend before relying on generated SQL in production.
