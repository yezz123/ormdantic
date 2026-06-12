# Drivers

Ormdantic's Python package talks to databases through native Rust drivers compiled into `ormdantic._ormdantic`.

Check your installed runtime:

```python
from ormdantic import runtime_capabilities

runtime_capabilities()
```

## Supported Backends

| Backend | URL schemes | Notes |
| --- | --- | --- |
| SQLite | `sqlite://`, `sqlite:///` | Best for local files, tests, and embedded deployments. |
| PostgreSQL | `postgresql://`, `postgres://`, `postgresql+asyncpg://` | Strongest fit for advanced schema features. |
| MySQL | `mysql://`, `mysql+pymysql://` | Uses MySQL-specific DDL and reflection behavior. |
| MariaDB | `mariadb://`, `mariadb+mariadbconnector://` | Separate dialect name with MySQL-family execution. |
| SQL Server | `mssql://`, `mssql+pyodbc://` | Feature-gated in Rust builds. |
| Oracle | `oracle://`, `oracle+oracledb://` | Feature-gated in Rust builds. |

## What Is Normalized

Ormdantic normalizes:

- identifier quoting;
- bind parameter syntax;
- basic scalar value conversion;
- common DDL operations;
- table, column, index, and constraint snapshots;
- migration history storage;
- relationship loading query shapes.

## What Remains Backend-Specific

Ormdantic documents instead of hiding:

- string key length rules;
- exact decimal representation;
- transaction DDL behavior;
- native enum support;
- tablespaces and filegroups;
- identity/autoincrement behavior;
- view and materialized view reflection;
- conflict syntax and returning/output behavior.
