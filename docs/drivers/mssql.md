# SQL Server

SQL Server support is available when the Rust extension is built with the SQL Server engine feature.

## URL

```python
Ormdantic("mssql://sa:Password123@localhost:1433/master?trust_cert=true")
Ormdantic("mssql+pyodbc://user:password@localhost:1433/app?trust_cert=true")
```

## Supported Metadata

SQL Server support includes:

- schemas;
- table and column comments through extended properties;
- index comments;
- filegroups for tables and indexes;
- clustered and nonclustered primary keys, unique constraints, and indexes;
- filtered indexes;
- include columns;
- identity options;
- sequences;
- regular views;
- `OUTPUT` rows for insert/update/delete where modeled.

## Type Notes

SQL Server cannot sort or key old `TEXT`/`NTEXT` values. Ormdantic renders strings as bounded `NVARCHAR` for normal model strings.

## Transactions

SQL Server supports transactions and savepoints. Some DDL operations have backend-specific transaction and batch behavior; migrations render SQL Server batches where needed.
