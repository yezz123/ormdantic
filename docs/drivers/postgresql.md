# PostgreSQL

PostgreSQL is the best fit when you want advanced schema metadata and strong reflection support.

## URL

```python
Ormdantic("postgresql://postgres:postgres@localhost:5432/postgres")
Ormdantic("postgresql+asyncpg://postgres:postgres@localhost:5432/postgres")
```

## Supported Metadata

PostgreSQL support includes:

- schemas through `DatabaseNamespace`;
- native enum types;
- comments on tables, columns, indexes, constraints, sequences, views, and enum types;
- tablespaces;
- indexes with methods, `WITH` storage parameters, operator classes, include columns, expressions, predicates, and `NULLS NOT DISTINCT`;
- exclusion constraints;
- sequences and identity options;
- regular and materialized views;
- transactional DDL where PostgreSQL supports it.

## Type Notes

- UUID can use PostgreSQL's native `UUID`.
- Decimal values use PostgreSQL numeric decoding.
- Bounded strings can render as `VARCHAR(n)`.
- Unbounded strings render as `TEXT`.

## Transactions

PostgreSQL supports isolation levels, read-only transactions, deferrable transactions, savepoints, and transactional DDL for most operations.

## Migrations

Reflection uses PostgreSQL catalogs to normalize default expressions, generated names, comments, namespaces, sequences, views, and enum types. When PostgreSQL rewrites view definitions or default expressions into catalog form, Ormdantic attempts to normalize equivalent definitions before reporting drift.
