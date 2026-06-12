# SQLite

SQLite is the easiest backend for local development, tests, and embedded applications.

## URL

```python
Ormdantic("sqlite:///app.sqlite3")
Ormdantic("sqlite:///:memory:")
```

## Behavior

- Uses SQLite identifier quoting with double quotes.
- Uses positional bind parameters internally.
- Supports table creation, indexes, checks, unique constraints, and foreign keys.
- Rebuilds tables for schema changes SQLite cannot alter in place.
- Uses exact decimal storage for Ormdantic-created decimal columns.

## Type Notes

SQLite is dynamically typed. Ormdantic still records logical column kinds in snapshots and DDL.

Important details:

- `str` fields render as `TEXT`.
- `bytes` fields render as `BLOB`.
- exact decimals created by Ormdantic use `DECIMAL_TEXT`;
- bounded string checks are represented with `CHECK` constraints;
- enum values can be represented by text checks.

## Migrations

SQLite migrations may need table rebuild operations for changes such as dropping columns, changing table options, or altering constraints. Ormdantic plans rebuild SQL rather than pretending SQLite supports every `ALTER TABLE` shape directly.
