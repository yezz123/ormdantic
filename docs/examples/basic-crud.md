# Basic CRUD

This guide shows the core table lifecycle: register a model, create schema, insert a row, query it, update it, count it, and delete it.

Use it after [First steps](../quickstart.md) when you want a complete runnable script.

## What the example covers

- `Ormdantic(...)` database creation
- `@db.table(...)` model registration
- `await db.init()` schema creation
- `db[Model]` table handles
- `insert`, `find_one`, `find_many`, `update`, `count`, and `delete`

```python
--8<-- "examples/basic_crud.py"
```

Run it locally:

```console
python examples/basic_crud.py
```

## Production notes

Use `db.init()` for new local databases and tests. For production schema changes, use migrations so you can inspect generated SQL and keep migration history.
