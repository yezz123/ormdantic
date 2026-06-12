# Database And Tables

The `Ormdantic` object is the registry for one database connection.

```python
from ormdantic import Ormdantic

db = Ormdantic("sqlite:///app.sqlite3")
```

It stores:

- the connection URL;
- registered table metadata;
- registered namespaces, sequences, and views;
- event handlers;
- the native Rust runtime once initialized;
- the migration manager.

## Register A Table

Use `@db.table(...)` on a Pydantic model:

```python
from pydantic import BaseModel


@db.table(pk="id", indexed=["email"], unique=["email"])
class User(BaseModel):
    id: str
    email: str
    active: bool = True
```

The model remains a Pydantic model. Ormdantic stores the database metadata beside it.

## Table Decorator Options

Common options:

| Option | Purpose |
| --- | --- |
| `pk` | Primary key field name. |
| `tablename` | Override the generated table name. |
| `indexed` | Create simple indexes for fields. |
| `unique` | Create simple unique constraints for fields. |
| `columns` | Per-field `TableColumn` metadata. |
| `indexes` | Explicit `TableIndex` objects. |
| `check_constraints` | Table-level `TableCheck` objects. |
| `unique_constraints` | Table-level `TableUnique` objects. |
| `foreign_key_constraints` | Composite or named `TableForeignKey` objects. |
| `comment` | Table comment where the backend supports it. |

Advanced backend options include PostgreSQL tablespaces and partitions, SQL Server filegroups and clustered primary keys, MySQL table options, SQLite conflict behavior, and Oracle table compression.

## Initialize Schema

```python
await db.init()
```

`init()` creates registered namespaces, sequences, tables, indexes, comments, backend-specific options, and views. For existing production databases, prefer migrations instead of calling `create_all()` blindly.

## Table Handles

Access a model's table handle with `db[Model]`:

```python
users = await db[User].find_many({"active": True})
```

The handle methods include:

- `find_one`;
- `find_many`;
- `insert`;
- `update`;
- `upsert`;
- `delete`;
- `count`;
- `select`;
- `update_where`.

See [Table API](../api/table.md).
