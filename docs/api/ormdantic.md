# Ormdantic

The `Ormdantic` class is the database registry and runtime facade.

Create one instance for one database URL:

```python
from ormdantic import Ormdantic

db = Ormdantic("sqlite:///app.sqlite3")
```

Use it to:

- decorate Pydantic models with `@db.table(...)`;
- initialize schema with `await db.init()`;
- access table handles with `db[Model]`;
- open `db.transaction()` and `db.session()` contexts;
- inspect the live database with `db.inspect()`;
- create snapshots, plans, and migration artifacts with `db.migrations`;
- register lifecycle hooks with `db.on(...)`.

::: ormdantic.orm.Ormdantic
