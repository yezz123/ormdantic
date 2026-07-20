# First steps

This guide creates two related tables, inserts data, queries it, uses a session, and previews a migration diff. You can read it top to bottom if you are new, or scan the headings if you already know async ORMs.

You will build a small database with suppliers and flavors:

- `Supplier` stores where a flavor comes from
- `Flavor` stores the flavor name and points at a supplier
- `selectinload("supplier")` loads the related supplier in a separate batched query

## Create a database

Start by creating one `Ormdantic` object. It owns the database URL, registered table metadata, events, and the native runtime once initialized.

```python
from pydantic import BaseModel, Field

from ormdantic import Ormdantic, TableForeignKey, selectinload

db = Ormdantic("sqlite:///quickstart.sqlite3")
```

SQLite works well for the first run because it only needs a local file. Use [Drivers](drivers/index.md) when you are ready to connect PostgreSQL, MySQL, MariaDB, SQL Server, or Oracle.

## Define tables

Use normal Pydantic models. The `@db.table(...)` decorator adds database metadata beside the model.

```python
@db.table(pk="id", indexed=["name"])
class Supplier(BaseModel):
    id: str
    name: str = Field(min_length=2)


@db.table(
    pk="id",
    indexed=["name"],
    foreign_key_constraints=[
        TableForeignKey(
            name="flavor_supplier_fk",
            columns=["supplier"],
            foreign_table="supplier",
            foreign_columns=["id"],
            on_delete="cascade",
        )
    ],
)
class Flavor(BaseModel):
    id: str
    name: str
    supplier: Supplier | str | None = None
```

The `supplier` field accepts either a loaded `Supplier` object, the supplier primary key as a string, or `None`. That keeps inserts ergonomic and makes relationship loading explicit.

The foreign key metadata names the database constraint. Production schemas should name constraints because migrations and database error messages become easier to read.

## Create the schema

Initialize the database after you register the tables:

```python
async def setup() -> None:
    await db.init()
```

`init()` builds the native runtime and creates registered tables, namespaces, sequences, and views. For a new local database or a test database, this is the shortest path.

For an existing production database, use migrations instead. Migrations let you inspect generated SQL before applying changes.

## Insert rows

Use `db[Model]` to get the table handle for one model. The table handle owns CRUD and query methods for that model.

```python
async def seed() -> None:
    supplier = await db[Supplier].insert(
        Supplier(id="s1", name="North Roasters")
    )
    await db[Flavor].insert(
        Flavor(id="f1", name="Vanilla", supplier=supplier)
    )
```

Passing `supplier=supplier` stores the relationship using the supplier primary key. When you load the row later, choose whether to load the related supplier.

## Query rows

Use dictionary filters for common comparisons. Use loader options when you want related models.

```python
async def query() -> None:
    flavors = await db[Flavor].find_many(
        {"name": {"like": "Van%"}},
        order_by=["name"],
        load=[selectinload("supplier")],
    )

    for flavor in flavors.data:
        print(flavor.name, flavor.supplier.name if flavor.supplier else None)
```

`find_many(...)` returns a result object. Its `data` attribute contains hydrated Pydantic models.

For more complex SQL, use [query expressions](concepts/querying.md#use-expression-queries).

## Use a session

Use a session when several writes must commit or roll back together. The session commits on successful context exit and rolls back if an exception is raised.

```python
async def with_session() -> None:
    async with db.session() as session:
        supplier = Supplier(id="s2", name="South Roasters")
        flavor = Flavor(id="f2", name="Mocha", supplier="s2")
        session.add(supplier)
        session.add(flavor)
```

The session stages models in memory, flushes them, and commits the native transaction on success.

## Preview a migration

Use a migration preview when the database already exists and you want planned SQL instead of direct schema creation.

```python
def preview_migration() -> list[str]:
    before = db.migrations.live_snapshot()
    after = db.migrations.snapshot()
    return db.migrations.dry_run(before=before, after=after)
```

`live_snapshot()` inspects the current database. `snapshot()` reads the registered models. `dry_run(...)` compares them and returns SQL statements without applying them.

## Recap, step by step

You now saw the core Ormdantic loop:

1. Create `db = Ormdantic("sqlite:///quickstart.sqlite3")`
2. Register Pydantic models with `@db.table(...)`
3. Call `await db.init()` for a new local schema
4. Use `db[Model]` for inserts, queries, updates, counts, and deletes
5. Use loader options such as `selectinload("supplier")` for relationships
6. Use `db.session()` when several writes belong in one unit of work
7. Use migrations before changing an existing production database

Next, read [Database and tables](concepts/database-and-tables.md) for the mental model, or build the [Todo reference application](tutorial/index.md) for a complete runnable project.
