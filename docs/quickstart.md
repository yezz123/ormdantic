# Quickstart

This guide creates two related tables, inserts data, queries it, and previews a migration diff.

## Create A Database

```python
from pydantic import BaseModel, Field

from ormdantic import Ormdantic, TableForeignKey, selectinload

db = Ormdantic("sqlite:///quickstart.sqlite3")
```

## Define Tables

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

`Supplier` and `Flavor` are normal Pydantic models. The decorators register database metadata with `db`.

## Create Schema

```python
async def setup() -> None:
    await db.init()
```

`init()` builds the native runtime and creates registered tables, namespaces, sequences, and views.

## Insert Rows

```python
async def seed() -> None:
    supplier = await db[Supplier].insert(
        Supplier(id="s1", name="North Roasters")
    )
    await db[Flavor].insert(
        Flavor(id="f1", name="Vanilla", supplier=supplier)
    )
```

`db[Model]` returns a `Table[Model]` handle. The handle owns CRUD and query methods for that model.

## Query Rows

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

Use dictionary filters for simple queries and expression helpers for composed SQL.

## Use A Session

```python
async def with_session() -> None:
    async with db.session() as session:
        supplier = Supplier(id="s2", name="South Roasters")
        flavor = Flavor(id="f2", name="Mocha", supplier_id="s2")
        session.add(supplier)
        session.add(flavor)
```

The session is a small async unit-of-work wrapper. It flushes staged changes and commits on successful context exit.

## Preview A Migration

```python
def preview_migration() -> list[str]:
    before = db.migrations.live_snapshot()
    after = db.migrations.snapshot()
    return db.migrations.dry_run(before=before, after=after)
```

Use migrations when the database already exists and you want planned SQL instead of `create_all()`.
