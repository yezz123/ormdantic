# Relationships

Relationships are derived from registered model fields and foreign key metadata.

## Scalar Relationship

```python
from __future__ import annotations

from pydantic import BaseModel


@db.table(pk="id")
class Supplier(BaseModel):
    id: str
    name: str


@db.table(pk="id")
class Flavor(BaseModel):
    id: str
    supplier: Supplier | str | None = None
```

The scalar field `supplier: Supplier | str | None` describes an optional relationship to `Supplier`. When you create a `Flavor`, the field can hold a `Supplier` instance or the supplier primary-key value. When you load the relationship, Ormdantic hydrates the field with a `Supplier` model.

## Collection Relationship

```python
from pydantic import Field


@db.table(pk="id", back_references={"flavors": "supplier"})
class Supplier(BaseModel):
    id: str
    name: str
    flavors: list["Flavor"] = Field(default_factory=list)


@db.table(pk="id")
class Flavor(BaseModel):
    id: str
    supplier: Supplier | str
```

Collections are loaded only when requested. Ormdantic does not run hidden I/O when you access `supplier.flavors`.

## Foreign Key Metadata

Simple foreign key options can live on a column:

```python
from ormdantic import TableColumn


@db.table(
    pk="id",
    column_options={
        "supplier": TableColumn(
            foreign_key_name="flavor_supplier_fk",
            on_delete="cascade",
        )
    },
)
class Flavor(BaseModel):
    id: str
    supplier: Supplier | str | None = None
```

Composite or table-level foreign keys use `TableForeignKey`:

```python
from ormdantic import TableForeignKey

TableForeignKey(
    name="flavor_supplier_pair_fk",
    columns=["supplier_id", "supplier_code"],
    foreign_table="supplier",
    foreign_columns=["id", "code"],
    on_delete="cascade",
    on_update="restrict",
)
```

## Relationship Expressions

Use `db.relation(...)` when you need relationship-aware query expressions:

```python
from ormdantic import column

supplier = db.relation(Flavor, "supplier")
query = supplier.has(column("name").like("North%"))
```

## Loading Related Data

Relationship loading is controlled by depth or loader options:

```python
from ormdantic import joinedload, selectinload

await db[Supplier].find_many(load=[selectinload("flavors")])
await db[Flavor].find_many(load=[joinedload("supplier")])
```

See [Loading Strategies](loading-strategies.md).
