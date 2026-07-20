# Field types and metadata

Ormdantic reads Pydantic model fields and converts them to runtime column metadata. Use this page when you need to understand how annotations, `Field(...)` constraints, and metadata objects become database schema.

## Supported field shapes

Common Python types map to database column kinds:

| Python annotation | Runtime intent |
| --- | --- |
| `str` | Text or bounded text, depending on constraints and backend. |
| `bytes` | Binary data. |
| `int` | Integer. |
| `float` | Real number. |
| `Decimal` | Exact decimal. |
| `bool` | Boolean or backend equivalent. |
| `datetime`, `date`, `time` | Temporal values. |
| `UUID` | Native UUID where supported, string fallback elsewhere. |
| `Enum` | Text check constraint or native enum where enabled. |
| `list[T]` | Relationship collection when `T` is a registered model. |
| `Model | PrimaryKeyType` | Relationship field that can hold a related model or its primary-key value. |
| `Model | PrimaryKeyType | None` | Optional relationship field. |

Pydantic `Field` constraints become column shape or check metadata where possible:

```python
from decimal import Decimal
from pydantic import BaseModel, Field


class Flavor(BaseModel):
    id: str
    name: str = Field(min_length=2, max_length=80)
    price: Decimal = Field(gt=0, max_digits=8, decimal_places=2)
```

## Column options

Use `TableColumn` for database-native options:

```python
from ormdantic import TableColumn


@db.table(
    pk="id",
    column_options={
        "id": TableColumn(identity=True),
        "name": TableColumn(comment="Public display name"),
        "price": TableColumn(numeric_precision=8, numeric_scale=2),
    },
)
class Flavor(BaseModel):
    id: int
    name: str
    price: Decimal
```

`TableColumn` supports comments, server defaults, computed columns, autoincrement, identity options, collations, numeric precision and scale, named foreign key options, enum type options, and SQLite conflict policies.

## Indexes and constraints

Use the short decorator options for ordinary cases:

```python
@db.table(pk="id", indexed=["name"], unique=["sku"])
class Product(BaseModel):
    id: str
    name: str
    sku: str
```

Use metadata models for named or backend-specific cases:

```python
from ormdantic import TableCheck, TableIndex, TableUnique


@db.table(
    pk="id",
    indexes=[
        TableIndex(
            name="product_name_idx",
            columns=["name"],
            comment="Lookup products by name",
        )
    ],
    check_constraints=[
        TableCheck(name="product_rating_check", expression="rating >= 0")
    ],
    unique_constraints=[
        TableUnique(name="product_sku_name_unique", columns=["sku", "name"])
    ],
)
class Product(BaseModel):
    id: str
    name: str
    sku: str
    rating: int
```

See [Metadata Models](../api/metadata.md).
