# Ormdantic Usage

## Create a database

Ormdantic uses a native Rust runtime. The Python API registers Pydantic models and then delegates DDL, CRUD, filtering, counting, relationship-depth planning, and execution to Rust-owned table handles.

```python
from ormdantic import Ormdantic

connection = "sqlite+aiosqlite:///db.sqlite3"

database = Ormdantic(connection)
```

## Create a table

To create tables decorate a pydantic model with the `database.table` decorator, passing the database information ex. `Primary key`, `schema`, `foreign keys`, `Indexes`, `back_references`, `unique_constraints` etc. to the decorator call.

### Table Restrictions

- Tables must have a single column primary key.
- The primary key column must be the first column.
- Relationships must `union-type` the foreign model and that models primary key.

```python
from decimal import Decimal
from enum import Enum
from uuid import UUID, uuid4
from pydantic import BaseModel, Field
from ormdantic import (
    TableCheck,
    TableColumn,
    TableExclusion,
    TableForeignKey,
    TableIndex,
    TableUnique,
)


class Roast(Enum):
    LIGHT = "light"
    DARK = "dark"


database.sequence(
    "flavor_id_seq",
    schema="inventory",
    data_type="bigint",
    start=10,
    increment=5,
)


@database.table(
    pk="id",
    schema="inventory",
    indexed=["name"],
    comment="Coffee flavor catalog metadata",
    tablespace="fastspace",
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
    mysql_collation="utf8mb4_unicode_ci",
    mysql_row_format="DYNAMIC",
    postgres_unlogged=True,
    sqlite_strict=True,
    sqlite_without_rowid=True,
    indexes=[
        TableIndex(
            name="flavor_active_name_idx",
            columns=["name"],
            where="deleted_at IS NULL",
            expressions=["LOWER(name)"],
            postgres_with={"fillfactor": 70},
        )
    ],
    unique_constraints=[
        TableUnique(
            name="flavor_name_created_unique",
            columns=["name", "created_at"],
            deferrable=True,
            initially_deferred=True,
            nulls_not_distinct=True,
            sqlite_on_conflict="IGNORE",
        )
    ],
    foreign_key_constraints=[
        TableForeignKey(
            name="flavor_supplier_pair_fk",
            columns=["supplier_id", "supplier_code"],
            foreign_table="supplier",
            foreign_columns=["id", "code"],
            on_delete="cascade",
        )
    ],
    exclusion_constraints=[
        TableExclusion(
            name="flavor_active_name_exclusion",
            columns=[("name", "=")],
            using="btree",
            where="deleted_at IS NULL",
        )
    ],
    column_options={
        "id": TableColumn(
            server_default="nextval('inventory.flavor_id_seq')",
            sqlite_on_conflict_primary_key="REPLACE",
        ),
        "name": TableColumn(
            comment="Display name shown to customers",
            sqlite_on_conflict_not_null="FAIL",
            sqlite_on_conflict_unique="IGNORE",
        ),
        "created_at": TableColumn(server_default="CURRENT_TIMESTAMP"),
        "name_lower": TableColumn(computed="LOWER(name)", computed_persisted=True),
        "roast": TableColumn(
            enum_type_name="coffee_roast_kind",
            enum_schema="inventory",
        ),
    },
    check_constraints=[
        TableCheck(
            name="flavor_name_not_empty_check",
            expression="LENGTH(name) > 0",
        )
    ],
)
class Flavor(BaseModel):
     """A coffee flavor."""

     id: UUID = Field(default_factory=uuid4)
     name: str = Field(max_length=63)
     roast: Roast
     supplier_id: UUID
     supplier_code: str = Field(pattern=r"^[A-Z0-9_-]+$")
     rating: int = Field(ge=0, le=100, multiple_of=5)
     price: Decimal = Field(max_digits=12, decimal_places=2)
     name_lower: str | None = None
     created_at: str | None = None
     deleted_at: str | None = None


database.view(
    "active_flavors",
    "SELECT id, name, roast FROM inventory.flavor WHERE deleted_at IS NULL",
    schema="inventory",
)
```

## Queries

After the models are registered, initialize the Rust runtime and create the tables.

### `Init()`

- Register models as ORM models and initialize the database.

`database.init()` discovers relationships, builds the Rust runtime table registry, and creates the tables.

```python
async def demo() -> None:
    async def _init() -> None:
        await db.init()
        await db.drop_all()
        await db.create_all()
    await _init()
```

### `Insert()`

Now let's imagine we have another table called `Coffee` that has a foreign key to `Flavor`.

```python
@database.table(pk="id")
class Coffee(BaseModel):
     """Drink it in the morning."""

     id: UUID = Field(default_factory=uuid4)
     sweetener: str | None = Field(max_length=63)
     sweetener_count: int | None = None
     flavor: Flavor | UUID
```

After we create the table, we can insert data into the table, using the `database.insert` method, is away we insert a Model Instance.

```python
# Create a Flavor called "Vanilla"
vanilla = Flavor(name="Vanilla")

# Insert the Flavor into the database
await database[Flavor].insert(vanilla)

# Create a Coffee with the Vanilla Flavor
coffee = Coffee(sweetener="Sugar", sweetener_count=1, flavor=vanilla)

# Insert the Coffee into the database
await database[Coffee].insert(coffee)
```

### Searching Queries

As we know, in SQL, we can search for data using different methods, ex. `WHERE`, `LIKE`, `IN`, `BETWEEN`, etc.

In Ormdantic, we can search for data using the `database.find_one` or `database.find_many` methods.

#### `Find_one()`

- `Find_one` used to find a Model instance by Primary Key, its could also find with `depth` parameter.

```python
     # Find one
     vanilla = await database[Flavor].find_one(flavor.id)
     print(vanilla.name)

     # Find one with depth.
     find_coffee = await database[Coffee].find_one(coffee.id, depth=1)
     print(find_coffee.flavor.name)
```

#### `Find_many()`

- `Find_many` used to find Model instances by some condition ex. `where`, `order_by`, `order`, `limit`, `offset`, `depth`.

```python
     # Find many
     await database[Flavor].find_many()

     # Get paginated results.
     await database[Flavor].find_many(
          where={"name": "vanilla"}, order_by=["id", "name"], limit=2, offset=2
     )
```

### `Update` / `Upsert` Queries

##### `Update()`

The modification of data that is already in the database is referred to as updating. You can update individual rows, all the rows in a table, or a subset of all rows. Each column can be updated separately; the other columns are not affected.

```python
     # Update a Flavor
     flavor.name = "caramel"
     await database[Flavor].update(flavor)
```

##### `Upsert`

The `Upsert` method is similar to the Synchronize method with one exception; the `Upsert` method does not delete any records. The `Upsert` method will result in insert or update operations. If the record exists, it will be updated. If the record does not exist, it will be inserted.

```python
     # Upsert a Flavor
     flavor.name = "mocha"
     await database[Flavor].upsert(flavor)
```

### `Delete()`

The `DELETE` statement is used to delete existing records in a table.

```python
     # Delete a Flavor
     await database[Flavor].delete(flavor.id)
```

### `Count()`

To count the number of rows of a table or in a result set you can use the `count` function.

```python
     # Count
     count = await database[Flavor].count()
     print(count)
```

- It's support also `Where` and `Depth`

```python
     count_advanced = await database[Coffee].count(
          where={"sweetener": 2}, depth=1
     )
     print(count_advanced)
```
