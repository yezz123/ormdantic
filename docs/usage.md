## Create SQLAlchemy engine

Ormdantic uses SQLAlchemy under hood to run different queries, which is why we need to initialize by creating an asynchronous engine.

> **Note**: You will use the `connection` parameter to pass the connection to the engine directly.

```python
from ormdantic import Ormdantic

connection = "sqlite+aiosqlite:///db.sqlite3"

database = Ormdantic(connection)
```

**Note**: You can use any asynchronous engine, check out the [documentation](https://docs.sqlalchemy.org/en/14/core/engines.html) for more information.

## Create a table

To create tables decorate a pydantic model with the `database.table` decorator, passing the database information ex. `Primary key`, `foreign keys`, `Indexes`, `back_references`, `unique_constraints` etc. to the decorator call.

### Table Restrictions

* Tables must have a single column primary key.
* The primary key column must be the first column.
* Relationships must `union-type` the foreign model and that models primary key.

```python
from uuid import uuid4
from pydantic import BaseModel, Field

@database.table(pk="id", indexed=["name"])
class Flavor(BaseModel):
     """A coffee flavor."""

     id: UUID = Field(default_factory=uuid4)
     name: str = Field(max_length=63)
```

## Queries

Now after we create the table, we can initialize the database with the table and then run different queries.

### `Init()`

* Register models as ORM models and initialize the database.

We use `database.init` will Populate relations information and create the tables.

```python
async def demo() -> None:
    async def _init() -> None:
        async with db._engine.begin() as conn:
            await db.init()
            await conn.run_sync(db._metadata.drop_all)
            await conn.run_sync(db._metadata.create_all)
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

* `Find_one`  used to find a Model instance by Primary Key, its could also find with `depth` parameter.

```python
     # Find one
     vanilla = await database[Flavor].find_one(flavor.id)
     print(vanilla.name)

     # Find one with depth.
     find_coffee = await database[Coffee].find_one(coffee.id, depth=1)
     print(find_coffee.flavor.name)
```

#### `Find_many()`

* `Find_many` used to find Model instances by some condition ex. `where`, `order_by`, `order`, `limit`, `offset`, `depth`.

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

* It's support also `Where` and `Depth`

```python
     count_advanced = await database[Coffee].count(
          where={"sweetener": 2}, depth=1
     )
     print(count_advanced)
```
