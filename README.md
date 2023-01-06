![Logo](https://raw.githubusercontent.com/yezz123/ormdantic/main/.github/logo.png)

<p align="center">
    <em>Asynchronous ORM that uses pydantic models to represent database tables ✨</em>
</p>

<p align="center">
<a href="https://github.com/yezz123/ormdantic/actions/workflows/test.yml" target="_blank">
    <img src="https://github.com/yezz123/ormdantic/actions/workflows/test.yml/badge.svg" alt="Test">
</a>
<a href="https://codecov.io/gh/yezz123/ormdantic">
    <img src="https://codecov.io/gh/yezz123/ormdantic/branch/main/graph/badge.svg"/>
</a>
<a href="https://pypi.org/project/ormdantic" target="_blank">
    <img src="https://img.shields.io/pypi/v/ormdantic?color=%2334D058&label=pypi%20package" alt="Package version">
</a>
<a href="https://pypi.org/project/ormdantic" target="_blank">
    <img src="https://img.shields.io/pypi/pyversions/ormdantic.svg?color=%2334D058" alt="Supported Python versions">
</a>
</p>

Ormdantic is a library for interacting with Asynchronous <abbr title='Also called "Relational databases"'>SQL databases</abbr> from Python code, with Python objects. It is designed to be intuitive, easy to use, compatible, and robust.

**Ormdantic** is based on [Pypika](https://github.com/kayak/pypika), and powered by <a href="https://pydantic-docs.helpmanual.io/" class="external-link" target="_blank">Pydantic</a> and <a href="https://sqlalchemy.org/" class="external-link" target="_blank">SQLAlchemy</a>, and Highly inspired by <a href="https://github.com/tiangolo/Sqlmodel" class="external-link" target="_blank">Sqlmodel</a>, Created by [@tiangolo](https://github.com/tiangolo).

> What is [Pypika](https://github.com/kayak/pypika)?
>
> PyPika is a Python API for building SQL queries. The motivation behind PyPika is to provide a simple interface for building SQL queries without limiting the flexibility of handwritten SQL. Designed with data analysis in mind, PyPika leverages the builder design pattern to construct queries to avoid messy string formatting and concatenation. It is also easily extended to take full advantage of specific features of SQL database vendors.

The key features are:

* **Easy to use**: It has sensible defaults and does a lot of work underneath to simplify the code you write.
* **Compatible**: It combines SQLAlchemy, Pydantic and Pypika tries to simplify the code you write as much as possible, allowing you to reduce the code duplication to a minimum, but while getting the best developer experience possible.
* **Extensible**: You have all the power of SQLAlchemy and Pypika underneath.
* **Short Queries**: You can write queries in a single line of code, and it will be converted to the appropriate syntax for the database you are using.

## Requirements

A recent and currently supported version of Python (right now, <a href="https://www.python.org/downloads/" class="external-link" target="_blank">Python supports versions 3.10 and above</a>).

As **Ormdantic** is based on **Pydantic** and **SQLAlchemy** and **Pypika**, it requires them. They will be automatically installed when you install Ormdantic.

## Installation

You can add Ormdantic in a few easy steps. First of all, install the dependency:

```shell
$ pip install ormdantic

---> 100%

Successfully installed Ormdantic
```

* Install The specific Asynchronous ORM library for your database.

```shell
# MySQL
$ pip install ormdantic[mysql]

# PostgreSQL
$ pip install ormdantic[postgres]

# SQLite
$ pip install ormdantic[sqlite]
```

## Example

To understand SQL, Sebastian the Creator of FastAPI and SQLModel created an amazing documentation that could help you understand the basics of SQL, ex. `CREATE TABLE`, `INSERT`, `SELECT`, `UPDATE`, `DELETE`, etc.

Check out the [documentation](https://sqlmodel.tiangolo.com/).

But let's see how to use Ormdantic.

### Create SQLAlchemy engine

Ormdantic uses SQLAlchemy under hood to run different queries, which is why we need to initialize by creating an asynchronous engine.

> **Note**: You will use the `connection` parameter to pass the connection to the engine directly.

```python
from ormdantic import Ormdantic

connection = "sqlite+aiosqlite:///db.sqlite3"

database = Ormdantic(connection)
```

**Note**: You can use any asynchronous engine, check out the [documentation](https://docs.sqlalchemy.org/en/14/core/engines.html) for more information.

### Create a table

To create tables decorate a pydantic model with the `database.table` decorator, passing the database information ex. `Primary key`, `foreign keys`, `Indexes`, `back_references`, `unique_constraints` etc. to the decorator call.

#### Table Restrictions

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

### Queries

Now after we create the table, we can initialize the database with the table and then run different queries.

#### Init()

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

#### Insert

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

#### Searching Queries

As we know, in SQL, we can search for data using different methods, ex. `WHERE`, `LIKE`, `IN`, `BETWEEN`, etc.

In Ormdantic, we can search for data using the `database.find_one` or `database.find_many` methods.

* `Find_one`  used to find a Model instance by Primary Key, its could also find with `depth` parameter.

```python
     # Find one
     vanilla = await database[Flavor].find_one(flavor.id)
     print(vanilla.name)

     # Find one with depth.
     find_coffee = await database[Coffee].find_one(coffee.id, depth=1)
     print(find_coffee.flavor.name)
```

* `Find_many` used to find Model instances by some condition ex. `where`, `order_by`, `order`, `limit`, `offset`, `depth`.

```python
     # Find many
     await database[Flavor].find_many()

     # Get paginated results.
     await database[Flavor].find_many(
          where={"name": "vanilla"}, order_by=["id", "name"], limit=2, offset=2
     )
```

#### Update / Upsert Queries

##### Update

The modification of data that is already in the database is referred to as updating. You can update individual rows, all the rows in a table, or a subset of all rows. Each column can be updated separately; the other columns are not affected.

```python
     # Update a Flavor
     flavor.name = "caramel"
     await database[Flavor].update(flavor)
```

##### Upsert

The `Upsert` method is similar to the Synchronize method with one exception; the `Upsert` method does not delete any records. The `Upsert` method will result in insert or update operations. If the record exists, it will be updated. If the record does not exist, it will be inserted.

```python
     # Upsert a Flavor
     flavor.name = "mocha"
     await database[Flavor].upsert(flavor)
```

### Delete

The `DELETE` statement is used to delete existing records in a table.

```python
     # Delete a Flavor
     await database[Flavor].delete(flavor.id)
```

### Count

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

## Generator Feature

We introduce a new feature called `Generator`, which is a way to generate a Model instance with random data.

So, Given a Pydantic model type can generate instances of that model with randomly generated values.

using `ormdantic.generator.Generator` to generate a Model instance.

```python
from enum import auto, Enum
from uuid import UUID

from ormdantic.generator import Generator
from pydantic import BaseModel


class Flavor(Enum):
    MOCHA = auto()
    VANILLA = auto()


class Brand(BaseModel):
    brand_name: str


class Coffee(BaseModel):
    id: UUID
    description: str
    cream: bool
    sweetener: int
    flavor: Flavor
    brand: Brand


print(Generator(Coffee))
```

so the results will be:

```shell
id=UUID('93b517c2-083b-457d-a0e5-6e1bd2a927e4')
description='ctWOb' cream=True sweetener=234
flavor=<Flavor.VANILLA: 2> brand=Brand(brand_name='LMrIf')
```

We can integrate this with our database while testing our application (Live Tests).

## Development 🚧

### Setup environment 📦

You should create a virtual environment and activate it:

```bash
python -m venv venv/
```

```bash
source venv/bin/activate
```

And then install the development dependencies:

```bash
# Install dependencies
pip install -e .[sqlite,postgresql,test,lint,docs]
```

### Run tests 🌝

You can run all the tests with:

```bash
bash scripts/test.sh
```

> Note: You can also generate a coverage report with:

```bash
bash scripts/test_html.sh
```

### Format the code 🍂

Execute the following command to apply `pre-commit` formatting:

```bash
bash scripts/format.sh
```

Execute the following command to apply `mypy` type checking:

```bash
bash scripts/lint.sh
```

## License

This project is licensed under the terms of the MIT license.
