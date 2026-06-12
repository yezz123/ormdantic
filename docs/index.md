# Ormdantic

<p align="center">
  <img src="logo.svg" alt="Ormdantic logo" width="540">
</p>

Ormdantic is a Rust-backed asynchronous ORM for Python applications that already use Pydantic models as their data boundary.

It keeps the public API Pythonic:

- declare tables with Pydantic v2 models;
- register them with `Ormdantic`;
- query, insert, update, delete, migrate, and reflect schemas from Python;
- let the Rust runtime handle SQL compilation, hydration, and database execution.

The result is an ORM that is intentionally smaller than SQLAlchemy, more database-aware than a generic repository layer, and explicit about async behavior.

## What Ormdantic Gives You

| Need | Ormdantic answer |
| --- | --- |
| One model shape for API and persistence | Pydantic models are the table models. |
| Fast SQL compilation and row hydration | Rust owns query compilation, result-shape planning, and native execution. |
| Async-safe relationship loading | Relationship paths are loaded explicitly with `joinedload`, `selectinload`, `lazyload`, or `noload`. |
| Cross-driver schema metadata | Table, column, index, constraint, sequence, namespace, and view options are modeled in Python. |
| Native migrations | Snapshots, diffs, plans, migration files, history, repair, rollback, and live reflection are built in. |
| Backend-specific control | PostgreSQL, MySQL, MariaDB, SQL Server, Oracle, and SQLite each have their own documented behavior. |

## First Example

```python
from pydantic import BaseModel, Field

from ormdantic import Ormdantic

db = Ormdantic("sqlite:///app.sqlite3")


@db.table(pk="id", indexed=["name"])
class Flavor(BaseModel):
    id: str
    name: str = Field(min_length=2, max_length=80)
    rating: int = 0


async def main() -> None:
    await db.init()
    await db[Flavor].insert(Flavor(id="vanilla", name="Vanilla", rating=5))

    result = await db[Flavor].find_many(
        {"rating": {"gte": 4}},
        order_by=["name"],
    )
    assert [flavor.name for flavor in result.data] == ["Vanilla"]
```

## Where To Go Next

- Read [Why Ormdantic](why-ormdantic.md) for the problem it solves.
- Read [Philosophy](philosophy.md) for the design boundaries.
- Use [Learning Path](learning-path.md) to choose the beginner, intermediate, or advanced reading order.
- Follow [Quickstart](quickstart.md) to build a small app.
- Use [Concepts](concepts/index.md) when you need to understand how a feature fits.
- Use [Drivers](drivers/index.md) when choosing or tuning a database backend.
- Use [Python API](api/reference.md) for generated reference pages.
