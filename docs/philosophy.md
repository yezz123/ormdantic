# Philosophy

Ormdantic is built around five boundaries.

## Pydantic Is The Object Layer

Application objects are normal Pydantic models. Ormdantic does not ask you to inherit from a custom declarative base. Table registration is attached to a database instance:

```python
from pydantic import BaseModel
from ormdantic import Ormdantic

db = Ormdantic("sqlite:///app.sqlite3")


@db.table(pk="id")
class User(BaseModel):
    id: str
    email: str
```

This keeps validation, serialization, default handling, and type annotations in the place Python developers already expect.

## Rust Owns The Hot Path

Python defines metadata and handles user-facing control flow. Rust handles:

- schema validation;
- dialect-aware DDL rendering;
- query compilation;
- bind ordering;
- result-shape planning;
- row hydration;
- native database execution.

The Python API should stay readable while the runtime avoids repeated Python-side query and hydration work.

## Async Behavior Must Be Explicit

Ormdantic does not perform hidden synchronous lazy loads when you access an attribute. Relationship work is requested through query depth or loader options:

```python
from ormdantic import selectinload

parents = await db[Parent].find_many(load=[selectinload("children")])
```

That rule is deliberate. It keeps I/O visible, makes tests easier to reason about, and avoids surprising event-loop behavior.

## Database Differences Are Real

SQLite, PostgreSQL, MySQL, MariaDB, SQL Server, and Oracle do not behave the same. Ormdantic normalizes what it can, but it documents backend-specific behavior instead of pretending it does not exist.

Examples:

- SQLite stores exact decimals as text-backed decimal values when Ormdantic creates the column.
- PostgreSQL supports native enum types, namespaces, tablespaces, and advanced indexes.
- MySQL and MariaDB need bounded keyable string types.
- SQL Server has filegroups, clustered indexes, and `OUTPUT` semantics.
- Oracle has identity options, table compression, tablespaces, and strict aliasing rules.

## Metadata Should Be Data

Indexes, constraints, namespaces, sequences, views, and column options are Pydantic models. That makes them inspectable, serializable, validated, and usable by migrations.

```python
from ormdantic import TableIndex

TableIndex(
    name="flavor_name_idx",
    columns=["name"],
    unique=True,
    comment="Lookup flavors by public name",
)
```

## The Non-Goal

Ormdantic is not trying to become a full SQLAlchemy clone. It should cover the persistence workflows most application teams need, make database behavior explicit, and leave the deepest SQL toolkit use cases to tools designed for that scope.
