# Existing Databases

Use `db.inspect()` when you are starting from a database that already has tables.
The inspector reflects live metadata and returns the same snapshot objects used by
migration autogeneration.

```python
from ormdantic import Ormdantic

db = Ormdantic("postgresql://user:password@localhost/app")

inspector = db.inspect()

tables = await inspector.table_names(schema="public", name_patterns=["crm_*"])
columns = await inspector.columns("crm_customer", schema="public")
indexes = await inspector.indexes("crm_customer", schema="public")
foreign_keys = await inspector.foreign_keys("crm_customer", schema="public")
constraints = await inspector.constraints("crm_customer", schema="public")
```

`name_patterns`, `include_tables`, and `exclude_tables` use shell-style matching.
For example, this reflects all `crm_*` tables except archive tables:

```python
snapshot = await inspector.schema(
    schema="public",
    name_patterns=["crm_*"],
    exclude_tables=["*_archive"],
)
```

## Generate Model Scaffolding

`scaffold_models()` returns editable Python source for Pydantic models and
`@db.table()` decorators:

```python
source = await inspector.scaffold_models(
    schema="public",
    include_tables=["crm_customer", "crm_order"],
)

print(source)
```

Example output:

```python
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from ormdantic import Ormdantic

db = Ormdantic("DATABASE_URL")


@db.table('crm_customer', pk='id', schema='public')
class CrmCustomer(BaseModel):
    id: str
    email: str
    name: str | None = None
    created_at: datetime | None = None
```

Review the generated source before using it as application code. Reflection can
infer column shapes, nullability, indexes, and constraints, but relationships and
domain-specific field validation usually need human naming and validation rules.

## Compare To Registered Models

After you edit the generated models, compare them against the live database:

```python
diff = await inspector.compare_to_models(
    schema="public",
    include_tables=["crm_customer", "crm_order"],
)

for item in diff.summary():
    print(item)
```

When the diff is expected, create a migration artifact from the same live
reflection data:

```python
artifact = db.migrations.autogenerate(
    "20260623_align_existing_database",
    schema="public",
    include_tables=["crm_customer", "crm_order"],
    description="Align reflected CRM tables with Ormdantic models",
    path="migrations",
)
```

## Cache Invalidation

The inspector caches reflected snapshots by schema and filter scope. Clear the
cache after DDL runs outside Ormdantic:

```python
inspector.invalidate_cache()
fresh_tables = await inspector.table_names()
```
