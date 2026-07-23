# Work with an existing database

Use `db.inspect()` when your database already contains tables. The inspector
returns the same snapshot objects used by migration planning.

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
Reflect a bounded schema rather than an entire shared database:

```python
snapshot = await inspector.schema(
    schema="public",
    name_patterns=["crm_*"],
    exclude_tables=["*_archive"],
)
```

## Generate model scaffolding

```python
source = await inspector.scaffold_models(
    schema="public",
    include_tables=["crm_customer", "crm_order"],
)
print(source)
```

Review generated source before using it. Reflection can infer columns,
nullability, indexes, and constraints, but it cannot infer your domain language,
authorization, or all relationship intent.

## Compare models and create a migration

```python
diff = await inspector.compare_to_models(
    schema="public",
    include_tables=["crm_customer", "crm_order"],
)
for item in diff.summary():
    print(item)

artifact = db.migrations.autogenerate(
    "20260720_align_existing_database",
    schema="public",
    include_tables=["crm_customer", "crm_order"],
    description="Align reflected CRM tables with registered models",
    path="migrations",
)
```

Inspect the artifact and generated SQL before applying it. Clear cached reflection
after DDL runs outside Ormdantic:

```python
inspector.invalidate_cache()
fresh_tables = await inspector.table_names()
```

See [Reflection API](../api/reflection.md) for filters and return types.
