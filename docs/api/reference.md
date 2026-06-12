# Python API Reference

The API reference is generated from Ormdantic's Python source with mkdocstrings and supplemented with usage notes.

## Public Surface

| Area | Page |
| --- | --- |
| Database registry and table decorator | [Ormdantic](ormdantic.md) |
| CRUD and query table handle | [Table](table.md) |
| Table, column, index, constraint, namespace, sequence, and view metadata | [Metadata Models](metadata.md) |
| SQL expression helpers | [Query Expressions](expressions.md) |
| Relationship loader helpers | [Relationship Loaders](loaders.md) |
| Unit of work | [Sessions](session.md) |
| Snapshots, plans, history, and migration files | [Migrations](migrations.md) |
| Live database inspection | [Reflection](reflection.md) |
| Event registry | [Events](events.md) |
| Direct native SQL execution | [Native Engine](engine.md) |
| Association and hybrid descriptors | [Associations](associations.md) |
| Exceptions | [Errors](errors.md) |

## Main Re-Exports

Most application code imports from `ormdantic`:

```python
from ormdantic import (
    Ormdantic,
    TableColumn,
    TableIndex,
    TableForeignKey,
    column,
    select_query,
    selectinload,
)
```

Lower-level modules remain importable when you need a narrower namespace.

## Core Database Objects

| Object | Use it for |
| --- | --- |
| `Ormdantic` | Create a database registry, register tables, initialize schema, open sessions and transactions, inspect live databases, and access migrations. |
| `Table` | Work with one registered model through `db[Model]`. |
| `Order` | Choose ascending or descending order for simple table queries. |

## Metadata Objects

Use these when decorator shortcuts are not enough.

| Object | Use it for |
| --- | --- |
| `TableColumn` | Column-level options such as comments, server defaults, computed values, identity, foreign key actions, enum options, and dialect-specific column behavior. |
| `TableIndex` | Named indexes, unique indexes, include columns, comments, tablespaces/filegroups, PostgreSQL ops, MySQL index length/prefix options, and dialect-specific index options. |
| `TableCheck` | Named check constraints. |
| `TableUnique` | Named unique constraints, including dialect-specific options. |
| `TableForeignKey` | Named or composite foreign key constraints. |
| `TableExclusion` | PostgreSQL exclusion constraints. |
| `DatabaseNamespace` | Schemas or namespaces where supported. |
| `DatabaseSequence` | Database sequences. |
| `DatabaseView` | Regular and materialized views. |

## Query Expression Objects

Use dictionary filters first. Use expressions when you need composed SQL.

| Object or helper | Use it for |
| --- | --- |
| `QueryExpression` | Boolean query predicates used in `where` and `having`. |
| `RelationExpression` | Relationship-aware predicates and aggregates from `db.relation(...)`. |
| `column` | Reference a SQL column. |
| `literal` | Inline a literal expression. |
| `projection` | Give a projected expression an alias. |
| `assignment` | Build expression-backed update assignments. |
| `select_query` | Build a serializable SELECT query object. |
| `update_query` | Build a serializable UPDATE query object. |
| `case`, `cast`, `tuple_` | Build SQL CASE, CAST, and tuple expressions. |
| `count`, `sum`, `avg`, `min`, `max` | Aggregate expressions. |
| `exists`, `not_exists`, `subquery`, `cte` | Subqueries and common table expressions. |
| `group`, `over` | Grouping and window expressions. |
| `not_` | Negate an expression. |
| `raw_sql_safe` | Opt in to raw SQL fragments only when the SQL is trusted. |

## Relationship Loader Helpers

| Helper | Use it for |
| --- | --- |
| `joinedload` / `joined` | Load a relationship path with joins. |
| `selectinload` / `selectin` | Load a relationship path with batched secondary queries. |
| `lazyload` / `lazy` | Mark a relationship path for explicit later loading. |
| `noload` | Prevent loading a relationship path. |
| `load` | Build a loader option with an explicit strategy. |

Loader options can be refined with:

- `filter(...)` for relationship-local filters;
- `sorted_by(...)` for relationship-local ordering;
- `batched(...)` for select-in batch size.

## Associations And Events

| Object | Use it for |
| --- | --- |
| `association_proxy` | Expose values through related objects. |
| `hybrid_property` | Define a Python property that can also participate in ORM logic. |
| `EventRegistry` | Register and dispatch lifecycle hooks. Most users use `db.on(...)` rather than constructing the registry directly. |

## Runtime And Errors

| Object | Use it for |
| --- | --- |
| `runtime_capabilities` | Check which native drivers are compiled into the installed extension. |
| `ConfigurationError` | Invalid ORM configuration. |
| `UndefinedBackReferenceError` | A configured back-reference does not exist. |
| `MismatchingBackReferenceError` | A back-reference points to the wrong model type. |
| `MustUnionForeignKeyError` | A relationship field does not include the foreign primary-key type. |
| `TypeConversionError` | A Python value cannot be converted for the runtime. |
