# Loading Strategies

Relationship loading is explicit. You choose the path and strategy.

## Strategies

| Strategy | Helper | Use when |
| --- | --- | --- |
| Joined | `joinedload("path")` | You want one SQL query with joins and the related row count is controlled. |
| Select-in | `selectinload("path")` | You want batched secondary queries for collections or larger graphs. |
| Lazy | `lazyload("path")` | You want to mark a path as loadable later through explicit APIs. |
| No load | `noload("path")` | You want to prevent a relationship path from loading. |

## Examples

```python
from ormdantic import joinedload, selectinload

flavors = await db[Flavor].find_many(load=[joinedload("supplier")])

suppliers = await db[Supplier].find_many(
    load=[selectinload("flavors").sorted_by("name").batched(100)]
)
```

Nested paths use dot notation:

```python
await db[Supplier].find_many(
    load=[
        selectinload("flavors"),
        selectinload("flavors.reviews"),
    ]
)
```

## Depth Loading

`depth` loads relationship paths by graph distance:

```python
await db[Supplier].find_many(depth=1)
```

Loader options are preferred for production code because they document the exact paths and strategies.

## Explicit Single-Model Loading

```python
supplier = await db[Supplier].find_one("s1")
flavors = await db.load(supplier, "flavors")
```

This still performs explicit async I/O. It is not hidden attribute access.
