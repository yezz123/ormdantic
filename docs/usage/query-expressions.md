# Query Expressions

Ormdantic accepts simple equality filters and operator-suffixed filter keys.

```python
await database[Flavor].find_many(where={"name": "mocha"})
await database[Flavor].find_many(where={"strength__gt": 2})
await database[Flavor].find_many(where={"name__like": "mo%"})
await database[Flavor].find_many(where={"id__in": [first_id, second_id]})
await database[Flavor].find_many(where={"strength__is_not_null": True})
```

Supported operators:

- `eq` (default)
- `ne`
- `lt`, `le`, `gt`, `ge`
- `like`, `ilike`
- `in`, `not_in`
- `is_null`, `is_not_null`

Rust normalizes these filters, expands bind parameters, and compiles them into dialect-specific SQL.

You can also build composable boolean expressions:

```python
from ormdantic import column

await database[Flavor].find_many(
    where=column("strength").ge(2) & column("name").like("mo%")
)

await database[Flavor].find_many(
    where=(column("name") == "mocha") | (column("name") == "latte")
)
```

The expression facade is normalized through the Rust filter contract, so `AND` and `OR` groups are supported for `find_many()` and `count()` filters. Subqueries, CTEs, projections, SQL functions, and window expressions are future typed-AST expansions and are not part of the current public API.
