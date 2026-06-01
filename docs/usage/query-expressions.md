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

The Rust SQL compiler expands these into dialect-specific SQL and bind parameters.

You can also build simple composable expressions:

```python
from ormdantic import column

await database[Flavor].find_many(
    where=column("strength").ge(2) & column("name").like("mo%")
)
```

The expression facade lowers into the Rust filter contract. Simple `AND` groups are supported today; native `OR`, subquery, CTE, and window lowering are tracked in the Rust SQL roadmap.
