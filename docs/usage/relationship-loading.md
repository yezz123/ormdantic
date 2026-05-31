# Relationship Loading

Existing `depth` loading remains supported:

```python
coffee = await database[Coffee].find_one(coffee_id, depth=1)
```

Loader options provide a clearer API:

```python
from ormdantic import joined, selectin, lazy

coffee = await database[Coffee].find_one(coffee_id, load=[joined("flavor")])
coffee = await database[Coffee].find_one(coffee_id, load=[selectin("flavor")])
coffee = await database[Coffee].find_one(coffee_id, load=[lazy("flavor")])
flavor = await database.load(coffee, "flavor")
```

Current behavior:

- `joined()` maps to Rust-compiled joined loading.
- `selectin()` currently maps to the same depth-based loader contract while the select-in planner is expanded.
- `lazy()` keeps the initial result shallow and requires explicit async loading.
