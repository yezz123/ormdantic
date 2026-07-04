# Querying

Every registered model has a `Table` handle. Use that handle for primary-key lookups, filtered lists, expression queries, counts, and bulk updates.

```python
flavors = db[Flavor]
```

## Use dictionary filters

Use dictionaries for common equality and comparison filters:

```python
result = await db[Flavor].find_many(
    {"rating": {"gte": 4}, "name": {"like": "Van%"}},
    order_by=["name"],
)
```

## Look up a primary key

```python
flavor = await db[Flavor].find_one("vanilla")
```

`find_one` accepts a primary key value or a filter expression.

## Use expression queries

Use expression helpers when dictionary filters are not enough:

```python
from ormdantic import column, select_query

query = select_query(
    "flavor",
    column("name"),
    column("rating"),
    where=column("rating") >= 4,
    order_by=[column("name").asc()],
)

rows = await db[Flavor].select(query)
```

Expression helpers include `column`, `literal`, `case`, `cast`, `tuple_`, aggregate helpers, `exists`, `subquery`, `cte`, `over`, and `raw_sql_safe`.

## Count and update rows in bulk

```python
count = await db[Flavor].count({"rating": {"gte": 4}})

updated = await db[Flavor].update_where(
    {"rating": {"lt": 0}},
    {"rating": 0},
)
```

## Read result objects

`find_many` returns `Result[Model]`:

```python
result.offset
result.limit
result.data
```

The data list contains hydrated Pydantic models.
