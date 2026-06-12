# Table

`Table` is the CRUD and query handle returned by `db[Model]`.

Most application code does not instantiate `Table` directly:

```python
flavors = db[Flavor]
result = await flavors.find_many({"rating": {"gte": 4}})
```

Use table handles for:

- primary-key lookup with `find_one`;
- filtered lists with `find_many`;
- writes with `insert`, `update`, `upsert`, and `delete`;
- counts with `count`;
- expression-backed reads with `select`;
- expression-backed bulk writes with `update_where`.

::: ormdantic.table.Order
::: ormdantic.table.Table
