# Transactions And Sessions

Use `transaction()` for explicit transaction scopes:

```python
async with database.transaction():
    await database[Flavor].insert(Flavor(id="1", name="mocha"))
```

Use `savepoint()` for a named savepoint inside a larger transaction:

```python
async with database.transaction():
    async with database.savepoint("before_update"):
        await database[Flavor].update(flavor)
```

Use `session()` for a small async unit-of-work layer:

```python
async with database.session() as session:
    session.add(Flavor(id="1", name="mocha"))
```

The session supports:

- `add(model)`
- `mark_dirty(model)`
- `delete(model)`
- `merge(model)`
- `expire(model)`
- `flush()`
- `commit()`
- `rollback()`
- `refresh(model)`
- identity-map lookup with `get_cached(model_type, pk)`

The session API is intentionally async-safe. Ormdantic does not perform hidden synchronous lazy loading on attribute access.
