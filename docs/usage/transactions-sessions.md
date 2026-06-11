# Transactions And Sessions

Use `transaction()` for explicit transaction scopes:

```python
async with database.transaction():
    await database[Flavor].insert(Flavor(id="1", name="mocha"))
```

Transaction scopes can request backend transaction options:

```python
async with database.transaction(
    isolation_level="serializable",
    read_only=True,
    deferrable=False,
):
    rows = await database[Flavor].find_many()
```

Supported isolation names are `read_uncommitted`, `read_committed`, `repeatable_read`, `serializable`, and `snapshot`. Backends apply the options they support through the native runtime; unsupported backend combinations fail at transaction start.

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

`session()` accepts the same transaction options as `transaction()`.

The session supports:

- `add(model)`
- relationship add cascades for staged object graphs
- automatic dirty tracking for loaded or refreshed models
- `mark_dirty(model)` for detached or manually managed instances
- `delete(model)`
- delete cascades for loaded relationship collections
- `merge(model)`
- `expire(model)`
- `flush()`
- `commit()`
- `rollback()`
- `refresh(model)`
- identity-map lookup with `get_cached(model_type, pk)`

Flushes order related inserts so referenced parents are inserted before dependents. Deletes reverse that order for staged related objects, so loaded children are deleted before their parent.

Loaded relationship graphs are remembered in the identity map. Mixed graph flushes can update a loaded parent, add new related children, and delete loaded or detached children in one unit of work.

`expire(model)` detaches that identity from the session cache and dirty-tracking snapshot. Mutating the expired Python object is ignored until you call `merge(model)` or `mark_dirty(model)`. A later `get()` for the same primary key reloads a fresh managed instance.

`merge(model)` copies a detached model into the managed instance when one is cached, or remembers the detached instance and stages it as dirty when it is not cached.

After `commit()` or `rollback()`, the session is closed. Mutating or loading operations on a closed session raise `RuntimeError("session is closed")`.

The session API is intentionally async-safe. Ormdantic does not perform hidden synchronous lazy loading on attribute access.
