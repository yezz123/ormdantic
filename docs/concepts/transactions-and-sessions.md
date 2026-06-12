# Transactions And Sessions

Ormdantic exposes two transaction layers:

- direct database transactions;
- the `Session` unit-of-work helper.

## Direct Transaction

```python
async with db.transaction():
    await db[Flavor].insert(Flavor(id="vanilla", name="Vanilla"))
```

The context manager commits on success and rolls back on error.

## Transaction Options

The transaction API accepts backend-aware options such as isolation level, read-only mode, and deferrable behavior where supported.

```python
async with db.transaction(isolation="serializable", read_only=False):
    ...
```

Unsupported options are rejected instead of silently ignored.

## Savepoints

```python
async with db.transaction():
    async with db.savepoint("before_optional_work"):
        await db[Flavor].insert(Flavor(id="trial", name="Trial"))
```

If the savepoint block raises, Ormdantic rolls back to the savepoint and releases it according to backend support.

## Sessions

Sessions stage object changes:

```python
async with db.session() as session:
    session.add(Flavor(id="vanilla", name="Vanilla"))
```

A session begins a transaction, tracks new/dirty/deleted models, uses an identity map, flushes before commit, and rolls back on context errors.

## Session Methods

| Method | Purpose |
| --- | --- |
| `add(model)` | Stage a new model and reachable relationship objects. |
| `mark_dirty(model)` | Stage an existing model for update. |
| `delete(model)` | Stage a model and loaded child collections for deletion. |
| `merge(model)` | Merge detached state into the identity map. |
| `flush()` | Write staged changes without closing the transaction. |
| `commit()` | Flush and commit. |
| `rollback()` | Discard staged changes and roll back. |
| `refresh(model)` | Reload by primary key. |
| `get(Model, pk)` | Return cached or loaded model. |
