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
async with db.transaction(isolation_level="serializable", read_only=False):
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

Sessions can also open nested savepoints. A session savepoint snapshots both the database savepoint and the in-memory unit-of-work state, so flushed rows and pending model state created inside the block are discarded if the block raises.

```python
async with db.session() as session:
    session.add(Flavor(id="base", name="Base"))
    await session.flush()

    try:
        async with session.savepoint("optional_flavor"):
            session.add(Flavor(id="trial", name="Trial"))
            await session.flush()
            raise RuntimeError("discard trial")
    except RuntimeError:
        pass
```

If a flush fails, the session restores its staged state to the pre-flush snapshot and enters a failed state. Call `rollback()` before using the session again. Context-managed sessions do this automatically when the failure leaves the context.

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
| `savepoint(name=None)` | Open a nested savepoint and restore session state on error. |
| `refresh(model)` | Reload by primary key. |
| `get(Model, pk)` | Return cached or loaded model. |

## Lifecycle Events

Transaction and session work dispatch lifecycle events in order:

- `before_begin`, `after_begin`;
- `before_flush`, `after_flush`;
- `before_commit`, `after_commit`;
- `before_rollback`, `after_rollback`;
- `before_savepoint`, `after_savepoint`;
- `before_release_savepoint`, `after_release_savepoint`;
- `before_rollback_to_savepoint`, `after_rollback_to_savepoint`.

Transaction and savepoint `after_*` events include `error=None` on success and the classified exception when the operation fails.
