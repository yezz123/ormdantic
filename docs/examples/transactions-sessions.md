# Transactions And Sessions

This guide shows direct transactions, savepoints, and the session unit-of-work helper.

## What The Example Covers

- `async with db.transaction()`;
- `async with db.savepoint("name")`;
- `async with db.session()`;
- staged inserts and session savepoints;
- commit-on-success and rollback-on-error behavior.

```python
--8<-- "examples/transactions_sessions.py"
```

Run it locally:

```console
python examples/transactions_sessions.py
```

## When To Use Which API

Use direct transactions when you already know each operation. Use sessions when you want to stage model objects and let Ormdantic flush them in dependency order.
