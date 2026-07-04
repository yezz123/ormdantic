# Relationships

This guide demonstrates relationship annotations and explicit loading.

Use it after [Relationships](../concepts/relationships.md) and [Loading strategies](../concepts/loading-strategies.md).

## What the example covers

- scalar relationship fields
- collection relationship fields
- relationship-derived hydration
- joined and select-in loading
- avoiding hidden lazy I/O

```python
--8<-- "examples/relationships.py"
```

Run it locally:

```console
python examples/relationships.py
```

## Choose a loader

Use `joinedload` for small related row sets where one joined query is efficient. Use `selectinload` for collections and larger graphs where batched secondary queries are easier to control.
