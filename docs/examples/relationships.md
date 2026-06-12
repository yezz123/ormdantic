# Relationships

This guide demonstrates relationship annotations and explicit loading.

## What The Example Covers

- scalar relationship fields;
- collection relationship fields;
- relationship-derived hydration;
- joined and select-in loading;
- avoiding hidden lazy I/O.

```python
--8<-- "examples/relationships.py"
```

Run it locally:

```console
python examples/relationships.py
```

## Choosing A Loader

Use `joinedload` for small related row sets where one joined query is efficient. Use `selectinload` for collections and larger graphs where batched secondary queries are easier to control.
