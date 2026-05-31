# Events

Register event handlers with `database.on()`.

```python
database.on("before_insert", lambda model, table: print(model))
database.on("after_flush", lambda session: print("flushed"))
```

Supported events:

- `before_insert`
- `after_insert`
- `before_update`
- `after_update`
- `before_delete`
- `after_delete`
- `before_flush`
- `after_flush`

Handlers may be normal functions or async functions.
