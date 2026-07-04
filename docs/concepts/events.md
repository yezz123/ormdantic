# Events

Events let application code observe Ormdantic lifecycle points.

```python
async def before_flush(session) -> None:
    print("flushing")


db.on("before_flush", before_flush)
```

Handlers can be synchronous or asynchronous.

## Common event use cases

- audit logging
- default metadata injection
- cache invalidation
- test instrumentation
- operational metrics

## Register and remove handlers

```python
db.on("before_flush", before_flush)
db.off("before_flush", before_flush)
db.clear_events("before_flush")
db.clear_events()
```

Event dispatch is internal to Ormdantic. Handlers should avoid expensive work unless they explicitly perform async I/O.

## Use runtime diagnostics

Enable debug diagnostics when handlers need generated SQL and bind names:

```python
db = Ormdantic("sqlite:///app.sqlite3", debug=True)


def record_query(**event) -> None:
    metrics.timing("db.query", event["duration_ms"], tags={"table": event["table_name"]})


db.on_query(record_query)
```

`before_execute` and `after_execute` fire around ORM query execution. Payloads include `operation`, `table_name`, `model_name`, `backend`, and, on `after_execute`, `duration_ms`, `row_count`, and `error`.

When `debug=True`, execute events also include `sql`, `bind_names`, and `parameters`. Parameter values whose bind names look sensitive, such as `password`, `token`, `secret`, or `api_key`, are replaced with `<redacted>`.

The same lifecycle model is used for:

- `before_create` and `after_create` around inserts
- `before_commit`, `after_commit`, `before_rollback`, and `after_rollback` with transaction timing metadata
- `before_migration` and `after_migration` for migration apply and rollback
- `before_reflection` and `after_reflection` for inspector calls
- `before_hydration` and `after_hydration` when native rows are converted into models

Use `db.runtime_diagnostics()` for non-secret runtime metadata such as backend, registered tables, debug flags, and compiled backend capabilities.
