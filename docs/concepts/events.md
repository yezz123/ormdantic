# Events

Events let application code observe Ormdantic lifecycle points.

```python
async def before_flush(session) -> None:
    print("flushing")


db.on("before_flush", before_flush)
```

Handlers can be synchronous or asynchronous.

## Common Event Use Cases

- audit logging;
- default metadata injection;
- cache invalidation;
- test instrumentation;
- operational metrics.

## Register And Remove

```python
db.on("before_flush", before_flush)
db.off("before_flush", before_flush)
db.clear_events("before_flush")
db.clear_events()
```

Event dispatch is internal to Ormdantic. Handlers should avoid expensive work unless they explicitly perform async I/O.
