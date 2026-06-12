# Native Engine

The public ORM uses the native Rust runtime internally. Ormdantic also exposes a small `NativeEngine` wrapper for direct SQL execution.

```python
from ormdantic.engine import NativeEngine

engine = NativeEngine("sqlite:///app.sqlite3")
result = await engine.execute("SELECT 1 AS value", ())
assert result.scalar() == 1
```

## When To Use It

Use `NativeEngine` when:

- you need a small amount of direct SQL beside the ORM;
- you are writing migration or operational tooling;
- you want native driver behavior without registering Pydantic models.

Prefer `Ormdantic` table handles for normal application persistence.

## Transactions

```python
async with engine.transaction():
    await engine.execute("INSERT INTO audit_log (message) VALUES (?)", ("created",))
```

The wrapper runs blocking Rust calls in a worker thread and returns a lightweight `NativeResult`.
