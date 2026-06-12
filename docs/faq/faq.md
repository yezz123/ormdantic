# Frequently Asked Questions

## What Is Ormdantic?

Ormdantic is an async ORM that uses Pydantic v2 models as database table models. The Python API handles declarations, decorators, sessions, events, and final Pydantic object construction. The Rust core handles schema validation, SQL compilation, row hydration, migrations, reflection, and native database execution.

## Does Ormdantic Depend On SQLAlchemy?

No. Ormdantic accepts SQLAlchemy-style database URLs because they are familiar, but runtime execution is handled by the Rust extension.

## Does Ormdantic Replace SQLAlchemy?

Not for every use case. Ormdantic is intentionally narrower. Use SQLAlchemy when you need its full SQL toolkit, mapper ecosystem, or Alembic workflows. Use Ormdantic when you want Pydantic-first models, explicit async loading, and a Rust-backed runtime.

## Does Ormdantic Use Pydantic Models Directly?

Yes. You decorate ordinary Pydantic models with `@db.table(...)`. The model remains usable anywhere a Pydantic model is expected.

## Are Relationship Loads Lazy?

Not implicitly. Ormdantic avoids hidden I/O on attribute access. Use `depth`, `joinedload`, `selectinload`, or `db.load(model, "path")` to load relationships explicitly.

## Which Databases Are Supported?

SQLite, PostgreSQL, MySQL, MariaDB, SQL Server, and Oracle are supported by the Rust runtime, depending on the compiled driver features in your installed wheel or local build.

## How Do I Check Driver Support?

```python
from ormdantic import runtime_capabilities

print(runtime_capabilities())
```

## How Do Migrations Work?

Ormdantic compares schema snapshots. A registered Python model snapshot can be compared with a live reflected database snapshot to generate diffs, SQL plans, migration artifacts, rollback operations, and history entries.

## How Can I Support The Project?

You can support the maintainer through GitHub Sponsors or the support links in the repository profile.
