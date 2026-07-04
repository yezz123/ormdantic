# Frequently asked questions

Use this page for short answers. If an answer needs setup or code, follow the linked concept or guide page.

## What is Ormdantic?

Ormdantic is an async ORM that uses Pydantic v2 models as database table models. The Python API handles declarations, decorators, sessions, events, and final Pydantic object construction. The Rust core handles schema validation, SQL compilation, row hydration, migrations, reflection, and native database execution.

## Does Ormdantic depend on SQLAlchemy?

No. Ormdantic accepts SQLAlchemy-style database URLs because they are familiar, but runtime execution is handled by the Rust extension.

## Does Ormdantic replace SQLAlchemy?

Not for every use case. Ormdantic is intentionally narrower. Use SQLAlchemy when you need its full SQL toolkit, mapper ecosystem, or Alembic workflows. Use Ormdantic when you want Pydantic-first models, explicit async loading, and a Rust-backed runtime.

## Does Ormdantic use Pydantic models directly?

Yes. You decorate ordinary Pydantic models with `@db.table(...)`. The model remains usable anywhere a Pydantic model is expected.

## Are relationship loads lazy?

Not implicitly. Ormdantic avoids hidden I/O on attribute access. Use `depth`, `joinedload`, `selectinload`, or `db.load(model, "path")` to load relationships explicitly.

## Which databases are supported?

SQLite, PostgreSQL, MySQL, MariaDB, SQL Server, and Oracle are supported by the Rust runtime, depending on the compiled driver features in your installed wheel or local build.

## How do I check driver support?

```python
from ormdantic import runtime_capabilities

print(runtime_capabilities())
```

## How do migrations work?

Ormdantic compares schema snapshots. A registered Python model snapshot can be compared with a live reflected database snapshot to generate diffs, SQL plans, migration artifacts, rollback operations, and history entries.

## How can I support the project?

You can support the maintainer through GitHub Sponsors or the support links in the repository profile.
