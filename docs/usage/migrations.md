# Migrations

Ormdantic migrations now support:

- Snapshot-to-snapshot generation.
- Live SQLite autogeneration (database -> models).
- Durable migration history with checksum, status, dirty state, and metadata.
- Explicit rollback behavior (no unsafe fallback to `up` SQL).
- Migration artifact V2 with checksum and safety metadata.

## Core Workflow

1. Generate a migration artifact.
2. Review SQL and warnings.
3. Apply with destructive opt-in when needed.
4. Inspect status/history/current.
5. Roll back with explicit `down` SQL.

## Generate From Snapshots

```python
from ormdantic.migrations import SchemaSnapshot

previous = SchemaSnapshot.read("schema.previous.json")
current = db.migrations.snapshot()

artifact = db.migrations.create_migration(
    "20260608_add_rating",
    previous,
    current,
    dialect="sqlite",
    description="add nullable flavor rating",
    path="migrations/20260608_add_rating.json",
)
```

## Live SQLite Autogenerate

```python
artifact = db.migrations.autogenerate(
    "20260608_live_autogen",
    description="sync live sqlite schema with models",
    include_tables=["flavor*"],
    exclude_tables=["legacy_*"],
)
if artifact is not None:
    artifact.write("migrations/20260608_live_autogen.json")
```

When there are no schema changes, `autogenerate()` returns `None` by default.

## Review, Apply, Status, Rollback

```python
for warning in artifact.warnings:
    print(warning.message)

print(artifact.to_plan().dry_run())
await db.migrations.apply_artifact(artifact)
print(await db.migrations.status())
print(await db.migrations.history())
print(await db.migrations.current())
```

Destructive operations require explicit opt-in:

```python
await db.migrations.apply_artifact(artifact, allow_destructive=True)
```

Rollback requires explicit `down` operations:

```python
await db.migrations.rollback_artifact(artifact)
```

If rollback SQL is unavailable, Ormdantic raises an error instead of replaying upgrade SQL.

## History Table

The `ormdantic_migrations` table stores:

- `revision`
- `description`
- `checksum`
- `applied_at`
- `execution_time_ms`
- `status` (`applied`, `failed`, `rolled_back`)
- `dirty`
- `artifact_version`
- `ormdantic_version`
- `metadata`

Re-applying the same revision with a different checksum raises a checksum mismatch error.

## CLI

```bash
# initialize migration metadata
ormdantic migrations init sqlite:///app.sqlite3

# snapshot and create
ormdantic migrations snapshot app.models:db --out schema.current.toml
ormdantic migrations create 20260608_add_rating \
  --from schema.previous.json \
  --to schema.current.toml \
  --dialect sqlite \
  --message "add nullable rating" \
  --out migrations/20260608_add_rating.toml

# live autogenerate
ormdantic migrations autogenerate app.models:db 20260608_live \
  --include-table "flavor*" \
  --out migrations/20260608_live.json

# preview/apply/apply-dir
ormdantic migrations preview migrations/20260608_add_rating.toml
ormdantic migrations apply sqlite:///app.sqlite3 migrations/20260608_add_rating.toml
ormdantic migrations apply-dir sqlite:///app.sqlite3 migrations/

# status/history/current
ormdantic migrations status sqlite:///app.sqlite3
ormdantic migrations history sqlite:///app.sqlite3
ormdantic migrations current sqlite:///app.sqlite3

# rollback/repair/check
ormdantic migrations rollback sqlite:///app.sqlite3 migrations/20260608_add_rating.toml
ormdantic migrations repair sqlite:///app.sqlite3 --clear-dirty
ormdantic migrations check migrations/
```

## Backend Notes

- **SQLite**: unsupported generic `ALTER TABLE` operations that require table rebuild are blocked with a clear error.
- **PostgreSQL / MySQL / MariaDB / SQL Server / Oracle**: apply/rollback history, checksum checks, and locking hooks are available; live autogenerate currently focuses on SQLite first.

## Recipes

- Initial migration: generate from empty snapshot -> current models.
- Add nullable column: safe by default.
- Add non-null column with backfill: split into add nullable -> data update -> enforce not-null.
- Rename/drop column: treat as unsafe/destructive and review manually.
- Add foreign key: review generated SQL and backend support.
- Rollback one revision: use `migrations rollback` with explicit artifact.
- Squash: collapse contiguous artifacts with checksum regeneration.
