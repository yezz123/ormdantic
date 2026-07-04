# Migrations and reflection

Ormdantic migrations are snapshot-based. A snapshot describes the schema Ormdantic sees, either from registered Python models or from a live database.

## Create snapshots

A snapshot is a structured representation of schema metadata:

```python
target = db.migrations.snapshot()
live = db.migrations.live_snapshot()
```

`snapshot()` comes from registered Python models. `live_snapshot()` reflects the connected database.

## Diff and plan changes

```python
diff = db.migrations.diff(before=live, after=target)
plan = db.migrations.generate_plan(before=live, after=target)
```

A diff describes changes. A plan contains SQL operations and rollback operations where available.

## Preview SQL with a dry run

```python
sql = db.migrations.dry_run(before=live, after=target)
```

Use this in review tooling to inspect SQL before applying it.

## Create migration files

```python
artifact = db.migrations.create_migration(
    revision="20260612_add_flavor_rating",
    before=live,
    after=target,
)
```

Migration artifacts include revision metadata, schema diff, planned operations, rollback operations, warnings, and checksums.

## Read the history table

Applied migrations are recorded in `ormdantic_migrations`. The manager can read current state, history, dirty flags, and repair metadata:

```python
await db.migrations.status()
await db.migrations.history()
await db.migrations.repair(clear_dirty=True)
```

## Inspect a live database

`db.inspect()` returns an async inspector:

```python
inspector = db.inspect()
tables = await inspector.table_names()
columns = await inspector.columns("flavor")
indexes = await inspector.indexes("flavor")
foreign_keys = await inspector.foreign_keys("flavor")
```

Reflection is also used by migrations to compare live database state against registered model metadata.
