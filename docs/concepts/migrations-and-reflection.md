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

The optional [playground](../playground/index.md) runs this comparison continuously. It renders model and live snapshots, structured drift, generated SQL, and diagnostic state without importing project models into the TUI process.

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

## Use the migration CLI

The CLI can take the database URL three ways. Use the form that fits your shell or deployment environment:

```bash
uv run ormdantic migrations init --url postgresql://postgres:postgres@localhost:5432/postgres
```

```bash
export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/postgres"
uv run ormdantic migrations init
```

```bash
printf '%s\n' 'DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres' > .env
uv run ormdantic migrations init
```

Exported environment variables win over `.env`, and explicit `--url` wins over both. If your project uses a different name, pass `--url-env ORMDANTIC_DATABASE_URL`.

After initialization, apply a directory of reviewed artifacts:

```bash
uv run ormdantic migrations apply-dir migrations
```

The command prints a short summary instead of only the raw revision:

```text
Applied 3 migrations from migrations.
- 001_initial
- 002_projects
- 003_tasks
Connection: postgresql://postgres:<redacted>@localhost:5432/postgres (source: DATABASE_URL)
```

The same directory command still accepts the older positional URL form:

```bash
uv run ormdantic migrations apply-dir "$DATABASE_URL" migrations
```

Use `current`, `status`, and `history` to inspect state:

```bash
uv run ormdantic migrations current
uv run ormdantic migrations status
uv run ormdantic migrations history
```

`apply-dir` is the usual path for application setup. `apply` applies one artifact, `rollback` rolls one artifact back when rollback SQL is available, and `repair` clears dirty metadata after you have investigated and fixed a failed migration.

For a fuller multi-table example, see `examples/fastapi_authx_postgres_mvc/migrations/001_initial.toml`. It creates `user_account`, `project`, and `task_item` tables with foreign keys, cascade deletes, check constraints, and indexes.

## Read the history table

Applied migrations are recorded in `ormdantic_migrations`. The manager can read current state, history, dirty flags, and repair metadata:

```python
await db.migrations.status()
await db.migrations.history()
await db.migrations.repair(clear_dirty=True)
```

Check whether history exists without creating its table:

```python
exists = await db.migrations.history_table_exists()
```

The playground uses this non-mutating check during reflection. Opening the playground against a new database does not initialize migration history.

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

See [Run migration workflows in the playground](../playground/migration-workflows.md) for guarded generation, apply, rollback, repair, and squash flows.
