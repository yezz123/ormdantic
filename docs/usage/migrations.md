# Migrations

Ormdantic migrations are built from schema snapshots. A snapshot is a portable record of the tables registered on an `Ormdantic` database object. Migration files can be stored as `.json` or `.toml`; both formats carry the same data.

The workflow is intentionally split:

- Python owns the ergonomic API and Typer CLI.
- Rust owns schema validation, schema diffing, dialect-aware DDL compilation, migration execution, and revision bookkeeping.
- Migration files store the reviewed SQL so applying a migration does not need to import application models.

A migration artifact stores:

- the revision identifier
- the source and target snapshots
- generated upgrade SQL
- generated rollback SQL where the reverse diff can be rendered
- diff summaries and unsafe-operation warnings

## Generate From Models

```python
from pydantic import BaseModel

from ormdantic import Ormdantic
from ormdantic.migrations import SchemaSnapshot

db = Ormdantic("sqlite:///app.sqlite3")


@db.table(pk="id")
class Flavor(BaseModel):
    id: str
    name: str
    rating: int | None = None


snapshot = db.migrations.snapshot()
snapshot.write("schema.current.json")
snapshot.write("schema.current.toml")
```

Compare an earlier snapshot to the current models and create a migration artifact:

```python
previous = SchemaSnapshot.read("schema.previous.json")
artifact = db.migrations.create_migration(
    "20260608_add_rating",
    previous,
    snapshot,
    dialect="sqlite",
    path="migrations/20260608_add_rating.json",
)

print(artifact.to_plan().dry_run())
```

The output format is inferred from the path extension. You can also pass `format="json"` or `format="toml"` to `write()`.

## Review And Apply

```python
artifact = db.migrations.create_migration(
    "20260608_add_rating",
    previous,
    snapshot,
    dialect="sqlite",
)

for warning in artifact.warnings:
    print(warning.message)

for sql in artifact.to_plan().dry_run():
    print(sql)
```

Applied revisions are stored in the native `ormdantic_migrations` table. Re-applying the same revision returns `False` and does not run the SQL again.

```python
applied = await db.migrations.apply_artifact(artifact)
```

Destructive migrations require explicit opt-in:

```python
await db.migrations.apply_artifact(artifact, allow_destructive=True)
```

Apply all artifacts in a directory. By default, Ormdantic reads both `.json` and `.toml` files in filename order:

```python
applied_revisions = await db.migrations.apply_directory("migrations")
```

Squash contiguous migration artifacts into one net migration:

```python
squashed = db.migrations.squash(
    "20260608_squashed",
    [
        "migrations/20260608_001.toml",
        "migrations/20260608_002.toml",
    ],
    dialect="sqlite",
    path="migrations/20260608_squashed.toml",
)
```

## CLI Workflow

The CLI is implemented with Typer and mirrors the Python API. Use `--interactive` to add prompts before overwriting files or applying destructive SQL.

Export a snapshot from a module-level database object:

```bash
ormdantic migrations snapshot app.models:db --out schema.current.toml --format toml
```

Create and preview a migration artifact:

```bash
ormdantic migrations create 20260608_add_rating \
  --from schema.previous.json \
  --to schema.current.toml \
  --dialect sqlite \
  --out migrations/20260608_add_rating.toml \
  --format toml \
  --interactive

ormdantic migrations preview migrations/20260608_add_rating.toml
ormdantic migrations preview migrations/20260608_add_rating.toml --rollback
```

Apply one migration or a directory of migration artifacts:

```bash
ormdantic migrations apply sqlite:///app.sqlite3 migrations/20260608_add_rating.toml
ormdantic migrations apply-dir sqlite:///app.sqlite3 migrations/
```

If a migration contains destructive SQL, either pass `--allow-destructive` or use `--interactive` and confirm the prompt:

```bash
ormdantic migrations apply sqlite:///app.sqlite3 migrations/20260608_drop_old_table.toml --interactive
```

Squash contiguous migration artifacts into one net migration:

```bash
ormdantic migrations squash 20260608_squashed \
  migrations/20260608_001.toml \
  migrations/20260608_002.toml \
  --dialect sqlite \
  --out migrations/20260608_squashed.toml \
  --format toml
```

## File Format Notes

Use JSON when you want the most literal machine format. Use TOML when you want migration files that are easier to scan in code review. Ormdantic generated DDL migrations do not need bound parameters, so TOML works well for the common model-change workflow. If you hand-write raw SQL operations with complex parameter values, JSON remains the more permissive format.
