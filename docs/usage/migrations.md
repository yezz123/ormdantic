# Migrations

Ormdantic migrations are built from schema snapshots. A snapshot is a JSON-safe record of the tables registered on an `Ormdantic` database object. A migration artifact stores:

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

## CLI Workflow

Export a snapshot from a module-level database object:

```bash
ormdantic migrations snapshot app.models:db --out schema.current.json
```

Create and preview a migration artifact:

```bash
ormdantic migrations create 20260608_add_rating \
  --from schema.previous.json \
  --to schema.current.json \
  --dialect sqlite \
  --out migrations/20260608_add_rating.json

ormdantic migrations preview migrations/20260608_add_rating.json
```

Apply one migration or a directory of migration artifacts:

```bash
ormdantic migrations apply sqlite:///app.sqlite3 migrations/20260608_add_rating.json
ormdantic migrations apply-dir sqlite:///app.sqlite3 migrations/
```

Squash contiguous migration artifacts into one net migration:

```bash
ormdantic migrations squash 20260608_squashed \
  migrations/20260608_001.json \
  migrations/20260608_002.json \
  --dialect sqlite \
  --out migrations/20260608_squashed.json
```
