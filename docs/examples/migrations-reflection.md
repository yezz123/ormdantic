# Migrations And Reflection

This guide shows how to compare registered model metadata with live database metadata.

## What The Example Covers

- `db.migrations.snapshot()`;
- `db.migrations.live_snapshot()`;
- diff generation;
- dry-run SQL;
- basic inspector calls.

```python
--8<-- "examples/migrations_reflection.py"
```

Run it locally:

```console
python examples/migrations_reflection.py
```

## Review Workflow

For application migrations, generate a migration artifact, review the SQL, run it in staging, and only then apply it in production. Ormdantic records applied revisions in the migration history table.
