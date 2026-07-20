# Migrations and reflection

This guide shows how to compare registered model metadata with live database metadata.

Use it when a database already exists and you want to inspect planned schema changes before applying them.

## What the example covers

- `db.migrations.snapshot()`
- `db.migrations.live_snapshot()`
- diff generation
- dry-run SQL
- basic inspector calls

```python
--8<-- "examples/migrations_reflection.py"
```

Run it locally:

```console
python examples/migrations_reflection.py
```

## Review workflow

For application migrations, generate a migration artifact, review the SQL, run it in staging, and only then apply it in production. Ormdantic records applied revisions in the migration history table.

Run the same review interactively when you have installed the optional playground:

```console
ormdantic playground
```

The [playground migration workflow](../playground/migration-workflows.md) keeps the full TOML artifact and per-operation SQL visible before a guarded database action.
