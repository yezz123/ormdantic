---
title: Explore schemas and migrations in the playground
contentType: Landing
---

# Explore schemas and migrations in the playground

The Ormdantic playground is an optional terminal user interface (TUI) for watching schemas, reviewing drift, editing migration artifacts, and running guarded migration workflows. It uses the same migration manager and artifact format as the scriptable CLI.

## Install and launch the playground

Install the optional Textual dependency with your package manager:

```console
uv add 'ormdantic[playground]'
```

```console
pip install 'ormdantic[playground]'
```

Launch from the project directory that contains `ormdantic.toml`:

```console
ormdantic playground
```

Use another configuration or environment when needed:

```console
ormdantic playground --config config/ormdantic.toml --environment staging
```

The first launch opens a setup screen when no configuration exists. Enter the Python import target, migration directory, and database URL environment variable. The setup screen writes `ormdantic.toml`; it never writes a database URL.

## Configure a project

This configuration watches a Python package and the canonical TOML migration directory. The database URL comes from `DATABASE_URL` or `.env`.

```toml
[project]
target = "app.database:db"
migrations_dir = "migrations"
format = "toml"
watch = ["app/**/*.py", "migrations/**/*.toml"]
database_poll_seconds = 5.0
debounce_milliseconds = 300

[environments.development]
url_env = "DATABASE_URL"
env_file = ".env"
safety = "confirm"
```

See [Configure the playground](configuration.md) for every key, default, and precedence rule.

## Use the seven workspace screens

Each screen answers one part of the migration review:

- **Overview**: check connection, watcher, drift, migration, and diagnostic health
- **Schemas**: compare normalized registered-model and live-database trees
- **Drift**: inspect structured changes, safety labels, generated SQL, and generation identity
- **Migrations**: browse TOML and JSON artifacts, dependencies, checksums, status, and risk
- **Editor**: edit the complete TOML document and each forward or rollback SQL operation
- **History & logs**: inspect durable revisions, dirty rows, durations, and redacted operation messages
- **Settings**: switch environments, pause watching, and edit validated `ormdantic.toml`

Press `?` for the in-application keyboard and safety reference.

## Choose the next guide

Use these pages while working:

- [Watch model and database schemas](schema-watching.md)
- [Run migration workflows](migration-workflows.md)
- [Edit TOML and SQL](editor.md)
- [Understand migration safeguards](safety.md)
- [Diagnose playground problems](troubleshooting.md)

## Keep scriptable commands in automation

The playground adds an interactive workflow; it does not replace `ormdantic migrations`. Keep non-interactive commands in continuous integration (CI), deployment scripts, and repeatable local tasks:

```console
ormdantic migrations status
ormdantic migrations apply-dir migrations
ormdantic migrations history
```

Both interfaces read the same artifacts and migration history.
