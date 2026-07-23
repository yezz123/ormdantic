# Configure development and production

This chapter makes the same application select SQLite or PostgreSQL entirely from
environment variables, while keeping credentials out of logs and API responses.

## Application settings

The settings loader accepts `development`, `test`, and `production`. Development
defaults to a local SQLite file. Test and production require an explicit
`DATABASE_URL`, which prevents an accidental production process from silently
creating a local file.

```python
--8<-- "examples/todo_app/app/config.py"
```

`Settings.safe_database_url` redacts authority credentials and common sensitive
query parameters. Use it for diagnostics; never print `database_url` directly.

## Register one database target

```python
--8<-- "examples/todo_app/app/database.py"
```

Importing `models` here is intentional. Both the migration CLI and Playground load
`app.database:db`; registration must finish before either tool takes a snapshot.

## Playground configuration

```toml
--8<-- "examples/todo_app/ormdantic.toml"
```

The development profile requires typed confirmation for writes. The environment
named `production` also receives the Playground's non-weakenable production
safety policy. The checked-in migration directory
is SQLite; select `migrations/postgresql` explicitly when applying production
artifacts because migration directories are project-wide in the current config
format.

Typical production settings are:

```console
APP_ENV=production
DATABASE_URL=postgresql://todo:replace-me@database:5432/todo
```

Keep the real URL in your secret manager. The `.env.example` file documents names
only; `.env` is ignored by Docker builds and should not be committed.

See [Playground configuration](../playground/configuration.md) for every supported
key and [driver URLs](../drivers/index.md) for backend-specific parameters.
