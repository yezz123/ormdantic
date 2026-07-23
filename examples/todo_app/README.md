# Ormdantic Todo reference application

This FastAPI application demonstrates one complete Ormdantic workflow: validated
configuration, Pydantic tables, relationships, typed queries, transactions,
dialect-specific migrations, SQLite development, and PostgreSQL deployment.

## Run locally with SQLite

From the repository root:

```bash
uv run --extra examples ormdantic migrations apply-dir \
  examples/todo_app/migrations/sqlite \
  --url sqlite:///examples/todo_app/todo-dev.sqlite3
cd examples/todo_app
DATABASE_URL=sqlite:///todo-dev.sqlite3 uv run --extra examples \
  uvicorn app.main:app --reload
```

Open <http://127.0.0.1:8000/docs> for the interactive API.

## Explore migrations in the Playground

```bash
cd examples/todo_app
uv run --extra playground ormdantic playground
```

The checked-in configuration watches the models and both migration directories.
Destructive operations require typed confirmation. The production environment is
locked to the production safety policy.

## Run with PostgreSQL

```bash
docker compose -f examples/todo_app/docker-compose.yml up --build
```

Compose waits for PostgreSQL, runs the PostgreSQL migration chain once, and then
starts the API. Set `TODO_API_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, or
`POSTGRES_PASSWORD` to override the demonstration defaults. Do not use the
demonstration password in a real deployment.

Compose preserves its named database volume when stopped. Remove that volume only
when you intentionally want to erase the example data.
